package meshflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Commands keeps the feeder's command WebSocket (ws/nodes/) open and passes commands on.
type Commands struct {
	URL      string // ws(s)://host[/prefix], without /ws/nodes/
	Key      string
	Feeder   uint32
	OnTrace  func(target uint32)
	OnState  func(connected bool, err error)
	Dialer   *websocket.Dialer
	connects atomic.Int64
}

// WSURL derives the WebSocket base from the API URL, or checks an explicit one.
func WSURL(apiURL, wsURL string) (string, error) {
	if s := strings.TrimSpace(wsURL); s != "" {
		u, err := url.Parse(s)
		if err != nil || (u.Scheme != "wss" && u.Scheme != "ws") || u.Host == "" {
			return "", errors.New("the WebSocket URL must look like wss://meshflow.example.org")
		}
		u.Path = strings.TrimSuffix(strings.TrimSuffix(u.Path, "/"), "/ws/nodes")
		return strings.TrimSuffix(u.String(), "/"), nil
	}
	base, err := BaseURL(apiURL)
	if err != nil {
		return "", err
	}
	return "ws" + strings.TrimPrefix(base, "http"), nil
}

const (
	pingEvery = 20 * time.Second
	readLimit = 90 * time.Second
	maxWait   = 5 * time.Minute
)

// Run connects and reconnects (1 s growing to 5 min) until ctx ends.
func (c *Commands) Run(ctx context.Context) {
	wait := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := c.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if c.OnState != nil {
			c.OnState(false, err)
		}
		if time.Since(start) > time.Minute {
			wait = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(maxWait, wait*3/2+time.Second)
	}
}

// Connects counts successful connections (tests, status).
func (c *Commands) Connects() int64 { return c.connects.Load() }

func (c *Commands) session(ctx context.Context) error {
	q := url.Values{"api_key": {c.Key}, "feeder_node_id": {strconv.FormatUint(uint64(c.Feeder), 10)}}
	endpoint := c.URL + "/ws/nodes/?" + q.Encode()
	// Django Channels checks Origin against its allowed hosts.
	origin := "http" + strings.TrimPrefix(c.URL, "ws")
	d := c.Dialer
	if d == nil {
		d = &websocket.Dialer{HandshakeTimeout: 20 * time.Second, Proxy: http.ProxyFromEnvironment}
	}
	conn, resp, err := d.DialContext(ctx, endpoint, http.Header{"Origin": {origin}})
	if err != nil {
		if resp != nil {
			return fmt.Errorf("command socket refused (%s): check the API key is linked to !%08x", resp.Status, c.Feeder)
		}
		return fmt.Errorf("command socket: %s", redact(err.Error(), c.Key))
	}
	defer conn.Close()
	c.connects.Add(1)
	if c.OnState != nil {
		c.OnState(true, nil)
	}

	_ = conn.SetReadDeadline(time.Now().Add(readLimit))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(readLimit)) })
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(pingEvery)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
				conn.Close()
				return
			case <-t.C:
				if conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)) != nil {
					conn.Close()
					return
				}
			}
		}
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("command socket closed: %s", redact(err.Error(), c.Key))
		}
		_ = conn.SetReadDeadline(time.Now().Add(readLimit))
		var cmd struct {
			Type   string          `json:"type"`
			Target json.RawMessage `json:"target"`
		}
		if json.Unmarshal(msg, &cmd) != nil {
			continue
		}
		if cmd.Type == "traceroute" && c.OnTrace != nil {
			if target, ok := parseTarget(cmd.Target); ok {
				c.OnTrace(target)
			}
		}
	}
}

// parseTarget accepts a node number as a JSON number or string, or a "!hex" id.
func parseTarget(raw json.RawMessage) (uint32, bool) {
	var n json.Number
	if json.Unmarshal(raw, &n) == nil {
		if v, err := strconv.ParseUint(n.String(), 10, 32); err == nil {
			return uint32(v), true
		}
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		s = strings.TrimSpace(s)
		if h, ok := strings.CutPrefix(s, "!"); ok {
			if v, err := strconv.ParseUint(h, 16, 32); err == nil {
				return uint32(v), true
			}
		} else if v, err := strconv.ParseUint(s, 10, 32); err == nil {
			return uint32(v), true
		}
	}
	return 0, false
}

// redact keeps the API key out of logs (it travels in the WebSocket URL).
func redact(s, key string) string {
	if key == "" {
		return s
	}
	s = strings.ReplaceAll(s, key, "***")
	return strings.ReplaceAll(s, url.QueryEscape(key), "***")
}
