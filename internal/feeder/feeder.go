// Package feeder makes each RepeaterTastic radio a Meshflow feeder, reporting as that radio's
// relay persona: it uploads what the relay persona would hear, keeps Meshflow's node list up to
// date and runs the traceroutes Meshflow asks for.
package feeder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/A13xB0/RepeaterTastic/pb"
	pluginv1 "github.com/A13xB0/RepeaterTastic/pluginapi/v1"

	"github.com/ScotMesh/repeatertastic-meshflow/internal/meshflow"
)

// Host is the part of the Plugin API the feeder calls.
type Host interface {
	ListNodes(ctx context.Context, in *pluginv1.ListNodesRequest, opts ...grpc.CallOption) (*pluginv1.ListNodesResponse, error)
	Traceroute(ctx context.Context, in *pluginv1.TracerouteRequest, opts ...grpc.CallOption) (*pluginv1.SendResponse, error)
}

// Settings are the plugin's settings (plugin.yaml).
type Settings struct {
	APIURL            string `json:"api_url"`
	APIKey            string `json:"api_key"`
	WSURL             string `json:"ws_url"`
	Radios            List   `json:"radios"`
	UploadPackets     *bool  `json:"upload_packets"`
	UploadNodes       *bool  `json:"upload_nodes"`
	AcceptTraceroutes *bool  `json:"accept_traceroutes"`
	IgnorePortnums    List   `json:"ignore_portnums"`
}

// List is a list setting. It also reads the comma-separated text older versions saved.
type List []string

func (l *List) UnmarshalJSON(b []byte) error {
	var items []string
	if err := json.Unmarshal(b, &items); err == nil {
		*l = items
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*l = splitList(s)
	return nil
}

func on(b *bool) bool { return b == nil || *b }

// Logf writes to the plugin's log in RepeaterTastic.
type Logf func(level, format string, args ...any)

// Feeder runs one reporter per radio.
type Feeder struct {
	host    Host
	log     Logf
	version string

	mu        sync.Mutex
	reporters []*reporter
	settings  Settings
	problem   string
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// New makes a stopped feeder; call Configure.
func New(host Host, log Logf, version string) *Feeder {
	return &Feeder{host: host, log: log, version: version}
}

// Configure (re)starts the reporters for these settings and radios.
func (f *Feeder) Configure(ctx context.Context, s Settings, radios []*pluginv1.Radio) {
	f.Stop()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settings, f.problem, f.reporters = s, "", nil
	if strings.TrimSpace(s.APIURL) == "" || strings.TrimSpace(s.APIKey) == "" {
		f.problem = "Set the Meshflow API URL and node API key"
		return
	}
	wsBase, err := meshflow.WSURL(s.APIURL, s.WSURL)
	if err != nil {
		f.problem = err.Error()
		return
	}
	want := s.Radios
	ignore := map[string]bool{}
	for _, p := range s.IgnorePortnums {
		ignore[strings.ToUpper(p)] = true
	}
	rctx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	for _, r := range radios {
		if len(want) > 0 && !slices.Contains(want, r.Id) {
			continue
		}
		if r.Relay == nil {
			continue
		}
		api, err := meshflow.NewClient(s.APIURL, s.APIKey, r.Relay.NodeNum, "repeatertastic-meshflow/"+f.version)
		if err != nil {
			f.problem = err.Error()
			cancel()
			return
		}
		rep := &reporter{f: f, radio: r, api: api, ignore: ignore, settings: s,
			queue: make(chan job, queueSize), sent: map[uint32]sentNode{}}
		if on(s.AcceptTraceroutes) {
			rep.ws = &meshflow.Commands{URL: wsBase, Key: s.APIKey, Feeder: r.Relay.NodeNum, OnTrace: rep.traceroute, OnState: rep.wsState}
		}
		f.reporters = append(f.reporters, rep)
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			rep.run(rctx)
		}()
	}
	if len(f.reporters) == 0 {
		f.problem = "No radio to report for: check the Radios setting"
	}
}

// Stop ends the reporters and waits for them.
func (f *Feeder) Stop() {
	f.mu.Lock()
	cancel := f.cancel
	f.cancel = nil
	f.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	f.wg.Wait()
}

// Packet hands a packet event to its radio's reporter.
func (f *Feeder) Packet(ev *pluginv1.PacketEvent) {
	if rep := f.reporter(ev.RadioId); rep != nil {
		rep.packet(ev)
	}
}

// Node hands a node event to its radio's reporter.
func (f *Feeder) Node(n *pluginv1.Node) {
	if rep := f.reporter(n.RadioId); rep != nil {
		rep.node(nil, n)
	}
}

func (f *Feeder) reporter(radioID string) *reporter {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.reporters {
		if r.radio.Id == radioID {
			return r
		}
	}
	return nil
}

// Status is a one-line summary, a state and a field per radio.
func (f *Feeder) Status() (summary, state string, fields map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.problem != "" {
		return f.problem, "warning", nil
	}
	fields = map[string]string{}
	var packets, nodes int64
	state = "ok"
	var problems []string
	for _, r := range f.reporters {
		st := r.stats()
		packets += st.packets
		nodes += st.nodes
		label := fmt.Sprintf("%s (%s)", r.radio.Name, r.radio.Relay.NodeId)
		if r.radio.Name == "" {
			label = fmt.Sprintf("%s (%s)", r.radio.Id, r.radio.Relay.NodeId)
		}
		fields[label] = st.line()
		if st.problem != "" {
			state = "warning"
			problems = append(problems, st.problem)
		}
	}
	summary = fmt.Sprintf("Feeding %d radio%s · %s packets · %s nodes", len(f.reporters), plural(len(f.reporters)), count(packets), count(nodes))
	if len(problems) > 0 {
		summary = problems[0]
	}
	return summary, state, fields
}

// -------------------------------------------------------------------------------------- reporter

const (
	queueSize   = 2000
	maxAttempts = 6
	nodeRefresh = 6 * time.Hour
)

type jobKind int

const (
	jobPacket jobKind = iota
	jobNode
)

type job struct {
	kind jobKind
	body []byte
	what string
}

type sentNode struct {
	key string
	at  time.Time
}

type reporter struct {
	f        *Feeder
	radio    *pluginv1.Radio
	api      *meshflow.Client
	ws       *meshflow.Commands
	ignore   map[string]bool
	settings Settings
	queue    chan job

	sentMu sync.Mutex
	sent   map[uint32]sentNode

	packets, nodes, rejected, dropped, retried, traces, traceSkipped atomic.Int64
	wsUp                                                             atomic.Bool
	lastErr                                                          atomic.Value // string
	authFailed                                                       atomic.Bool
	lastLog                                                          sync.Map // kind → time.Time
}

func (r *reporter) run(ctx context.Context) {
	if r.ws != nil {
		go r.ws.Run(ctx)
	}
	go func() {
		if err := r.api.ReportVersion(ctx); err != nil {
			r.fail("version", err)
		}
		if on(r.settings.UploadNodes) {
			r.uploadAllNodes(ctx)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case j := <-r.queue:
			r.send(ctx, j)
		}
	}
}

// uploadAllNodes sends the node database once at start, as meshflow-bot does on connect.
func (r *reporter) uploadAllNodes(ctx context.Context) {
	resp, err := r.f.host.ListNodes(ctx, &pluginv1.ListNodesRequest{RadioId: r.radio.Id})
	if err != nil {
		if status.Code(err) != codes.Canceled {
			r.logOnce("nodes", "warn", "can't list nodes on %s: %v", r.radio.Id, err)
		}
		return
	}
	for _, n := range resp.Nodes {
		r.node(ctx, n)
	}
}

func (r *reporter) packet(ev *pluginv1.PacketEvent) {
	if !on(r.settings.UploadPackets) || ev.Direction != "rx" || !ev.Decoded || ev.RelayChannelIndex < 0 {
		return
	}
	// First sightings only: echoes of our own packets, duplicates and legacy packets are what a
	// Meshtastic node never hands its client either.
	switch ev.Kind {
	case "heard", "delivered", "relayed":
	default:
		return
	}
	p := &pb.MeshPacket{}
	if proto.Unmarshal(ev.MeshPacket, p) != nil {
		return
	}
	d := p.GetDecoded()
	if d == nil || r.ignore[d.Portnum.String()] || p.From == r.radio.Relay.NodeNum {
		return
	}
	if ev.ChannelHash == 0 && p.To == r.radio.Relay.NodeNum {
		p.PkiEncrypted = true
	}
	body, err := meshflow.PacketJSON(p)
	if err != nil {
		return // not a port or shape meshflow-api takes
	}
	r.enqueue(job{kind: jobPacket, body: body, what: d.Portnum.String()})
}

// node queues a node upsert. With a context (the start-up batch) it waits for room in the queue.
func (r *reporter) node(ctx context.Context, n *pluginv1.Node) {
	if !on(r.settings.UploadNodes) {
		return
	}
	body, key := meshflow.NodeJSON(n, time.Now())
	if body == nil {
		return
	}
	r.sentMu.Lock()
	last, ok := r.sent[n.NodeNum]
	if ok && last.key == key && time.Since(last.at) < nodeRefresh {
		r.sentMu.Unlock()
		return
	}
	r.sent[n.NodeNum] = sentNode{key: key, at: time.Now()}
	r.sentMu.Unlock()
	if ctx != nil {
		select {
		case r.queue <- job{kind: jobNode, body: body, what: n.NodeId}:
		case <-ctx.Done():
		}
		return
	}
	r.enqueue(job{kind: jobNode, body: body, what: n.NodeId})
}

func (r *reporter) enqueue(j job) {
	select {
	case r.queue <- j:
	default:
		r.dropped.Add(1)
		r.logOnce("queue", "warn", "upload queue for %s is full; dropping (is Meshflow reachable?)", r.radio.Id)
	}
}

func (r *reporter) send(ctx context.Context, j job) {
	wait := 2 * time.Second
	for attempt := 1; ; attempt++ {
		var err error
		if j.kind == jobPacket {
			err = r.api.Ingest(ctx, j.body)
		} else {
			err = r.api.UpsertNode(ctx, j.body)
		}
		if err == nil {
			if j.kind == jobPacket {
				r.packets.Add(1)
			} else {
				r.nodes.Add(1)
			}
			if r.authFailed.Swap(false) {
				r.lastErr.Store("")
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		if !meshflow.Retryable(err) || attempt >= maxAttempts {
			r.rejected.Add(1)
			r.fail(j.what, err)
			return
		}
		r.retried.Add(1)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(time.Minute, wait*2)
	}
}

func (r *reporter) fail(what string, err error) {
	var he *meshflow.HTTPError
	if errors.As(err, &he) && (he.Status == 401 || he.Status == 403) {
		r.authFailed.Store(true)
		r.lastErr.Store(fmt.Sprintf("Meshflow refused the key for %s (%d): link it to this node in Meshflow", r.radio.Relay.NodeId, he.Status))
		r.logOnce("auth", "error", "Meshflow refused %s for %s: %v", what, r.radio.Relay.NodeId, err)
		return
	}
	r.lastErr.Store(err.Error())
	r.logOnce("upload-"+what, "warn", "Meshflow didn't take %s from %s: %v", what, r.radio.Relay.NodeId, err)
}

func (r *reporter) traceroute(target uint32) {
	r.logOnce("trace-cmd", "info", "Meshflow asked %s for a traceroute to !%08x", r.radio.Relay.NodeId, target)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := r.f.host.Traceroute(ctx, &pluginv1.TracerouteRequest{RadioId: r.radio.Id, Target: fmt.Sprintf("!%08x", target)})
	if err != nil {
		r.traceSkipped.Add(1)
		r.logOnce("trace", "info", "traceroute to !%08x not sent: %s", target, status.Convert(err).Message())
		return
	}
	r.traces.Add(1)
}

func (r *reporter) wsState(up bool, err error) {
	r.wsUp.Store(up)
	if up {
		r.f.log("info", "command socket connected for %s", r.radio.Relay.NodeId)
	} else if err != nil {
		r.logOnce("ws", "warn", "%v", err)
	}
}

// logOnce rate-limits a kind of log line to one a minute.
func (r *reporter) logOnce(kind, level, format string, args ...any) {
	now := time.Now()
	if v, ok := r.lastLog.Load(kind); ok && now.Sub(v.(time.Time)) < time.Minute {
		return
	}
	r.lastLog.Store(kind, now)
	r.f.log(level, format, args...)
}

type reporterStats struct {
	packets, nodes, rejected, dropped, traces, skipped int64
	queued                                             int
	ws                                                 string
	problem                                            string
}

func (r *reporter) stats() reporterStats {
	st := reporterStats{packets: r.packets.Load(), nodes: r.nodes.Load(), rejected: r.rejected.Load(), dropped: r.dropped.Load(),
		traces: r.traces.Load(), skipped: r.traceSkipped.Load(), queued: len(r.queue)}
	switch {
	case r.ws == nil:
		st.ws = "commands off"
	case r.wsUp.Load():
		st.ws = "commands connected"
	default:
		st.ws = "commands reconnecting"
	}
	if r.authFailed.Load() {
		st.problem, _ = r.lastErr.Load().(string)
	}
	return st
}

func (s reporterStats) line() string {
	parts := []string{count(s.packets) + " packets", count(s.nodes) + " nodes"}
	if s.traces+s.skipped > 0 {
		parts = append(parts, fmt.Sprintf("%d traceroutes (%d not sent)", s.traces, s.skipped))
	}
	if s.rejected > 0 {
		parts = append(parts, count(s.rejected)+" refused")
	}
	if s.dropped > 0 {
		parts = append(parts, count(s.dropped)+" dropped")
	}
	if s.queued > 50 {
		parts = append(parts, fmt.Sprintf("%d waiting", s.queued))
	}
	parts = append(parts, s.ws)
	return strings.Join(parts, " · ")
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func count(n int64) string {
	s := fmt.Sprint(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
