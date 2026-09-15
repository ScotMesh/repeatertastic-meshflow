// Package feeder makes each RepeaterTastic radio a Meshflow feeder, reporting as that radio's
// relay persona. It uploads what the relay persona itself would hear, keeps Meshflow's node list
// up to date from those packets, and runs the traceroutes Meshflow asks for.
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
	f.problem, f.reporters = "", nil
	if strings.TrimSpace(s.APIURL) == "" || strings.TrimSpace(s.APIKey) == "" {
		f.problem = "Set the Meshflow API URL and node API key"
		return
	}
	wsBase, err := meshflow.WSURL(s.APIURL, s.WSURL)
	if err != nil {
		f.problem = err.Error()
		return
	}
	ignore := map[string]bool{}
	for _, p := range s.IgnorePortnums {
		ignore[strings.ToUpper(strings.TrimSpace(p))] = true
	}
	rctx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	for _, r := range radios {
		if (len(s.Radios) > 0 && !slices.Contains(s.Radios, r.Id)) || r.Relay == nil {
			continue
		}
		api, err := meshflow.NewClient(s.APIURL, s.APIKey, r.Relay.NodeNum, "repeatertastic-meshflow/"+f.version)
		if err != nil {
			f.problem = err.Error()
			cancel()
			return
		}
		rep := &reporter{f: f, ctx: rctx, radio: r, api: api, ignore: ignore, settings: s,
			queue: make(chan job, queueSize), nodes: map[uint32]*nodeState{}}
		if on(s.AcceptTraceroutes) {
			rep.ws = &meshflow.Commands{URL: wsBase, Key: s.APIKey, Feeder: r.Relay.NodeNum, OnTrace: rep.traceroute, OnState: rep.wsState}
		}
		f.reporters = append(f.reporters, rep)
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			rep.run()
		}()
	}
	if len(f.reporters) == 0 {
		f.problem = "No radio to report for: check the Radios setting"
	}
}

// Stop ends the reporters and everything they started, and waits for them.
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
		name := r.radio.Name
		if name == "" {
			name = r.radio.Id
		}
		fields[fmt.Sprintf("%s (%s)", name, r.radio.Relay.NodeId)] = st.line()
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
	queueSize       = 2000
	maxNodes        = 5000
	nodeRefresh     = 6 * time.Hour    // resend an unchanged node this often
	metricsRefresh  = 30 * time.Minute // resend a node for new metrics at most this often
	maxRetryBackoff = 2 * time.Minute
	broadcast       = 0xffffffff
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
	done func(ok bool) // node jobs: record the result
}

// nodeState is a node as this relay persona has heard it, and what Meshflow has of it.
type nodeState struct {
	meshflow.Node
	lastHeard     time.Time
	sentKey       string
	sentAt        time.Time
	sentMetricsAt time.Time
	pending       bool
}

type reporter struct {
	f        *Feeder
	ctx      context.Context
	radio    *pluginv1.Radio
	api      *meshflow.Client
	ws       *meshflow.Commands
	ignore   map[string]bool
	settings Settings
	queue    chan job

	nodesMu sync.Mutex
	nodes   map[uint32]*nodeState

	packets, nodesSent, refused, dropped, traces, traceSkipped atomic.Int64
	wsUp                                                       atomic.Bool
	authFailed                                                 atomic.Bool
	authErr, unreachable                                       atomic.Value // string
	lastLog                                                    sync.Map     // fixed kind → time.Time
}

func (r *reporter) run() {
	var wg sync.WaitGroup
	if r.ws != nil {
		wg.Go(func() { r.ws.Run(r.ctx) })
	}
	wg.Go(func() {
		if err := r.api.ReportVersion(r.ctx); err != nil && r.ctx.Err() == nil {
			r.fail("version", err)
		}
		if on(r.settings.UploadNodes) {
			r.uploadSelf()
		}
	})
	defer wg.Wait()
	for {
		select {
		case <-r.ctx.Done():
			return
		case j := <-r.queue:
			r.send(j)
		}
	}
}

// uploadSelf sends the relay persona's own node. Other nodes come only from packets the relay
// persona hears: the radio's node database is shared by every identity and can hold names and
// positions learned on channels the relay persona can't read.
func (r *reporter) uploadSelf() {
	resp, err := r.f.host.ListNodes(r.ctx, &pluginv1.ListNodesRequest{RadioId: r.radio.Id})
	if err != nil {
		if status.Code(err) != codes.Canceled && r.ctx.Err() == nil {
			r.logOnce("nodes", "warn", "can't read %s's own node: %v", r.radio.Relay.NodeId, err)
		}
		return
	}
	for _, n := range resp.Nodes {
		if n.NodeNum != r.radio.Relay.NodeNum {
			continue
		}
		u := &pb.User{}
		if len(n.User) == 0 || proto.Unmarshal(n.User, u) != nil {
			return
		}
		r.learn(n.NodeNum, time.Now(), func(st *nodeState) { st.User = u })
	}
}

// packet uploads what the relay persona would hear itself: broadcasts on its channels and
// packets addressed to it. RepeaterTastic decodes with every identity's channels and keys, so
// anything else (DMs between other nodes, other identities' traffic) is left out.
func (r *reporter) packet(ev *pluginv1.PacketEvent) {
	if ev.Direction != "rx" || !ev.Decoded || ev.RelayChannelIndex < 0 {
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
	relay := r.radio.Relay.NodeNum
	d := p.GetDecoded()
	if d == nil || p.From == relay || (p.To != broadcast && p.To != relay) {
		return
	}
	if r.ignore[d.Portnum.String()] {
		return
	}
	heard := time.Now()
	if p.RxTime != nil {
		heard = time.Unix(int64(p.GetRxTime()), 0)
	}
	if on(r.settings.UploadNodes) {
		r.learnFromPacket(p, d, heard)
	}
	if !on(r.settings.UploadPackets) {
		return
	}
	body, err := meshflow.PacketJSON(p)
	if err != nil {
		return // not a port or shape meshflow-api takes
	}
	r.enqueue(job{kind: jobPacket, body: body, what: "packet"})
}

func (r *reporter) learnFromPacket(p *pb.MeshPacket, d *pb.Data, heard time.Time) {
	switch d.Portnum {
	case pb.PortNum_NODEINFO_APP:
		u := &pb.User{}
		if proto.Unmarshal(d.Payload, u) == nil {
			r.learn(p.From, heard, func(st *nodeState) { st.User = u })
		}
	case pb.PortNum_POSITION_APP:
		pos := &pb.Position{}
		if proto.Unmarshal(d.Payload, pos) == nil && pos.LatitudeI != nil && pos.LongitudeI != nil {
			at := heard
			if pos.Time != 0 {
				at = time.Unix(int64(pos.Time), 0)
			}
			r.learn(p.From, heard, func(st *nodeState) { st.Position, st.PosAt = pos, at })
		}
	case pb.PortNum_TELEMETRY_APP:
		t := &pb.Telemetry{}
		if proto.Unmarshal(d.Payload, t) == nil && t.GetDeviceMetrics() != nil {
			at := heard
			if t.Time != 0 {
				at = time.Unix(int64(t.Time), 0)
			}
			r.learn(p.From, heard, func(st *nodeState) { st.Metrics, st.MetricsAt = t.GetDeviceMetrics(), at })
		}
	}
}

// learn updates a node and queues an upsert when Meshflow's copy is out of date.
func (r *reporter) learn(num uint32, heard time.Time, update func(*nodeState)) {
	r.nodesMu.Lock()
	st := r.nodes[num]
	if st == nil {
		if len(r.nodes) >= maxNodes {
			r.evictOldestLocked()
		}
		st = &nodeState{Node: meshflow.Node{Num: num}}
		r.nodes[num] = st
	}
	update(st)
	if heard.After(st.lastHeard) {
		st.lastHeard = heard
	}
	if st.pending {
		r.nodesMu.Unlock()
		return
	}
	body, key := meshflow.NodeJSON(&st.Node)
	now := time.Now()
	due := body != nil && (key != st.sentKey || now.Sub(st.sentAt) > nodeRefresh ||
		(st.MetricsAt.After(st.sentMetricsAt) && now.Sub(st.sentAt) > metricsRefresh))
	if !due {
		r.nodesMu.Unlock()
		return
	}
	st.pending = true
	metricsAt := st.MetricsAt
	r.nodesMu.Unlock()

	r.enqueue(job{kind: jobNode, body: body, what: "node", done: func(ok bool) {
		r.nodesMu.Lock()
		defer r.nodesMu.Unlock()
		st.pending = false
		if ok {
			st.sentKey, st.sentAt, st.sentMetricsAt = key, time.Now(), metricsAt
		}
	}})
}

func (r *reporter) evictOldestLocked() {
	var oldest uint32
	var at time.Time
	found := false
	for num, st := range r.nodes {
		if !st.pending && (!found || st.lastHeard.Before(at)) {
			oldest, at, found = num, st.lastHeard, true
		}
	}
	if found {
		delete(r.nodes, oldest)
	}
}

func (r *reporter) enqueue(j job) {
	select {
	case r.queue <- j:
	default:
		r.dropped.Add(1)
		if j.done != nil {
			j.done(false)
		}
		r.logOnce("queue", "warn", "upload queue for %s is full; dropping until Meshflow catches up", r.radio.Relay.NodeId)
	}
}

// send uploads one job. Network and server errors hold it at the head of the queue and retry
// with backoff until Meshflow is back; a refusal (4xx) drops it.
func (r *reporter) send(j job) {
	finish := func(ok bool) {
		if j.done != nil {
			j.done(ok)
		}
	}
	wait := 2 * time.Second
	for {
		var err error
		if j.kind == jobPacket {
			err = r.api.Ingest(r.ctx, j.body)
		} else {
			err = r.api.UpsertNode(r.ctx, j.body)
		}
		if r.ctx.Err() != nil {
			finish(false)
			return
		}
		switch {
		case err == nil:
			if j.kind == jobPacket {
				r.packets.Add(1)
			} else {
				r.nodesSent.Add(1)
			}
			if prev, _ := r.unreachable.Swap("").(string); prev != "" {
				r.f.log("info", "Meshflow is reachable again for %s", r.radio.Relay.NodeId)
			}
			r.authFailed.Store(false)
			finish(true)
			return
		case meshflow.Retryable(err):
			r.unreachable.Store(err.Error())
			r.logOnce("unreachable", "warn", "Meshflow unreachable for %s, retrying: %v", r.radio.Relay.NodeId, err)
			select {
			case <-r.ctx.Done():
				finish(false)
				return
			case <-time.After(wait):
			}
			wait = min(maxRetryBackoff, wait*2)
		default:
			r.refused.Add(1)
			r.fail(j.what, err)
			finish(false)
			return
		}
	}
}

func (r *reporter) fail(what string, err error) {
	var he *meshflow.HTTPError
	if errors.As(err, &he) && (he.Status == 401 || he.Status == 403) {
		r.authFailed.Store(true)
		r.authErr.Store(fmt.Sprintf("Meshflow refused the key for %s (%d): link it to this node in Meshflow", r.radio.Relay.NodeId, he.Status))
		r.logOnce("auth", "error", "Meshflow refused a %s from %s: %v", what, r.radio.Relay.NodeId, err)
		return
	}
	r.logOnce("refused-"+what, "warn", "Meshflow refused a %s from %s: %v", what, r.radio.Relay.NodeId, err)
}

func (r *reporter) traceroute(target uint32) {
	if r.ctx.Err() != nil {
		return
	}
	r.logOnce("trace-cmd", "info", "Meshflow asked %s for a traceroute to !%08x", r.radio.Relay.NodeId, target)
	ctx, cancel := context.WithTimeout(r.ctx, 10*time.Second)
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
	} else if err != nil && r.ctx.Err() == nil {
		r.logOnce("ws", "warn", "%v", err)
	}
}

// logOnce rate-limits a kind of log line to one a minute. Kinds are a fixed set.
func (r *reporter) logOnce(kind, level, format string, args ...any) {
	now := time.Now()
	if v, ok := r.lastLog.Load(kind); ok && now.Sub(v.(time.Time)) < time.Minute {
		return
	}
	r.lastLog.Store(kind, now)
	r.f.log(level, format, args...)
}

type reporterStats struct {
	packets, nodes, refused, dropped, traces, skipped int64
	queued                                            int
	ws, problem                                       string
}

func (r *reporter) stats() reporterStats {
	st := reporterStats{packets: r.packets.Load(), nodes: r.nodesSent.Load(), refused: r.refused.Load(), dropped: r.dropped.Load(),
		traces: r.traces.Load(), skipped: r.traceSkipped.Load(), queued: len(r.queue)}
	switch {
	case r.ws == nil:
		st.ws = "commands off"
	case r.wsUp.Load():
		st.ws = "commands connected"
	default:
		st.ws = "commands reconnecting"
	}
	if msg, _ := r.unreachable.Load().(string); msg != "" {
		st.problem = "Meshflow unreachable: " + msg
	}
	if r.authFailed.Load() {
		st.problem, _ = r.authErr.Load().(string)
	}
	return st
}

func (s reporterStats) line() string {
	parts := []string{count(s.packets) + " packets", count(s.nodes) + " nodes"}
	if s.traces+s.skipped > 0 {
		parts = append(parts, fmt.Sprintf("%d traceroutes (%d not sent)", s.traces, s.skipped))
	}
	if s.refused > 0 {
		parts = append(parts, count(s.refused)+" refused")
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
