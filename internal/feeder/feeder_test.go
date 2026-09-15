package feeder

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/A13xB0/RepeaterTastic/pb"
	pluginv1 "github.com/A13xB0/RepeaterTastic/pluginapi/v1"
)

const relayNum = 0x11cbe35a

type fakeHost struct{}

// ListNodes returns the relay persona and a node the radio's other identities learned about.
func (fakeHost) ListNodes(context.Context, *pluginv1.ListNodesRequest, ...grpc.CallOption) (*pluginv1.ListNodesResponse, error) {
	self, _ := proto.Marshal(&pb.User{LongName: "The Pole Relay", ShortName: "POLE"})
	private, _ := proto.Marshal(&pb.User{LongName: "Private Channel Node", ShortName: "PRIV"})
	return &pluginv1.ListNodesResponse{Nodes: []*pluginv1.Node{
		{NodeNum: relayNum, NodeId: "!11cbe35a", RadioId: "main", User: self, Local: true},
		{NodeNum: 99, NodeId: "!00000063", RadioId: "main", User: private},
	}}, nil
}

func (fakeHost) Traceroute(context.Context, *pluginv1.TracerouteRequest, ...grpc.CallOption) (*pluginv1.SendResponse, error) {
	return &pluginv1.SendResponse{}, nil
}

type fakeMeshflow struct {
	mu       sync.Mutex
	requests []string
	fail     atomic.Int32 // answer this many requests with 503 first
	refuse   atomic.Int32 // then this many with 400
}

func (m *fakeMeshflow) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	if m.fail.Load() > 0 {
		m.fail.Add(-1)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if m.refuse.Load() > 0 && !strings.Contains(r.URL.Path, "bot-version") {
		m.refuse.Add(-1)
		http.Error(w, `{"long_name":["This field may not be blank."]}`, http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	m.requests = append(m.requests, r.Method+" "+r.URL.Path+" "+string(body))
	m.mu.Unlock()
	w.WriteHeader(http.StatusCreated)
}

func (m *fakeMeshflow) find(sub string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, r := range m.requests {
		if strings.Contains(r, sub) {
			out = append(out, r)
		}
	}
	return out
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func event(t *testing.T, kind string, relayIndex int32, from, to uint32, port pb.PortNum, payload []byte) *pluginv1.PacketEvent {
	t.Helper()
	b, _ := proto.Marshal(&pb.MeshPacket{From: from, To: to, Id: 55, RxTime: proto.Uint32(1700000000), Channel: uint32(max(relayIndex, 0)),
		PayloadVariant: &pb.MeshPacket_Decoded{Decoded: &pb.Data{Portnum: port, Payload: payload}}})
	return &pluginv1.PacketEvent{RadioId: "main", Direction: "rx", Kind: kind, MeshPacket: b, Decoded: true, ChannelHash: 8,
		RelayChannelIndex: relayIndex, ReporterNodeNum: relayNum}
}

func start(t *testing.T, mf *fakeMeshflow, s Settings) *Feeder {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(mf.handler))
	t.Cleanup(srv.Close)
	f := New(fakeHost{}, func(string, string, ...any) {}, "test")
	no := false
	s.APIURL, s.APIKey, s.AcceptTraceroutes = srv.URL, "k", &no
	f.Configure(context.Background(), s, []*pluginv1.Radio{
		{Id: "main", Name: "LongFast", Relay: &pluginv1.Identity{NodeId: "!11cbe35a", NodeNum: relayNum}},
		{Id: "mf", Name: "MediumFast", Relay: &pluginv1.Identity{NodeId: "!22222222", NodeNum: 0x22222222}},
	})
	t.Cleanup(f.Stop)
	return f
}

// Only what the relay persona itself would hear is uploaded, as it; nodes come from those packets.
func TestFeederReportsOnlyWhatTheRelayPersonaHears(t *testing.T) {
	mf := &fakeMeshflow{}
	f := start(t, mf, Settings{Radios: List{"main"}, IgnorePortnums: List{"POSITION_APP"}})
	text := []byte("hi")

	f.Packet(event(t, "delivered", 1, 7, broadcast, pb.PortNum_TEXT_MESSAGE_APP, text))                  // uploaded
	f.Packet(event(t, "dup", 1, 7, broadcast, pb.PortNum_TEXT_MESSAGE_APP, text))                        // duplicate
	f.Packet(event(t, "heard", -1, 7, broadcast, pb.PortNum_TEXT_MESSAGE_APP, text))                     // a channel the relay doesn't hold
	f.Packet(event(t, "heard", 1, 7, 8, pb.PortNum_TEXT_MESSAGE_APP, []byte("between others")))          // DM between other nodes
	f.Packet(event(t, "delivered", 0, 7, relayNum, pb.PortNum_TEXT_MESSAGE_APP, []byte("to the relay"))) // uploaded
	pos, _ := proto.Marshal(&pb.Position{LatitudeI: proto.Int32(1), LongitudeI: proto.Int32(1)})
	f.Packet(event(t, "heard", 0, 7, broadcast, pb.PortNum_POSITION_APP, pos)) // ignored port
	other := event(t, "heard", 0, 7, broadcast, pb.PortNum_TEXT_MESSAGE_APP, []byte("other radio"))
	other.RadioId = "mf"
	f.Packet(other) // radio not fed
	info, _ := proto.Marshal(&pb.User{LongName: "Heard Node", ShortName: "HRD"})
	f.Packet(event(t, "heard", 0, 7, broadcast, pb.PortNum_NODEINFO_APP, info)) // uploaded, and makes node 7

	waitFor(t, "uploads", func() bool {
		return len(mf.find("/ingest/")) >= 3 && len(mf.find("/nodes/")) >= 2 && len(mf.find("bot-version")) >= 1
	})
	time.Sleep(200 * time.Millisecond)
	ingests := mf.find("/ingest/")
	if len(ingests) != 3 {
		t.Fatalf("ingests = %v", ingests)
	}
	for _, in := range ingests {
		if !strings.HasPrefix(in, "POST /api/v3/packets/298574682/ingest/") || strings.Contains(in, "between others") || strings.Contains(in, "other radio") {
			t.Errorf("uploaded %s", in)
		}
	}
	var body map[string]any
	_ = json.Unmarshal([]byte(ingests[0][strings.Index(ingests[0], "{"):]), &body)
	if body["channel"] != float64(1) || body["decoded"].(map[string]any)["text"] != "hi" {
		t.Errorf("uploaded %v", body)
	}
	nodes := strings.Join(mf.find("/nodes/"), "\n")
	if !strings.Contains(nodes, "The Pole Relay") || !strings.Contains(nodes, "Heard Node") || strings.Contains(nodes, "Private Channel Node") {
		t.Errorf("node upserts:\n%s", nodes)
	}
	// Heard again, unchanged: not re-sent.
	f.Packet(event(t, "heard", 0, 7, broadcast, pb.PortNum_NODEINFO_APP, info))
	time.Sleep(200 * time.Millisecond)
	upserts := 0
	for _, n := range mf.find("/nodes/") {
		if strings.Contains(n, "Heard Node") {
			upserts++
		}
	}
	if upserts != 1 {
		t.Errorf("unchanged node upserted %d times", upserts)
	}
	summary, state, fields := f.Status()
	if state != "ok" || !strings.Contains(summary, "Feeding 1 radio") || len(fields) != 1 {
		t.Errorf("status %q %q %v", summary, state, fields)
	}
}

// Meshflow down: uploads wait and go through when it's back, and the status says so meanwhile.
func TestFeederHoldsUploadsWhileMeshflowIsDown(t *testing.T) {
	mf := &fakeMeshflow{}
	mf.fail.Store(3)
	f := start(t, mf, Settings{UploadNodes: new(bool)})
	f.Packet(event(t, "heard", 0, 7, broadcast, pb.PortNum_TEXT_MESSAGE_APP, []byte("wait for me")))
	waitFor(t, "unreachable status", func() bool {
		s, state, _ := f.Status()
		return state == "warning" && strings.Contains(s, "unreachable")
	})
	waitFor(t, "the held upload", func() bool { return len(mf.find("wait for me")) == 1 })
	waitFor(t, "status to recover", func() bool { _, state, _ := f.Status(); return state == "ok" })
}

// A refused node upsert is tried again when the node is next heard, not skipped for hours.
func TestRefusedNodeIsRetried(t *testing.T) {
	mf := &fakeMeshflow{}
	f := start(t, mf, Settings{UploadPackets: new(bool)})
	waitFor(t, "own node", func() bool { return len(mf.find("The Pole Relay")) == 1 })
	mf.refuse.Store(1)
	info, _ := proto.Marshal(&pb.User{LongName: "Flaky", ShortName: "FLK"})
	f.Packet(event(t, "heard", 0, 7, broadcast, pb.PortNum_NODEINFO_APP, info))
	waitFor(t, "the refusal", func() bool { return mf.refuse.Load() == 0 })
	time.Sleep(100 * time.Millisecond)
	f.Packet(event(t, "heard", 0, 7, broadcast, pb.PortNum_NODEINFO_APP, info))
	waitFor(t, "the retried node", func() bool { return len(mf.find("Flaky")) == 1 })
}

func TestListReadsOldAndNewSettings(t *testing.T) {
	var s Settings
	if err := json.Unmarshal([]byte(`{"radios":["main","mf"],"ignore_portnums":"TEXT_MESSAGE_APP, position_app"}`), &s); err != nil {
		t.Fatal(err)
	}
	if len(s.Radios) != 2 || len(s.IgnorePortnums) != 2 {
		t.Fatalf("settings = %+v", s)
	}
}

func TestFeederNeedsSettings(t *testing.T) {
	f := New(fakeHost{}, func(string, string, ...any) {}, "test")
	f.Configure(context.Background(), Settings{}, nil)
	if summary, state, _ := f.Status(); state != "warning" || !strings.Contains(summary, "API URL") {
		t.Fatalf("status %q %q", summary, state)
	}
}
