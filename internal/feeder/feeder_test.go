package feeder

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/A13xB0/RepeaterTastic/pb"
	pluginv1 "github.com/A13xB0/RepeaterTastic/pluginapi/v1"
)

type fakeHost struct {
	mu     sync.Mutex
	traces []string
}

func (h *fakeHost) ListNodes(context.Context, *pluginv1.ListNodesRequest, ...grpc.CallOption) (*pluginv1.ListNodesResponse, error) {
	u, _ := proto.Marshal(&pb.User{LongName: "Known", ShortName: "KNWN"})
	return &pluginv1.ListNodesResponse{Nodes: []*pluginv1.Node{{NodeNum: 99, NodeId: "!00000063", RadioId: "main", User: u}}}, nil
}

func (h *fakeHost) Traceroute(_ context.Context, in *pluginv1.TracerouteRequest, _ ...grpc.CallOption) (*pluginv1.SendResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.traces = append(h.traces, in.RadioId+" "+in.Target)
	return &pluginv1.SendResponse{}, nil
}

type fakeMeshflow struct {
	mu       sync.Mutex
	requests []string
}

func (m *fakeMeshflow) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
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

func packetEvent(t *testing.T, kind string, relayIndex int32, port pb.PortNum, payload []byte) *pluginv1.PacketEvent {
	t.Helper()
	b, _ := proto.Marshal(&pb.MeshPacket{From: 7, To: 0xffffffff, Id: 55, RxTime: proto.Uint32(1700000000), Channel: uint32(max(relayIndex, 0)),
		PayloadVariant: &pb.MeshPacket_Decoded{Decoded: &pb.Data{Portnum: port, Payload: payload}}})
	return &pluginv1.PacketEvent{RadioId: "main", Direction: "rx", Kind: kind, MeshPacket: b, Decoded: true, ChannelHash: 8,
		RelayChannelIndex: relayIndex, ReporterNodeNum: 0x11cbe35a}
}

func TestFeederUploadsAsTheRelayPersona(t *testing.T) {
	mf := &fakeMeshflow{}
	srv := httptest.NewServer(http.HandlerFunc(mf.handler))
	defer srv.Close()
	host := &fakeHost{}
	var logMu sync.Mutex
	var logs []string
	f := New(host, func(level, format string, args ...any) {
		logMu.Lock()
		logs = append(logs, level)
		logMu.Unlock()
	}, "test")
	radios := []*pluginv1.Radio{
		{Id: "main", Name: "LongFast", Relay: &pluginv1.Identity{NodeId: "!11cbe35a", NodeNum: 0x11cbe35a}},
		{Id: "mf", Name: "MediumFast", Relay: &pluginv1.Identity{NodeId: "!22222222", NodeNum: 0x22222222}},
	}
	no := false
	f.Configure(context.Background(), Settings{APIURL: srv.URL, APIKey: "k", Radios: "main", AcceptTraceroutes: &no, IgnorePortnums: "position_app"}, radios)
	defer f.Stop()

	f.Packet(packetEvent(t, "delivered", 1, pb.PortNum_TEXT_MESSAGE_APP, []byte("hi")))   // uploaded
	f.Packet(packetEvent(t, "dup", 1, pb.PortNum_TEXT_MESSAGE_APP, []byte("hi")))         // duplicate
	f.Packet(packetEvent(t, "heard", -1, pb.PortNum_TEXT_MESSAGE_APP, []byte("private"))) // not the relay persona's channel
	pos, _ := proto.Marshal(&pb.Position{LatitudeI: proto.Int32(1), LongitudeI: proto.Int32(1)})
	f.Packet(packetEvent(t, "heard", 0, pb.PortNum_POSITION_APP, pos)) // ignored port
	other := packetEvent(t, "heard", 0, pb.PortNum_TEXT_MESSAGE_APP, []byte("other radio"))
	other.RadioId = "mf"
	f.Packet(other) // radio not fed

	deadline := time.Now().Add(5 * time.Second)
	for len(mf.find("/ingest/")) < 1 || len(mf.find("/nodes/")) < 1 || len(mf.find("bot-version")) < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("requests: %v", mf.find(""))
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	ingests := mf.find("/ingest/")
	if len(ingests) != 1 || !strings.HasPrefix(ingests[0], "POST /api/v3/packets/298574682/ingest/") {
		t.Fatalf("ingests = %v", ingests)
	}
	var body map[string]any
	_ = json.Unmarshal([]byte(ingests[0][strings.Index(ingests[0], "{"):]), &body)
	if body["channel"] != float64(1) || body["decoded"].(map[string]any)["text"] != "hi" {
		t.Errorf("uploaded %v", body)
	}
	if n := mf.find("/nodes/"); len(n) != 1 || !strings.Contains(n[0], `"long_name":"Known"`) {
		t.Errorf("node upserts = %v", n)
	}
	// The same node again, unchanged: not re-sent.
	u, _ := proto.Marshal(&pb.User{LongName: "Known", ShortName: "KNWN"})
	f.Node(&pluginv1.Node{NodeNum: 99, NodeId: "!00000063", RadioId: "main", User: u})
	time.Sleep(100 * time.Millisecond)
	if n := mf.find("/nodes/"); len(n) != 1 {
		t.Errorf("unchanged node re-sent: %v", n)
	}
	summary, state, fields := f.Status()
	if state != "ok" || !strings.Contains(summary, "Feeding 1 radio") || len(fields) != 1 {
		t.Errorf("status %q %q %v", summary, state, fields)
	}
}

func TestFeederNeedsSettings(t *testing.T) {
	f := New(&fakeHost{}, func(string, string, ...any) {}, "test")
	f.Configure(context.Background(), Settings{}, nil)
	if summary, state, _ := f.Status(); state != "warning" || !strings.Contains(summary, "API URL") {
		t.Fatalf("status %q %q", summary, state)
	}
}
