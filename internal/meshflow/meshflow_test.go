package meshflow

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/A13xB0/RepeaterTastic/pb"
)

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func decode(t *testing.T, b []byte) map[string]any {
	t.Helper()
	out := map[string]any{}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func u32(v uint32) *uint32 { return &v }
func i32(v int32) *int32   { return &v }

// The shape meshflow-bot uploads: camelCase, enum names, fromId/toId, per-port decoded object.
func TestPacketJSONShapes(t *testing.T) {
	base := func(port pb.PortNum, payload []byte) *pb.MeshPacket {
		return &pb.MeshPacket{From: 0x433d4494, To: 0xffffffff, Id: 1234, Channel: 0, RxTime: u32(1700000000), RxSnr: 6.25, RxRssi: i32(-97),
			HopLimit: 2, HopStart: 3, RelayNode: 0x94,
			PayloadVariant: &pb.MeshPacket_Decoded{Decoded: &pb.Data{Portnum: port, Payload: payload}}}
	}

	b, err := PacketJSON(base(pb.PortNum_TEXT_MESSAGE_APP, []byte("hello mesh")))
	if err != nil {
		t.Fatal(err)
	}
	m := decode(t, b)
	dec := m["decoded"].(map[string]any)
	for k, want := range map[string]any{"from": float64(0x433d4494), "to": float64(0xffffffff), "id": float64(1234), "channel": float64(0),
		"rxTime": float64(1700000000), "rxSnr": 6.25, "rxRssi": float64(-97), "hopLimit": float64(2), "hopStart": float64(3),
		"fromId": "!433d4494", "toId": "^all", "relayNode": float64(0x94)} {
		if m[k] != want {
			t.Errorf("%s = %v, want %v", k, m[k], want)
		}
	}
	if dec["portnum"] != "TEXT_MESSAGE_APP" || dec["text"] != "hello mesh" {
		t.Errorf("decoded = %v", dec)
	}

	pos := base(pb.PortNum_POSITION_APP, mustMarshal(t, &pb.Position{LatitudeI: i32(559533000), LongitudeI: i32(-31883000), Altitude: i32(52),
		Time: 1700000001, LocationSource: pb.Position_LOC_MANUAL}))
	b, err = PacketJSON(pos)
	if err != nil {
		t.Fatal(err)
	}
	p := decode(t, b)["decoded"].(map[string]any)["position"].(map[string]any)
	if p["latitudeI"] != float64(559533000) || p["locationSource"] != "LOC_MANUAL" || p["latitude"].(float64) < 55.95 || p["longitude"].(float64) > -3.18 {
		t.Errorf("position = %v", p)
	}
	if _, err := PacketJSON(base(pb.PortNum_POSITION_APP, mustMarshal(t, &pb.Position{Time: 5}))); !errors.Is(err, ErrSkip) {
		t.Errorf("position request without a location: %v", err)
	}

	b, err = PacketJSON(base(pb.PortNum_NODEINFO_APP, mustMarshal(t, &pb.User{Id: "!433d4494", LongName: "Pole", ShortName: "POLE",
		HwModel: pb.HardwareModel_HELTEC_V3, Role: pb.Config_DeviceConfig_CLIENT_MUTE, PublicKey: []byte{1, 2, 3}})))
	if err != nil {
		t.Fatal(err)
	}
	u := decode(t, b)["decoded"].(map[string]any)["user"].(map[string]any)
	if u["hwModel"] != "HELTEC_V3" || u["role"] != "CLIENT_MUTE" || u["publicKey"] != "AQID" || u["longName"] != "Pole" {
		t.Errorf("user = %v", u)
	}

	// Telemetry without its own time gets the receive time (meshflow-api requires one).
	b, err = PacketJSON(base(pb.PortNum_TELEMETRY_APP, mustMarshal(t, &pb.Telemetry{Variant: &pb.Telemetry_DeviceMetrics{DeviceMetrics: &pb.DeviceMetrics{
		BatteryLevel: u32(101), Voltage: proto.Float32(4.2)}}})))
	if err != nil {
		t.Fatal(err)
	}
	tel := decode(t, b)["decoded"].(map[string]any)["telemetry"].(map[string]any)
	if tel["time"] != float64(1700000000) || tel["deviceMetrics"].(map[string]any)["batteryLevel"] != float64(101) {
		t.Errorf("telemetry = %v", tel)
	}

	b, err = PacketJSON(base(pb.PortNum_TRACEROUTE_APP, mustMarshal(t, &pb.RouteDiscovery{Route: []uint32{7, 8}, SnrTowards: []int32{24, -128}})))
	if err != nil {
		t.Fatal(err)
	}
	tr := decode(t, b)["decoded"].(map[string]any)["traceroute"].(map[string]any)
	if len(tr["route"].([]any)) != 2 || tr["snrTowards"].([]any)[1] != float64(-128) {
		t.Errorf("traceroute = %v", tr)
	}

	soil := mustMarshal(t, &pb.Telemetry{Variant: &pb.Telemetry_SoilWaterMetrics{SoilWaterMetrics: &pb.SoilWaterMetrics{}}})
	if _, err := PacketJSON(base(pb.PortNum_TELEMETRY_APP, soil)); !errors.Is(err, ErrSkip) {
		t.Errorf("telemetry meshflow-api doesn't take: %v", err)
	}
	if _, err := PacketJSON(base(pb.PortNum_ROUTING_APP, nil)); !errors.Is(err, ErrSkip) {
		t.Errorf("routing packet: %v", err)
	}
	if _, err := PacketJSON(&pb.MeshPacket{PayloadVariant: &pb.MeshPacket_Encrypted{Encrypted: []byte{1}}}); !errors.Is(err, ErrSkip) {
		t.Errorf("encrypted packet: %v", err)
	}
}

func TestNodeJSON(t *testing.T) {
	n := &Node{Num: 0x433d4494, User: &pb.User{LongName: "Pole", ShortName: "POLE", HwModel: pb.HardwareModel_HELTEC_V3},
		Position: &pb.Position{LatitudeI: i32(559533000), LongitudeI: i32(-31883000), Altitude: i32(52)}, PosAt: time.Unix(1700000000, 0),
		Metrics: &pb.DeviceMetrics{BatteryLevel: u32(80)}, MetricsAt: time.Unix(1700000100, 0)}
	b, key := NodeJSON(n)
	m := decode(t, b)
	if m["id"] != float64(0x433d4494) || m["meshtastic_hw_model"] != "HELTEC_V3" || m["user"].(map[string]any)["short_name"] != "POLE" {
		t.Errorf("node = %v", m)
	}
	pos := m["position"].(map[string]any)
	if pos["reported_time"] != "2023-11-14T22:13:20Z" || pos["meshtastic_location_source"] != "UNSET" {
		t.Errorf("position = %v", pos)
	}
	dm := m["device_metrics"].(map[string]any)
	for _, f := range []string{"battery_level", "voltage", "meshtastic_channel_utilization", "meshtastic_air_util_tx", "uptime_seconds"} {
		if _, ok := dm[f]; !ok {
			t.Errorf("device_metrics has no %s", f)
		}
	}
	if dm["reported_time"] != "2023-11-14T22:15:00Z" {
		t.Errorf("metrics reported_time = %v, want the time they were reported", dm["reported_time"])
	}
	n.Metrics, n.MetricsAt = &pb.DeviceMetrics{BatteryLevel: u32(20)}, time.Unix(1800000000, 0)
	if _, key2 := NodeJSON(n); key != key2 {
		t.Error("the change key depends on metrics")
	}
	if b, _ := NodeJSON(&Node{Num: 1}); b != nil {
		t.Error("a node without user info was rendered")
	}
	if b, _ := NodeJSON(&Node{Num: 1, User: &pb.User{ShortName: "X"}}); b != nil {
		t.Error("a node without a long name was rendered (meshflow-api refuses a blank one)")
	}
}

func TestClientPathsAuthAndErrors(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got = append(got, r.Method+" "+r.URL.Path+" "+r.Header.Get("Authorization")+" "+string(body))
		switch {
		case strings.Contains(r.URL.Path, "/ingest/") && strings.Contains(string(body), "bad"):
			http.Error(w, `{"decoded.portnum":"Unknown packet type"}`, http.StatusBadRequest)
		case strings.Contains(r.URL.Path, "/nodes/"):
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer srv.Close()
	c, err := NewClient(srv.URL+"/api/", "k3y", 1127973616, "repeatertastic-meshflow/test")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.Ingest(ctx, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	err = c.Ingest(ctx, []byte(`"bad"`))
	if err == nil || Retryable(err) {
		t.Fatalf("400 should be final: %v", err)
	}
	if err := c.UpsertNode(ctx, []byte(`{}`)); !Retryable(err) {
		t.Fatalf("503 should be retryable: %v", err)
	}
	if err := c.ReportVersion(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"POST /api/v3/packets/1127973616/ingest/ Token k3y {}",
		`POST /api/v3/packets/1127973616/ingest/ Token k3y "bad"`,
		"POST /api/v3/packets/1127973616/nodes/ Token k3y {}",
		`PUT /api/v3/packets/1127973616/bot-version/ Token k3y {"bot_version":"repeatertastic-meshflow/test"}`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("requests:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if _, err := BaseURL("ftp://x"); err == nil {
		t.Error("ftp URL accepted")
	}
	if u, _ := WSURL("https://mf.example.org/api", ""); u != "wss://mf.example.org" {
		t.Errorf("derived ws url = %s", u)
	}
}

func TestCommandsTraceroute(t *testing.T) {
	var query, origin atomic.Value
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws/nodes/" {
			http.NotFound(w, r)
			return
		}
		query.Store(r.URL.RawQuery)
		origin.Store(r.Header.Get("Origin"))
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"traceroute","target":1127973616}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"traceroute","target":"!00000007"}`))
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()
	targets := make(chan uint32, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Commands{URL: "ws" + strings.TrimPrefix(srv.URL, "http"), Key: "s3cret key", Feeder: 42, OnTrace: func(n uint32) { targets <- n }}
	go c.Run(ctx)
	for _, want := range []uint32{1127973616, 7} {
		select {
		case n := <-targets:
			if n != want {
				t.Fatalf("target %d, want %d", n, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no traceroute command")
		}
	}
	if q := query.Load().(string); !strings.Contains(q, "feeder_node_id=42") || !strings.Contains(q, "api_key=s3cret+key") {
		t.Errorf("query = %s", q)
	}
	if o := origin.Load().(string); o != srv.URL {
		t.Errorf("origin = %s", o)
	}
	if got := redact("dial wss://x/?api_key=s3cret+key failed", "s3cret key"); strings.Contains(got, "s3cret") {
		t.Errorf("key not redacted: %s", got)
	}
}
