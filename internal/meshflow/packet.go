// Package meshflow talks to a meshflow-api server the way meshflow-bot does: packet ingest,
// node upserts and bot version over HTTP, and feeder commands over a WebSocket.
package meshflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/A13xB0/RepeaterTastic/pb"
)

// Ports meshflow-api ingests; anything else it answers 400.
var Ports = map[pb.PortNum]bool{
	pb.PortNum_TEXT_MESSAGE_APP: true,
	pb.PortNum_NODEINFO_APP:     true,
	pb.PortNum_POSITION_APP:     true,
	pb.PortNum_TELEMETRY_APP:    true,
	pb.PortNum_TRACEROUTE_APP:   true,
}

// ErrSkip: the packet is one meshflow-api would refuse, so it isn't sent.
var ErrSkip = errors.New("not ingestible")

const broadcast = 0xffffffff

var toDict = protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}

// PacketJSON renders a decoded MeshPacket as the Meshtastic Python library hands it to
// meshflow-bot (MessageToDict plus fromId/toId and the per-port decoded object), after the bot's
// sanitising: camelCase keys, enums by name, bytes as base64, no "raw".
func PacketJSON(p *pb.MeshPacket) ([]byte, error) {
	d := p.GetDecoded()
	if d == nil {
		return nil, fmt.Errorf("%w: encrypted", ErrSkip)
	}
	if !Ports[d.Portnum] {
		return nil, fmt.Errorf("%w: %s", ErrSkip, d.Portnum)
	}
	out, err := messageDict(p)
	if err != nil {
		return nil, err
	}
	out["from"] = p.From
	out["to"] = p.To
	out["id"] = p.Id
	out["channel"] = p.Channel // MessageToDict drops 0; the bot puts it back
	out["fromId"] = nodeID(p.From)
	if p.To == broadcast {
		out["toId"] = "^all"
	} else {
		out["toId"] = nodeID(p.To)
	}
	dec, _ := out["decoded"].(map[string]any)
	if dec == nil {
		dec = map[string]any{}
		out["decoded"] = dec
	}
	dec["portnum"] = d.Portnum.String()

	switch d.Portnum {
	case pb.PortNum_TEXT_MESSAGE_APP:
		dec["text"] = string(d.Payload)
	case pb.PortNum_POSITION_APP:
		pos := &pb.Position{}
		m, err := subDict(d.Payload, pos)
		if err != nil {
			return nil, err
		}
		if pos.LatitudeI == nil || pos.LongitudeI == nil {
			return nil, fmt.Errorf("%w: position without a location", ErrSkip)
		}
		m["latitude"] = float64(pos.GetLatitudeI()) * 1e-7
		m["longitude"] = float64(pos.GetLongitudeI()) * 1e-7
		dec["position"] = m
	case pb.PortNum_NODEINFO_APP:
		m, err := subDict(d.Payload, &pb.User{})
		if err != nil {
			return nil, err
		}
		dec["user"] = m
	case pb.PortNum_TELEMETRY_APP:
		t := &pb.Telemetry{}
		m, err := subDict(d.Payload, t)
		if err != nil {
			return nil, err
		}
		if !telemetryVariants(t) {
			return nil, fmt.Errorf("%w: telemetry meshflow-api doesn't take", ErrSkip)
		}
		if t.Time == 0 && p.RxTime != nil {
			m["time"] = p.GetRxTime() // meshflow-api needs a reading time
		}
		dec["telemetry"] = m
	case pb.PortNum_TRACEROUTE_APP:
		m, err := subDict(d.Payload, &pb.RouteDiscovery{})
		if err != nil {
			return nil, err
		}
		dec["traceroute"] = m
	}
	return json.Marshal(out)
}

// telemetryVariants: the telemetry objects meshflow-api ingests; others are answered 400.
func telemetryVariants(t *pb.Telemetry) bool {
	switch t.Variant.(type) {
	case *pb.Telemetry_DeviceMetrics, *pb.Telemetry_LocalStats, *pb.Telemetry_EnvironmentMetrics, *pb.Telemetry_AirQualityMetrics,
		*pb.Telemetry_PowerMetrics, *pb.Telemetry_HealthMetrics, *pb.Telemetry_HostMetrics, *pb.Telemetry_TrafficManagementStats:
		return true
	}
	return false
}

func messageDict(m proto.Message) (map[string]any, error) {
	b, err := toDict.Marshal(m)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return out, dec.Decode(&out)
}

func subDict(payload []byte, m proto.Message) (map[string]any, error) {
	if err := proto.Unmarshal(payload, m); err != nil {
		return nil, fmt.Errorf("%w: undecodable payload: %v", ErrSkip, err)
	}
	return messageDict(m)
}

func nodeID(n uint32) string { return fmt.Sprintf("!%08x", n) }
