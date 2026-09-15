package meshflow

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/A13xB0/RepeaterTastic/pb"
	pluginv1 "github.com/A13xB0/RepeaterTastic/pluginapi/v1"
)

// NodeJSON is the v3 node upsert body (meshtastic_* fields) for a node RepeaterTastic knows, or
// nil when the node has no user info yet (meshflow-bot skips those too). key identifies the
// content apart from timestamps, so unchanged nodes needn't be sent again.
func NodeJSON(n *pluginv1.Node, now time.Time) (body []byte, key string) {
	u := &pb.User{}
	if len(n.User) == 0 || proto.Unmarshal(n.User, u) != nil {
		return nil, ""
	}
	out := map[string]any{
		"id":                    n.NodeNum,
		"macaddr":               b64(u.Macaddr),
		"meshtastic_hw_model":   u.HwModel.String(),
		"meshtastic_public_key": b64(u.PublicKey),
		"user":                  map[string]any{"long_name": u.LongName, "short_name": u.ShortName},
	}
	pos := &pb.Position{}
	if len(n.Position) > 0 && proto.Unmarshal(n.Position, pos) == nil && (pos.GetLatitudeI() != 0 || pos.GetLongitudeI() != 0) {
		reported := now
		if pos.Time != 0 {
			reported = time.Unix(int64(pos.Time), 0)
		}
		out["position"] = map[string]any{
			"reported_time":              stamp(reported),
			"latitude":                   float64(pos.GetLatitudeI()) * 1e-7,
			"longitude":                  float64(pos.GetLongitudeI()) * 1e-7,
			"altitude":                   pos.GetAltitude(),
			"meshtastic_location_source": locationSource(pos.LocationSource),
		}
	}
	dm := &pb.DeviceMetrics{}
	if len(n.DeviceMetrics) > 0 && proto.Unmarshal(n.DeviceMetrics, dm) == nil {
		out["device_metrics"] = map[string]any{
			"reported_time":                  stamp(now),
			"battery_level":                  dm.GetBatteryLevel(),
			"voltage":                        dm.GetVoltage(),
			"meshtastic_channel_utilization": dm.GetChannelUtilization(),
			"meshtastic_air_util_tx":         dm.GetAirUtilTx(),
			"uptime_seconds":                 dm.GetUptimeSeconds(),
		}
	}
	body, _ = json.Marshal(out)
	// The key leaves out times and metrics, which change on every report.
	keyed := map[string]any{"id": out["id"], "mac": out["macaddr"], "hw": out["meshtastic_hw_model"], "pk": out["meshtastic_public_key"], "user": out["user"]}
	if p, ok := out["position"].(map[string]any); ok {
		keyed["pos"] = []any{p["latitude"], p["longitude"], p["altitude"]}
	}
	k, _ := json.Marshal(keyed)
	return body, string(k)
}

func b64(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(b)
}

func stamp(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05Z") }

func locationSource(s pb.Position_LocSource) string {
	if s == pb.Position_LOC_UNSET {
		return "UNSET"
	}
	return s.String()
}
