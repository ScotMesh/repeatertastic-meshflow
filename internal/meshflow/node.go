package meshflow

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/A13xB0/RepeaterTastic/pb"
)

// Node is what a feeder knows about one node: learned from packets it heard itself.
type Node struct {
	Num       uint32
	User      *pb.User
	Position  *pb.Position
	PosAt     time.Time // when the position was reported
	Metrics   *pb.DeviceMetrics
	MetricsAt time.Time // when the metrics were reported
}

// NodeJSON is the v3 node upsert body (meshtastic_* fields), or nil when meshflow-api would refuse
// the node (no user info or no long name). key identifies the node's identity and position, so
// unchanged nodes needn't be sent again; metrics are left out of it because they change on every
// report.
func NodeJSON(n *Node) (body []byte, key string) {
	u := n.User
	if u == nil || strings.TrimSpace(u.LongName) == "" {
		return nil, ""
	}
	out := map[string]any{
		"id":                    n.Num,
		"macaddr":               b64(u.Macaddr),
		"meshtastic_hw_model":   u.HwModel.String(),
		"meshtastic_public_key": b64(u.PublicKey),
		"user":                  map[string]any{"long_name": u.LongName, "short_name": u.ShortName},
	}
	keyed := map[string]any{"id": n.Num, "mac": out["macaddr"], "hw": out["meshtastic_hw_model"], "pk": out["meshtastic_public_key"], "user": out["user"]}
	if pos := n.Position; pos != nil && (pos.GetLatitudeI() != 0 || pos.GetLongitudeI() != 0) {
		out["position"] = map[string]any{
			"reported_time":              stamp(n.PosAt),
			"latitude":                   float64(pos.GetLatitudeI()) * 1e-7,
			"longitude":                  float64(pos.GetLongitudeI()) * 1e-7,
			"altitude":                   pos.GetAltitude(),
			"meshtastic_location_source": locationSource(pos.LocationSource),
		}
		keyed["pos"] = []any{pos.GetLatitudeI(), pos.GetLongitudeI(), pos.GetAltitude()}
	}
	if dm := n.Metrics; dm != nil {
		out["device_metrics"] = map[string]any{
			"reported_time":                  stamp(n.MetricsAt),
			"battery_level":                  dm.GetBatteryLevel(),
			"voltage":                        dm.GetVoltage(),
			"meshtastic_channel_utilization": dm.GetChannelUtilization(),
			"meshtastic_air_util_tx":         dm.GetAirUtilTx(),
			"uptime_seconds":                 dm.GetUptimeSeconds(),
		}
	}
	body, _ = json.Marshal(out)
	k, _ := json.Marshal(keyed)
	return body, string(k)
}

func b64(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(b)
}

func stamp(t time.Time) string {
	if t.IsZero() {
		t = time.Now()
	}
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func locationSource(s pb.Position_LocSource) string {
	if s == pb.Position_LOC_UNSET {
		return "UNSET"
	}
	return s.String()
}
