// Command repeatertastic-meshflow is a RepeaterTastic plugin that feeds Meshflow
// (github.com/pskillen/meshflow-api) as each radio's relay persona.
//
// RepeaterTastic starts it with RT_PLUGIN_* in the environment. To run it elsewhere, attach it
// in RepeaterTastic and set RT_PLUGIN_ID, RT_PLUGIN_ADDR and RT_PLUGIN_TOKEN.
package main

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/A13xB0/RepeaterTastic/pluginsdk"

	"github.com/ScotMesh/repeatertastic-meshflow/internal/feeder"
)

var version = "dev"

// manifest is sent when attached, so RepeaterTastic can show the settings form.
//
//go:embed plugin.yaml
var manifest string

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println("repeatertastic-meshflow", version)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	opts := pluginsdk.Options{Version: version}
	if os.Getenv("RT_PLUGIN_SOCKET") == "" {
		opts.ManifestYAML = manifest
		if os.Getenv("RT_PLUGIN_ID") == "" {
			opts.ID = "meshflow"
		}
	}
	c, err := pluginsdk.Connect(ctx, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	logf := func(level, format string, args ...any) { _ = c.Log(level, format, args...) }
	f := feeder.New(c.Host, logf, version)
	defer f.Stop()
	configure := func() {
		var s feeder.Settings
		if err := c.Settings(&s); err != nil {
			logf("error", "reading settings: %v", err)
		}
		f.Configure(c.Context(), s, c.Welcome.Radios)
	}
	configure()
	logf("info", "repeatertastic-meshflow %s started", version)

	report := func() {
		summary, state, fields := f.Status()
		_ = c.Status(summary, state, fields)
	}
	report()
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			report()
		case msg, ok := <-c.Events():
			if !ok {
				if err := c.Err(); err != nil {
					log.Printf("session ended: %v", err)
				}
				return
			}
			switch {
			case msg.GetPacket() != nil:
				f.Packet(msg.GetPacket())
			case msg.GetNode() != nil:
				f.Node(msg.GetNode().Node)
			case msg.GetSettings() != nil:
				logf("info", "settings changed; reconnecting to Meshflow")
				configure()
				report()
			case msg.GetStop() != nil:
				return
			}
		}
	}
}
