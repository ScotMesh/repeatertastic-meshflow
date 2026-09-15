// Command repeatertastic-meshflow is a RepeaterTastic plugin that feeds Meshflow
// (github.com/pskillen/meshflow-api) as each radio's relay persona.
//
// RepeaterTastic starts it with RT_PLUGIN_* in the environment and restarts it if it exits. To run
// it elsewhere, attach it in RepeaterTastic and set RT_PLUGIN_ID, RT_PLUGIN_ADDR and
// RT_PLUGIN_TOKEN; it then reconnects by itself.
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

	pluginv1 "github.com/A13xB0/RepeaterTastic/pluginapi/v1"
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
	managed := os.Getenv("RT_PLUGIN_SOCKET") != ""
	if !managed {
		opts.ManifestYAML = manifest
		if os.Getenv("RT_PLUGIN_ID") == "" {
			opts.ID = "meshflow"
		}
	}
	if managed {
		// RepeaterTastic restarts a managed plugin that exits.
		if err := session(ctx, opts); err != nil {
			log.Fatal(err)
		}
		return
	}
	// Attached: keep trying. RepeaterTastic refuses the session until the settings are filled in,
	// and the connection comes and goes with restarts.
	wait := 2 * time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := session(ctx, opts)
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > time.Minute {
			wait = 2 * time.Second
		}
		log.Printf("not connected to RepeaterTastic (%v); retrying in %s", err, wait)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(time.Minute, wait*2)
	}
}

// session runs one connection to RepeaterTastic until it ends.
func session(ctx context.Context, opts pluginsdk.Options) error {
	c, err := pluginsdk.Connect(ctx, opts)
	if err != nil {
		return err
	}
	defer c.Close()

	logf := func(level, format string, args ...any) { _ = c.Log(level, format, args...) }
	f := feeder.New(c.Host, logf, version)
	defer f.Stop()
	radios := c.Welcome.Radios
	configure := func() {
		var s feeder.Settings
		if err := c.Settings(&s); err != nil {
			logf("error", "reading settings: %v", err)
		}
		// The radios (and their relay personas) may have changed since Welcome.
		lctx, cancel := context.WithTimeout(c.Context(), 10*time.Second)
		if resp, err := c.Host.ListRadios(lctx, &pluginv1.ListRadiosRequest{}); err == nil {
			radios = resp.Radios
		}
		cancel()
		f.Configure(c.Context(), s, radios)
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
			return nil
		case <-tick.C:
			report()
		case msg, ok := <-c.Events():
			if !ok {
				if err := c.Err(); err != nil {
					return fmt.Errorf("session ended: %w", err)
				}
				return fmt.Errorf("session ended")
			}
			switch {
			case msg.GetPacket() != nil:
				f.Packet(msg.GetPacket())
			case msg.GetSettings() != nil:
				logf("info", "settings changed; reconnecting to Meshflow")
				configure()
				report()
			case msg.GetStop() != nil:
				return nil
			}
		}
	}
}
