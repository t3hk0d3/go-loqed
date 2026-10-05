// Command loqed-mqtt bridges LOQED smart locks to MQTT and Home Assistant.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/app"
	"github.com/t3hk0d3/go-loqed/internal/config"
)

var version = "dev"

const optionsFile = "/data/options.json"

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	if len(args) > 0 && args[0] == "healthcheck" {
		return healthcheck(args[1:])
	}
	fs := flag.NewFlagSet("loqed-mqtt", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to a YAML config file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg, err := loadConfig(*configPath)
	if err == nil {
		err = cfg.Validate()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loqed-mqtt: invalid configuration:", err)
		return 2
	}
	log := newLogger(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := config.ResolveMQTT(ctx, &cfg, os.Getenv("SUPERVISOR_TOKEN"), config.SupervisorURL, &http.Client{Timeout: 10 * time.Second}, nil); err != nil {
		if ctx.Err() != nil {
			return 0
		}
		log.Error("cannot determine the MQTT broker", "err", err)
		return 1
	}
	log.Info("starting loqed-mqtt", "version", version)
	if err := app.Run(ctx, app.Options{Config: cfg, Log: log, Version: version}); err != nil {
		log.Error("loqed-mqtt stopped", "err", err)
		return 1
	}
	return 0
}

func newLogger(cfg config.Config) *slog.Logger {
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.LogLevel))
	opts := &slog.HandlerOptions{Level: level}
	if cfg.LogFormat == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func loadConfig(path string) (config.Config, error) {
	return config.Load(config.Sources{OptionsFile: optionsFile, ConfigFile: path, Environ: os.Environ()})
}

// healthcheck is used by Docker HEALTHCHECK: distroless has no shell or curl.
// Docker passes no flags, so configure the listen address via the
// environment (LOQED_WEBHOOK__LISTEN) when it is not the default.
func healthcheck(args []string) int {
	fs := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to a YAML config file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg, err := loadConfig(*configPath)
	if err != nil {
		return 1
	}
	url, err := healthURL(cfg.Webhook.Listen)
	if err != nil {
		return 1
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(url) //nolint:gosec // G107: local health endpoint
	if err != nil {
		return 1
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

// healthURL maps a listen address to a dialable URL: an empty or
// unspecified host means loopback.
func healthURL(listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/healthz", nil
}
