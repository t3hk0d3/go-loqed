package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"

	loqed "github.com/t3hk0d3/go-loqed"
)

const (
	SupervisorURL  = "http://supervisor"
	DefaultMQTTURL = "tcp://localhost:1883"
)

// LookupHost resolves a host name (net.DefaultResolver.LookupHost in production).
type LookupHost func(ctx context.Context, host string) ([]string, error)

// ResolveMQTT fills mqtt.url (and credentials if unset). An explicit URL
// wins; under the HA Supervisor the MQTT service is queried; otherwise
// DefaultMQTTURL is used. A Supervisor-provided host that does not resolve
// (host-network add-ons may not see the Supervisor DNS) falls back to
// 127.0.0.1, where the Mosquitto add-on also listens. lookup may be nil.
func ResolveMQTT(ctx context.Context, c *Config, supervisorToken, supervisorURL string, hc *http.Client, lookup LookupHost) error {
	if c.MQTT.URL != "" {
		return nil
	}
	if supervisorToken == "" {
		c.MQTT.URL = DefaultMQTTURL
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, supervisorURL+"/services/mqtt", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+supervisorToken)
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("config: asking the Supervisor for MQTT settings: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("config: Supervisor MQTT service lookup failed with HTTP %d; install the Mosquitto add-on or set mqtt.url", resp.StatusCode)
	}
	var out struct {
		Result string `json:"result"`
		Data   struct {
			Host     string `json:"host"`
			Port     int    `json:"port"`
			SSL      bool   `json:"ssl"`
			Username string `json:"username"`
			Password string `json:"password"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("config: Supervisor MQTT response: %w", err)
	}
	if out.Result != "ok" || out.Data.Host == "" {
		return errors.New("config: the Supervisor has no MQTT service; install the Mosquitto add-on or set mqtt.url")
	}
	scheme := "tcp"
	if out.Data.SSL {
		scheme = "ssl"
	}
	if lookup == nil {
		lookup = net.DefaultResolver.LookupHost
	}
	host := out.Data.Host
	if _, err := lookup(ctx, host); err != nil {
		host = "127.0.0.1"
	}
	c.MQTT.URL = scheme + "://" + net.JoinHostPort(host, strconv.Itoa(out.Data.Port))
	if c.MQTT.Username == "" {
		c.MQTT.Username, c.MQTT.Password = out.Data.Username, loqed.Secret(out.Data.Password)
	}
	return nil
}
