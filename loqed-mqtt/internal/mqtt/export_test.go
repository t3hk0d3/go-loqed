package mqtt

import paho "github.com/eclipse/paho.mqtt.golang"

// ConnectFailed and ConnectSucceeded drive the client's connect-attempt
// handling without a broker, so tests can use a fake clock.
func (c *Client) ConnectFailed(err error) { c.onConnectFailed(err) }
func (c *Client) ConnectSucceeded()       { c.onConnected() }

// SetPaho replaces the paho client, so tests can control publish tokens.
func (c *Client) SetPaho(mc paho.Client) { c.mc = mc }

// PahoOptions exposes the options NewClient gave paho.
func (c *Client) PahoOptions() paho.ClientOptionsReader { return c.mc.OptionsReader() }

// Reconnected runs the on-connect work (subscribe, status, discovery,
// republish) synchronously.
func (c *Client) Reconnected() { c.onConnect() }

// Command delivers a command message as if it arrived from the broker.
func (c *Client) Command(m paho.Message) { c.onCommand(nil, m) }
