package mqtt

// ConnectFailed and ConnectSucceeded drive the client's connect-attempt
// handling without a broker, so tests can use a fake clock.
func (c *Client) ConnectFailed(err error) { c.onConnectFailed(err) }
func (c *Client) ConnectSucceeded()       { c.onConnected() }
