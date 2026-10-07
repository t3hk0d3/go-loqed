package bridge

import (
	"context"
	"fmt"
	"net/http"
)

// Command asks the lock to open, unlock (day_lock) or lock (night_lock).
// The bridge only acknowledges receipt; the outcome arrives as a webhook.
func (c *Client) Command(ctx context.Context, a Action) error {
	switch a {
	case ActionOpen, ActionUnlock, ActionLock:
	default:
		return fmt.Errorf("bridge: unknown action %d", a)
	}
	cmd := signCommand(c.keySecret, c.localID, a, c.now().Unix())
	_, err := c.do(ctx, http.MethodGet, "/to_lock?command_signed_base64="+encodeCommand(cmd), nil, nil)
	return err
}
