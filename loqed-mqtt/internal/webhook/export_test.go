package webhook

import "net/http"

// SeenCount reports how many applied bridge webhook deliveries h remembers
// for lockID.
func SeenCount(h http.Handler, lockID string) int {
	return h.(*server).seen.count(lockID)
}
