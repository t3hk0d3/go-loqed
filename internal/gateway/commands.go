package gateway

import "context"

// Placeholder until Task 11 replaces this file with command handling.

func (s *Supervisor) onCommand(_ context.Context, m CommandMsg) {
	s.log.Warn("lock commands are not implemented yet", "command", m.Command)
}
