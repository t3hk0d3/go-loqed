package gateway

import (
	"context"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/model"
)

// Placeholder until Task 10 replaces this file with cloud and offline modes.

func (s *Supervisor) enterCloud(context.Context) {
	s.setMode(model.ModeCloud)
	s.publish()
}

func (s *Supervisor) tickCloud(context.Context, time.Time)   {}
func (s *Supervisor) tickOffline(context.Context, time.Time) {}

func (s *Supervisor) pollCloud(context.Context, Priority, time.Time) bool { return false }
