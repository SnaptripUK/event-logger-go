package ingest

import (
	"context"
	"github.com/SnaptripUK/event-logger-go/event"
)

type Noop struct {
}

func NewNoop() (*Noop, error) {
	return &Noop{}, nil
}

func (n *Noop) Insert(ctx context.Context, events []event.Event) (err error) {
	return nil
}
