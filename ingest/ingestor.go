package ingest

import (
	"context"
	"snaptrip.com/event-logger-go/event"
)

type Ingestor interface {
	Insert(ctx context.Context, events []event.Event) (err error)
}
