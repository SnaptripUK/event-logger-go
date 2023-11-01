package ingest

import (
	"context"
	"github.com/SnaptripUK/event-logger-go/event"
)

type Ingestor interface {
	Insert(ctx context.Context, events []event.Event) (err error)
}
