package eventlogger

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"snaptrip.com/event-logger-go/event"
	"testing"
	"time"
)

type MockIngestor struct {
	InsertFunc func(ctx context.Context, events []event.Event) error
}

func (mi *MockIngestor) Insert(ctx context.Context, events []event.Event) error {
	return mi.InsertFunc(ctx, events)
}

func TestMain(m *testing.M) {
	fmt.Println("Setting up test suite...")
	c := m.Run()
	fmt.Println("Tearing down test suite...")
	os.Exit(c)
}

func TestEventLogger_SubmitAndFlush(t *testing.T) {
	mockIngestor := &MockIngestor{
		InsertFunc: func(ctx context.Context, events []event.Event) error {
			assert.Equal(t, 1, len(events), "Expected 1 event to be ingested")
			assert.Equal(t, "test_event", events[0].Name, "Expected event name to be 'test_event'")
			return nil
		},
	}
	testFlushInterval := 1 * time.Second
	Init(
		WithIngestor(mockIngestor),
		WithFlushInterval(testFlushInterval),
	)

	testEvent := event.Event{
		CreatedAt: time.Now(),
		Name:      "test_event",
		Attrs:     map[string]interface{}{"key1": "value1", "key2": "value2"},
	}
	SubmitEvent(testEvent)

	// Wait to ensure the flush interval has passed, allowing events to be ingested
	time.Sleep(testFlushInterval + (500 * time.Millisecond)) // adding a bit of delay beyond the flush interval
	Close()
}

// Test the event dropping when the channel is full
func TestEventLogger_EventDropping(t *testing.T) {
	eventsDropped := 0
	mockSlowIngestor := &MockIngestor{
		InsertFunc: func(ctx context.Context, events []event.Event) error {
			time.Sleep(1 * time.Second)
			return nil
		},
	}
	// Initialize with a small buffer and a slow ingestor to test dropping
	Init(
		WithIngestor(mockSlowIngestor),
		WithMaxChannelBuffer(1),
		WithBatchSize(1),
		WithEventDroppedCallback(func(event event.Event) {
			fmt.Printf("Event dropped: %v\n", event)
			eventsDropped++
		}),
	)

	// Try sending more events than the buffer can handle
	SubmitEvent(event.Event{Name: "test_event_1"}) //handled immediately, channel back to 1
	SubmitEvent(event.Event{Name: "test_event_2"}) //channel down to 0
	SubmitEvent(event.Event{Name: "test_event_3"}) //should get dropped

	assert.Equal(t, 1, eventsDropped, "Expected 1 event to be dropped")
	Close()
}

func TestEventLogger_IngestionError(t *testing.T) {
	errorOccurred := false
	mockIngestor := &MockIngestor{
		InsertFunc: func(ctx context.Context, events []event.Event) error {
			return errors.New("ingestion error")
		},
	}

	// Initialize with test configurations
	Init(
		WithFlushInterval(1*time.Second),
		WithIngestor(mockIngestor),
		WithErrorListener(func(msg string, err error) {
			errorOccurred = true
		}),
	)

	SubmitEvent(event.Event{Name: "test_event"})
	time.Sleep(2 * time.Second)
	assert.True(t, errorOccurred, "Expected error to be logged")
	Close()
}

func TestEventLoggerShutdown(t *testing.T) {
	mockIngestor := &MockIngestor{
		InsertFunc: func(ctx context.Context, events []event.Event) error {
			assert.ElementsMatch(t, []event.Event{{Name: "test_event"}}, events,
				"Expected 1 event to be ingested")
			return nil
		},
	}
	Init(
		WithFlushInterval(10*time.Second), //so it's long enough to not interfere with the test
		WithIngestor(mockIngestor),
	)

	SubmitEvent(event.Event{Name: "test_event"})

	// Perform the shutdown which should trigger the final flush
	Close()
}
