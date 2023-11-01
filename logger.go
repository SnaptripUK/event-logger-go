package eventlogger

import (
	"context"
	"github.com/SnaptripUK/event-logger-go/event"
	"github.com/SnaptripUK/event-logger-go/ingest"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// TODO: when we introduce more 'ingestors' this top-level config wont make much sense and will have to be moved to ingest package
// for now these values strictly apply to Mongo.
// we will also need to move options from top level to ingest so each ingestor can be configured separately.
const (
	defaultMaxBatchSize     = 100   // mongo bulk write limit
	defaultMaxChannelBuffer = 10000 // This is the maximum number of events that can be submitted without dropping any
	defaultFlushInterval    = 10 * time.Second
)

var pendingEvents int64

type eventLogger struct {
	eventChannel chan event.Event
	shutdown     chan struct{}
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	options
}
type options struct {
	eventDroppedCallback func(event event.Event)
	errorListener        func(msg string, err error)
	maxBatchSize         int
	maxChannelBuffer     int
	flushInterval        time.Duration
	ingestor             ingest.Ingestor
}
type Option func(options *options)

func WithBatchSize(batchSize int) Option {
	return func(options *options) {
		options.maxBatchSize = batchSize
	}
}

func WithFlushInterval(flushInterval time.Duration) Option {
	return func(options *options) {
		options.flushInterval = flushInterval
	}
}

func WithMaxChannelBuffer(maxChannelBuffer int) Option {
	return func(options *options) {
		options.maxChannelBuffer = maxChannelBuffer
	}
}

func WithEventDroppedCallback(callback func(event event.Event)) Option {
	return func(options *options) {
		options.eventDroppedCallback = callback
	}
}

func WithErrorListener(errorListener func(msg string, err error)) Option {
	return func(options *options) {
		options.errorListener = errorListener
	}
}

func WithIngestor(ingestor ingest.Ingestor) Option {
	return func(options *options) {
		options.ingestor = ingestor
	}
}

var (
	instance            *eventLogger
	once                sync.Once
	mu                  sync.Mutex // used to lock around instance recreation checks
	defaultDropCallback = func(event event.Event) {
		log.Printf("event-logger-go: Event channel is full. Event dropped: %#v\n", event)
	}
	defaultErrorListener = func(msg string, err error) {
		log.Printf("event-logger-go: error: %s: %s\n", msg, err)
	}
)

func Init(opts ...Option) {
	mu.Lock()
	defer mu.Unlock()

	// If the instance already exists and is running, don't create a new one.
	if instance != nil && instance.ctx.Err() == nil {
		return
	}

	// Allow re-initialization if instance has been stopped previously
	once = sync.Once{} // Resetting the Once instance
	once.Do(func() {
		initLogger(opts...)
	})
}

// SubmitEvent allows the submission of a new event to the logger.
func SubmitEvent(event event.Event) {
	select {
	case instance.eventChannel <- event:
		atomic.AddInt64(&pendingEvents, 1)
	default:
		// log & drop
		instance.eventDroppedCallback(event)
	}
}

// Close would be the method to properly shut everything down.
// After closing, you would be able to call Init again to create a new logger.
func Close() {
	mu.Lock()
	defer mu.Unlock()

	if instance != nil {
		instance.stop()
		instance = nil
	}
}

// internal initialization of eventLogger instance
func initLogger(opts ...Option) {

	optsApplied := options{
		eventDroppedCallback: defaultDropCallback,
		errorListener:        defaultErrorListener,
		maxBatchSize:         defaultMaxBatchSize,
		maxChannelBuffer:     defaultMaxChannelBuffer,
		flushInterval:        defaultFlushInterval,
	}
	for _, opt := range opts {
		opt(&optsApplied)
	}
	if optsApplied.ingestor == nil {
		ingestor, err := ingest.NewMongoDB()
		if err != nil {
			log.Fatal(err)
		}
		optsApplied.ingestor = ingestor
	}
	instance = &eventLogger{
		eventChannel: make(chan event.Event, optsApplied.maxChannelBuffer),
		shutdown:     make(chan struct{}),
		options:      optsApplied,
	}
	log.Printf("event-logger-go: Starting event logger with options: %#v\n", optsApplied)
	instance.ctx, instance.cancel = context.WithCancel(context.Background())
	// Start the batch sender
	instance.wg.Add(1)
	go instance.batchSender(instance.ctx)

	// Set up signal catching
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Listen for the termination signals
	go func() {
		<-signals
		instance.stop()
	}()
}

func (el *eventLogger) stop() {
	// Signal the shutdown
	close(el.shutdown)
	// Cancel the context
	el.cancel()
	// Wait for the batch sender to finish
	el.wg.Wait()
}

func (el *eventLogger) batchSender(ctx context.Context) {
	defer el.wg.Done()
	batch := make([]event.Event, 0, el.maxBatchSize)
	ticker := time.NewTicker(el.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-el.shutdown:
			if len(batch) > 0 {
				el.recordBatch(ctx, batch) // Send remaining events before shutting down
			}
			return
		case event := <-el.eventChannel:
			atomic.AddInt64(&pendingEvents, -1)
			batch = append(batch, event)
			if len(batch) == el.maxBatchSize {
				el.recordBatch(ctx, batch)
				batch = batch[:0] // Clear the batch
			}
		case <-ticker.C:
			if len(batch) > 0 {
				el.recordBatch(ctx, batch) // Periodic flushing
				batch = batch[:0]
			}
		}
	}
}

func (el *eventLogger) recordBatch(ctx context.Context, batch []event.Event) {

	err := el.ingestor.Insert(ctx, batch)
	if err != nil {
		el.errorListener("Error sending batch to TD", err)
		return
	}
}
