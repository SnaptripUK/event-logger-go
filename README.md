# event-logger-go
a simple framework for rapidly collecting events in our Go apps

a trival usage example:
```
package main

import (
	eventlogger "github.com/SnaptripUK/event-logger-go"
	"github.com/SnaptripUK/event-logger-go/event"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	os.Setenv("MONOGODB_EVENTDB_DBNAME", "td_ingestion")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	eventlogger.Init(eventlogger.WithBatchSize(100))

	for {
		time.Sleep(1 * time.Microsecond)
		select {
		case <-signals:
			return
		default:
			eventlogger.SubmitEvent(event.Event{
				Attrs: map[string]interface{}{
					"propertyId": rand.Int(),
					"checkin":    time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
					"checkout":   time.Date(2020, 1, 3, 0, 0, 0, 0, time.UTC),
					"guests": []map[string]interface{}{
						{
							"adults":   2,
							"children": 1,
						},
						{
							"adults":   2,
							"children": 0,
						},
					},
				},
			})
		}
	}
}

```
