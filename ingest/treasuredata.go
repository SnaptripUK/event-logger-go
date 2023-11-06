package ingest

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/SnaptripUK/event-logger-go/event"
	"github.com/google/uuid"
	"log"
	"net/http"
	"sync"
)

const (
	regionUS          region = "https://us01.records.in.treasuredata.com"
	ingestContentType        = "application/vnd.treasuredata.v1+json"
)

type region string
type eventCat string
type sampleRate int

type TdRecords struct {
	Records []event.Event `json:"records"`
}

type TdIngestor struct {
	region region
	tdIngestorOptions
}

type tdIngestorOptions struct {
	apiKey         string
	samplingConfig map[eventCat]sampleRate
	dbName         string
}
type TdIngestorOption func(options *tdIngestorOptions)

func NewTdIngestor(opts ...TdIngestorOption) (*TdIngestor, error) {
	optsApplied := tdIngestorOptions{
		samplingConfig: map[eventCat]sampleRate{},
	}
	for _, opt := range opts {
		opt(&optsApplied)
	}
	if optsApplied.apiKey == "" {
		return nil, fmt.Errorf("ingest.TdIngestor missing TD api key")
	}
	if optsApplied.dbName == "" {
		return nil, fmt.Errorf("ingest.TdIngestor missing TD db name")
	}

	return &TdIngestor{
		region:            regionUS,
		tdIngestorOptions: optsApplied,
	}, nil
}

func WithTdApiKey(apiKey string) TdIngestorOption {
	return func(options *tdIngestorOptions) {
		options.apiKey = apiKey
	}
}

func WithTdEvent(eventType eventCat, sampleRate sampleRate) TdIngestorOption {
	return func(options *tdIngestorOptions) {
		options.samplingConfig[eventType] = sampleRate
	}
}

func WithTdDbName(dbName string) TdIngestorOption {
	return func(options *tdIngestorOptions) {
		options.dbName = dbName
	}
}

func (td *TdIngestor) Insert(ctx context.Context, events []event.Event) (err error) {
	groups := map[eventCat][]event.Event{}
	for _, e := range events {
		cat := eventCat(e.Cat)
		if td.isSampled(cat, e.Tag) {
			groups[cat] = append(groups[cat], e)
		}
	}
	wg := sync.WaitGroup{}
	wg.Add(len(groups))
	for cat, events := range groups {
		go func(cat eventCat, events []event.Event) {
			defer wg.Done()
			err := td.doInsert(ctx, string(cat), events)
			if err != nil {
				log.Printf("ingest.TdIngestor: failed to insert events: %v", err)
			}
		}(cat, events)
	}
	wg.Wait()
	return err
}

func (td *TdIngestor) doInsert(ctx context.Context, table string, events []event.Event) (err error) {

	url := fmt.Sprintf("%s/%s/%s", td.region, td.dbName, table)
	payload, err := json.Marshal(TdRecords{
		Records: events,
	})
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "TD1 "+td.apiKey)
	req.Header.Set("Content-Type", ingestContentType)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close() // Ensure the response body is closed
	if resp.StatusCode != 200 {
		return fmt.Errorf("ingest.TdIngestor: response status: %s", resp.Status)
	}
	return
}

// isSampled returns true if the event should be sampled based on a uuid v4 tag
// it's a very simple context-less sampling that relies on the fact that digits in uuid v4 have similar distribution
// and the last digit is "random" enough
// TODO: move sampling up to the event logger
func (td *TdIngestor) isSampled(cat eventCat, tag string) bool {
	if _, err := uuid.Parse(tag); err != nil {
		return false
	}
	samplingRate := td.samplingConfig[cat]
	if samplingRate <= 0 {
		return false
	}
	if samplingRate >= 100 {
		return true
	}
	//two chars give resolution of 256
	//one wouldn't be enough as it would give only 16 possible values, 6.25% resolution
	lastTwoChars := tag[len(tag)-2:]
	value, err := hex.DecodeString(lastTwoChars)
	if err != nil {
		return false
	}

	// Calculate the threshold for sampling based on the rate
	// 256 possible values (byte), so we take the percentage of this
	threshold := (256 * samplingRate) / 100
	// If the numeric value is less than the threshold, sample this UUID
	return int(value[0]) < int(threshold)
}
