package ingest

import (
	"context"
	"github.com/SnaptripUK/event-logger-go/event"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	os.Setenv("EVENT_LOGGER_DBNAME", "event-logger-test-db")
	os.Exit(m.Run())
}

func TestMongodb_Insert(t *testing.T) {
	mt := mtest.New(t)
	defer mt.Close()

	ctx := context.Background()
	ingestor, err := NewMongoDbIngestor()
	assert.NoError(t, err)

	mt.RunOpts("successful insert",
		mtest.NewOptions().ClientType(mtest.Mock), func(mt *mtest.T) {
			ingestor.collection = mt.Coll
			mt.AddMockResponses(mtest.CreateSuccessResponse())
			err := ingestor.Insert(ctx, []event.Event{{Cat: "test"}})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(mt.GetAllSucceededEvents()))
		})

	mt.RunOpts("noop if no events",
		mtest.NewOptions().ClientType(mtest.Mock), func(mt *mtest.T) {
			ingestor.collection = mt.Coll
			mt.AddMockResponses(mtest.CreateSuccessResponse())
			err := ingestor.Insert(ctx, []event.Event{})
			assert.NoError(t, err)
			assert.Equal(t, 0, len(mt.GetAllSucceededEvents()))
		})
}
