package ingest

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"os"
	"snaptrip.com/event-logger-go/event"
	"testing"
)

func TestMain(m *testing.M) {
	os.Setenv("MONOGODB_EVENTDB_DBNAME", "mongodb://localhost:27017")
	os.Exit(m.Run())
}

func TestMongodb_Insert(t *testing.T) {
	mt := mtest.New(t)
	defer mt.Close()

	ctx := context.Background()
	ingestor, err := NewMongoDB()
	assert.NoError(t, err)

	mt.RunOpts("successful insert",
		mtest.NewOptions().ClientType(mtest.Mock), func(mt *mtest.T) {
			ingestor.collection = mt.Coll
			mt.AddMockResponses(mtest.CreateSuccessResponse())
			err := ingestor.Insert(ctx, []event.Event{{Name: "test"}})
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
