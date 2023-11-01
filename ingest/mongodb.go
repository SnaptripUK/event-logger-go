package ingest

import (
	"context"
	"fmt"
	"github.com/SnaptripUK/event-logger-go/event"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	envNameMongodbUri = "MONOGODB_EVENTDB_URI"
	envNameDbName     = "MONOGODB_EVENTDB_DBNAME"
	mongoUri          = "mongodb://localhost:27017"
	collectionPrefix  = "events_"
)

var writeConcern = writeconcern.New(writeconcern.W(0), writeconcern.J(false))

type Mongodb struct {
	client     *mongo.Client
	dbName     string
	collection *mongo.Collection
}

func NewMongoDB() (*Mongodb, error) {
	ctx := context.Background()
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	uri := os.Getenv(envNameMongodbUri)
	if uri == "" {
		uri = mongoUri
	}
	mongoClient, err := connectMongoDB(ctx, uri)
	if err != nil {
		return nil, err
	}
	go func() {
		<-stopChan
		log.Println("event-logger-go: Stopping MongoDB connection")
		_ = mongoClient.Disconnect(ctx)
	}()
	dbName := os.Getenv(envNameDbName)
	if dbName == "" {
		log.Fatal(envNameDbName + " env variable unspecified")
	}
	return &Mongodb{
		client: mongoClient,
		dbName: dbName,
	}, nil
}

func connectMongoDB(ctx context.Context, uri string) (*mongo.Client, error) {
	client, err := mongo.Connect(ctx, options.Client().
		ApplyURI(uri).
		SetWriteConcern(writeConcern))
	if err != nil {
		return nil, err
	}
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}
	log.Println("event-logger-go: Connected to MongoDB!")
	return client, nil
}

func (mdb *Mongodb) Insert(ctx context.Context, events []event.Event) (err error) {
	if len(events) == 0 {
		return nil
	}
	var collection *mongo.Collection
	if collection = mdb.collection; collection == nil {
		collectionName := collectionPrefix + time.Now().Format("2006_01_02")
		collection = mdb.client.
			Database(mdb.dbName).
			Collection(collectionName)
	}

	// Prepare the slice of write models for the bulk write operation.
	var writes []mongo.WriteModel
	for _, event := range events {
		model := mongo.NewInsertOneModel().SetDocument(event)
		writes = append(writes, model)
	}

	// Execute the bulk write operation
	_, err = collection.BulkWrite(ctx, writes)
	if err != nil {
		return
	}

	fmt.Printf("event-logger-go: Bulk inserted %d events\n",
		len(events))
	return
}
