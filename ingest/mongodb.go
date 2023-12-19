package ingest

import (
	"context"
	"fmt"
	"github.com/SnaptripUK/event-logger-go/event"
	"go.mongodb.org/mongo-driver/bson"
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
	envNameMongodbUri  = "EVENT_LOGGER_MONOGODB_URI"
	envNameDbName      = "EVENT_LOGGER_DBNAME"
	mongoUri           = "mongodb://localhost:27017"
	collectionName     = "events"
	sevenDaysInSeconds = int32(7 * 24 * 60 * 60)
)

var writeConcern = writeconcern.New(writeconcern.W(0), writeconcern.J(false))

type MongodbIngestor struct {
	client     *mongo.Client
	collection *mongo.Collection
}

func NewMongoDbIngestor() (*MongodbIngestor, error) {
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
	var collection *mongo.Collection
	if dbName == "" {
		return nil,
			fmt.Errorf("%s env variable unspecified. Event logging disabled", envNameDbName)
	}
	collection = mongoClient.Database(dbName).Collection(collectionName)
	_, err = collection.Indexes().CreateOne(ctx,
		mongo.IndexModel{
			Keys:    bson.D{{Key: "createdat", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(sevenDaysInSeconds),
		},
		options.CreateIndexes().SetMaxTime(1*time.Second))
	if err != nil {
		return nil, err
	}
	return &MongodbIngestor{
		client:     mongoClient,
		collection: collection,
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

func (mdb *MongodbIngestor) Insert(ctx context.Context, events []event.Event) (err error) {
	if len(events) == 0 {
		return nil
	}
	collection := mdb.collection
	if collection == nil {
		return fmt.Errorf("collection nil on Insert")
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

	log.Printf("event-logger-go: Bulk inserted %d events\n",
		len(events))
	return
}
