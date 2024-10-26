package nosql

import (
	"context"
	"log"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const mongoURI = "mongodb://admin:password@localhost:27017"

var (
	clientOnce sync.Once
)

type MongoDBClient struct {
	Client *mongo.Client
	dbName string
}

func NewMongoDBClient(dbName string) *MongoDBClient {
	return &MongoDBClient{
		dbName: dbName,
	}
}

// InitMongoClient initializes a MongoDB client (singleton).
func (m *MongoDBClient) InitMongoClient() error {
	var err error
	clientOnce.Do(func() {
		clientOptions := options.Client().ApplyURI(mongoURI)

		m.Client, err = mongo.Connect(context.TODO(), clientOptions)
		if err != nil {
			log.Fatalf("Failed to connect to MongoDB: %v", err)
		}

		// Ping MongoDB to ensure the connection is alive
		if err = m.Client.Ping(context.TODO(), nil); err != nil {
			log.Fatalf("Failed to ping MongoDB: %v", err)
		}
		log.Println("Connected to MongoDB successfully.")
	})
	return err
}

// GetCollection returns a MongoDB collection.
func (m *MongoDBClient) GetCollection(collectionName string) *mongo.Collection {
	switch collectionName {
	case "tracked_posts":
		return m.Client.Database(m.dbName).Collection("tracked_posts")
	case "posts":
		return m.Client.Database(m.dbName).Collection("posts")
	case "comments":
		return m.Client.Database(m.dbName).Collection("comments")
	default:
		return nil
	}
}

// DisconnectMongoClient disconnects the MongoDB client.
func (m *MongoDBClient) DisconnectMongoClient() {
	if m.Client != nil {
		if err := m.Client.Disconnect(context.TODO()); err != nil {
			log.Fatalf("Failed to close MongoDB connection: %v", err)
		}
		log.Println("Disconnected from MongoDB.")
	}
}

func (m *MongoDBClient) VerifyMongoClient(ctx context.Context) bool {
	if err := m.Client.Ping(ctx, nil); err != nil {
		return false
	}
	return true
}
