package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/TensorBeat/Datalake/internal/util"
	"github.com/benweissmann/memongo"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
)

var mongoRepo *MongoRepository
var logger *zap.SugaredLogger
var ctx context.Context

func TestMain(m *testing.M) {

	logger = util.MakeLogger()
	mongoServer, err := memongo.Start("4.4.0")
	ctx = context.Background()

	if err != nil {
		logger.Fatalf("Couldn't start memongo: %v", err)
	}
	defer mongoServer.Stop()

	mongoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mongoClient, err := mongo.Connect(mongoCtx, options.Client().ApplyURI(mongoServer.URI()))
	if err != nil {
		logger.Fatalf("Couldn't connect to mongo: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	err = mongoClient.Ping(ctx, readpref.Primary())
	if err != nil {
		logger.Fatalf("Couldn't ping mongo: %v", err)
	}
	mongoRepo = NewMongoRepository(mongoClient, logger, memongo.RandomDatabase())

	os.Exit(m.Run())
}

func TestGetSongsByTags(t *testing.T) {

	addSongs := []*File{
		{
			Name: "SuperPop Song",
			Uri:  "gs://example.mp3",
		},
	}

	mongoRepo.AddSongs(ctx, addSongs)
}
