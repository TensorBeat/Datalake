package controller_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/TensorBeat/Datalake/internal/controller"
	"github.com/TensorBeat/Datalake/internal/repository"
	"github.com/TensorBeat/Datalake/internal/util"
	"github.com/TensorBeat/Datalake/pkg/proto"
	"github.com/joho/godotenv"
	"go.uber.org/zap/zaptest"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var datalakeService *controller.DatalakeServiceServer
var ctx context.Context

func TestMain(m *testing.M) {

	logger := util.MakeLogger()

	//Dotenv
	err := godotenv.Load("../../.env") // .env in base directory
	if err != nil {
		logger.Warnf("No .env loaded: %v", err)
	}

	MongoURI := os.Getenv("MONGO_URI")

	ctx = context.Background()

	mongoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mongoClient, err := mongo.Connect(mongoCtx, options.Client().ApplyURI(MongoURI))
	if err != nil {
		logger.Fatalf("Couldn't connect to mongo: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	err = mongoClient.Ping(ctx, readpref.Primary())
	if err != nil {
		logger.Fatalf("Couldn't ping mongo: %v", err)
	}

	dbName := "test"
	repository := repository.NewMongoRepository(mongoClient, logger, dbName)
	datalakeService = controller.NewDatalakeServiceServer(repository, logger)

	// TODO: Example to seed data - should be a unit-test at somepoint
	// datalakeService.AddSongs(ctx, &proto.AddSongsRequest{
	// 	Songs: []*proto.File{
	// 		{
	// 			Uri: "gs://test-tensorbeat-songs/song.mp3",
	// 			Metadata: map[string]string{
	// 				"genre": "unknown",
	// 			},
	// 		},
	// 	},
	// })

	// TODO: Example get data - should be a unit-test at somepoint

	os.Exit(m.Run())

}

func TestGetSongsByIDs(t *testing.T) {

	logger := zaptest.NewLogger(t).Sugar()

	res, _ := datalakeService.GetSongsByIDs(ctx, &proto.GetSongsByIDsRequest{Ids: []string{"602b29014accf1b3f3d462d0"}})
	logger.Infof("%v", res)

}

func TestGetSongsByTags(t *testing.T) {

	logger := zaptest.NewLogger(t).Sugar()

	req := &proto.GetSongsByTagsRequest{
		Tags: map[string]string{
			"genre": "Hip Hop",
		},
		Filter: proto.Filter_ALL,
	}

	res, _ := datalakeService.GetSongsByTags(ctx, req)
	logger.Infof("%v", res)

}

func TestAddTags(t *testing.T) {

	logger := zaptest.NewLogger(t).Sugar()

	req := &proto.AddTagsRequest{
		Id: "60330f9e6fdbdb246a93b7a6",
		Tags: map[string]string{
			"heavyness": "heavy",
		},
	}

	res, _ := datalakeService.AddTags(ctx, req)
	logger.Infof("%v", res)

}

func TestRemoveTags(t *testing.T) {

	logger := zaptest.NewLogger(t).Sugar()

	req := &proto.RemoveTagsRequest{
		Id: "60330f9e6fdbdb246a93b7a6",
		Tags: map[string]string{
			"heavyness": "",
		},
	}

	res, _ := datalakeService.RemoveTags(ctx, req)
	logger.Infof("%v", res)

}

func TestAddSongs(t *testing.T) {

	logger := zaptest.NewLogger(t).Sugar()

	req := &proto.AddSongsRequest{
		Songs: []*proto.AddFile{
			{
				Name:     "Rock Song",
				Uri:      "gs://test-tensorbeat-songs/song.mp3",
				MimeType: "audio/mpeg",
				Tags: map[string]string{
					"test": "test",
				},
			},
		},
	}

	res, _ := datalakeService.AddSongs(ctx, req)
	logger.Infof("%v", res)

}
