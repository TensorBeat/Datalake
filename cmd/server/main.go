package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/TensorBeat/Datalake/internal/controller"
	"github.com/TensorBeat/Datalake/internal/repository"
	"github.com/TensorBeat/Datalake/internal/util"
	"github.com/TensorBeat/Datalake/pkg/proto"
	"github.com/joho/godotenv"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {

	logger := util.MakeLogger()

	//Dotenv
	err := godotenv.Load(".env") // .env in base directory
	if err != nil {
		logger.Warnf("No .env loaded: %v", err)
	}

	ListenAddress := ":" + os.Getenv("PORT")
	MongoURI := os.Getenv("MONGO_URI")
	IsProduction := os.Getenv("ENVIRONMENT") == "prod"

	ctx := context.Background()

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

	var dbName string
	if IsProduction {
		dbName = "prod"
	} else {
		dbName = "test"
	}
	repository := repository.NewMongoRepository(mongoClient, logger, dbName)

	listener, err := net.Listen("tcp", ListenAddress)
	if err != nil {
		logger.Fatalf("Unable to listen on %v: %v", ListenAddress, err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()

	datalakeService := controller.NewDatalakeServiceServer(repository, logger)
	proto.RegisterDatalakeServiceServer(grpcServer, datalakeService)
	reflection.Register(grpcServer)

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
	// res, err := datalakeService.GetSongs(ctx, &proto.GetSongsRequest{})
	// logger.Infof("%v", res)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			logger.Fatalf("Failed to serve: %v", err)
		}
	}()

	logger.Infof("Server succesfully started on %v", ListenAddress)

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)
	<-c

}
