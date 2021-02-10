package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/TensorBeat/Datalake/internal/controller"
	"github.com/TensorBeat/Datalake/pkg/proto"
	"github.com/joho/godotenv"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {

	//Setup Logging
	logger, err := zap.NewProduction()

	if err != nil {
		log.Fatalf("Couldn't start zap logger: %v", err)
	}

	defer logger.Sync() // flushes buffer, if any
	sugar := logger.Sugar()

	//Dotenv
	err = godotenv.Load("../../.env") // .env in base directory
	if err != nil {
		sugar.Warnf("No .env loaded: %v", err)
	}

	listenAddress := ":" + os.Getenv("DL_PORT")
	MongoURI := os.Getenv("MONGO_URI")

	ctx := context.Background()

	mongoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mongoClient, err := mongo.Connect(mongoCtx, options.Client().ApplyURI(MongoURI))
	if err != nil {
		sugar.Fatalf("Couldn't connect to mongo: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	err = mongoClient.Ping(ctx, readpref.Primary())
	if err != nil {
		sugar.Fatalf("Couldn't ping mongo: %v", err)
	}

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		sugar.Fatalf("Unable to listen on %v: %v", listenAddress, err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()

	datalakeService := &controller.DatalakeServiceServer{}
	proto.RegisterDatalakeServiceServer(grpcServer, datalakeService)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			sugar.Fatalf("Failed to serve: %v", err)
		}
	}()

	sugar.Infof("Server succesfully started on %v", listenAddress)

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)
	<-c

}
