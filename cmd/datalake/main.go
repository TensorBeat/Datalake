package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"

	"github.com/TensorBeat/Datalake/internal/controller"
	"github.com/TensorBeat/Datalake/pkg/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {

	portPtr := flag.Int("port", 3912, "port to run grpc service")
	flag.Parse()
	listenAddress := ":" + strconv.Itoa(*portPtr)

	logger, err := zap.NewProduction()

	if err != nil {
		log.Fatalf("Couldn't start zap logger: %v", err)
	}

	defer logger.Sync() // flushes buffer, if any
	sugar := logger.Sugar()

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
