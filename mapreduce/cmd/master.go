package main

import (
	"net"

	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/master"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
	"google.golang.org/grpc"

	log "github.com/sirupsen/logrus"
)

func main() {
	inputFiles, pluginFile, masterIP, nReduce, totalWorker := mapreduce.ParseArgs()

	baseServer := grpc.NewServer()
	ms := master.NewMaster(totalWorker, nReduce)
	proto_gen.RegisterMasterServer(baseServer, ms)

	lis, err := net.Listen("tcp", masterIP)

	if err != nil {
		log.Fatalf("Cannnot listen on ip: %v", masterIP)
	}

	go baseServer.Serve(lis)

	log.Info("[Master] Master gRPC server start")
}
