package main

import (
	"net"
	"sync"

	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/master"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
	"google.golang.org/grpc"

	log "github.com/sirupsen/logrus"
)

func main() {
	_, _, masterIP, nReduce, totalWorker := mapreduce.ParseArgs()

	baseServer := grpc.NewServer()
	ms := master.NewMaster(totalWorker, nReduce)
	proto_gen.RegisterMasterServer(baseServer, ms)

	lis, err := net.Listen("tcp", masterIP)

	if err != nil {
		log.Fatalf("Cannnot listen on ip: %v", masterIP)
	}

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		baseServer.Serve(lis)
	}()

	log.Info("[Master] Master gRPC server start")

	wg.Wait()
}
