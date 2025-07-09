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

func init() {
	log.SetLevel(log.TraceLevel)
}

func main() {
	_, _, masterIP, _, nReduce, totalWorker := mapreduce.ParseArgs()

	baseServer := grpc.NewServer()
	ms := master.NewMaster(totalWorker, nReduce)
	proto_gen.RegisterMasterServer(baseServer, ms)

	lis, err := net.Listen("tcp", masterIP)

	if err != nil {
		log.Fatalf("Cannnot listen on ip: %v", masterIP)
	}

	// will be change later, just used for testing purpose
	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		baseServer.Serve(lis)
	}()

	log.Infof("[Master] Master gRPC server start on %v", masterIP)

	wg.Wait()

	// TODO: distributed work -> chia cac file thanh cac phan file nho de chia ra lam map task

	// TODO: distributed map task for worker (handle fault tolerance)

	// TODO: distributed reduce task for worker (handle fault tolerance)

	// TODO: after done, send signal worker to terminal (graceful shutdown)
}
