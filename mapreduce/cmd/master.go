package main

import (
	"net"

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
	files, _, masterIP, _, nReduce, totalWorker := mapreduce.ParseArgs()

	baseServer := grpc.NewServer()
	ms := master.NewMaster(totalWorker, nReduce)
	proto_gen.RegisterMasterServer(baseServer, ms)

	lis, err := net.Listen("tcp", masterIP)

	if err != nil {
		log.Fatalf("Cannnot listen on ip: %v", masterIP)
	}

	go baseServer.Serve(lis)

	log.Infof("[Master] Master gRPC server start on %v", masterIP)

	masterStruct := ms.(*master.Master)

	// TODO: wait enough worker registers =)) because we need to distribute workload across all workers
	masterStruct.WaitForEnoughWorker()

	// TODO: need some health check right here
	go masterStruct.CheckPeriodHealth()

	// TODO: distributed work -> chia cac file thanh cac phan file nho de chia ra lam map task
	masterStruct.DistributeWorkload(files)

	// TODO: distributed map task for worker (handle fault tolerance)
	masterStruct.DistributeMapTask()

	// TODO: distributed reduce task for worker (handle fault tolerance)
	masterStruct.DistributeReduceTask()

	// TODO: after done, send signal worker to terminal (graceful shutdown)
	masterStruct.EndChan <- true
	masterStruct.EndWorker()

	baseServer.GracefulStop()
}
