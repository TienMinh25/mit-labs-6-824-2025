package main

import (
	"net"
	"plugin"
	"time"

	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/types"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/worker"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func init() {
	log.SetLevel(log.TraceLevel)
}

func main() {
	_, pluginFile, masterIP, workerIP, nReduce, _ := mapreduce.ParseArgs()

	lis, err := net.Listen("tcp", workerIP)

	if err != nil {
		log.Fatalf("Worker [IP: %v] cannot server gRPC server", workerIP)
	}

	ws := worker.NewWorker(nReduce, masterIP)
	baseServer := grpc.NewServer()

	proto_gen.RegisterWorkerServer(baseServer, ws)
	// run new thread right here to serve gRPC
	go baseServer.Serve(lis)
	log.Infof("[Worker] Worker gRPC server start on %v", workerIP)

	workerStruct := ws.(*worker.Worker)

	// assign map and reduce plugin to worker
	workerStruct.Mapf, workerStruct.Reducef = loadPlugin(pluginFile)

	// register worker with master
	workerID, err := workerStruct.MasterClient.RegisterWorker(&proto_gen.RegisterWorkerReq{
		WorkerIp: workerIP,
		Uuid:     workerStruct.UUID,
	}, masterIP)

	if err != nil {
		baseServer.GracefulStop()
		log.Fatalf("Register worker with master failed with reason: %v", err.Error())
	}

	workerStruct.ID = workerID

	// used for signal terminal worker from master
	<-workerStruct.ChanEnd

	// Sleep for a while for waiting the End Grpc response sent to master
	time.Sleep(500 * time.Millisecond)
	baseServer.GracefulStop()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []types.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v, err msg: %v", filename, err.Error())
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []types.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
