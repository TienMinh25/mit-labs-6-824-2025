package worker

import (
	"context"
	"time"

	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type IRpcClient interface {
	RegisterWorker(data *proto_gen.RegisterWorkerReq) int
}

type masterClient struct {
	client proto_gen.MasterClient
	conn   *grpc.ClientConn
}

func NewRPCMasterClient(masterIP, UUID, workerIP string) IRpcClient {
	clientConn, err := grpc.NewClient(masterIP, grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Init master client for worker [worker ip: %v, worker identifider: %v]\n", UUID, workerIP)
	}

	return &masterClient{
		client: proto_gen.NewMasterClient(clientConn),
		conn:   clientConn,
	}
}

// RegisterWorker implements IRpcClient.
func (m *masterClient) RegisterWorker(data *proto_gen.RegisterWorkerReq) int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	log.Trace("With time out")
	defer cancel()

	log.Tracef("Worker ip: %v is starting to register master", data.WorkerIp)

	res, err := m.client.RegisterWorker(ctx, data)

	log.Tracef("Worker ip: %v end", data.WorkerIp)
	if err != nil {
		respErr, ok := status.FromError(err)
		// used to retry??
		if ok {
			log.Panic(respErr.Message())
		} else {
			log.Panic(err)
		}
	}

	if !res.IsSuccess {
		panic("Register Error")
	}

	return int(res.Id)
}
