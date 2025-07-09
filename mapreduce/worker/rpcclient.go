package worker

import (
	"context"
	"time"

	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
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
	defer cancel()

	log.Tracef("Worker ip: %v is starting to register master", data.WorkerIp)

	res, err := m.client.RegisterWorker(ctx, data)

	log.Tracef("Worker ip: %v register end", data.WorkerIp)
	if err != nil {
		for retry := 1; retry <= 3; retry++ {
			res, err = m.client.RegisterWorker(ctx, data)

			if retry == 3 && err != nil {
				log.Fatalf("[Worker] Cannot register worker with master")
			}
		}
	}

	if !res.IsSuccess {
		panic("Register Error")
	}

	return int(res.Id)
}
