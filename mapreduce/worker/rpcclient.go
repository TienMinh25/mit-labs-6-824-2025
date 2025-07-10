package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type IRpcClient interface {
	RegisterWorker(data *proto_gen.RegisterWorkerReq) (int, error)
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
func (m *masterClient) RegisterWorker(data *proto_gen.RegisterWorkerReq) (int, error) {
	log.Tracef("Worker ip: %v is starting to register master", data.WorkerIp)

	var res *proto_gen.RegisterWorkerRes
	var err error

	for retry := 1; retry <= 3; retry++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err = m.client.RegisterWorker(ctx, data)
		cancel()

		// if retry success, stop retry
		if err == nil && res.IsSuccess {
			log.Tracef("Worker ip: %v register successful", data.WorkerIp)
			return int(res.Id), nil
		}

		log.Warnf("Worker ip: %v register attempt %d failed: %v", data.WorkerIp, retry, err)

		// delay before retry
		if retry <= 2 {
			time.Sleep(time.Millisecond * 500)
		}
	}

	log.Errorf("Worker ip: %v failed to register after 3 attempts", data.WorkerIp)
	log.Tracef("Worker ip: %v register end", data.WorkerIp)

	if err != nil {
		return 0, fmt.Errorf("register worker failed: %w", err)
	}

	return 0, err
}
