package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type IRpcClient interface {
	Connect(ip string) (proto_gen.MasterClient, *grpc.ClientConn)
	RegisterWorker(data *proto_gen.RegisterWorkerReq, masterIP string) (int, error)
	UpdateIDMFiles(data *proto_gen.UpdateIMDFilesReq, masterIP string) bool
}

type masterClient struct {
}

func NewRPCMasterClient() IRpcClient {
	return &masterClient{}
}

func (client *masterClient) Connect(ip string) (proto_gen.MasterClient, *grpc.ClientConn) {
	clientConn, err := grpc.NewClient(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("Connect server [ip: %v] failed!\n", ip)
	}

	return proto_gen.NewMasterClient(clientConn), clientConn
}

// RegisterWorker implements IRpcClient.
func (m *masterClient) RegisterWorker(data *proto_gen.RegisterWorkerReq, masterIP string) (int, error) {
	log.Tracef("Worker ip: %v is starting to register master", data.WorkerIp)

	client, conn := m.Connect(masterIP)
	defer conn.Close()

	var res *proto_gen.RegisterWorkerRes
	var err error

	for retry := 1; retry <= 3; retry++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err = client.RegisterWorker(ctx, data)
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

func (m *masterClient) UpdateIDMFiles(data *proto_gen.UpdateIMDFilesReq, masterIP string) bool {
	log.Tracef("Worker ip: %v is update intermediate files", data.Uuid)

	client, conn := m.Connect(masterIP)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result, err := client.UpdateIMDFiles(ctx, data)

	if err != nil {
		log.Fatalf("Update intermediate files failed, err: %v", err)
	}

	return result.Result
}
