package worker

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type IWorkerClient interface {
	Connect(ip string) (proto_gen.WorkerClient, *grpc.ClientConn)
	ReadIDMFile(data *proto_gen.GetIMDFileReq, workerIP string) []types.KeyValue
}

type workerClient struct {
}

func NewRPCWorkerClient() IWorkerClient {
	return &workerClient{}
}

// Connect implements IWorkerClient.
func (w *workerClient) Connect(ip string) (proto_gen.WorkerClient, *grpc.ClientConn) {
	clientConn, err := grpc.NewClient(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("Connect worker [ip: %v] failed!\n", ip)
	}

	return proto_gen.NewWorkerClient(clientConn), clientConn
}

// ReadIDMFiles implements IWorkerClient.
func (w *workerClient) ReadIDMFile(data *proto_gen.GetIMDFileReq, workerIP string) []types.KeyValue {
	log.Tracef("[Worker] Read intermediate files from worker ip: %v", workerIP)

	client, conn := w.Connect(workerIP)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := client.GetIMDFile(ctx, data)

	if err != nil {
		log.Warnf("Get immediate files failed, err: %v", err)
		return nil
	}

	kvs := make([]types.KeyValue, 0)

	for _, imdKV := range result.KeyValues {
		kvs = append(kvs, types.KeyValue{
			Key:   imdKV.Key,
			Value: imdKV.Value,
		})
	}

	return kvs
}
