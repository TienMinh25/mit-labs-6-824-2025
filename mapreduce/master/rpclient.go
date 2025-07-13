package master

import (
	"context"
	"time"

	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type IWorkerRpcClient interface {
	Connect(workerIP string) (*grpc.ClientConn, proto_gen.WorkerClient)
	CheckHealth(workerIP string) WorkerStatus
	AssignMapTask(data *proto_gen.AssignMapTaskReq, workerIP string) bool
}

type workerRpcClient struct {
}

func NewWorkerRpcClient() IWorkerRpcClient {
	return &workerRpcClient{}
}

// Connect implements IWorkerRpcClient.
func (client *workerRpcClient) Connect(workerIP string) (*grpc.ClientConn, proto_gen.WorkerClient) {
	clientConn, err := grpc.NewClient(workerIP, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Warn(err)
		return nil, nil
	}

	return clientConn, proto_gen.NewWorkerClient(clientConn)
}

// CheckHealth implements IWorkerRpcClient.
func (client *workerRpcClient) CheckHealth(workerIP string) WorkerStatus {
	log.Tracef("[Master] Start check health worker ip: %v", workerIP)
	conn, c := client.Connect(workerIP)

	if conn == nil {
		return WORKER_UNKNOWN
	}

	defer conn.Close()

	// retry mechanism
	for retry := 1; retry <= 3; retry++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		res, err := c.Health(ctx, &proto_gen.Empty{})
		cancel()

		if err == nil {
			return WorkerStatus(res.Status)
		}

		// delay before every retry
		if retry <= 2 {
			time.Sleep(time.Millisecond * 500)
		}
	}

	log.Tracef("[Master] End check health worker ip: %v, worker status: %#v", workerIP, WORKER_UNKNOWN)

	return WORKER_UNKNOWN
}

func (client *workerRpcClient) AssignMapTask(data *proto_gen.AssignMapTaskReq, workerIP string) bool {
	log.Tracef("[Master] Start assigned map task for worker: %v", workerIP)

	conn, c := client.Connect(workerIP)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	r, err := c.AssignMapTask(ctx, data)

	if err != nil {
		status, _ := status.FromError(err)

		log.Warn("[Master]: " + status.Message())

		return false
	}

	return r.Result
}
