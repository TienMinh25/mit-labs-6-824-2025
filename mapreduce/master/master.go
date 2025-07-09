package master

import (
	"context"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
)

func init() {
	log.SetLevel(log.TraceLevel)
}

// The master needs to manage:
// - Worker info 			(worker's health, worker's status, IP of worker)
// - Map task 				(file execute, which worker is doing task, status of task)
// - Reduce task 			(where is file execute [on which worker?, filename?], status of task)
// Besides that, master need have some attributes like:
//   - nReduceTask 			(number of reduce tasks)
//   - nCurrentWorker 	(number of worker is registering master)
//   - nTotalWorker 		(to know exactly what is total worker machine?)
//   - mutex 						(because when have multiple machine worker concurrency and make request to master, we
//     need one mutex lock to lock the data structure of Master -> to have safe thread and
//     avoid data race)
type Master struct {
	nCurrentWorker int
	nTotalWorker   int
	nReduceTask    int
	WorkerInfo     []*WorkerInfo
	ReduceTasks    []*ReduceTask
	MapTasks       []*MapTask
	mutex          sync.Mutex
	proto_gen.UnimplementedMasterServer
}

// TODO: because master will be need to implemented gRPC server, so now just initialize master
// like this
func NewMaster(nTotalWorker int, nReduceTask int) proto_gen.MasterServer {
	return &Master{
		nCurrentWorker: 0,
		nTotalWorker:   nTotalWorker,
		nReduceTask:    nReduceTask,
		WorkerInfo:     []*WorkerInfo{},
	}
}

// RegisterWorker implements proto_gen.MasterServer.
func (m *Master) RegisterWorker(ctx context.Context, data *proto_gen.RegisterWorkerReq) (*proto_gen.RegisterWorkerRes, error) {
	log.Infof("[Worker IP: %v, Worker Identifier: %v] is registering", data.WorkerIp, data.Uuid)

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.WorkerInfo = append(m.WorkerInfo, NewWorkerInfo(data.WorkerIp, data.Uuid))
	m.nCurrentWorker++

	return &proto_gen.RegisterWorkerRes{
		IsSuccess: true,
		// return for worker, id is used to gen file out for immediately files from phase map
		// and final files from phase reduce
		Id:        int64(m.nCurrentWorker - 1),
	}, nil
}
