package master

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
)

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
	ReduceTasks    []ReduceTaskInfo
	MapTasks       []MapTaskInfo
	mutex          sync.Mutex
	EndChan        chan bool
	workerClient   IWorkerRpcClient
	proto_gen.UnimplementedMasterServer
}

func NewMaster(nTotalWorker int, nReduceTask int) proto_gen.MasterServer {
	return &Master{
		nCurrentWorker: 0,
		nTotalWorker:   nTotalWorker,
		nReduceTask:    nReduceTask,
		WorkerInfo:     []*WorkerInfo{},
		workerClient:   NewWorkerRpcClient(),
		EndChan:        make(chan bool),
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
		Id: int64(m.nCurrentWorker - 1),
	}, nil
}

func (m *Master) WaitForEnoughWorker() {
	log.Trace("[Master] Wait for enough workers")
	for m.nCurrentWorker < m.nTotalWorker {
		// loop wait
	}
	log.Trace("[Master] Enough workers!")
}

func (m *Master) DistributeWorkload(inputFiles []string) {
	log.Trace("[Master] Start distribute workload")

	m.MapTasks = NewMapTasks(m.nTotalWorker)

	for _, file := range inputFiles {
		totalLine := mapreduce.LineNums(file)

		baseWorkload := totalLine / m.nTotalWorker
		from := 0

		for idx := 0; idx < m.nTotalWorker; idx++ {
			workload := baseWorkload

			if idx < totalLine%m.nTotalWorker {
				workload++
			}

			m.MapTasks[idx].Files = append(m.MapTasks[idx].Files, FileInfo{
				FileName: file,
				From:     from,
				To:       from + workload,
			})

			from += workload
		}
	}

	log.Trace("[Master] End distribute workload")
}

func (m *Master) CheckPeriodHealth() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.EndChan:
			log.Info("[Master] Shutdown background job check health")
			return
		case <-ticker.C:
			for _, workerInfo := range m.WorkerInfo {
				workerInfo.mutex.Lock()
				status := m.workerClient.CheckHealth(workerInfo.WorkerIP)
				workerInfo.WorkerStatus = status
				workerInfo.mutex.Unlock()
			}
		}
	}
}

func (m *Master) DistributeMapTask() {
	log.Trace("[Master] Start assign map task to worker")

	// y tuong la assign task cho worker, neu worker ko reply -> cu cho la no die va assign lai task cho worker khac
	// cu lap di lap lai nhu vay cho den khi map task hoan thanh
	

	// muon co the assign task cho worker khac -> can biet worker nao available -> tuc la worker nao dang ranh de co the assign
	// thong tin ve task duoc assign cho worker can phai duoc luu lai
	// thong tin ve worker nao fail co the duoc luu lai -> or maybe luu lai channel cho map task cai nao fail chu ko can
	// luu lai thong tin ve worker nao fail -> co 2 huong
	// tuy nhien vi 

	// can 1 cai bucket nao day de luu tru lai cac cai map task failed va reassign lai cho worker khac
	for idx := range m.MapTasks {
		m.MapTasks[idx].State = TASK_IN_PROGRESS

	}
	
}