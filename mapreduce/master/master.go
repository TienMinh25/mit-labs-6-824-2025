package master

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
)

// The master needs to manage:
// - Worker info 			(worker's health, worker's status, IP of worker)
// - Map task 				(file execute, which worker is doing task, status of task)
// - Reduce task 			(where is file execute [on where worker?, filename?], status of task)
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
		ReduceTasks:    NewReduceTasks(nReduceTask),
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
		// return for worker, id is used to gen file out for intermediate files from phase map
		// and final files from phase reduce
		Id: int64(m.nCurrentWorker - 1),
	}, nil
}

// UpdateIMDFiles implements proto_gen.MasterServer.
func (m *Master) UpdateIMDFiles(ctx context.Context, data *proto_gen.UpdateIMDFilesReq) (*proto_gen.UpdateResult, error) {
	if m.WorkerInfo[data.Id].IsBroken() {
		return &proto_gen.UpdateResult{
			Result: false,
		}, nil
	}

	workerIP := m.serviceDiscovery(data.Uuid)

	for _, filename := range data.Filenames {
		splitReduceName := strings.Split(filename, "-")
		splitTaskID, err := strconv.Atoi(splitReduceName[len(splitReduceName)-1])

		if err != nil {
			log.Warn("File name contains reduce task id is not integer type")
			return nil, err
		}

		m.ReduceTasks[splitTaskID].Files = append(m.ReduceTasks[splitTaskID].Files, IMDFileInfo{
			FileName: filename,
			WorkerIP: workerIP,
		})
	}

	return &proto_gen.UpdateResult{
		Result: true,
	}, nil
}

func (m *Master) WaitForEnoughWorker() {
	log.Trace("[Master] Wait for enough workers")
	nCurrentWorkers, nTotalWorkers := m.getWorkerNum()
	for nCurrentWorkers < nTotalWorkers {
		nCurrentWorkers, nTotalWorkers = m.getWorkerNum()
	}
	log.Trace("[Master] Enough workers!")
}

func (m *Master) getWorkerNum() (int, int) {
	m.mutex.Lock()
	nWorkers := m.nCurrentWorker
	nTotalWorkers := m.nTotalWorker
	m.mutex.Unlock()
	return nWorkers, nTotalWorkers
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
	ticker := time.NewTicker(20 * time.Second)
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
				log.Printf("Health from worker %v is %v", workerInfo.WorkerIP, status)
				workerInfo.WorkerStatus = status
				workerInfo.mutex.Unlock()
			}
		}
	}
}

func (m *Master) DistributeMapTask() {
	log.Trace("[Master] Start assign map task to worker")

	mapTaskFailed := make(chan *MapTaskInfo, 100)

	totalWorkerAvailable, workers := m.getAvailableWorkers(m.nTotalWorker)
	workerID := 0
	countDoneMapTasks := 0

	done := make(chan int, 100)
	defer close(done)

	for idx := range m.MapTasks {
		m.MapTasks[idx].State = TASK_IN_PROGRESS

		go func(data *MapTaskInfo, workerID int) {
			msg := data.ToGRPCMsg()
			workers[workerID].UpdateStatus(WORKER_BUSY)
			isDone := m.workerClient.AssignMapTask(msg, workers[workerID].WorkerIP)

			if isDone {
				data.State = TASK_COMPLETED
				workers[workerID].UpdateStatus(WORKER_IDLE)
				done <- 1
			} else {
				workers[workerID].UpdateStatus(WORKER_UNKNOWN)
				data.State = TASK_TO_DO
				mapTaskFailed <- &m.MapTasks[idx]
			}

		}(&m.MapTasks[idx], workerID)

		workerID = (workerID + 1) % totalWorkerAvailable
	}

Loop:
	for {
		select {
		case mapTaskRetry, more := <-mapTaskFailed:
			if !more {
				break Loop
			}

			log.Infof("[Master] Re-execute Map Task %v", mapTaskRetry.UUID)

			_, workers := m.getAvailableWorkers(1)
			mapTaskRetry.State = TASK_IN_PROGRESS

			go func(data *MapTaskInfo) {
				msg := data.ToGRPCMsg()
				isDone := m.workerClient.AssignMapTask(msg, workers[0].WorkerIP)

				if !isDone {
					workers[0].UpdateStatus(WORKER_UNKNOWN)
					data.State = TASK_TO_DO
					mapTaskFailed <- data
				} else {
					data.State = TASK_COMPLETED
					workers[0].UpdateStatus(WORKER_IDLE)
					done <- 1
				}
			}(mapTaskRetry)
		case <-done:
			countDoneMapTasks++
			if countDoneMapTasks == len(m.MapTasks) {
				close(mapTaskFailed)
			}
		}
	}

	log.Trace("[Master]: End distributed map task and process map task!")
}

func (m *Master) DistributeReduceTask() {
	log.Trace("[Master] Start assign reduce task to worker")

	reduceTaskFailed := make(chan *ReduceTaskInfo, 100)
	totalWorkerAvailable, workers := m.getAvailableWorkers(m.nTotalWorker)
	workerID := 0

	done := make(chan int, 100)
	defer close(done)
	countDoneReduceTasks := 0

	for idx := range m.ReduceTasks {
		// set status for task
		m.ReduceTasks[idx].State = TASK_IN_PROGRESS

		go func(data *ReduceTaskInfo, workerID int) {
			workers[workerID].UpdateStatus(WORKER_BUSY)

			isDone := m.workerClient.AssignReduceTask(&proto_gen.AssignReduceTaskReq{
				FileInfo: data.ToGRPCMsg(),
			}, workers[workerID].WorkerIP)

			if isDone {
				workers[workerID].UpdateStatus(WORKER_IDLE)
				data.State = TASK_COMPLETED
				done <- 1
			} else {
				workers[workerID].UpdateStatus(WORKER_UNKNOWN)
				data.State = TASK_TO_DO
				reduceTaskFailed <- data
			}

		}(&m.ReduceTasks[idx], workerID)

		workerID = (workerID + 1) % totalWorkerAvailable
	}

Loop:
	for {
		select {
		case reduceTaskRetry, more := <-reduceTaskFailed:
			if !more {
				break Loop
			}

			log.Infof("[Master] Re-execute Reduce Task, files: [%v]", reduceTaskRetry.Files)

			reduceTaskRetry.State = TASK_IN_PROGRESS
			_, workers := m.getAvailableWorkers(1)
			workers[0].UpdateStatus(WORKER_BUSY)

			isDone := m.workerClient.AssignReduceTask(&proto_gen.AssignReduceTaskReq{
				FileInfo: reduceTaskRetry.ToGRPCMsg(),
			}, workers[0].WorkerIP)

			if isDone {
				reduceTaskRetry.State = TASK_COMPLETED
				workers[0].UpdateStatus(WORKER_IDLE)
				done <- 1
			} else {
				reduceTaskRetry.State = TASK_IN_PROGRESS
				workers[0].UpdateStatus(WORKER_UNKNOWN)
				reduceTaskFailed <- reduceTaskRetry
			}
		case <-done:
			countDoneReduceTasks++

			if countDoneReduceTasks == m.nReduceTask {
				close(reduceTaskFailed)
			}
		}
	}

	log.Trace("[Master]: End distributed reduce task and process reduce task!")
}

func (m *Master) getAvailableWorkers(numWorkers int) (int, []*WorkerInfo) {
	log.Infof("[Master] Finding available workers to execute %v / %v", numWorkers, len(m.WorkerInfo))

	res := make([]*WorkerInfo, 0)

OuterLoop:
	for {
		total := 0
		broken := 0

		for _, worker := range m.WorkerInfo {
			if worker.IsAvailable() {
				total += 1
				res = append(res, worker)
			} else if worker.IsBroken() {
				log.Infof("Worker ip: %v, status: %v", worker.WorkerIP, worker.WorkerStatus)
				broken += 1
			}

			if total >= numWorkers {
				log.Println("[Master] Finding enough available workers to execute!")
				break OuterLoop
			}
		}

		if broken == len(m.WorkerInfo) {
			log.Warn("Not enough worker to execute!")
		}

		log.Infof("[Master] Only %d/%d workers available, waiting...", total, numWorkers)
		time.Sleep(2 * time.Second)
	}

	return len(res), res
}

func (m *Master) serviceDiscovery(uuid string) string {
	res := ""

	for _, worker := range m.WorkerInfo {
		if worker.UUID == uuid {
			res = worker.WorkerIP
			break
		}
	}

	return res
}

func (m *Master) EndWorker(baseServer *grpc.Server) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, workerInfo := range m.WorkerInfo {
		if isShutDown := m.workerClient.End(workerInfo.WorkerIP); !isShutDown {
			log.Warnf("Worker %v shutdown failed", workerInfo.WorkerIP)
		}

		log.Infof("Worker %v shutdown successfully", workerInfo.WorkerIP)
	}

	baseServer.Stop()
}
