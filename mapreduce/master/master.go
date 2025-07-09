package master

import "sync"

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
	WorkerInfo     []WorkerInfo
	ReduceTasks    []ReduceTask
	MapTasks       []MapTask
	mutex          sync.Mutex
}

// TODO: because master will be need to implemented gRPC server, so now just initialize master
// like this
func NewMaster(nTotalWorker int, nReduceTask int) *Master {
	return &Master{
		nCurrentWorker: 0,
		nTotalWorker:   nTotalWorker,
		nReduceTask:    nReduceTask,
		WorkerInfo:     []WorkerInfo{},
	}
}
