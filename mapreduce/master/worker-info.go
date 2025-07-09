package master

import "sync"

type WorkerStatus int

const (
	WORKER_UNKNOWN WorkerStatus = iota
	WORKER_IDLE
	WORKER_BUSY
)

// WorkerInfo structure explain:
// - WorkerIP 			(used to connect the worker and assign map task or reduce task to it)
// - WorkerStatus 	(status of worker: busy, idle, unknown [broken])
// - UUID 					(identifier of worker machine)
// - mutex					(used to case concurrency -> because can have case that happen in the same time:
//   - Master check health of worker -> if worker die or network is corruption -> update status of worker is unknown
//   - Worker update status of task and also update status -> idle)
type WorkerInfo struct {
	WorkerIP     string
	WorkerStatus WorkerStatus
	UUID         string
	mutex        sync.Mutex
}

func NewWorkerInfo(WorkerIP string, UUID string) *WorkerInfo {
	return &WorkerInfo{
		WorkerIP:     WorkerIP,
		UUID:         UUID,
		WorkerStatus: WORKER_IDLE,
	}
}

func (w *WorkerInfo) GetIP() string {
	w.mutex.Lock()
	workerIP := w.WorkerIP
	w.mutex.Unlock()
	return workerIP
}

func (w *WorkerInfo) UpdateStatus(status WorkerStatus) {
	w.mutex.Lock()
	w.WorkerStatus = status
	w.mutex.Unlock()
}

func (w *WorkerInfo) CheckHealth() bool {
	w.mutex.Lock()
	isLive := w.WorkerStatus != WORKER_UNKNOWN
	w.mutex.Unlock()
	return isLive
}

func (w *WorkerInfo) IsAvailable() bool {
	w.mutex.Lock()
	isAvailable := w.WorkerStatus == WORKER_IDLE
	w.mutex.Unlock()
	return isAvailable
}
