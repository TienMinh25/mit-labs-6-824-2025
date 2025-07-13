package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/master"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/types"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type MapfFormat func(string, string) []types.KeyValue
type ReducefFormat func(string, []string) string

type Worker struct {
	MasterIP     string
	WorkerStatus master.WorkerStatus
	Mapf         MapfFormat
	Reducef      ReducefFormat
	ID           int
	UUID         string
	ChanEnd      chan bool
	nReduce      int
	MasterClient IRpcClient
	mux          sync.Mutex
	proto_gen.UnimplementedWorkerServer
}

func NewWorker(nReduce int, masterIP string) proto_gen.WorkerServer {
	return &Worker{
		MasterIP:     masterIP,
		WorkerStatus: master.WORKER_IDLE,
		nReduce:      nReduce,
		UUID:         uuid.NewString(),
		ChanEnd:      make(chan bool),
		MasterClient: NewRPCMasterClient(),
	}
}

// Health implements proto_gen.WorkerServer.
func (w *Worker) Health(_ context.Context, _ *proto_gen.Empty) (*proto_gen.HealthRes, error) {
	w.mux.Lock()
	defer w.mux.Unlock()
	return &proto_gen.HealthRes{
		Status: int64(w.WorkerStatus),
	}, nil
}

// End implements proto_gen.WorkerServer.
func (w *Worker) End(_ context.Context, _ *proto_gen.Empty) (*proto_gen.Empty, error) {
	log.Tracef("[Worker] Worker [UUID: %v, ID: %v] is terminating", w.UUID, w.ID)
	w.ChanEnd <- true
	return &proto_gen.Empty{}, nil
}

func (w *Worker) AssignMapTask(_ context.Context, data *proto_gen.AssignMapTaskReq) (*proto_gen.Result, error) {
	log.Printf("[Worker] Worker %v start doing map task", w.ID)
	w.mux.Lock()
	w.WorkerStatus = master.WORKER_BUSY
	w.mux.Unlock()

	// handle from files -> read contents from offset start to offset end
	// to optimize performance, can read multiple files in the same time
	kvChan := make(chan types.KeyValue, 1000)
	done := make(chan int, len(data.FileInfo))

	for _, fileInfo := range data.FileInfo {
		go func(file *proto_gen.MapFileInfo) {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("[Worker] Panic in file reader: %v", r)
				}
			}()

			contents := w.readPartitionContent(file.FileName, file.From, file.To)

			kvArrays := w.Mapf(fileInfo.FileName, contents)

			for _, kvVal := range kvArrays {
				kvChan <- kvVal
			}

			done <- 1
		}(fileInfo)
	}

	// partitioning the bucket to write files
	// can parallel partitioning with read file above
	imdKVs := make([][]types.KeyValue, w.nReduce)
	count := 0

Loop:
	for {
		select {
		case content, more := <-kvChan:
			if more {
				// partition content to bucket?
				idxBucket := mapreduce.IHash(content.Key) % w.nReduce
				imdKVs[idxBucket] = append(imdKVs[idxBucket], content)
			} else {
				break Loop
			}
		case <-done:
			count++
			if count == len(data.FileInfo) {
				close(kvChan)
			}
		}
	}

	// writes file parallel
	fileIMDs := w.writeFilesParallel(imdKVs, w.ID)

	// update immediate files info to master
	updateSuccess := w.MasterClient.UpdateIDMFiles(&proto_gen.UpdateIMDFilesReq{
		Uuid:      w.UUID,
		Filenames: fileIMDs,
	}, w.MasterIP)

	if updateSuccess {
		log.Infof("[Worker] Update immediate files successfully [filenames: %v, worker-id: %v]", fileIMDs, w.ID)
	}

	w.mux.Lock()
	defer w.mux.Unlock()
	w.WorkerStatus = master.WORKER_IDLE

	// return
	return &proto_gen.Result{
		Uuid:   w.UUID,
		Result: true,
	}, nil
}

func (w *Worker) readPartitionContent(filename string, startOffset, endOffset int64) string {
	fd, err := os.Open(filename)

	if err != nil {
		log.Panicf("[Worker] Failed to read file %s: , err: %v", filename, err)
	}

	scanner := bufio.NewScanner(fd)
	contents := ""

	// because when distributed workload, file split line from 0
	var line int64 = 0

	for scanner.Scan() {
		if line > endOffset {
			break
		}

		if line >= startOffset && line <= endOffset {
			contents += scanner.Text() + "\n"
		}

		line++
	}

	return contents
}

// writeFilesParallel returns array of path file name
func (w *Worker) writeFilesParallel(imdKVs [][]types.KeyValue, workerID int) []string {
	res := make([]string, 0)
	var mux sync.Mutex
	var wg sync.WaitGroup

	for idx := range imdKVs {
		wg.Add(1)
		go func(imdSubKVs []types.KeyValue, mapReduceID int) {
			defer wg.Done()
			content_byte, _ := json.Marshal(imdSubKVs)

			tempFile, err := os.CreateTemp("./mapreduce/output/temp/", "mr-temp-*")
			if err != nil {
				log.Panicf("create temp file error: %v", err)
			}

			tempFileName := tempFile.Name()
			defer os.Remove(tempFileName)

			outputFile := fmt.Sprintf("./mapreduce/output/mr-%v-%v", workerID, mapReduceID)

			if _, err = tempFile.Write(content_byte); err != nil {
				tempFile.Close()
				log.Panicf("write temp file error: %v", err)
			}

			if err = tempFile.Close(); err != nil {
				log.Panicf("close temp file error: %v", err)
			}

			if err = os.Rename(tempFileName, outputFile); err != nil {
				log.Panicf("rename file temp error: %v", err)
			}

			mux.Lock()
			res = append(res, outputFile)
			mux.Unlock()

		}(imdKVs[idx], idx)
	}

	wg.Wait()

	return res
}
