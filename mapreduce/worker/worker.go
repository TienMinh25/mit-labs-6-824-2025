package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
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
	workerClient IWorkerClient
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
		workerClient: NewRPCWorkerClient(),
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

// AssignMapTask implements proto_gen.WorkerServer.
func (w *Worker) AssignMapTask(_ context.Context, data *proto_gen.AssignMapTaskReq) (*proto_gen.Result, error) {
	log.Printf("[Worker] Worker %v start doing map task", w.ID)
	w.mux.Lock()
	w.WorkerStatus = master.WORKER_BUSY
	w.mux.Unlock()

	// handle from files -> read contents from offset start to offset end
	// to optimize performance, can read multiple files in the same time
	kvChan := make(chan types.KeyValue, 1000)
	done := make(chan int, len(data.FileInfo))

	defer close(done)

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

	// update intermediate files info to master
	updateSuccess := w.MasterClient.UpdateIDMFiles(&proto_gen.UpdateIMDFilesReq{
		Uuid:      w.UUID,
		Filenames: fileIMDs,
		Id:        int64(w.ID),
	}, w.MasterIP)

	if updateSuccess {
		log.Infof("[Worker] Update intermediate files successfully [filenames: %v, worker-id: %v]", fileIMDs, w.ID)
	} else {
		log.Infof("[Worker] Update intermediate files failed because worker is in WORKER_UNKNOWN before!")
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

// AssignReduceTask implements proto_gen.WorkerServer.
func (w *Worker) AssignReduceTask(_ context.Context, data *proto_gen.AssignReduceTaskReq) (*proto_gen.Result, error) {
	log.Printf("[Worker] Worker %v start doing reduce task", w.ID)
	w.mux.Lock()
	w.WorkerStatus = master.WORKER_BUSY
	w.mux.Unlock()

	defer func() {
		w.mux.Lock()
		w.WorkerStatus = master.WORKER_IDLE
		w.mux.Unlock()
	}()

	contents := make([]types.KeyValue, 0)
	var mux sync.Mutex

	errChan := make(chan error, 3)
	done := make(chan int, 5)
	cntDone := 0

	defer close(errChan)

	// read contents from other workers that contains file
	for _, fileInfo := range data.FileInfo {
		go func(info *proto_gen.ReduceFileInfo) {
			result := w.workerClient.ReadIDMFile(&proto_gen.GetIMDFileReq{
				FileName: info.FileName,
			}, info.WorkerIp)

			if result == nil {
				log.Warnf("The worker contains file intermediate has some problem")
				errChan <- errors.New("the worker contains file intermediate is die")
			}

			mux.Lock()
			contents = append(contents, result...)
			mux.Unlock()

			done <- 1
		}(fileInfo)
	}

Loop:
	for {
		select {
		case err := <-errChan:
			return &proto_gen.Result{
				Uuid:   w.UUID,
				Result: false,
			}, err
		case <-done:
			cntDone++

			if cntDone == len(data.FileInfo) {
				break Loop
			}
		}
	}

	// sort contents
	sort.Sort(types.ByKey(contents))

	reduceTaskIdStrs := strings.Split(data.FileInfo[0].FileName, "-")
	reduceTaskId := reduceTaskIdStrs[len(reduceTaskIdStrs)-1]
	outputFinal := fmt.Sprintf("mapreduce/output/mr-out-%s", reduceTaskId)

	tempFile, err := os.CreateTemp("./mapreduce/output/temp/", "mr-temp-*")
	if err != nil {
		log.Panicf("create temp file error: %v", err)
	}

	tempFileName := tempFile.Name()
	defer os.Remove(tempFileName)

	// write to final files
	i := 0

	for i < len(contents) {
		j := i + 1

		for j < len(contents) && contents[j].Key == contents[i].Key {
			j++
		}

		values := []string{}

		for k := i; k < j; k++ {
			values = append(values, contents[k].Value)
		}

		// reduce phase
		output := w.Reducef(contents[i].Key, values)

		fmt.Fprintf(tempFile, "%v %v\n", contents[i].Key, output)

		i = j
	}

	if err = tempFile.Close(); err != nil {
		log.Panicf("close temp file error: %v", err)
	}

	if err = os.Rename(tempFileName, outputFinal); err != nil {
		log.Panicf("rename file temp error: %v", err)
	}

	return &proto_gen.Result{
		Uuid:   w.UUID,
		Result: true,
	}, nil
}

// GetIMDFiles implements proto_gen.WorkerServer.
func (w *Worker) GetIMDFile(_ context.Context, data *proto_gen.GetIMDFileReq) (*proto_gen.GetIMDFileRes, error) {
	// other worker can call into worker to get result from imme
	fd, err := os.Open(data.FileName)

	if err != nil {
		log.Warnf("Open file have error: %v", err)
	}

	defer fd.Close()

	decoder := json.NewDecoder(fd)
	var res []types.KeyValue

	if err = decoder.Decode(&res); err != nil {
		log.Warnf("Decode file error: %v", err)
		return nil, err
	}

	return &proto_gen.GetIMDFileRes{
		KeyValues: transformIntermediateFile(res),
	}, nil
}

func transformIntermediateFile(kvs []types.KeyValue) []*proto_gen.KeyValue {
	result := make([]*proto_gen.KeyValue, 0)

	for _, kv := range kvs {
		result = append(result, &proto_gen.KeyValue{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}

	return result
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

			outputFile := fmt.Sprintf("./mapreduce/output/mr-imd-%v-%v", workerID, mapReduceID)

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
