package worker

import (
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/master"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/types"
	"github.com/google/uuid"
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
	nReduce      int
	proto_gen.UnimplementedWorkerServer
}

func NewWorker(nReduce int, masterIP string) proto_gen.WorkerServer {
	return &Worker{
		MasterIP: masterIP,
		nReduce:  nReduce,
		UUID:     uuid.NewString(),
	}
}


