package worker

import (
	"context"

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
	proto_gen.UnimplementedWorkerServer
}

func NewWorker(nReduce int, masterIP string) proto_gen.WorkerServer {
	return &Worker{
		MasterIP:     masterIP,
		WorkerStatus: master.WORKER_IDLE,
		nReduce:      nReduce,
		UUID:         uuid.NewString(),
		ChanEnd:      make(chan bool),
	}
}

func (w *Worker) End(_ context.Context, _ *proto_gen.Empty) (*proto_gen.Empty, error) {
	log.Tracef("[Worker] Worker [UUID: %v, ID: %v] is terminating", w.UUID, w.ID)
	w.ChanEnd <- true
	return &proto_gen.Empty{}, nil
}
