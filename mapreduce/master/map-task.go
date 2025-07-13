package master

import (
	"github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"
	"github.com/google/uuid"
)

type TaskState int

const (
	TASK_TO_DO TaskState = iota
	TASK_IN_PROGRESS
	TASK_COMPLETED
)

// vi 1 worker co the duoc chia cho nhieu map task
// 1 files -> N split files
// n files -> N * n split files
// the split file of each original file will be assigned for one worker -> one map task can have
// many split file
type MapTaskInfo struct {
	State TaskState
	Files []FileInfo
	UUID  string
}

type FileInfo struct {
	FileName string
	From     int
	To       int
}

func newMapTask() MapTaskInfo {
	return MapTaskInfo{
		State: TASK_TO_DO,
		Files: make([]FileInfo, 0),
		UUID:  uuid.NewString(),
	}
}

// map task need to be initialize by num of workers
func NewMapTasks(numOfWorkers int) []MapTaskInfo {
	var res []MapTaskInfo

	for idx := 1; idx <= numOfWorkers; idx++ {
		res = append(res, newMapTask())
	}

	return res
}

func (mti *MapTaskInfo) ToGRPCMsg() *proto_gen.AssignMapTaskReq {
	data := make([]*proto_gen.MapFileInfo, 0)

	for _, fileInfo := range mti.Files {
		data = append(data, &proto_gen.MapFileInfo{
			FileName: fileInfo.FileName,
			From:     int64(fileInfo.From),
			To:       int64(fileInfo.To),
		})
	}

	return &proto_gen.AssignMapTaskReq{
		FileInfo: data,
	}
}
