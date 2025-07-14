package master

import "github.com/TienMinh25/mit-labs-6-824-2025/mapreduce/proto/proto_gen"

type ReduceTaskInfo struct {
	State TaskState
	Files []IMDFileInfo
}

// information about the intermediate files, which includes
// filename and worker ip (worker contains files)
type IMDFileInfo struct {
	FileName string
	WorkerIP string
}

func newReduceTask() ReduceTaskInfo {
	return ReduceTaskInfo{
		State: TASK_TO_DO,
		Files: make([]IMDFileInfo, 0),
	}
}

func NewReduceTasks(nReduce int) []ReduceTaskInfo {
	res := make([]ReduceTaskInfo, 0)

	for idx := 0; idx < nReduce; idx++ {
		res = append(res, newReduceTask())
	}

	return res
}

func (task *ReduceTaskInfo) ToGRPCMsg() []*proto_gen.ReduceFileInfo {
	res := make([]*proto_gen.ReduceFileInfo, 0)

	for _, file := range task.Files {
		res = append(res, &proto_gen.ReduceFileInfo{
			FileName: file.FileName,
			WorkerIp: file.WorkerIP,
		})
	}

	return res
}
