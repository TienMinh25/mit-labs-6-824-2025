package master

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
