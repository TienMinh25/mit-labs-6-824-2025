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
