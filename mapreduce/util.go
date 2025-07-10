package mapreduce

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// ParseArgs return
// - files					([]string) 	(required)
// - plugin file		(string) 		(required)
// - master ip  		(string)		(required)
// - worker ip 			(string)		(required for worker, optional for master)
// - nReduce				(int)				(optional)
// - totalWorker		(int) 			(optional)
func ParseArgs() ([]string, string, string, string, int, int) {
	var files []string
	var nReducer int
	var totalWorker int
	var plugin string
	var port int
	var portWorker int

	rootCmd := &cobra.Command{
		Use:   "mr",
		Short: "MapReduce is an easy-to-use parallel framework by Tien Minh",
		Long:  "MapReudce is an easy-to-use Map Reduce Go parallel-computing framework inspired by 2025 6.824 lab1.",
		Run: func(cmd *cobra.Command, args []string) {
			tempFiles := []string{}

			for _, f := range files {
				expandFiles, err := filepath.Glob(f)

				if err != nil {
					panic(err)
				}

				tempFiles = append(tempFiles, expandFiles...)
			}

			pluginFiles, err := filepath.Glob(plugin)

			if err != nil {
				panic(err)
			} else if len(pluginFiles) == 0 {
				panic("No such file")
			}

			plugin = pluginFiles[0]
			files = tempFiles
		},
	}

	rootCmd.PersistentFlags().StringSliceVarP(&files, "input", "i", []string{}, "Input files")
	rootCmd.MarkPersistentFlagRequired("input")
	rootCmd.PersistentFlags().StringVarP(&plugin, "plugin", "p", "", "Plugin .so file")
	rootCmd.MarkPersistentFlagRequired("plugin")
	rootCmd.PersistentFlags().IntVarP(&nReducer, "reduce", "r", 1, "Number of Reducers")
	rootCmd.PersistentFlags().IntVarP(&totalWorker, "worker", "w", 4, "Number of Workers(for master node)\nID of worker(for worker node)")
	rootCmd.PersistentFlags().IntVarP(&port, "port", "m", 40000, "Master port number")
	rootCmd.PersistentFlags().IntVarP(&portWorker, "port worker", "P", 40001, "Worker port number")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return files, plugin, fmt.Sprintf("127.0.0.1:%v", port), fmt.Sprintf("127.0.0.1:%v", portWorker), nReducer, totalWorker
}

func LineNums(inputFile string) int {
	fd, err := os.Open(inputFile)

	if err != nil {
		log.Tracef("[Master] Have error when open file: %v, err: %s", inputFile, err.Error())
	}
	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	line := 0

	for scanner.Scan() {
		line++
	}

	return line
}