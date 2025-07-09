package mapreduce

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

// ParseArgs return
// - files				([]string) 	(required)
// - plugin file		(string) 	(required)
// - master ip  		(string)	(required)
// - nReduce			(int)		(optional)
// - totalWorker		(int) 		(optional)
func ParseArgs() ([]string, string, string, int, int){
	var files []string
	var nReducer int
	var totalWorker int
	var plugin string
	var port int64

	rootCmd := &cobra.Command{
		Use:   "mr",
		Short: "MapReduce is an easy-to-use parallel framework by Tien Minh",
		Long:  "MapReudce is an easy-to-use Map Reduce Go parallel-computing framework inspired by 2025 6.824 lab1.",
		Run: func(cmd *cobra.Command, args []string) {
			tempFiles := []string{}
			fmt.Println(files)

			for _, f := range files {
				expandFiles, err := filepath.Glob(f)

				if err != nil {
					panic(err)
				}
				fmt.Println(expandFiles)
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
	rootCmd.PersistentFlags().Int64Var(&port, "port", 10000, "Port number")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return files, plugin, fmt.Sprintf("127.0.0.1:%v", port), nReducer, totalWorker 
}
