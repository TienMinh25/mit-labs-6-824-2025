deps:
	go mod tidy -v
	go mod vendor -v

gen-protoc-mapreduce:
	(cd mapreduce/proto && protoc --proto_path=. --go_out=. --go_opt=paths=import --go-grpc_out=. --go-grpc_opt=paths=import *.proto)

build-plugin:
	go build --buildmode=plugin -o ./mapreduce/mrapps/ ./mapreduce/mrapps/wc.go && \
	go build --buildmode=plugin -o ./mapreduce/mrapps/ ./mapreduce/mrapps/crash.go && \
	go build --buildmode=plugin -o ./mapreduce/mrapps/ ./mapreduce/mrapps/early_exit.go && \
	go build --buildmode=plugin -o ./mapreduce/mrapps/ ./mapreduce/mrapps/indexer.go && \
	go build --buildmode=plugin -o ./mapreduce/mrapps/ ./mapreduce/mrapps/jobcount.go && \
	go build --buildmode=plugin -o ./mapreduce/mrapps/ ./mapreduce/mrapps/mtiming.go && \
	go build --buildmode=plugin -o ./mapreduce/mrapps/ ./mapreduce/mrapps/nocrash.go && \
	go build --buildmode=plugin -o ./mapreduce/mrapps/ ./mapreduce/mrapps/rtiming.go

run-with-wc:
	@echo "Starting MapReduce cluster..."
	@bash -c ' \
	cleanup() { \
		echo "Stopping all processes..."; \
		jobs -p | xargs -r kill -TERM 2>/dev/null || true; \
		exit $$1; \
	}; \
	trap "cleanup 1" ERR; \
	trap "cleanup 0" SIGINT SIGTERM; \
	\
	go run mapreduce/cmd/master.go mr -i "mapreduce/input/pg-*.txt" -p mapreduce/mrapps/wc.so -w 4 -m 40000 & \
	sleep 2; \
	\
	for port in 40001 40002 40003 40004; do \
		go run mapreduce/cmd/worker.go mr -i "mapreduce/input/pg-*.txt" -p mapreduce/mrapps/wc.so -w 4 -P $$port & \
		sleep 0.5; \
	done; \
	\
	echo "All processes started. Press Ctrl+C to stop."; \
	wait'
	