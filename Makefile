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
	go run mapreduce/cmd/master.go mr -i "mapreduce/input/pg-*.txt" -p mapreduce/mrapps/wc.so && \
	