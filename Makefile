deps:
	go mod tidy -v
	go mod vendor -v

gen-protoc-mapreduce:
	(cd mapreduce/proto && protoc --proto_path=. --go_out=. --go_opt=paths=import --go-grpc_out=. --go-grpc_opt=paths=import *.proto)