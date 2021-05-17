module github.com/Agent-Plus/go-grpc-broker

go 1.15

require (
	github.com/Agent-Plus/go-grpc-broker/api v0.0.0
	github.com/golang/protobuf v1.5.0
	github.com/satori/go.uuid v1.2.0
	google.golang.org/grpc v1.37.1
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.1.0 // indirect
)

replace github.com/Agent-Plus/go-grpc-broker/api => ./api
