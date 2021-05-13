module github.com/Agent-Plus/go-grpc-broker/client

go 1.15

require (
	github.com/Agent-Plus/go-grpc-broker/api v0.0.0
	github.com/golang/protobuf v1.5.0
	github.com/pkg/errors v0.9.1
	github.com/satori/go.uuid v1.2.0
	google.golang.org/grpc v1.37.1
)

replace github.com/Agent-Plus/go-grpc-broker/api => ../api
