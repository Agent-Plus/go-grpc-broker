## go-grpc-broker 

go-grpc-broker is uncomplicated module to construct exchange service, like publisher-subscriber, where publisher can push message to all subscribers in fanout mode, or communicate with one consumer in RPC mode. 

#### Recreate api

```
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/api.proto
```
