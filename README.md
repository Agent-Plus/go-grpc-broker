## go-grpc-broker 

go-grpc-broker is uncomplicated module to construct exchange service, like publisher-subscriber, where publisher can push message to all subscribers in fanout mode, or communicate with one consumer in RPC mode.

### Fanout mode
Fanout mode idea is to deliver to all subscribers of the topic mentioned by the producer without acknowledgement warranty.

```
                           +----------------+ 
                           |Exchange        |         +-----------+ 
                           |                |      -- |Subscriber | 
                           |+--------------+|  ---/   +-----------+ 
 Publisher             --- || Topic A      ||-/
 (Topic=A, Message) --/    |+--------------+|---\     +-----------+ 
                           |                |    ---- |Subscriber | 
                           |                |         +-----------+ 
                           +----------------+ 
```

#### Recreate api

```
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/api.proto
```
