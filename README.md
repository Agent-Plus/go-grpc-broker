## go-grpc-broker 

go-grpc-broker is plain module to construct exchange service, like publisher-subscriber, where publisher can push message to all subscribers in fanout mode, or communicate with one consumer in RPC mode.

### RPC mode
RPC mode is designed to run function on the remote service and wait the result. Topic in exclusive mode combines only two members. If one side produces message and the other side not ready to operate its, the broker returns error.

```
                  +-------------+
                  |Exchange     |
                  ++-----------++
           -----> | TopicA      | ------->
Publisher         | (exclusive) |         Subscriber
           <----- |             | <-------
                  ++-----------++
                  |             |
                  +-------------+
```

Simple ping-pong

```
package main

import (
    cbr "github.com/Agent-Plus/go-grpc-broker/client"

    "flag"
    "fmt"
    "time"
)

var (
    guid = flag.String("guid", "", "client identifier")
    ping = flag.Bool("ping", false, "start ping request")
)

func main() {
    flag.Parse()

    cli := cbr.New(cbr.WithAuthentication(*guid, "secret"))
    if err := cli.Dial("127.0.0.1:8090"); err != nil {
        panic(err)
    }

    hdFunc := func(msg *cbr.Message) {
        resp := cbr.NewMessage()
        switch msg.Id {
        case "ping":
            resp.Id = "pong"
        case "pong":
            resp.Id = "ping"
        default:
            return
        }

        time.Sleep(1 * time.Second)
        fmt.Println("received: ", msg.Id, ", send: ", resp.Id)
        if err := cli.Publish("foo-ping", resp, nil); err != nil {
            panic(err)
        }
    }

    cc := cli.StartServe(hdFunc, "foo-ping", "", true)
    defer cc.Close()

    cc := cli.StartServe(hdFunc, "foo-ping", "", true)
    defer cc.Close()

    if *ping {
        go func() {
            for {
                timer := time.NewTimer(1 * time.Second)
                select {
                case <-timer.C:
                    msg := cbr.NewMessage()
                    msg.Id = "ping"
                    if err := cli.Publish("foo-ping", msg, nil); err == nil {
                        return
                    } else {
                        fmt.Println(err, ", retry at 1 sec")
                    }
                }
                timer.Stop()
            }
        }()
    }

    stop := make(chan struct{})
    <-stop
}
```

Start Sender A

```
go run . -ping -guid GUID
```

Start Sender B

```
go run . -guid GUID
```

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

### Run gRPC server

```
package main

import (
	gbr "github.com/Agent-Plus/go-grpc-broker"
)

func main() {
    users := map[string]string{
	    "6704be61-3d72-4241-a740-ffb0d6c56da8": "secret",
    }
	auth := gbr.NewDummyAuthentication(users)

	broker := gbr.NewExchangeServer(auth)

	broker.Run("127.0.0.1:8090")
}
```

#### Recreate api

Required plugin

```
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
```

Create

```
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative api/api.proto
```
