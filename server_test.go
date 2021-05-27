package broker

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Agent-Plus/go-grpc-broker/api"
	"github.com/golang/protobuf/ptypes/empty"

	uuid "github.com/satori/go.uuid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufsz = 1024 * 1024

var (
	lis   *bufconn.Listener
	users map[string]string
	sr    *ExchangeServer
)

func init() {
	lis = bufconn.Listen(bufsz)

	users = make(map[string]string)
	users["6704be61-3d72-4241-a740-ffb0d6c56da8"] = "secret"
	users["d94c4a7c-28f0-4e34-a359-c036900b476d"] = "secret"
	auth := NewDummyAuthentication(users)

	sr = NewExchangeServer(auth)

	go func() {
		err := sr.gs.Serve(lis)
		if err != nil {
			panic(err)
		}
	}()
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

type clientChann struct {
	m sync.Mutex

	ch     api.ExchangeClient
	conn   *grpc.ClientConn
	ctx    context.Context
	ops    int
	stream api.Exchange_ConsumeClient
	tk     *api.Token
}

func newClient(name string) (*clientChann, error) {
	c := &clientChann{
		ctx: context.Background(),
	}

	var err error
	c.conn, err = grpc.DialContext(c.ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to dial bufnet: %w", err)
	}
	c.ch = api.NewExchangeClient(c.conn)

	c.tk, err = c.ch.Authenticate(c.ctx, &api.Identity{
		Id:     name,
		Secret: users[name],
	})
	if err != nil {
		c.conn.Close()
		return nil, err
	}

	return c, nil
}

func TestAuthenticationOk(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := api.NewExchangeClient(conn)
	resp, err := client.Authenticate(ctx, &api.Identity{
		Id:     "6704be61-3d72-4241-a740-ffb0d6c56da8",
		Secret: users["6704be61-3d72-4241-a740-ffb0d6c56da8"],
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Key) == 0 {
		t.Error("expected token GID")
	}
}

func TestAuthenticationError(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := api.NewExchangeClient(conn)
	_, err = client.Authenticate(ctx, &api.Identity{
		Id:     "6704be61-3d72-4241-a740-ffb0d6c56da8",
		Secret: "",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatal("expected error code:", codes.Unauthenticated)
	}
}

func TestSubscribe(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := api.NewExchangeClient(conn)

	var tk *api.Token
	tk, err = client.Authenticate(ctx, &api.Identity{
		Id:     "6704be61-3d72-4241-a740-ffb0d6c56da8",
		Secret: users["6704be61-3d72-4241-a740-ffb0d6c56da8"],
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tk.Key)
	_, err = client.Subscribe(ctx, &api.SubscribeRequest{
		Name: "foo",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestConsume(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := api.NewExchangeClient(conn)

	var tk *api.Token
	tk, err = client.Authenticate(ctx, &api.Identity{
		Id:     "6704be61-3d72-4241-a740-ffb0d6c56da8",
		Secret: users["6704be61-3d72-4241-a740-ffb0d6c56da8"],
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tk.Key)
	_, err = client.Subscribe(ctx, &api.SubscribeRequest{
		Name: "foo",
	})
	if err != nil {
		t.Fatal(err)
	}

	var (
		ops    int32
		stream api.Exchange_ConsumeClient
		wg     sync.WaitGroup
	)

	stream, err = client.Consume(ctx, &empty.Empty{})
	if err != nil {
		t.Fatal(err)
	}

	if header, err := stream.Header(); err != nil {
		t.Fatal(err)
	} else {
		if v := header.Get("x-state"); len(v) > 0 && v[0] == "established" {
			//t.Log("x-state:", v)
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			_, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}

			atomic.AddInt32(&ops, 1)
			if ops == 5 {
				return
			}
		}
	}()

	pub := NewChannel()
	sr.AddChannel(pub)

	go func() {
		for i := 1; i < 6; i++ {
			_, err := pub.Publish("foo", &api.Message{
				Body: []byte(strconv.Itoa(i)),
			}, nil)
			if err != nil {
				if _, ok := err.(*CircuitErrors); ok {
					t.Log("WARN:", err)
				} else {
					t.Error(err)
				}
			}
		}
	}()

	wg.Wait()

	if ops != 5 {
		t.Error("expected 5 messagesm but got:", ops)
	}
}

func TestRPCDialog(t *testing.T) {
	// prepare client A
	cla, err := newClient("6704be61-3d72-4241-a740-ffb0d6c56da8")
	if err != nil {
		t.Fatal(err)
	}
	defer cla.conn.Close()

	cla.ctx = metadata.AppendToOutgoingContext(cla.ctx, "authorization", "Bearer "+cla.tk.Key)
	_, err = cla.ch.Subscribe(cla.ctx, &api.SubscribeRequest{
		Name:      "foo-rpc",
		Exclusive: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// prepare client B
	var clb *clientChann
	clb, err = newClient("d94c4a7c-28f0-4e34-a359-c036900b476d")
	if err != nil {
		t.Fatal(err)
	}
	defer clb.conn.Close()

	clb.ctx = metadata.AppendToOutgoingContext(clb.ctx, "authorization", "Bearer "+clb.tk.Key)
	_, err = clb.ch.Subscribe(clb.ctx, &api.SubscribeRequest{
		Name:      "foo-rpc",
		Exclusive: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// dialog
	var wg sync.WaitGroup

	ops := struct {
		corids map[string]int
		m      sync.Mutex
	}{
		corids: make(map[string]int),
	}

	p2p := func(cl *clientChann) {
		defer wg.Done()

		var err error
		ctx := metadata.AppendToOutgoingContext(cl.ctx, "authorization", "Bearer "+cl.tk.Key)
		cl.stream, err = cl.ch.Consume(ctx, &empty.Empty{})
		if err != nil {
			t.Fatal(err)
		}

		if header, err := cl.stream.Header(); err != nil {
			t.Fatal(err)
		} else {
			if v := header.Get("x-state"); len(v) > 0 && v[0] == "established" {
				//t.Log("x-state:", v)
			}
		}

		for i := 0; i < 3; i++ {
			id := uuid.NewV4()
			_, err = cl.ch.Publish(cl.ctx, &api.PublishRequest{
				Topic: "foo-rpc",
				Message: &api.Message{
					Body: []byte("ping"),
					Id:   id.String(),
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			ops.m.Lock()
			ops.corids[id.String()] = 1
			ops.m.Unlock()
		}

		for {
			msg, err := cl.stream.Recv()
			if err != nil {
				t.Fatal(err)
			}

			if string(msg.Body) == "ping" {
				_, err = cl.ch.Publish(cl.ctx, &api.PublishRequest{
					Topic: "foo-rpc",
					Message: &api.Message{
						Body: []byte("pong"),
					},
				})
				if err != nil {
					t.Fatal(err)
				}

				ops.m.Lock()
				if _, ok := ops.corids[msg.Id]; ok {
					ops.corids[msg.Id] |= 2
				}
				ops.m.Unlock()
			}

			cl.m.Lock()
			cl.ops++
			cl.m.Unlock()

			if cl.ops == 3 {
				return
			}
		}
	}

	wg.Add(1)
	go p2p(cla)
	wg.Add(1)
	go p2p(clb)

	wg.Wait()
	for k, v := range ops.corids {
		if (v & 3) == 0 {
			t.Error("message ", k, " diesn't have response")
		}
	}
}
