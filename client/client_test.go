package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	broker "github.com/Agent-Plus/go-grpc-broker"
	"github.com/Agent-Plus/go-grpc-broker/api"
	"github.com/stretchr/testify/assert"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufsz = 1024 * 1024

var (
	lis   *bufconn.Listener
	users map[string]string
	sr    *broker.ExchangeServer
)

func init() {
	lis = bufconn.Listen(bufsz)

	users = make(map[string]string)
	users["6704be61-3d72-4241-a740-ffb0d6c56da8"] = "secret"
	users["d94c4a7c-28f0-4e34-a359-c036900b476d"] = "secret"
	users["910353f5-a2e9-4f5d-b0dd-bd5d9b3660ea"] = "secret"
	auth := broker.NewDummyAuthentication(users)

	sr = broker.NewExchangeServer(auth)

	go func() {
		err := sr.Serve(lis)
		if err != nil {
			panic(err)
		}
	}()
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func newClient() (*ExchangeClient, error) {
	c := &ExchangeClient{}

	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to dial bufnet: %w", err)
	}
	c.api = api.NewExchangeClient(conn)

	return c, nil
}

func TestAuthenticate(t *testing.T) {
	cl, err := newClient()
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Authenticate("6704be61-3d72-4241-a740-ffb0d6c56da8", "secret")
	if err != nil {
		t.Error(err)
	}

	if len(cl.token) == 0 {
		t.Error("empty session token")
	}
}

func TestSubscribe(t *testing.T) {
	cl, err := newClient()
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Authenticate("6704be61-3d72-4241-a740-ffb0d6c56da8", "secret")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Subscribe("foo", "", false)
	if err != nil {
		t.Error(err)
	}

	if cl.subscribed != "foo" {
		t.Error("empty subscription")
	}
}

func TestConsumeAndPublish(t *testing.T) {
	var (
		err      error
		pub, sub *ExchangeClient
	)
	pub, err = newClient()
	if err != nil {
		t.Fatal(err)
	}

	sub, err = newClient()
	if err != nil {
		t.Fatal(err)
	}

	err = pub.Authenticate("6704be61-3d72-4241-a740-ffb0d6c56da8", "secret")
	if err != nil {
		t.Fatal(err)
	}

	err = sub.Authenticate("d94c4a7c-28f0-4e34-a359-c036900b476d", "secret")
	if err != nil {
		t.Fatal(err)
	}

	err = sub.Subscribe("foo", "", false)
	if err != nil {
		t.Error(err)
	}

	dlv, err := sub.Consume()
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	go func() {
		wg.Add(1)
		defer wg.Done()
		for {
			m := <-dlv
			t.Log(m)
			return
		}
	}()

	pub.Publish("foo", Message{
		Id:   "xyz",
		Body: []byte("bar"),
	}, nil)

	wg.Wait()
}

func TestConsumeAndPublishTags(t *testing.T) {
	var (
		mt sync.Mutex
		dn sync.WaitGroup
	)

	atm := make(map[string][]int64)

	fn := func(id, secret, topic, tag string) {
		sub, err := newClient()
		if err != nil {
			t.Fatal(err)
		}

		err = sub.Authenticate(id, secret)
		if err != nil {
			t.Fatal(err)
		}

		err = sub.Subscribe(topic, tag, false)
		if err != nil {
			t.Error(err)
		}

		dlv, err := sub.Consume()
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			defer dn.Done()
			for {
				m, ok := <-dlv
				if !ok {
					break
				}

				i, err := m.Headers.GetInt64("attempt")
				if err != nil {
					t.Fatal(err)
				}

				mt.Lock()
				atm[tag] = append(atm[tag], i)
				mt.Unlock()

				if i == 4 {
					return
				}
			}
		}()
	}

	dn.Add(2)
	fn("6704be61-3d72-4241-a740-ffb0d6c56da8", "secret", "foo", "route_a")
	fn("d94c4a7c-28f0-4e34-a359-c036900b476d", "secret", "foo", "route_b")

	pub, err := newClient()
	if err != nil {
		t.Fatal(err)
	}

	err = pub.Authenticate("910353f5-a2e9-4f5d-b0dd-bd5d9b3660ea", "secret")
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i < 5; i++ {
		var tags []string

		switch i {
		case 2:
			tags = []string{"route_a"}
		case 3:
			tags = []string{"route_b"}
		case 4:
			tags = []string{"route_a", "route_b"}
		}

		msg := Message{Headers: make(Header)}
		msg.Headers.SetInt64("attempt", int64(i))

		pub.Publish("foo", msg, tags)
	}

	dn.Wait()

	assert.Contains(t, atm, "route_a")
	assert.Contains(t, atm, "route_b")
	assert.ElementsMatch(t, []int64{1, 2, 4}, atm["route_a"])
	assert.ElementsMatch(t, []int64{1, 3, 4}, atm["route_b"])
}
