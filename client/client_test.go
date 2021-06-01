package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

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

func newClient(opt ...ClientOption) (*ExchangeClient, error) {
	c := New(opt...)

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

	if len(cl.aa.token) == 0 {
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
	wg.Add(1)

	go func() {
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

func TestMuxTimeoutPublishAndResponse(t *testing.T) {
	var pub *ServeMux
	if c, err := newClient(); err != nil {
		t.Fatal(err)
	} else {
		pub = NewServeMux(c)
	}
	pub.daedline = time.Now().Add(10 * time.Millisecond)

	err := pub.Authenticate("6704be61-3d72-4241-a740-ffb0d6c56da8", "secret")
	if err != nil {
		t.Fatal(err)
	}

	err = pub.Subscribe("foo", "", true)
	if err != nil {
		t.Error(err)
	}

	_, err = pub.PublishRequest("foo", Message{
		Id: "123456",
	}, nil)

	if err == nil || !errors.Is(err, ErrTimeout) {
		t.Error("required error on timeout, but got: ", err)
	}
}

func TestMuxPublishAndResponse(t *testing.T) {
	// remote subscriber A
	var sub *ServeMux
	if c, err := newClient(); err != nil {
		t.Fatal(err)
	} else {
		sub = NewServeMux(c)
	}

	// remote subscriber B
	var pub *ServeMux
	if c, err := newClient(); err != nil {
		t.Fatal(err)
	} else {
		pub = NewServeMux(c)
	}

	sub.Get("ping", HandlerFunc(func(w ResponseWriter, msg *Message) {
		if string(msg.Body) != "ping" {
			t.Errorf("wrong request message body: %s", msg.Body)
		}

		w.SetId("123457")
		w.SetBody([]byte("pong"))
		w.Publish("foo-rpc", nil)
	}))

	go func() {
		err := sub.StartServe(AuthOption{
			Id:     "910353f5-a2e9-4f5d-b0dd-bd5d9b3660ea",
			Secret: "secret",
		}, SubOption{
			Topic:     "foo-rpc",
			Exclusive: true,
		})

		if err != nil {
			t.Fatal(err)
		}
	}()

	go func() {
		err := pub.StartServe(AuthOption{
			Id:     "6704be61-3d72-4241-a740-ffb0d6c56da8",
			Secret: "secret",
		}, SubOption{
			Topic:     "foo-rpc",
			Exclusive: true,
		})

		if err != nil {
			t.Fatal(err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if sub.serving.isSet() && pub.serving.isSet() {
				break
			}
		}
	}()

	wg.Wait()

	msg := Message{
		Id:   "123456",
		Body: []byte("ping"),
	}
	SetMessageActionGet(&msg, "ping")
	resp, err := pub.PublishRequest("foo-rpc", msg, nil)

	if err != nil {
		t.Error(err)
	}

	if resp == nil {
		t.Error("response must not be nil")
	} else {
		if resp.Id != "123457" {
			t.Errorf("wrong id: %s", resp.Id)
		}

		if string(resp.Body) != "pong" {
			t.Errorf("wrong response body: %s", resp.Body)
		}
	}
}

func TestAuthConcurrence(t *testing.T) {
	cl, err := newClient()
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			err := cl.Authenticate("6704be61-3d72-4241-a740-ffb0d6c56da8", "secret")
			if err != nil {
				t.Error(err)
			}

			wg.Done()
		}()
	}

	wg.Wait()
}
