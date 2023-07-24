package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	broker "github.com/Agent-Plus/go-grpc-broker"
	uuid "github.com/google/uuid"
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
	err := c.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	return c, err
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

	id, err := cl.Subscribe("foo", "", FanoutMode)
	if err != nil {
		t.Error(err)
	}

	if len(id) == 0 {
		t.Error("Subscribe did not return subscription id")
	}
}

func TestConsumeAndPublish(t *testing.T) {
	type args struct {
		mode  ModeType
		topic string
		wait  bool
	}
	tests := []struct {
		name    string
		args    args
		gotErr  error
		wantErr bool
		wantRes bool
	}{
		{
			name: "publish fanout and no wait",
			args: args{
				mode:  FanoutMode,
				topic: "foo-fanout",
				wait:  false,
			},
			wantErr: false,
			wantRes: false,
		},
		{
			name: "publish rpc and wait",
			args: args{
				mode:  RPCMode,
				topic: "foo-rpc",
				wait:  true,
			},
			wantErr: false,
			wantRes: true,
		},
	}

	pub, err := newClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := pub.Close()
		if err != nil {
			t.Errorf("Close(), error = %v", err)
		}
	}()

	err = pub.Authenticate("6704be61-3d72-4241-a740-ffb0d6c56da8", "secret")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub, err := newClient()
			if err != nil {
				t.Errorf("newClient(), error = %v", err)
				return
			}
			defer func() {
				err := sub.Close()
				if err != nil {
					t.Errorf("Close(), error = %v", err)
				}
			}()

			err = sub.Authenticate("d94c4a7c-28f0-4e34-a359-c036900b476d", "secret")
			if err != nil {
				t.Errorf("Authenticate(), error = %v", err)
				return
			}

			var subId string
			subId, err = sub.Subscribe(tt.args.topic, "", tt.args.mode)
			if err != nil {
				t.Errorf("Subscribe(), error = %v", err)
				return
			}

			wg.Add(1)
			fn := func(sub *ExchangeClient, tpName string, wait bool) func() {
				dlv, err := sub.Consume(subId)
				if err != nil {
					t.Errorf("Consume(), error = %v", err)
					return nil
				}

				return func() {
					defer wg.Done()

					msg := <-dlv

					if wait {
						m := NewMessage()
						m.corId = msg.Id

						sub.Publish(tpName, m, nil, false)
					}
				}
			}(sub, tt.args.topic, tt.args.wait)

			go fn()

			m := NewMessage()
			m.Id = uuid.NewString()
			res, err := pub.Publish(tt.args.topic, m, nil, tt.args.wait)

			wg.Wait()

			if (err != nil) != tt.wantErr || err != tt.gotErr {
				t.Errorf("Publish(), wait = %v, want error = %v, error = %v", tt.args.wait, tt.gotErr, err)
			}

			if nn := (res != nil); nn != tt.wantRes {
				t.Errorf("Publish(), wait = %v, want response = %v", tt.args.wait, tt.wantRes)
			} else if nn && tt.wantRes {
				if res.corId != m.Id {
					t.Errorf("Publish(), want response corId = %v, got corId = %v", m.Id, res.corId)
				}
			}
		})
	}
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

		sid, err := sub.Subscribe(topic, tag, FanoutMode)
		if err != nil {
			t.Error(err)
		}

		dlv, err := sub.Consume(sid)
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

		msg := NewMessage()
		msg.Headers.SetInt64("attempt", int64(i))

		pub.Publish("foo", msg, tags, false)
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
	pub.daedline = 10 * time.Millisecond

	err := pub.Authenticate("6704be61-3d72-4241-a740-ffb0d6c56da8", "secret")
	if err != nil {
		t.Fatal(err)
	}

	_, err = pub.Subscribe("foo", "", (RPCMode | ExclusiveMode))
	if err != nil {
		t.Error(err)
	}

	m := NewMessage()
	m.Id = "123456"
	_, err = pub.PublishRequest("foo", m, nil)

	if err == nil || !errors.Is(err, ErrTimeout) {
		t.Error("required error on timeout, but got: ", err)
	}
}

func TestMuxPublishRequest(t *testing.T) {
	topicName := "foo-exclusive"

	var sub *ServeMux
	if c, err := newClient(WithAuthentication("910353f5-a2e9-4f5d-b0dd-bd5d9b3660ea", "secret")); err != nil {
		t.Errorf("newClient(), error = %v", err)
		return
	} else {
		sub = NewServeMux(c)
	}
	defer func() {
		if err := sub.Close(); err != nil {
			t.Errorf("Close(), error = %v", err)
		}
	}()

	sub.Get("ping", HandlerFunc(func(w ResponseWriter, msg *Message) {
		if string(msg.Body) != "ping" {
			t.Errorf("wrong request message body: %s", msg.Body)
		}

		w.SetId("123457")
		w.SetBody([]byte("pong"))
		w.PublishResponse()
	}))

	// wait clients are ready to test ping-pong with response wait
	wt := func(so *observer) {
		for !so.serving.isSet() {
		}
	}

	sc := sub.StartServe(topicName, "", RPCMode|ExclusiveMode)
	defer sc.Close()

	wt(sc.(*observer))

	var pub *ServeMux
	if c, err := newClient(WithAuthentication("6704be61-3d72-4241-a740-ffb0d6c56da8", "secret")); err != nil {
		t.Errorf("newClient(), error = %v", err)
		return
	} else {
		pub = NewServeMux(c)
	}
	defer func() {
		if err := pub.Close(); err != nil {
			t.Errorf("Close(), error = %v", err)
		}
	}()

	pc := pub.StartServe(topicName, "", (RPCMode | ExclusiveMode))
	defer pc.Close()

	wt(pc.(*observer))

	msg := NewMessage()
	msg.Id = "123456"
	msg.Body = []byte("ping")

	SetMessageActionGet(msg, "ping")

	res, err := pub.PublishRequest(topicName, msg, nil)

	if err != nil {
		t.Errorf("PublishRequest(),  got error = %v", err)
	}

	if res == nil {
		t.Errorf("PublishRequest(), want response, got nil")
	} else {
		if res.corId != msg.Id {
			t.Errorf("PublishRequest(), want response corId = %v, got corId = %v", msg.Id, res.corId)
		}

		if res.Id != "123457" {
			t.Errorf("wrong id: %s", res.Id)
		}

		if string(res.Body) != "pong" {
			t.Errorf("wrong response body: %s", res.Body)
		}
	}
}

func TestMuxPublish(t *testing.T) {
	type args struct {
		mode  ModeType
		topic string
		wait  bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		wantRes bool
	}{
		{
			name: "publish request in exclusive mode",
			args: args{
				mode:  (RPCMode | ExclusiveMode),
				topic: "foo-rpc",
				wait:  false,
			},
			wantErr: true,
			wantRes: false,
		},
		{
			name: "publish request in rpc mode no wait",
			args: args{
				mode:  RPCMode,
				topic: "foo-rpc",
				wait:  false,
			},
			wantErr: false,
			wantRes: false,
		},
	}

	// wait clients are ready to test ping-pong with response wait
	wt := func(so *observer) {
		for !so.serving.isSet() {
		}
	}

	var sub *ServeMux
	if c, err := newClient(WithAuthentication("910353f5-a2e9-4f5d-b0dd-bd5d9b3660ea", "secret")); err != nil {
		t.Errorf("newClient(), error = %v", err)
		return
	} else {
		sub = NewServeMux(c)
	}
	defer func() {
		if err := sub.Close(); err != nil {
			t.Errorf("Close(), error = %v", err)
		}
	}()

	sub.Get("ping", HandlerFunc(func(w ResponseWriter, msg *Message) {
		if string(msg.Body) != "ping" {
			t.Errorf("wrong request message body: %s", msg.Body)
		}

		w.SetId("123457")
		w.SetBody([]byte("pong"))
		w.PublishResponse()
	}))

	// remote subscriber B
	var pub *ServeMux
	if c, err := newClient(WithAuthentication("6704be61-3d72-4241-a740-ffb0d6c56da8", "secret")); err != nil {
		t.Errorf("newClient(), error = %v", err)
		return
	} else {
		pub = NewServeMux(c)
	}
	defer func() {
		if err := pub.Close(); err != nil {
			t.Errorf("Close(), error = %v", err)
		}
	}()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := sub.StartServe(tt.args.topic, "", tt.args.mode)
			defer sc.Close()

			wt(sc.(*observer))

			msg := NewMessage()
			msg.Id = "123456"
			msg.Body = []byte("ping")

			SetMessageActionGet(msg, "ping")

			res, err := pub.Publish(tt.args.topic, msg, nil, tt.args.wait)

			if (err != nil) != tt.wantErr {
				t.Errorf("Publish(), want error = %v, got error = %v", tt.wantErr, err)
			}

			if nn := (res != nil); nn != tt.wantRes {
				t.Errorf("Publish(), wait = %v, want response = %v", tt.args.wait, tt.wantRes)
			} else if nn && tt.wantRes {
				if res.corId != msg.Id {
					t.Errorf("Publish(), want response corId = %v, got corId = %v", msg.Id, res.corId)
				}

				if res.Id != "123457" {
					t.Errorf("wrong id: %s", res.Id)
				}

				if string(res.Body) != "pong" {
					t.Errorf("wrong response body: %s", res.Body)
				}
			}
		})
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
