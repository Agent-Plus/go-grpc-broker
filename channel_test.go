package broker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Agent-Plus/go-grpc-broker/api"
	uuid "github.com/google/uuid"
)

func TestFanoutMessages(t *testing.T) {
	// instatinate exchange
	ex := New()

	var wg sync.WaitGroup

	var cnt int32

	for i := 0; i < 10; i++ {
		// tie from exchange channel
		fn := func() func() {
			ch := NewChannel()
			ex.AddChannel(ch)

			// subscribe receiver
			id, err := ch.Subscribe("foo", "", 0)
			if err != nil {
				t.Error(err)
				return nil
			}

			msg := ch.Consume(id)
			wg.Add(1)

			return func() {
				defer func() {
					ch.StopConsume(id)
					wg.Done()
				}()

				for {
					<-msg
					atomic.AddInt32(&cnt, 1)
					return
				}
			}
		}()

		go fn()
	}

	// tie from exchange channel
	pub := NewChannel()
	ex.AddChannel(pub)

	_, _, err := pub.Publish("foo", &api.Message{}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if cnt != 10 {
		t.Error("expected 10 responses, but got:", cnt)
	}
}

func TestFanoutCircuitNonBreaking(t *testing.T) {
	ex := New()

	var tmmtx sync.Mutex
	tmwbuf := make(map[string]int32)

	for i := 0; i < 11; i++ {
		wk := func(i int) func() {
			ch := NewChannel()
			ex.AddChannel(ch)

			// subscribe receiver
			id, err := ch.Subscribe("foo", "", 0)
			if err != nil {
				t.Errorf("Subscribe(), error = %v", err)
				return nil
			}

			msg := ch.Consume(id)

			return func() {
				defer ch.StopConsume(id)

				if (i % 5) == 0 {
					tmmtx.Lock()
					tmwbuf[ch.Token()] = 0
					tmmtx.Unlock()

					// endless
					for {
					}
				} else {
					for {
						<-msg
					}
				}
			}
		}(i)

		go wk()
	}

	pb := NewChannel()
	ex.AddChannel(pb)

	for i := 0; i < 10; i++ {
		ack, _, err := pb.Publish("foo", &api.Message{}, nil, false)

		if err != nil {
			t.Log(i, ":", err)
			if er, ok := err.(*CircuitErrors); ok {
				tmmtx.Lock()
				a, b := len(er.err), len(tmwbuf)
				tmmtx.Unlock()
				if a != b {
					t.Fatalf("expected %d errors, but got %d", b, a)
				}
				for _, v := range er.err {
					if !errors.Is(v, ErrWaitTimeout) {
						t.Fatalf("unexpected error %v", v)
					}
				}
			} else {
				t.Fatal(err)
			}
		}

		if ack != 8 {
			t.Fatalf("axpected 8 success pushes, but got %d", ack)
		}
	}
}

func TestSubscriptionLimit(t *testing.T) {
	type args struct {
		mode  modeType
		topic string
	}
	tests := []struct {
		name    string
		args    args
		gotErr  error
		wantErr bool
	}{
		{
			name: "RPC mode with one pulling channel",
			args: args{
				mode:  RPCMode,
				topic: "pull-me",
			},
			gotErr:  ErrSubscribeRejected,
			wantErr: true,
		},
		{
			name: "Exclusive mode with two pulling channels",
			args: args{
				mode:  RPCMode | ExclusiveMode,
				topic: "exclusive-me",
			},
			gotErr:  ErrSubscribeRejected,
			wantErr: true,
		},
	}

	// instatinate exchange
	ex := New()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// channel A
			cha := NewChannel()
			ex.AddChannel(cha)
			// declasre exclusive
			if _, err := cha.Subscribe(tt.args.topic, "", tt.args.mode); err != nil {
				t.Errorf("consumer A: %v", err)
			}

			// channel B
			chb := NewChannel()
			ex.AddChannel(chb)
			// declare exclusive
			_, err := chb.Subscribe(tt.args.topic, "", tt.args.mode)
			if (err != nil) != tt.wantErr && (tt.args.mode&ExclusiveMode) == 0 {
				t.Errorf("Subscribe(), Channel B, err = %v, want error = %v", err, tt.wantErr)
			}

			if (tt.args.mode&ExclusiveMode) == 0 && !errors.Is(err, tt.gotErr) {
				t.Errorf("Subscribe(), Channel B, err = %v, want error = %v", err, tt.gotErr)
			}

			// one more
			if tt.args.mode&ExclusiveMode != 0 {
				chc := NewChannel()
				ex.AddChannel(chc)
				// declare exclusive
				_, err := chc.Subscribe(tt.args.topic, "", tt.args.mode)

				if !errors.Is(err, tt.gotErr) {
					t.Errorf("Subscribe(), Channel C, err = %v, want error = %v", err, tt.gotErr)
				}
			}
		})
	}
}

func TestConsumingError(t *testing.T) {
	type args struct {
		mode  modeType
		topic string
	}
	tests := []struct {
		name    string
		args    args
		gotErr  error
		wantErr bool
	}{
		{
			name: "RPC mode, no consumer",
			args: args{
				mode:  RPCMode,
				topic: "rpc-topic",
			},
			gotErr:  ErrChannelNotConsumed,
			wantErr: true,
		},
		{
			name: "Exclusive mode, no consumer",
			args: args{
				mode:  RPCMode | ExclusiveMode,
				topic: "exclusive-topic",
			},
			gotErr:  ErrChannelNotConsumed,
			wantErr: true,
		},
	}

	// instatinate exchange
	ex := New()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// channel A
			cha := NewChannel()
			ex.AddChannel(cha)

			// declare exclusive
			if _, err := cha.Subscribe(tt.args.topic, "", tt.args.mode); err != nil {
				t.Fatal(err)
			}
			// channel A will not pull data

			// channel B
			chb := NewChannel()
			ex.AddChannel(chb)

			if (tt.args.mode & ExclusiveMode) != 0 {
				if _, err := chb.Subscribe(tt.args.topic, "", tt.args.mode); err != nil {
					t.Fatal(err)
				}
			}

			_, _, err := chb.Publish(tt.args.topic, &api.Message{}, nil, false)

			if tt.gotErr != nil && !errors.Is(err, tt.gotErr) {
				t.Errorf("Publish(), want error: %v, error: %v", tt.gotErr, err)
			}
		})
	}
}

func TestExclusiveTimeoutError(t *testing.T) {
	// instatinate exchange
	ex := New()

	// channel A
	cha := NewChannel()
	ex.AddChannel(cha)
	// declasre exclusive
	sid, err := cha.Subscribe("foo", "", (RPCMode | ExclusiveMode))
	if err != nil {
		t.Error(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func(ch *Channel, id uuid.UUID) {
		msg := ch.Consume(id)
		defer ch.StopConsume(id)

		rttl, cancel := context.WithDeadline(
			context.Background(),
			time.Now().Add(1*time.Second),
		)
		cancel()
		defer cancel()

		// busy
		time.Sleep(1100 * time.Millisecond)

		select {
		case <-msg:
		case <-rttl.Done():
		}

		wg.Done()
	}(cha, sid)

	// channel B
	chb := NewChannel()
	ex.AddChannel(chb)
	_, _, err = chb.Publish("foo", &api.Message{}, nil, false)
	wg.Wait()

	if !errors.Is(err, ErrChannelNotConsumed) {
		t.Errorf("expected error: %v, but got: %v", ErrChannelNotConsumed, err)
	}
}

// publisher and consumer must be subscribed to one channel.
func TestExclusivePublisherError_notSubscribed(t *testing.T) {
	// instatinate exchange
	ex := New()

	// channel A
	cha := NewChannel()
	ex.AddChannel(cha)

	// declare exclusive
	sid, err := cha.Subscribe("foo", "", (RPCMode | ExclusiveMode))
	if err != nil {
		t.Fatal(err)
	}

	cha.Consume(sid)
	defer cha.StopConsume(sid)

	// channel B
	chb := NewChannel()
	ex.AddChannel(chb)
	// publish without subscription
	_, _, err = chb.Publish("foo", &api.Message{}, nil, false)

	if !errors.Is(err, ErrChannelNotConsumed) {
		t.Error("expected error: ", ErrChannelNotConsumed)
	}
}

func TestConsumingRPC(t *testing.T) {
	type args struct {
		mode  modeType
		topic string
		wait  bool
	}
	tests := []struct {
		name    string
		args    args
		gotErr  error
		wantErr bool
	}{
		{
			name: "Exclusive conversation",
			args: args{
				mode:  (RPCMode | ExclusiveMode),
				topic: "exclusive-topic",
			},
			wantErr: false,
		},
		{
			name: "RPC conversation",
			args: args{
				mode:  RPCMode,
				topic: "rpc-topic",
				wait:  true,
			},
			wantErr: false,
		},
	}

	// instatinate exchange
	ex := New()

	var (
		wg   sync.WaitGroup
		wait atomicBool
	)

	// channel A
	cha := NewChannel()
	ex.AddChannel(cha)

	// channel B
	chb := NewChannel()
	ex.AddChannel(chb)

	worker := func(t *testing.T, ch *Channel, ar args, sid uuid.UUID, ping *int32) func() {
		msg := ch.Consume(sid)
		wg.Add(1)

		return func() {
			defer func() {
				ch.StopConsume(sid)
				wg.Done()
				wait.setFalse()
			}()

			for {
				select {
				case m := <-msg:
					if len(m.GetId()) == 0 {
						t.Errorf("Consume(), message id required")
					}

					i, err := strconv.Atoi(string(m.Body))
					if err != nil {
						t.Errorf("Consume(), message parse error = %v", err)
						return
					}

					if ar.mode&ExclusiveMode != 0 {
						if atomic.LoadInt32(ping) > 0 {
							return
						}
					}

					ack, _, err := ch.Publish(ar.topic, &api.Message{Body: []byte("1"), CorId: m.GetId()}, nil, false)
					if err != nil {
						t.Errorf("Publish(), error = %v", err)
						return
					}
					if ack != 1 {
						t.Errorf("Publish(), got ack = %d, want act = 1", ack)
						return
					}

					atomic.AddInt32(ping, int32(i))

					if ar.mode&(RPCMode|ExclusiveMode) == RPCMode {
						return
					}

				default:
					if !wait.isSet() {
						return
					}
				}
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wait.setTrue()

			// declare exclusive
			sid, err := cha.Subscribe(tt.args.topic, "", tt.args.mode)
			if err != nil {
				t.Fatalf("Subscribe(), channel A: %v", err)
			}

			var ping int32
			wk := worker(t, cha, tt.args, sid, &ping)
			go wk()

			if tt.args.mode&ExclusiveMode != 0 {
				// declare exclusive
				sid, err := chb.Subscribe(tt.args.topic, "", tt.args.mode)
				if err != nil {
					t.Fatalf("Subscribe(), channel B: %v", err)
				}

				wk := worker(t, chb, tt.args, sid, &ping)
				go wk()
			}

			mid := uuid.New()
			ack, res, err := chb.Publish(tt.args.topic, &api.Message{Id: mid.String(), Body: []byte("1")}, nil, tt.args.wait)
			if err != nil {
				t.Fatal(err)
			}
			wg.Wait()

			if tt.args.mode&(RPCMode|ExclusiveMode) == RPCMode {
				if ack != 1 {
					t.Errorf("Publish(), got ack = %d, want act = 1", ack)
				}

				if res.GetCorId() != mid.String() {
					t.Errorf("Publish(), got corId = %s, want corId = %s", res.CorId, mid.String())
				}
			}
		})
	}
}

func TestPublishTagRouting(t *testing.T) {
	// instatinate exchange
	ex := New()

	cha := NewChannel()
	ex.AddChannel(cha)

	// tie from exchange channel
	chb := NewChannel()
	ex.AddChannel(chb)

	var wg sync.WaitGroup

	p2sub := func(ch *Channel, ping *int, tag string) {
		// declare exclusive
		sid, err := ch.Subscribe("foo", tag, 0)
		if err != nil {
			t.Error(err)
			return
		}

		msg := ch.Consume(sid)
		defer func() {
			ch.StopConsume(sid)
			wg.Done()
		}()

		wg.Done()

		for {
			m := <-msg
			*ping++

			if string(m.Body) == "2" {
				break
			}
		}
	}

	var pa, pb int

	//t.Log("prepare consume")
	wg.Add(2)
	go p2sub(cha, &pa, "cha")
	go p2sub(chb, &pb, "chb")
	wg.Wait()

	//t.Log("first push")
	pub := NewChannel()
	ex.AddChannel(pub)

	wg.Add(2)
	_, _, err := pub.Publish("foo", &api.Message{Body: []byte("1")}, []string{"cha"}, false)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = pub.Publish("foo", &api.Message{Body: []byte("1")}, []string{"chb"}, false)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = pub.Publish("foo", &api.Message{Body: []byte("2")}, []string{"cha", "chb"}, false)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if pa != pb && pa != 2 {
		t.Error("expected 2")
	}
}

func BenchmarkFanout(b *testing.B) {
	// instatinate exchange
	ex := New()
	stop := make(chan struct{})
	chBuf := make(chan struct{}, 2000)

	sub := func(i int) {
		for ; i > 0; i-- {
			ch := NewChannel()
			ex.AddChannel(ch)

			sid, err := ch.Subscribe("foo-bench", "", FanoutMode)
			if err != nil {
				b.Fatal(err)
			}

			wk := func(ch *Channel, msg <-chan *api.Message) {
				defer func() {
					ex.CloseChannel(ch.token)
					<-chBuf
				}()

				for {
					select {
					case <-msg:
					case <-stop:
						return
					}
				}
			}

			go wk(ch, ch.Consume(sid))
			chBuf <- struct{}{}
		}
	}

	unsub := func() {
		for i := len(chBuf); i > 0; i-- {
			stop <- struct{}{}
		}
	}

	pb := NewChannel()
	ex.AddChannel(pb)

	b.ResetTimer()
	for k := 0.; k <= 10; k++ {
		n := int(math.Pow(2, k))

		b.StopTimer()
		sub(n)
		b.StartTimer()

		b.Run(fmt.Sprintf("consumers/%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				pb.Publish("foo-bench", &api.Message{}, nil, false)
			}
		})

		b.StopTimer()
		unsub()
		b.StartTimer()
	}
}

//
func BenchmarkRPC(b *testing.B) {
	ex := New()

	cons := NewChannel()
	ex.AddChannel(cons)

	sid, err := cons.Subscribe("foo-bench-rpc", "", RPCMode)
	if err != nil {
		b.Fatal(err)
	}

	go func(ch *Channel, msg <-chan *api.Message) {
		defer func() {
			ex.CloseChannel(ch.token)
		}()

		for {
			select {
			case m := <-msg:
				cons.Publish("foo-bench-rpc", &api.Message{CorId: m.GetId()}, nil, false)
			}
		}
	}(cons, cons.Consume(sid))

	stop := make(chan struct{})
	chBuf := make(chan struct{}, 2000)
	msgs := make(chan *api.Message, 2000)

	pub := func(i int) {
		for ; i > 0; i-- {
			ch := NewChannel()
			ex.AddChannel(ch)

			wk := func(ch *Channel) {
				defer func() {
					ex.CloseChannel(ch.token)
					<-chBuf
				}()

				for {
					select {
					case msg := <-msgs:
						ch.Publish("foo-bench-rpc", msg, nil, true)
					case <-stop:
						return
					}
				}
			}

			go wk(ch)
			chBuf <- struct{}{}
		}
	}

	unpub := func() {
		for i := len(chBuf); i > 0; i-- {
			stop <- struct{}{}
		}
	}

	b.ResetTimer()
	for k := 0.; k <= 8; k++ {
		n := int(math.Pow(2, k))

		b.StopTimer()
		pub(n)
		b.StartTimer()

		b.Run(fmt.Sprintf("publish/%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				msgs <- &api.Message{}
			}
		})

		b.StopTimer()
		unpub()
		b.StartTimer()
	}
}
