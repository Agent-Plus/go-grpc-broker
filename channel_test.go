package broker

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Agent-Plus/go-grpc-broker/api"
	uuid "github.com/satori/go.uuid"
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
				t.Fatal(err)
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

	_, err := pub.Publish("foo", &api.Message{}, nil)
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

	var wg sync.WaitGroup

	for i := 0; i < 11; i++ {
		wg.Add(1)
		go func(i int) {
			ch := NewChannel()
			ex.AddChannel(ch)

			// subscribe receiver
			id, err := ch.Subscribe("foo", "", 0)
			if err != nil {
				t.Fatal(err)
			}
			defer ch.StopConsume(id)

			msg := ch.Consume(id)
			wg.Done()

			if (i % 5) == 0 {
				tmmtx.Lock()
				tmwbuf[ch.Token()] = 0
				tmmtx.Unlock()
				for {
				}
			} else {
				for {
					<-msg
				}
			}
		}(i)
	}

	wg.Wait()

	pb := NewChannel()
	ex.AddChannel(pb)

	for i := 0; i < 10; i++ {
		ack, err := pb.Publish("foo", &api.Message{}, nil)

		if err != nil {
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
			gotErr:  ErrSubscribeRCPFull,
			wantErr: true,
		},
		{
			name: "Exclusive mode with two pulling channels",
			args: args{
				mode:  RPCMode | ExclusiveMode,
				topic: "exclusive-me",
			},
			gotErr:  ErrSubscribeRCPFull,
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

			if (tt.args.mode&ExclusiveMode) == 0 && err != tt.gotErr {
				t.Errorf("Subscribe(), Channel B, err = %v, want error = %v", err, tt.gotErr)
			}

			// one more
			if tt.args.mode&ExclusiveMode != 0 {
				chc := NewChannel()
				ex.AddChannel(chc)
				// declare exclusive
				_, err := chc.Subscribe(tt.args.topic, "", tt.args.mode)

				if err != tt.gotErr {
					t.Errorf("Subscribe(), Channel C, err = %v, want error = %v", err, tt.gotErr)
				}
			}
		})
	}
}

func TestExclusiveConsumingError(t *testing.T) {
	// instatinate exchange
	ex := New()

	// channel A
	cha := NewChannel()
	ex.AddChannel(cha)

	// declare exclusive
	if _, err := cha.Subscribe("foo", "", (RPCMode | ExclusiveMode)); err != nil {
		t.Fatal(err)
	}
	// channel A will not pull data

	// channel B
	chb := NewChannel()
	ex.AddChannel(chb)

	if _, err := chb.Subscribe("foo", "", (RPCMode | ExclusiveMode)); err != nil {
		t.Fatal(err)
	}

	_, err := chb.Publish("foo", &api.Message{}, nil)

	if err != ErrPublishExclusiveNotConsumed {
		t.Errorf("expected error: %v, but got: %v", ErrPublishExclusiveNotConsumed, err)
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
	_, err = chb.Publish("foo", &api.Message{}, nil)
	wg.Wait()

	if err != ErrNotSubscribedExclusive {
		t.Errorf("expected error: %v, but got: %v", ErrNotSubscribedExclusive, err)
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
	_, err = chb.Publish("foo", &api.Message{}, nil)

	if err != ErrNotSubscribedExclusive {
		t.Error("expected error: ", ErrNotSubscribedExclusive)
	}
}

func TestExchlusiveChannelDialog(t *testing.T) {
	// instatinate exchange
	ex := New()

	// channel A
	cha := NewChannel()
	ex.AddChannel(cha)

	// channel B
	chb := NewChannel()
	ex.AddChannel(chb)

	var wg sync.WaitGroup
	var stop atomicBool
	var pa, pb int

	p2p := func(ch *Channel, ping *int, who string) {
		// declare exclusive
		sid, err := ch.Subscribe("foo-ping-rpc", "", (RPCMode | ExclusiveMode))
		if err != nil {
			t.Fatal(err)
		}

		msg := ch.Consume(sid)
		defer func() {
			ch.StopConsume(sid)
			wg.Done()
		}()

		wg.Done()

		for {
			select {
			case m := <-msg:
				i, err := strconv.Atoi(string(m.Body))
				if err != nil {
					t.Fatal(err)
				}

				i++
				*ping = i

				if i > 4 {
					stop.setTrue()
					return
				}

				ack, err := ch.Publish("foo-ping-rpc", &api.Message{Body: []byte(strconv.Itoa(i))}, nil)
				if ack != 1 {
					t.Fatal("excpected ack=1 but got: ", ack)
				}
				if err != nil {
					t.Fatal(err)
				}

			default:
				if stop.isSet() {
					return
				}
			}
		}
	}

	//t.Log("prepare consume")
	wg.Add(2)
	go p2p(cha, &pa, "cha")
	go p2p(chb, &pb, "chb")
	wg.Wait()

	//t.Log("first push")
	wg.Add(2)
	_, err := chb.Publish("foo-ping-rpc", &api.Message{Body: []byte("1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if pa != 4 {
		t.Error("channel `a` must do 4 pushes")
	}

	if pb != 5 {
		t.Error("channel `b` must do 5 pushes")
	}
}

func BenchmarkFanout_1000ch(b *testing.B) {
	// instatinate exchange
	ex := New()
	ch := NewChannel()
	ex.AddChannel(ch)

	var wg sync.WaitGroup

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(ch *Channel, i int) {

			// declare exclusive
			sid, err := ch.Subscribe("foo-bench", "", 0)
			if err != nil {
				b.Fatal(err)
			}
			defer ch.StopConsume(sid)

			msg := ch.Consume(sid)
			wg.Done()

			for {
				<-msg
			}
		}(ch, i)
	}

	wg.Wait()

	pb := NewChannel()
	ex.AddChannel(pb)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pb.Publish("foo-bench", &api.Message{}, nil)
		b.StopTimer()
		if err != nil {
			if _, ok := err.(*CircuitErrors); ok {
				b.Log(err)
			} else {
				b.Fatal(err)
			}
		}
		b.StartTimer()
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
			t.Fatal(err)
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
	_, err := pub.Publish("foo", &api.Message{Body: []byte("1")}, []string{"cha"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = pub.Publish("foo", &api.Message{Body: []byte("1")}, []string{"chb"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = pub.Publish("foo", &api.Message{Body: []byte("2")}, []string{"cha", "chb"})
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if pa != pb && pa != 2 {
		t.Error("expected 2")
	}
}

//func BenchmarkFanoutSliceStack(b *testing.B) {
//	// predefine
//	st := newSliceStore()
//	for i := 0; i < 10; i++ {
//		st.add(NewChannel())
//	}
//
//	addch := make(chan *Channel)
//	rmch := make(chan uuid.UUID)
//
//	go func() {
//		for {
//			ch := <-addch
//
//			st.Lock()
//			st.add(ch)
//			st.Unlock()
//		}
//	}()
//
//	go func() {
//		for {
//			ch := <-rmch
//			st.Lock()
//			st.remove(ch)
//			st.Unlock()
//		}
//	}()
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		b.StopTimer()
//		ch := NewChannel()
//		b.StartTimer()
//
//		addch <- ch
//
//		if (i % 3) == 0 {
//			b.StopTimer()
//			st.Lock()
//			idx := st.len() - 5
//			tk := st.registry[idx].token
//			st.Unlock()
//			b.StartTimer()
//
//			rmch <- tk
//		}
//	}
//}
//
