package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Agent-Plus/go-grpc-broker/api"

	uuid "github.com/satori/go.uuid"
)

func TestFanoutMessages(t *testing.T) {
	// instatinate exchange
	ex := New()

	var wg sync.WaitGroup
	dbuf := struct {
		m    sync.Mutex
		msgs []*api.Message
	}{msgs: make([]*api.Message, 0, 10)}

	chbuf := make([]*Channel, 0, 10)

	for i := 0; i < 10; i++ {
		// tie from exchange channel
		ch := ex.NewChannel()

		// subscribe receiver
		ch.Subscribe("foo", "", false)

		chbuf = append(chbuf, ch)

		wg.Add(1)
		go func(ch *Channel, msg <-chan *api.Message) {
			for m := range msg {
				dbuf.m.Lock()
				dbuf.msgs = append(dbuf.msgs, m)
				dbuf.m.Unlock()
				break
			}

			ch.StopConsume()
			wg.Done()
		}(ch, ch.Consume())
	}

	// tie from exchange channel
	pub := ex.NewChannel()

	_, err := pub.Publish("foo", &api.Message{})
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	if rln := len(dbuf.msgs); rln != len(dbuf.msgs) {
		t.Error("expected 10 responses, but got:", rln)
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
			ch := ex.NewChannel()
			defer ch.StopConsume()

			// subscribe receiver
			ch.Subscribe("foo", "", false)

			msg := ch.Consume()
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

	pb := ex.NewChannel()

	for i := 0; i < 10; i++ {
		ack, err := pb.Publish("foo", &api.Message{})

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

// нужно протестировать rpc подписку, не более 2-х
func TestRPCSubscriptionLimit(t *testing.T) {
	// instatinate exchange
	ex := New()

	// channel A
	cha := ex.NewChannel()

	// declasre exclusive
	if err := cha.Subscribe("foo", "", true); err != nil {
		t.Error(err)
	}

	// channel B
	chb := ex.NewChannel()
	// declare exclusive
	if err := chb.Subscribe("foo", "", true); err != nil {
		t.Error(err)
	}

	// one more
	chc := ex.NewChannel()
	// declare exclusive
	err := chc.Subscribe("foo", "", true)

	if err != ErrSubscribeRPCFull {
		t.Error("expected error:", ErrSubscribeRPCFull)
	}
}

func TestRPCNotpullingConsumers(t *testing.T) {
	// instatinate exchange
	ex := New()

	// channel A
	cha := ex.NewChannel()

	// declasre exclusive
	if err := cha.Subscribe("foo", "", true); err != nil {
		t.Error(err)
	}

	// channel B
	chb := ex.NewChannel()
	_, err := chb.Publish("foo", &api.Message{})

	if err != ErrNotSubscribedExclusive {
		t.Errorf("expected error: %v, but got: %v", ErrNotSubscribedExclusive, err)
	}
}

func TestRPCWithWaitTimeoutError(t *testing.T) {
	// instatinate exchange
	ex := New()

	// channel A
	cha := ex.NewChannel()
	// declasre exclusive
	if err := cha.Subscribe("foo", "", true); err != nil {
		t.Error(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func(ch *Channel, m <-chan *api.Message) {
		rttl, cancel := context.WithDeadline(
			context.Background(),
			time.Now().Add(1*time.Second),
		)
		cancel()
		defer cancel()

		// busy
		time.Sleep(1100 * time.Millisecond)

		select {
		case <-m:
		case <-rttl.Done():
		}

		ch.StopConsume()
		wg.Done()
	}(cha, cha.Consume())

	// channel B
	chb := ex.NewChannel()
	_, err := chb.Publish("foo", &api.Message{})
	wg.Wait()

	if err != ErrNotSubscribedExclusive {
		t.Errorf("expected error: %v, but got: %v", ErrNotSubscribedExclusive, err)
	}
}

// This case tests exclusive channel subscription for the both communicators:
// publisher and consumer must be subscribed to one channel.
func TestRPCUnsubscribedPublishError(t *testing.T) {
	// instatinate exchange
	ex := New()

	// channel A
	cha := ex.NewChannel()

	// declare exclusive
	if err := cha.Subscribe("foo", "", true); err != nil {
		t.Error(err)
	}

	go func(m <-chan *api.Message) {}(cha.Consume())

	// channel B
	chb := ex.NewChannel()
	// publish without subscription
	_, err := chb.Publish("foo", &api.Message{})

	if err != ErrNotSubscribedExclusive {
		t.Error("expected error: ", ErrNotSubscribedExclusive)
	}
}

func BenchmarkFanout_1000ch(b *testing.B) {
	// instatinate exchange
	ex := New()
	var wg sync.WaitGroup

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			ch := ex.NewChannel()

			// declare exclusive
			if err := ch.Subscribe("foo-bench", "", false); err != nil {
				b.Fatal(err)
			}
			defer ch.StopConsume()

			msg := ch.Consume()
			wg.Done()
			for {
				<-msg
			}
		}(i)
	}

	wg.Wait()

	pb := ex.NewChannel()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pb.Publish("foo-bench", &api.Message{})
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

func BenchmarkFanoutSliceStack(b *testing.B) {
	// predefine
	st := newSliceStore()
	for i := 0; i < 10; i++ {
		st.add(NewChannel())
	}

	addch := make(chan *Channel)
	rmch := make(chan uuid.UUID)

	go func() {
		for {
			ch := <-addch

			st.Lock()
			st.add(ch)
			st.Unlock()
		}
	}()

	go func() {
		for {
			ch := <-rmch
			st.Lock()
			st.remove(ch)
			st.Unlock()
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ch := NewChannel()
		b.StartTimer()

		addch <- ch

		if (i % 3) == 0 {
			b.StopTimer()
			st.Lock()
			idx := st.len() - 5
			tk := st.registry[idx].token
			st.Unlock()
			b.StartTimer()

			rmch <- tk
		}
	}
}
