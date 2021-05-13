package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

// topic represents container, where each subscriber will receive message sent by publisher.
// topic keeps modes: fanout or exclusive. The fanout mode push to each
// receiver except publisher. The exclusive mode means one-to-one communication like RPC.
type topic struct {
	channels *sliceStore

	// topic is exclusive to use as RPC
	exclusive bool

	// topic name
	name string

	// message time to live
	ttl time.Duration
}

func (tp *topic) send(ctx context.Context, pb *publisher) error {
	tm := time.NewTimer(tp.ttl)
	// don't start immediately
	tm.Stop()

	var errStack *CircuitErrors
	if !tp.exclusive {
		errStack = &CircuitErrors{}
	}

	for _, ch := range tp.channels.registry {
		//fmt.Println(ch.token.String())
		// skip self receiver
		if uuid.Equal(ch.token, pb.channel.token) {
			continue
		}

		tm.Reset(tp.ttl)
		err := ch.send(ctx, pb.request, tm.C)
		tm.Stop()

		if err != nil {
			if tp.exclusive {
				return err
			} else {
				if ln, cp := len(errStack.err), cap(errStack.err); ln == cp {
					buf := make([]error, ln, ln+5)
					copy(buf, errStack.err)
					errStack.err = buf
				}

				errStack.err = append(
					errStack.err,
					fmt.Errorf("topic=(%s), channel=(%s): %w", tp.name, ch.token.String(), err),
				)

				// TODO: maybe for the fanout this case should raise StopConsume channel?..
			}
		} else {
			pb.ack++
		}

	}

	if errStack != nil && len(errStack.err) > 0 {
		return errStack
	}

	return nil
}

// topics represents storage of the declared topics by consumers and publishers with self race control.
type subscriptions struct {
	// to control just write action
	sync.RWMutex

	subsr map[string]*topic
}

func (s *subscriptions) topic(name string) *topic {
	s.RLock()
	tp, _ := s.subsr[name]
	s.RUnlock()

	return tp
}

func (s *subscriptions) subscribe(ch *Channel, name string, exc bool) error {
	tp := s.topic(name)

	if tp == nil {
		tp = &topic{
			channels:  newSliceStore(),
			exclusive: exc,
			name:      name,
			ttl:       10 * time.Millisecond,
		}

		s.Lock()
		s.subsr[name] = tp
		s.Unlock()
	}

	if tp.exclusive {
		tp.channels.Lock()
		ln := tp.channels.len()
		tp.channels.Unlock()

		if ln == 2 {
			return ErrSubscribeRPCFull
		}
	}

	tp.channels.Lock()
	if idx := tp.channels.index(ch.token); idx < 0 {
		tp.channels.add(ch)
	}
	tp.channels.Unlock()

	return nil
}

func (s *subscriptions) unsubscribe(id uuid.UUID, name string) {
	tp := s.topic(name)

	tp.channels.Lock()
	if ch := tp.channels.remove(id); ch != nil {
		ch.StopConsume()
	}
	tp.channels.Unlock()
}
