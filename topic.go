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
	dlv *deliveryStore

	// topic identifier
	id uuid.UUID

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

	for _, d := range tp.dlv.registry {
		if !d.ready.isSet() {
			// delivery container is not pulled
			continue
		}

		// skip self receiver for the exclusive topic
		if tp.exclusive {
			if uuid.Equal(d.chId, pb.channel.token) {
				continue
			}

		}

		if len(pb.tags) > 0 && !d.isTag(pb.tags) {
			continue
		}

		tm.Reset(tp.ttl)
		err := d.send(ctx, pb.request, tm.C)
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
					fmt.Errorf("topic=(%s): %w", tp.name, err),
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
	s.Lock()
	tp, _ := s.subsr[name]
	s.Unlock()

	return tp
}

func (s *subscriptions) subscribe(ch *Channel, name, tag string, exc bool) (uuid.UUID, error) {
	tp := s.topic(name)

	if tp == nil {
		tp = &topic{
			dlv:       newDeliveryStore(),
			exclusive: exc,
			name:      name,
			ttl:       10 * time.Millisecond,
		}

		s.Lock()
		s.subsr[name] = tp
		s.Unlock()
	}

	// exclusive topic means only two subscribers to communicate,
	// topic is RPC bus
	if tp.exclusive {
		cnt := 0
		tp.dlv.Lock()
		for _, d := range tp.dlv.registry {
			if _, ok := ch.pool[d.id]; ok {
				tp.dlv.Unlock()
				return d.id, nil
			}

			cnt++
		}
		tp.dlv.Unlock()

		if cnt < 2 {
			// ok
		} else {
			return uuid.UUID{}, ErrSubscribeExclusiveFull
		}
	}

	// create delivery container
	dlv := &delivery{
		id:     uuid.NewV4(),
		chId:   ch.token,
		tpName: name,
		tag:    tag,
	}

	// add to channel pool of the delivery
	ch.mutex.Lock()
	ch.pool[dlv.id] = dlv
	ch.mutex.Unlock()

	// add to the topic delivery registry
	tp.dlv.Lock()
	tp.dlv.add(dlv)
	tp.dlv.Unlock()

	return dlv.id, nil
}
