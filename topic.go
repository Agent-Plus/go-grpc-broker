package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

// modeType describes subscription type
type modeType uint8

// Fanout is default subscription mode where each consumer recieves message
const FanoutMode modeType = 0

const (
	// RPCMode is subscription where in the topic is one consumer and many publishers
	RPCMode modeType = 1 << iota

	// ExclusiveMode is RPC where conversation one to one
	ExclusiveMode
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

	mode modeType

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
			// delivery container is not ready to consume messages
			continue
		}

		// skip self
		if uuid.Equal(d.chId, pb.channel.token) {
			continue
		}

		// exclude those are not in the passed tags list
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

func (s *subscriptions) subscribe(ch *Channel, name, tag string, mode modeType) (uuid.UUID, error) {
	tp := s.topic(name)

	if tp == nil {
		tp = &topic{
			dlv:  newDeliveryStore(),
			mode: mode,
			name: name,
			ttl:  10 * time.Millisecond,
		}

		s.Lock()
		s.subsr[name] = tp
		s.Unlock()
	} else {
		cnt := 0
		tp.dlv.Lock()
		for _, d := range tp.dlv.registry {
			// already subscribed and can consume topic messages
			if _, ok := ch.consumes[d.id]; ok {
				tp.dlv.Unlock()
				return d.id, nil
			}

			cnt++
		}
		tp.dlv.Unlock()

		// RPC flags are set
		if cnt > 0 && (tp.mode&(RPCMode|ExclusiveMode)) == RPCMode {
			return uuid.UUID{}, ErrSubscribeRCPFull
		} else if cnt > 1 && (tp.mode&ExclusiveMode) != 0 {
			return uuid.UUID{}, ErrSubscribeRCPFull
		}
	}

	// create delivery container
	dlv := &delivery{
		id:     uuid.NewV4(),
		chId:   ch.token,
		tpName: name,
		tag:    tag,
	}

	// Add meta data to the channel about prepared subscription,
	// this information is short step to start consume.
	ch.mutex.Lock()
	ch.consumes[dlv.id] = dlv
	ch.mutex.Unlock()

	// Append to the topic registry prepared consumer
	tp.dlv.Lock()
	tp.dlv.add(dlv)
	tp.dlv.Unlock()

	return dlv.id, nil
}
