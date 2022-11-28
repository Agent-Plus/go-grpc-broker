package broker

import (
	"context"
	"fmt"
	"sync"

	uuid "github.com/google/uuid"
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
// topic keeps modes: fanout or rpc and/or exclusive. The fanout mode push to each
// receiver except publisher. The rpc mode means one consumer.
type topic struct {
	// delivery registry
	dlv *deliveryStore

	// topic identifier
	id uuid.UUID

	// topic mode
	mode modeType

	// topic name
	name string
}

func (tp *topic) doFanout(ctx context.Context, pb *publisher) error {
	// prepare pool with
	dp := newDeliveryPool(4, 60, 20)
	defer dp.Close()
	// start pool hadnlers
	dp.start()

	tp.dlv.walk(func(idx int, dlv *delivery) error {
		dp.schedule(dp.wrapTask(ctx, pb, dlv))
		return nil
	}, &pb.channel.token, pb.tags)

	dp.WaitWithContext(ctx)
	return dp.Err()
}

func (tp *topic) doRPC(ctx context.Context, pb *publisher) error {
	if (tp.mode & ExclusiveMode) != 0 {
		var fnd bool

		pb.channel.mutex.Lock()
		for _, dlv := range pb.channel.consumes {
			if dlv.tpName == tp.name && dlv.ready.isSet() {
				fnd = true
				break
			}
		}
		pb.channel.mutex.Unlock()

		if !fnd {
			return fmt.Errorf("publish to exclusive topic=(%s), publisher: %w", tp.name, ErrChannelNotConsumed)
		}
	}

	return tp.dlv.walk(func(idx int, dlv *delivery) error {
		wt := waitingChanStore(pb, dlv.chId, tp.mode)
		if wt != nil {
			wt.add(pb.request.Id, pb.recv)
		}

		err := dlv.send(ctx, pb.request, nil)
		if err != nil && wt != nil {
			wt.delete(pb.request.Id)
		}
		if err == nil {
			pb.ackAdd()
		}

		return err
	}, &pb.channel.token, pb.tags)
}

func waitingChanStore(pb *publisher, chId uuid.UUID, tMode modeType) *waitStore {
	if tMode&(RPCMode|ExclusiveMode) != RPCMode || !pb.wait {
		return nil
	}

	if ex := pb.channel.ex; ex != nil {
		ex.channels.Lock()
		defer ex.channels.Unlock()

		if ch := ex.channels.channel(chId); ch != nil {
			return ch.waiting
		}
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

				if mode != tp.mode {
					return d.id, fmt.Errorf("topic mode=(%d), requested=(%d): %w", tp.mode, mode, ErrChangeTopicMode)
				}

				return d.id, nil
			}

			cnt++
		}
		tp.dlv.Unlock()

		// RPC flags are set
		if cnt > 0 && (tp.mode&(RPCMode|ExclusiveMode)) == RPCMode {
			return uuid.UUID{}, fmt.Errorf("topic=(%s), mode=(%d), has consumer: %w", tp.name, tp.mode, ErrSubscribeRejected)
		} else if cnt > 1 && (tp.mode&ExclusiveMode) != 0 {
			return uuid.UUID{}, fmt.Errorf("topic=(%s), mode=(%d), has consumer: %w", tp.name, tp.mode, ErrSubscribeRejected)
		}
	}

	// create delivery container
	dlv := &delivery{
		id:     uuid.New(),
		chId:   ch.token,
		tpName: name,
		tag:    tag,
	}

	// Add meta data to the channel about prepared subscription,
	// this information is short step to start consume.
	ch.mutex.Lock()
	ch.consumes[dlv.id] = dlv
	ch.mutex.Unlock()

	if (tp.mode & (RPCMode | ExclusiveMode)) == RPCMode {
		ch.waiting = newWaitStore()
	}

	// Append to the topic registry prepared consumer
	tp.dlv.Lock()
	tp.dlv.add(dlv)
	tp.dlv.Unlock()

	return dlv.id, nil
}

func (s *subscriptions) unsubscribe(ch *Channel, id uuid.UUID) {
	dlv, ok := ch.consumes[id]
	if !ok {
		return
	}

	ch.closeConsume(id)

	delete(ch.consumes, dlv.id)

	// Remove from topic
	tp := s.topic(dlv.tpName)
	if tp == nil {
		return
	}

	tp.dlv.Lock()
	tp.dlv.remove(dlv.id)
	rl := tp.dlv.len()
	tp.dlv.Unlock()

	if (tp.mode&(RPCMode|ExclusiveMode)) != 0 && rl == 0 {
		s.Lock()
		delete(s.subsr, tp.name)
		s.Unlock()

		tp.dlv = nil
	}
}
