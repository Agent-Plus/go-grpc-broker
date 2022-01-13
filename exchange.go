package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	uuid "github.com/google/uuid"
)

var (
	// ErrAttempExceeded is raised when consumer cannot receive message and attempts to deliver were exceeded
	ErrAttempExceeded = errors.New("attempt exceeded")

	// ErrChangeTopicMode is raised when new subscriber tries to change existing topic mode
	ErrChangeTopicMode = errors.New("cannot change topic mode")

	// ErrChannelNotConsumed is raised in the topics with none FanoutMode:
	// - publisher is not consumed to the topic with ExclusiveMode
	// - there is no consumer in the topic with RPCMode or ExclusiveMode
	ErrChannelNotConsumed = errors.New("channel not consumed")

	// ErrDeliveryTimeout is raised when consumer channel does not pull message in time
	ErrDeliveryTimeout = errors.New("delivery timeout")

	// ErrInvalidUserPass is raised on invalid authentication: unknown user or wrong password
	ErrInvalidUserPass = errors.New("invalid users name or password")

	// ErrSubscribeStandAloneChannel rejects `Subscribe` action for the channel which is not in the `Exchange` scope.
	ErrStandAloneChannel = errors.New("stand alone channel")

	// ErrSubscribeRPCFull is raised when subscription attempt is rejected at any reason
	ErrSubscribeRejected = errors.New("subscribe rejected")

	// ErrUknonwChannel is raised on attempt to retreive uknown channel by token identifier from registry
	ErrUknonwChannel = errors.New("uknown channel")

	// ErrUknonwToken is raised metadata does not contain required token
	ErrUknonwToken = errors.New("uknown token")

	// ErrUknonwTopic is raised on attempt to retreive uknown topic
	ErrUnknonwTopic = errors.New("unknown topic")

	// ErrWaitTimeout is raised on attempt to push to the blocked channel
	ErrWaitTimeout = errors.New("wait timeout")
)

// Exchange represents the collection of the client channels and topics with their subscribers.
type Exchange struct {
	channels *kvStore
	subscriptions
}

// New creates Exchange
func New() *Exchange {
	return &Exchange{
		//Authenticator: auth,
		channels: newkvStore(),
		subscriptions: subscriptions{
			subsr: make(map[string]*topic),
		},
	}
}

// Channel retreives channel by its GID token
func (ex *Exchange) Channel(tk string) (*Channel, error) {
	id, err := uuid.Parse(tk)
	if err != nil {
		return nil, err
	}

	ex.channels.Lock()
	ch := ex.channels.channel(id)
	ex.channels.Unlock()

	if ch == nil {
		return nil, ErrUknonwChannel
	}
	return ch, nil
}

// AddChannel appends given Channel to the exchange scope
func (ex *Exchange) AddChannel(ch *Channel) {
	if bytes.Equal(ch.token[:], uuid.Nil[:]) {
		ch.token = uuid.New()
	}
	ch.ex = ex

	ex.channels.Lock()
	ex.channels.add(ch)
	ex.channels.Unlock()
}

func (ex *Exchange) CloseChannel(id uuid.UUID) {
	if bytes.Equal(id[:], uuid.Nil[:]) {
		return
	}

	ex.channels.Lock()
	ch := ex.channels.channel(id)
	ex.channels.Unlock()

	if ch == nil {
		return
	}

	// stop listening
	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	// gather all topics where channel is subscribed.
	// drop read
	tps := make([]string, 0, len(ch.consumes))
	for id, d := range ch.consumes {
		// drop ready
		ch.closeConsume(id)
		// delete object
		delete(ch.consumes, id)
		// store topic name
		tps = append(tps, d.tpName)
	}

	// remove all subscriptions
	for _, name := range tps {
		tp := ex.topic(name)

		tp.dlv.walk(func(idx int, d *delivery) error {
			if d != nil && bytes.Equal(d.chId[:], ch.token[:]) {
				tp.dlv.Lock()
				tp.dlv.removeAt(idx)
				tp.dlv.Unlock()
			}

			return nil
		}, nil, nil)
	}

	// remove from registry
	ex.channels.Lock()
	ex.channels.remove(ch)
	ex.channels.Unlock()
	// clear exchange pointer
	ch.ex = nil
}

func (ex *Exchange) send(ctx context.Context, pb *publisher) (err error) {
	tp := ex.topic(pb.topic)
	if tp == nil {
		err = ErrUnknonwTopic
		return
	}

	switch tp.mode & (RPCMode | ExclusiveMode) {
	case 0:
		err = tp.doFanout(ctx, pb)
	default:
		err = tp.doRPC(ctx, pb)

		if err == nil && (tp.mode&(RPCMode|ExclusiveMode)) != 0 && pb.ack == 0 {
			err = fmt.Errorf("target topic=(%s): %w", tp.name, ErrChannelNotConsumed)
		}
	}

	return
}

// Validate implements Authenticator interface
func (da *DummyAuthentication) Validate(client, secret string) (ok bool, err error) {
	if len(da.users[client]) == 0 {
		if da.strict {
			err = ErrInvalidUserPass
		}

		return !da.strict, err
	}

	if pass, _ := da.users[client]; pass != secret {
		return false, ErrInvalidUserPass
	}

	return true, nil
}

// CircuitErrors represents the collection of the errors heppends during loop.
// This CircuitErrors can be raised while processing consumers circuit in the topic
// and some receiver taked long time to receive message.
//
// Maybe used as warning or debug information.
type CircuitErrors struct {
	sync.Mutex
	err []error
}

// Error implements errors interface
func (e *CircuitErrors) Error() string {
	var buf strings.Builder
	for _, v := range e.err {
		buf.WriteString(v.Error())
		buf.WriteString("\n")
	}
	return buf.String()
}
