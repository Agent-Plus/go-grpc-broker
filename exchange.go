package broker

import (
	"context"
	"errors"
	"strings"

	uuid "github.com/satori/go.uuid"
)

var (
	// ErrChannelNotConsumed is raised on sending to channel with flag false `pulling`
	ErrChannelNotConsumed = errors.New("channel not consumed")

	// ErrInvalidUserPass is raised on invalid authentication: unknown user or wrong password
	ErrInvalidUserPass = errors.New("invalid users name or password")

	// ErrNotSubscribedExclusive is raised on `Publish` action when publisher attempts to write
	// to the exclusive channel without subscription to its
	ErrNotSubscribedExclusive = errors.New("not subscribed to exclusive")

	// ErrSubscribeRPCFull is raised to reject more than two subscriptions to the exclusive channel
	ErrSubscribeRPCFull = errors.New("exclusive topic is full")

	// ErrSubscribeStandAloneChannel rejects `Subscribe` action for the channel which is not in the `Exchange` scope.
	ErrSubscribeStandAloneChannel = errors.New("cannot subscribe stand alone channel")

	// ErrUknonwChannel is raised on attempt to retreive uknown channel by token identifier from registry
	ErrUknonwChannel = errors.New("uknown channel")

	// ErrUknonwTopic is raised on attempt to retreive uknown topic
	ErrUknonwTopic = errors.New("uknown topic")

	// ErrWaitTimeout is raised on attempt to push to the blocked channel
	ErrWaitTimeout = errors.New("wait timeout")
)

// Exchange represents the collection of the client channels and topics with their subscribers.
type Exchange struct {
	channels *mapStore
	subscriptions
}

// New creates Exchange
func New() *Exchange {
	return &Exchange{
		//Authenticator: auth,
		channels: newMapStore(),
		subscriptions: subscriptions{
			subsr: make(map[string]*topic),
		},
	}
}

// Channel retreives channel by its GID token
func (ex *Exchange) Channel(tk string) (*Channel, error) {
	id, err := uuid.FromString(tk)
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

// NewChannel creates new Channel in the exchange scope
func (ex *Exchange) NewChannel() *Channel {
	ch := NewChannel()
	ch.ex = ex

	ex.channels.Lock()
	ex.channels.add(ch)
	ex.channels.Unlock()

	return ch
}

func (ex *Exchange) send(ctx context.Context, pb *publisher) error {
	tp := ex.topic(pb.topic)
	if tp == nil {
		return ErrUknonwTopic
	}

	if tp.exclusive {
		if ch := tp.channels.channel(pb.channel.token); ch == nil {
			return ErrNotSubscribedExclusive
		}
	}

	tp.channels.Lock()
	err := tp.send(ctx, pb)
	tp.channels.Unlock()

	return err
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
// This CircuitErrors can be raised while going circute of the topic subscribers
// and some receiver taked long time to receive message.
//
// Maybe used as warning or debug information.
type CircuitErrors struct {
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