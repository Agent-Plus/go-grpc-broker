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
	ErrSubscribeExclusiveFull = errors.New("exclusive topic is full")

	// ErrPublishExclusiveNotConsumed is raised on `Publish` to exclusive topic where topic hasn't receiver
	ErrPublishExclusiveNotConsumed = errors.New("exclusive topic not consumed")

	// ErrSubscribeStandAloneChannel rejects `Subscribe` action for the channel which is not in the `Exchange` scope.
	ErrStandAloneChannel = errors.New("stand alone channel")

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

// AddChannel appends given Channel to the exchange scope
func (ex *Exchange) AddChannel(ch *Channel) {
	if uuid.Equal(ch.token, uuid.UUID{}) {
		ch.token = uuid.NewV4()
	}
	ch.ex = ex

	ex.channels.Lock()
	ex.channels.add(ch)
	ex.channels.Unlock()
}

func (ex *Exchange) CloseChannel(id uuid.UUID) {
	if uuid.Equal(id, uuid.UUID{}) {
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
	tps := make([]string, 0, len(ch.pool))
	for id, d := range ch.pool {
		// drop ready
		ch.closeConsume(id)
		// delete object
		delete(ch.pool, id)
		// store topic name
		tps = append(tps, d.tpName)
	}

	// remove all subscriptions
	for _, name := range tps {
		tp := ex.topic(name)

		for idx, d := range tp.dlv.registry {
			if d != nil && !uuid.Equal(d.chId, ch.token) {
				continue
			}

			tp.dlv.Lock()
			tp.dlv.removeAt(idx)
			tp.dlv.Unlock()
		}
	}

	// remove from registry
	ex.channels.Lock()
	ex.channels.remove(ch)
	ex.channels.Unlock()
	// clear exchange pointer
	ch.ex = nil
}

func (ex *Exchange) send(ctx context.Context, pb *publisher) error {
	tp := ex.topic(pb.topic)
	if tp == nil {
		return ErrUnknonwTopic
	}

	if tp.exclusive {
		fnd := 0

		tp.dlv.Lock()
		rgs := tp.dlv.registry
		tp.dlv.Unlock()

		for _, d := range rgs {
			if uuid.Equal(pb.channel.token, d.chId) {
				// sender is subscribed to this exclusive, next step is available
				fnd++
				break
			}
		}

		if fnd == 0 {
			return ErrNotSubscribedExclusive
		}
	}

	tp.dlv.Lock()
	err := tp.send(ctx, pb)
	tp.dlv.Unlock()

	if err == nil && tp.exclusive && pb.ack == 0 {
		err = ErrPublishExclusiveNotConsumed
	}

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
