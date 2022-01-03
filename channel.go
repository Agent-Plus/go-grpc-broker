package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Agent-Plus/go-grpc-broker/api"

	uuid "github.com/satori/go.uuid"
)

type atomicBool int32

func (b *atomicBool) isSet() bool { return atomic.LoadInt32((*int32)(b)) != 0 }
func (b *atomicBool) setTrue()    { atomic.StoreInt32((*int32)(b), 1) }
func (b *atomicBool) setFalse()   { atomic.StoreInt32((*int32)(b), 0) }

// Channel represents adapter to pull message or to push
// message to others channels of the exchange topic.
type Channel struct {
	mutex sync.RWMutex

	// consumes represents the collection of the delivery properties
	// where this channel is ssubscribed through Subscribe call.
	consumes map[uuid.UUID]*delivery

	// client identifier
	cid string

	// exchange
	ex *Exchange

	// determines push attemption time to wait busy channel
	optTTL time.Duration

	// tag
	tag string

	// authorization token and identifier of the channel
	token uuid.UUID
}

// NewChannel allocates new channel to keep communication.
func NewChannel() *Channel {
	return &Channel{
		optTTL:   10 * time.Second,
		consumes: make(map[uuid.UUID]*delivery),
	}
}

// Consumes creates channel to pull messages from the subscription,
// given id is the subscription identifier made by Subscribe call.
func (ch *Channel) Consume(id uuid.UUID) <-chan *api.Message {
	ch.mutex.Lock()
	dlv, ok := ch.consumes[id]
	ch.mutex.Unlock()

	if ok {
		// close previous
		if dlv.ready.isSet() {
			close(dlv.dlv)
		}

		dlv.dlv = make(chan *api.Message)
		dlv.ready.setTrue()

		return dlv.dlv
	}

	return nil
}

type publisher struct {
	// acknowledgement counter, shows success writes to the topic channels
	ack int

	// channel sender
	channel *Channel

	// message
	request *api.Message

	// tags
	tags []string

	// topic name
	topic string
}

// Publish sends given message to the given topic
func (ch *Channel) Publish(name string, msg *api.Message, tags []string) (int, error) {
	// set daedline for the whole operation
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if ch.ex != nil {
		pb := &publisher{
			channel: ch,
			request: msg,
			tags:    tags,
			topic:   name,
		}
		err := ch.ex.send(ctx, pb)

		return pb.ack, err
	}

	return 0, ErrStandAloneChannel
}

// StopConsume marks channel not ready to receive messages,
// but does not remove subscription.
func (ch *Channel) StopConsume(id uuid.UUID) {
	ch.mutex.Lock()
	ch.closeConsume(id)
	ch.mutex.Unlock()
}

func (ch *Channel) closeConsume(id uuid.UUID) {
	if dlv, ok := ch.consumes[id]; ok && dlv.ready.isSet() {
		dlv.ready.setFalse()
		close(dlv.dlv)
	}
}

// Subscribe appends channel to the topic with given name. This action reasonable only in the Exchange collection scope,
// which is provided only through the Exchange.NewChannel() allocation.
// Subscriptions may be two types according given `exc` parameter:
// - (default: false) fanout, when all members of the topic recieve message except sender;
// - (true) RPC, topic must have only two members to exchange with messages.
// Subscribed channel can receive messages just mark with tag, this is additional filter
// in the topic group.
func (ch *Channel) Subscribe(name, tag string, mode modeType) (uuid.UUID, error) {
	if ch.ex == nil {
		return uuid.UUID{}, ErrStandAloneChannel
	}

	return ch.ex.subscribe(ch, name, tag, mode)
}

func (ch *Channel) Token() string {
	return ch.token.String()
}

// kvStore represents key-value store of the channels with manager to control race.
type kvStore struct {
	sync.Mutex
	registry map[uuid.UUID]*Channel
}

func newkvStore() *kvStore {
	return &kvStore{
		registry: make(map[uuid.UUID]*Channel),
	}
}

func (s *kvStore) channel(gid uuid.UUID) *Channel {
	ch, _ := s.registry[gid]
	return ch
}

func (s *kvStore) add(ch *Channel) {
	s.registry[ch.token] = ch
}

func (s *kvStore) len() int {
	return len(s.registry)
}

func (s *kvStore) remove(ch *Channel) {
	delete(s.registry, ch.token)
}
