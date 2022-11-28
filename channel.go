package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Agent-Plus/go-grpc-broker/api"

	uuid "github.com/google/uuid"
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

	waiting *waitStore
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

func (ch *Channel) Exchange() (ex *Exchange, err error) {
	ch.mutex.Lock()
	ex = ch.ex
	ch.mutex.Unlock()

	if ex == nil {
		err = ErrStandAloneChannel
	}

	return
}

type publisher struct {
	// acknowledgement counter, shows success writes to the topic channels
	ack int32

	// channel sender
	channel *Channel

	// message
	request *api.Message

	// response message in case wait flag in Publish call
	response chan *api.Message

	// tags
	tags []string

	// topic name
	topic string

	// wait reponse on published message
	// exclusive topic drops this flag in false
	wait bool
}

func (pb *publisher) ackAdd() {
	atomic.AddInt32(&pb.ack, 1)
}

func (pb *publisher) ackLoad() int {
	return int(atomic.LoadInt32(&pb.ack))
}

func (pb *publisher) dest() {
	pb.channel = nil
	pb.request = nil

	if pb.response != nil {
		close(pb.response)
	}
}

func (pb *publisher) recv(msg *api.Message) {
	if pb.response != nil && pb.wait && msg != nil {
		pb.response <- msg
	}
}

func publisherResp(ctx context.Context, pb *publisher) (ack int, msg *api.Message, err error) {
	if !pb.wait {
		ack = pb.ackLoad()
		return
	}

	dn := ctx.Done()

	select {
	case <-dn:
		err = ctx.Err()
	case msg = <-pb.response:
	}

	ack = pb.ackLoad()

	return
}

// Publish sends given message to the given topic
func (ch *Channel) Publish(name string, msg *api.Message, tags []string, wait bool) (int, *api.Message, error) {
	ex, err := ch.Exchange()
	if err != nil {
		return 0, nil, err
	}

	// set daedline for the whole operation
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if len(msg.Id) == 0 {
		msg.Id = uuid.NewString()
	}

	if id := msg.CorId; ch.waiting != nil && len(id) > 0 {
		ack := ch.waiting.do(id, msg)
		return int(ack), nil, nil
	}

	pb := &publisher{
		channel: ch,
		request: msg,
		tags:    tags,
		topic:   name,
		wait:    wait,
	}

	if wait {
		pb.response = make(chan *api.Message, 1)
	}

	defer pb.dest()

	err = ex.send(ctx, pb)
	if err != nil {
		return pb.ackLoad(), nil, err
	}

	return publisherResp(ctx, pb)
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

// Subscribe appends channel to the topic with given name. This action reasonable
// only in the Exchange collection scope and provides  through invoke Exchange.NewChannel().
// Tag marks channel to receive messages published with only with given tag.
// Mode declares new topic with given mode: topic in mode RPC or Exclusive can have only one consumer
// to response on published message or consumed them silently
func (ch *Channel) Subscribe(name, tag string, mode modeType) (uuid.UUID, error) {
	ex, err := ch.Exchange()
	if err != nil {
		return uuid.UUID{}, err
	}

	return ex.subscribe(ch, name, tag, mode)
}

func (ch *Channel) Token() string {
	return ch.token.String()
}

func (ch *Channel) Unsubscribe(id uuid.UUID) error {
	ex, err := ch.Exchange()
	if err != nil {
		return err
	}

	ch.mutex.Lock()
	ex.unsubscribe(ch, id)
	ch.mutex.Unlock()

	return nil
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
