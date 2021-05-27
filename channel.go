package broker

import (
	"context"
	"sync"
	"time"

	"github.com/Agent-Plus/go-grpc-broker/api"

	uuid "github.com/satori/go.uuid"
)

// Channel represents adapter to pull message or to push
// message to others channels of the exchange topic.
type Channel struct {
	mutex sync.RWMutex

	// chanel to pull
	pull chan *api.Message

	// client identifier
	cid string

	// exchange
	ex *Exchange

	// determines push attemption time to wait busy channel
	optTTL time.Duration

	// channel is pulling at the moment
	pulling bool

	// tag
	tag string

	// authorization token and identifier of the channel
	token uuid.UUID
}

// NewChannel allocates new channel to keep communication.
func NewChannel() *Channel {
	return &Channel{
		optTTL: 10 * time.Second,
	}
}

// Consume returns delivering messages channel.
// Receiver must range over the chan to pull all messages.
func (ch *Channel) Consume() <-chan *api.Message {
	ch.mutex.Lock()
	ch.pull = make(chan *api.Message)
	ch.pulling = true
	ch.mutex.Unlock()

	return ch.pull
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

	err := ch.send(ctx, msg, nil)
	if err != nil {
		return 0, err
	}

	return 1, nil
}

// send writes message to the Channel chan buffer with.
func (ch *Channel) send(ctx context.Context, msg *api.Message, tm <-chan time.Time) (err error) {
	ch.mutex.Lock()
	if !ch.pulling {
		ch.mutex.Unlock()
		return ErrChannelNotConsumed
	}
	ch.mutex.Unlock()

	select {
	// message picked up through channel
	case ch.pull <- msg:

	// context was canceled by timeout or any other reason
	case <-ctx.Done():
		err = ctx.Err()

	// the circuit non breaking timeout per message
	case <-tm:
		err = ErrWaitTimeout
	}

	return
}

// StopConsume marks channel not ready to receive messages,
// but does not remove subscription.
func (ch *Channel) StopConsume() {
	ch.mutex.Lock()
	if ch.pulling {
		close(ch.pull)
	}
	ch.pulling = false
	ch.mutex.Unlock()
}

// Subscribe appends channel to the topic with given name. This action reasonable only in the Exchange collection scope,
// which is provided only through the Exchange.NewChannel() allocation.
// Subscriptions may be two types according given `exc` parameter:
// - (default: false) fanout, when all members of the topic recieve message except sender;
// - (true) RPC, topic must have only two members to exchange with messages.
// Subscribed channel can receive messages just mark with tag, this is additional filter
// in the topic group.
func (ch *Channel) Subscribe(name, tag string, exc bool) error {
	if ch.ex == nil {
		return ErrSubscribeStandAloneChannel
	}

	err := ch.ex.subscribe(ch, name, exc)
	if err != nil {
		return err
	}

	if len(tag) > 0 {
		ch.mutex.Lock()
		ch.tag = tag
		ch.mutex.Unlock()
	}

	return nil
}

func (ch *Channel) Token() string {
	return ch.token.String()
}

func (ch *Channel) isTag(tags []string) bool {
	if len(tags) > 0 && len(ch.tag) == 0 {
		return false
	}

	for _, tg := range tags {
		if tg == ch.tag {
			return true
		}
	}

	return false
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

// sliceStore represents the store of the channels with manager to control race.
type sliceStore struct {
	sync.Mutex
	registry []*Channel
}

func newSliceStore() *sliceStore {
	return &sliceStore{
		// ready for exclusive
		registry: make([]*Channel, 0, 2),
	}
}

func (s *sliceStore) add(ch *Channel) {
	if cp, ln := cap(s.registry), len(s.registry); cp == ln {
		buf := make([]*Channel, ln, cp+10)
		copy(buf, s.registry)
		s.registry = buf
	}

	s.registry = append(s.registry, ch)
}

func (s *sliceStore) channel(id uuid.UUID) *Channel {
	for _, ch := range s.registry {
		if uuid.Equal(ch.token, id) {
			return ch
		}
	}
	return nil
}

func (s *sliceStore) index(id uuid.UUID) int {
	for i, ch := range s.registry {
		if uuid.Equal(ch.token, id) {
			return i
		}
	}
	return -1
}

func (s *sliceStore) len() int {
	return len(s.registry)
}

func (s *sliceStore) remove(id uuid.UUID) (ch *Channel) {
	for i, ln := 0, len(s.registry); i < ln; i++ {
		if uuid.Equal(s.registry[i].token, id) {
			ch = s.registry[i]

			s.registry[i] = s.registry[ln-1]
			s.registry[ln-1] = nil
			s.registry = s.registry[:ln-1]
			break
		}
	}

	return
}
