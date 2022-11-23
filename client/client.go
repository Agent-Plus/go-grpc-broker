package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Agent-Plus/go-grpc-broker/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidArgument   = errors.New("invalid argument")
	ErrNotConnected      = errors.New("not connected")
	ErrServerUnavailable = errors.New("server unavailable")
	ErrTimeout           = errors.New("timeout")
	ErrUnauthenticated   = errors.New("unauthenticated")
)

// ModeType describes subscription type
type ModeType uint8

// Fanout is default subscription mode where each consumer recieves message
const FanoutMode ModeType = 0

const (
	// RPCMode is subscription where in the topic is one consumer and many publishers
	RPCMode ModeType = 1 << iota

	// ExclusiveMode is RPC where conversation one to one
	ExclusiveMode
)

type ExchangeClient struct {
	api api.ExchangeClient

	// connection
	conn *grpc.ClientConn

	// server address
	address string

	// authentication supplier
	aa *authentication

	// client is subscribed
	subscribed string
}

type ClientOption interface {
	apply(*ExchangeClient)
}

func New(opt ...ClientOption) *ExchangeClient {
	c := &ExchangeClient{
		aa: &authentication{},
	}

	if len(opt) > 0 {
		for _, f := range opt {
			f.apply(c)
		}
	}
	return c
}

func (ec *ExchangeClient) Close() error {
	if ec.conn != nil {
		return ec.conn.Close()
	}
	return ErrNotConnected
}

func (ec *ExchangeClient) Dial(addr string, opt ...grpc.DialOption) error {
	opts := make([]grpc.DialOption, 0, len(opt)+2)
	if ec.aa != nil {
		opts = append(opts, grpc.WithStatsHandler(ec.aa))
	}
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, opt...)

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return err
	}

	ec.address = addr
	ec.api = api.NewExchangeClient(conn)
	ec.conn = conn

	return nil
}

func (ec *ExchangeClient) DialContext(ctx context.Context, target string, opts ...grpc.DialOption) error {
	conn, err := grpc.DialContext(ctx, target, opts...)
	if err != nil {
		return err
	}
	ec.api = api.NewExchangeClient(conn)
	ec.conn = conn

	return nil
}

func (ec *ExchangeClient) metadata() context.Context {
	ctx := context.Background()

	ec.aa.Lock()
	tk := ec.aa.token
	ec.aa.Unlock()

	if len(tk) > 0 {
		ctx = metadata.AppendToOutgoingContext(
			ctx,
			"authorization", "Bearer "+tk,
		)
	}

	return ctx
}

type authentication struct {
	identity *api.Identity

	// stores authenticated session
	token string

	sync.Mutex
}

type authCaller interface {
	Authenticate(context.Context, *api.Identity, ...grpc.CallOption) (*api.Token, error)
}

func (a *authentication) do(af authCaller) error {
	if a.identity == nil {
		// not configures
		return nil
	}

	if len(a.token) > 0 {
		// already authenticated
		return nil
	}

	tk, err := af.Authenticate(context.Background(), a.identity)
	if err != nil {
		return onError(err)
	}

	a.token = tk.Key
	return nil
}

func (a *authentication) reset() {
	a.Lock()
	a.token = ""
	a.Unlock()
}

func (a *authentication) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return ctx
}
func (a *authentication) HandleConn(ctx context.Context, s stats.ConnStats) {
	if _, ok := s.(*stats.ConnEnd); ok {
		a.reset()
	}
}

func (a *authentication) HandleRPC(ctx context.Context, s stats.RPCStats) {}
func (a *authentication) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return ctx
}

// Authenticate implements gRPC client Authentication method,
// it sends given application id and secret to the server and on
// success response keeps session token for the futher calls.
// Given credential overwrites previuos known, which were configured
// through the ClientOption.
// Authenticate is concurrent safe.
func (ec *ExchangeClient) Authenticate(id, secret string) (err error) {
	ec.aa.Lock()
	// overwrite credential
	ec.aa.identity = &api.Identity{Id: id, Secret: secret}
	// call authentication
	err = ec.aa.do(ec.api)
	ec.aa.Unlock()

	return nil
}

// Message represents data preset to be send or receive through gRPC api.Message
type Message struct {
	// Body is any data
	Body []byte

	// Error is state to notify consumer that message is abnormal
	// and he must react in different way on its
	Err bool

	// Headers contains message header fields.
	Headers Header

	// Id is message identifier
	Id string

	corId string
}

func NewMessage() *Message {
	return &Message{
		Headers: make(Header),
	}
}

func newMessageFromApi(msg *api.Message) *Message {
	m := &Message{
		Err:   msg.Error,
		Id:    msg.Id,
		corId: msg.CorId,
	}

	if ln := len(msg.Body); ln > 0 {
		m.Body = make([]byte, ln)
		copy(m.Body, msg.Body)
	}

	if ln := len(msg.Headers); ln > 0 {
		m.Headers = make(Header, ln)
		for k, v := range msg.Headers {
			m.Headers[k] = strings.Split(v, headerValuesDelimitger)
		}
	}

	return m
}

// Action returns the value of the header `rpc-action` if was set
func (m *Message) Action() string {
	return m.Headers.GetString(headerAction)
}

// Resource returns the value of the header `rpc-resource` if was set
func (m *Message) Resource() string {
	return m.Headers.GetString(headerResource)
}

type streamWorker struct {
	close   chan struct{}
	deliver chan *Message
}

func (ec *ExchangeClient) Consume(id string) (<-chan *Message, error) {
	ctx := ec.metadata()
	stream, err := ec.api.Consume(ctx, &api.ConsumeRequest{Id: id})
	if err != nil {
		return nil, onError(err)
	}

	if header, err := stream.Header(); err != nil {
		return nil, err
	} else {
		if v := header.Get("x-state"); len(v) > 0 && v[0] == "established" {
			//t.Log("x-state:", v)
		}
	}

	resp := make(chan *Message)

	go readStrem(stream, &streamWorker{
		close:   make(chan struct{}),
		deliver: resp,
	})

	return resp, nil
}

const headerValuesDelimitger = ";;"

func readStrem(stream api.Exchange_ConsumeClient, worker *streamWorker) {
	for {
		select {
		case <-worker.close:
			return

		default:
			msg, err := stream.Recv()
			if err != nil {
				close(worker.deliver)
				return
			}

			worker.deliver <- newMessageFromApi(msg)
		}
	}
}

func (ec *ExchangeClient) Publish(topic string, msg *Message, tags []string, wait bool) (*Message, error) {
	ec.aa.Lock()
	err := ec.aa.do(ec.api)
	ec.aa.Unlock()

	if err != nil {
		return nil, err
	}

	ctx := ec.metadata()

	m := &api.Message{
		Error: msg.Err,
		Id:    msg.Id,
		CorId: msg.corId,
	}

	if ln := len(msg.Body); ln > 0 {
		m.Body = make([]byte, ln)
		copy(m.Body, msg.Body)
	}

	if ln := len(msg.Headers); ln > 0 {
		m.Headers = make(map[string]string, ln)
		for k, v := range msg.Headers {
			m.Headers[k] = strings.Join(v, headerValuesDelimitger)
		}
	}

	var res *api.PublishResponse
	res, err = ec.api.Publish(ctx, &api.PublishRequest{
		Topic:   topic,
		Tag:     tags,
		Message: m,
		Wait:    wait,
	})
	if err != nil {
		return nil, onError(err)
	}

	if wait && res.Message != nil {
		return newMessageFromApi(res.Message), nil
	}

	return nil, nil
}

func (ec *ExchangeClient) Subscribe(name, tag string, mode ModeType) (string, error) {
	ctx := ec.metadata()
	res, err := ec.api.Subscribe(ctx, &api.SubscribeRequest{
		Name: name,
		Tag:  tag,
		Mode: api.SubscribeMode(mode),
	})
	if err != nil {
		return "", onError(err)
	}

	ec.subscribed = name
	return res.Id, nil
}

type MessageHandlerFunc func(*Message)

// StartServe is complex
func (ec *ExchangeClient) StartServe(hd MessageHandlerFunc, name, tag string, mode ModeType) Closer {
	o := &observer{
		topic: name,
		tag:   tag,
		mode:  mode,

		cl:     ec,
		hd:     hd,
		reconn: make(chan int8),
	}

	go o.serve("")
	o.reconn <- 1

	return o
}

func (ec *ExchangeClient) Unsubscribe(id string) error {
	ctx := ec.metadata()
	_, err := ec.api.Unsubscribe(ctx, &api.UnsubscribeRequest{Id: id})
	if err != nil {
		return onError(err)
	}

	return nil
}

const (
	stateMask int8 = (1 << 6) + (1 << 6) - 1
	startBit  int8 = 1
	firstBit  int8 = startBit << 1
	lastBit   int8 = 1 << 6
)

func rotatebit(i int8) int8 {
	if (i & (stateMask & ^startBit)) == 0 {
		return ((1 << 1) & stateMask) | (i & startBit)
	}
	return (((i & (stateMask & ^startBit)) << 1) & stateMask) | (i & startBit)
}

type observer struct {
	mode  ModeType
	topic string
	tag   string

	// client connection
	cl *ExchangeClient

	// message handler
	hd MessageHandlerFunc

	// reconn is connection state, first bit is run/stop,
	// next bits are attempts to reconnect to the server
	reconn chan int8

	// authorized, subscribed and comsuming
	serving atomicBool
}

type Closer interface {
	Close()
}

func (o *observer) Close() {
	o.reconn <- 0
}

func (o *observer) close(sid string) {
	if o.serving.isSet() {
		o.serving.setFalse()
		o.cl.Unsubscribe(sid)
	}
}

func (o *observer) serve(sid string) {
	var (
		err      error
		delivery <-chan *Message
	)

	for {
		state := <-o.reconn

		// stop signal
		if (state & startBit) == 0 {
			o.close(sid)
			return
		}

		if (state & lastBit) != 0 {
			// long wait
			time.Sleep(300 * time.Second)
		} else if (state & (stateMask & ^startBit)) != 0 {
			// short wait
			time.Sleep(10 * time.Second)
		}

		delivery, sid, err = o.calls(sid)
		if err != nil {
			if errors.Is(err, ErrUnauthenticated) ||
				errors.Is(err, ErrServerUnavailable) {
				o.cl.aa.reset()
			}

			o.serving.setFalse()
			go func(st int8) { o.reconn <- st }(rotatebit(state))

			continue
		}

		o.serving.setTrue()
		go o.read(sid, delivery)

		// stop this routine
		return
	}
}

func (o *observer) read(sid string, delivery <-chan *Message) {
	for {
		select {
		case state := <-o.reconn:
			// stop signal
			if (state & startBit) == 0 {
				o.close(sid)
				return
			}

		case msg, ok := <-delivery:
			if !ok {
				// channel was lost, try to reconnect
				o.serving.setFalse()
				go o.serve(sid)
				o.reconn <- 1

				return
			}

			o.hd(msg)
		}
	}
}

func (o *observer) calls(sid string) (dlv <-chan *Message, id string, err error) {
	o.cl.aa.Lock()
	err = o.cl.aa.do(o.cl.api)
	o.cl.aa.Unlock()

	if err != nil {
		return
	}

	if !o.serving.isSet() {
		id, err = o.cl.Subscribe(o.topic, o.tag, o.mode)
		if err != nil {
			return
		}
	} else {
		id = sid
	}

	dlv, err = o.cl.Consume(id)
	if err != nil {
		id = ""
	}
	return
}

func onError(err error) error {
	if e, ok := status.FromError(err); ok {
		switch e.Code() {
		case codes.Unauthenticated:
			err = fmt.Errorf("%v: %w", err, ErrUnauthenticated)

		case codes.Unavailable:
			err = fmt.Errorf("%v: %w", err, ErrServerUnavailable)

		case codes.InvalidArgument:
			err = fmt.Errorf("%v: %w", err, ErrInvalidArgument)

		case codes.AlreadyExists:
		}
	}

	return err
}

// WithAuthentication creates client option with given application id and secret
func WithAuthentication(id, secret string) ClientOption {
	return &identity{
		api.Identity{Id: id, Secret: secret},
	}
}

type identity struct {
	api.Identity
}

func (id *identity) apply(ec *ExchangeClient) {
	ec.aa.Lock()
	ec.aa.identity = &api.Identity{
		Id:     id.Id,
		Secret: id.Secret,
	}
	ec.aa.Unlock()
}
