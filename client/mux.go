package client

import (
	"errors"
	"fmt"
	"time"
)

const (
	// headerAction is name of the header to store ActionType
	headerAction = "rpc-action"

	// headerResource is name of the header to store resource
	headerResource = "rpc-resource"

	headerResponse = "rpc-response-corid"
)

// ActionType is action type
type ActionType uint8

const (
	actionNoop ActionType = iota

	// ActionCreate requires to create resource
	ActionCreate

	// ActionDelete requires to delete resource
	ActionDelete

	// ActionGet requires resource data
	ActionGet

	// ActionUpdate requires to update data
	ActionUpdate
)

var (
	// ErrRequired is returned when some item was missed
	ErrRequired = errors.New("required")

	// ErrConflict is returned when new item conflicts with existing
	ErrConflict = errors.New("conflict")
)

// HandlerFunc represents function to proccess with incoming message
type HandlerFunc func(ResponseWriter, *Message)

func (hf HandlerFunc) ServeMessage(w ResponseWriter, m *Message) {
	hf(w, m)
}

type Handler interface {
	ServeMessage(ResponseWriter, *Message)
}

// Middleware is function type to return Handler
type Middleware func(Handler) Handler

type ServeMux struct {
	*ExchangeClient

	reconn  chan int8
	serving atomicBool

	daedline time.Time
	trees    map[ActionType]*node
	waitRPC  *rpcRegistry

	NotFoundHandler Handler
	PanicHandler    func(*Message, interface{})
}

type AuthOption struct {
	Id     string
	Secret string
}

type SubOption struct {
	Tag        string
	Topic      string
	Exclusive  bool
	subscribed bool
}

// NewServeMux
func NewServeMux(client *ExchangeClient) *ServeMux {
	return &ServeMux{
		ExchangeClient: client,
		daedline:       time.Now().Add(10 * time.Second),
		waitRPC: &rpcRegistry{
			resps: make(map[string]*rpcResponse),
		},
	}
}

// Handle registers for given action and resource handler function
func (m *ServeMux) Handle(action ActionType, resource string, handle Handler) {
	if len(resource) == 0 {
		panic(fmt.Errorf("resource: %w", ErrRequired))
	}

	if handle == nil {
		panic(fmt.Errorf("handler: %w", ErrRequired))
	}

	if m.trees == nil {
		m.trees = make(map[ActionType]*node)
	}

	root := m.trees[action]
	if root == nil {
		root = new(node)
		m.trees[action] = root
	}

	root.addRoute(resource, handle)
}

// Create registers given resource name and handler function on action type ActionCreate
func (m *ServeMux) Create(resource string, handle HandlerFunc, md ...Middleware) {
	m.Handle(ActionCreate, resource, wrapMiddlewares(handle, md))
}

// Delete registers given resource name and handler function on action type ActionCreate
func (m *ServeMux) Delete(resource string, handle HandlerFunc, md ...Middleware) {
	m.Handle(ActionDelete, resource, wrapMiddlewares(handle, md))
}

// Get registers given resource name and handler function on action type ActionGet
func (m *ServeMux) Get(resource string, handle HandlerFunc, md ...Middleware) {
	m.Handle(ActionGet, resource, wrapMiddlewares(handle, md))
}

// Update registers given resource name and handler function on action type ActionCreate
func (m *ServeMux) Update(resource string, handle HandlerFunc, md ...Middleware) {
	m.Handle(ActionUpdate, resource, wrapMiddlewares(handle, md))
}

// ServeMessage routes given Message to the handler function according toe the Message.Action and Message.Resource.
func (m *ServeMux) ServeMessage(w ResponseWriter, msg *Message) {
	if m.PanicHandler != nil {
		defer m.recv(msg)
	}

	action, _ := msg.Headers.GetInt64(headerAction)
	resource := msg.Headers.GetString(headerResource)
	corid := msg.Headers.GetString(headerResponse)

	if ActionType(action) == actionNoop {
		if len(corid) > 0 {
			if resp := m.waitRPC.get(corid); resp != nil {
				go resp.serve(msg)
			}
		}
	} else {
		if n, ok := m.trees[ActionType(action)]; ok {
			leaf := n.children[resource]
			if leaf != nil {
				leaf.handle.ServeMessage(w, msg)
				return
			}
		}
	}

	if m.NotFoundHandler != nil {
		m.NotFoundHandler.ServeMessage(w, msg)
	}
}

func (m *ServeMux) PublishRequest(topic string, msg Message, tags []string) (*Message, error) {
	delivery, stop, tmOut := m.waitRPC.add(msg.Id, m.daedline)
	err := m.Publish(topic, msg, tags)
	if err != nil {
		stop()
		return nil, err
	}

	rmsg := <-delivery
	stop()

	m.waitRPC.delete(msg.Id)

	if tmOut() {
		return nil, ErrTimeout
	}

	return rmsg, nil
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

func (m *ServeMux) StartServe(opts ...interface{}) error {
	var (
		auth *AuthOption
		sub  *SubOption
	)

	for _, v := range opts {
		if sb, ok := v.(SubOption); ok {
			sub = &sb
		} else {
			if sb, ok := v.(*SubOption); ok {
				sub = sb
			}
		}

		if au, ok := v.(AuthOption); ok {
			auth = &au
		} else {
			if au, ok := v.(*AuthOption); ok {
				auth = au
			}
		}
	}

	return m.serve(m.call(auth, sub))
}

func (m *ServeMux) serve(fn func() (<-chan *Message, error)) error {
	var (
		err      error
		delivery <-chan *Message
	)

	m.reconn = make(chan int8, 1)
	// write start bit
	m.reconn <- 1
	defer func() {
		m.serving.setFalse()
	}()

	for {
		select {
		case state := <-m.reconn:
			if (state & startBit) == 0 {
				return nil
			}

			if (state & lastBit) != 0 {
				// long wait
				time.Sleep(300 * time.Second)
			} else if (state & (stateMask & ^startBit)) != 0 {
				// short wait
				time.Sleep(10 * time.Second)
			}

			delivery, err = fn()
			if err != nil {
				if errors.Is(err, ErrUnauthenticated) {
					return err
				}

				m.serving.setFalse()
				m.reconn <- rotatebit(state)
				continue
			}

			m.serving.setTrue()

		case msg, ok := <-delivery:
			if !ok {
				// channel was lost, try to reconnect
				m.reconn <- 1
				continue
			}

			m.ServeMessage(m.newResponse(msg), msg)
		}
	}
}

func (m *ServeMux) newResponse(msg *Message) *response {
	rr := new(response)
	rr.mux = m
	rr.msg = &Message{
		Headers: make(Header),
	}

	rr.msg.Headers.SetInt64(headerAction, int64(actionNoop))
	rr.msg.Headers.SetString(headerResponse, msg.Id)

	return rr
}

func (m *ServeMux) call(auth *AuthOption, sub *SubOption) func() (<-chan *Message, error) {
	return func() (<-chan *Message, error) {
		if auth != nil {
			if err := m.Authenticate(auth.Id, auth.Secret); err != nil {
				return nil, err
			}
		}

		if sub != nil && !sub.subscribed {
			err := m.Subscribe(sub.Topic, sub.Tag, sub.Exclusive)
			if err != nil {
				sub.subscribed = false
				return nil, err
			}

			sub.subscribed = true
		}

		return m.Consume()
	}
}

func (m *ServeMux) recv(msg *Message) {
	if rcv := recover(); rcv != nil {
		m.PanicHandler(msg, rcv)
	}
}

type node struct {
	children map[string]*node
	handle   Handler
}

func (n *node) addRoute(resource string, handle Handler) {
	if n.children != nil {
		if _, ok := n.children[resource]; ok {
			panic(fmt.Errorf("resource %s: %w", resource, ErrConflict))
		}
	} else {
		n.children = make(map[string]*node)
	}

	n.children[resource] = &node{
		handle: handle,
	}
}

func wrapMiddlewares(endpoint Handler, md []Middleware) Handler {
	if len(md) == 0 {
		return endpoint
	}

	h := md[len(md)-1](endpoint)
	for i := len(md) - 2; i >= 0; i-- {
		h = md[i](h)
	}

	return h
}

func SetMessageActionCreate(m *Message, resource string) {
	m.Headers.SetInt64(headerAction, int64(ActionCreate))
	m.Headers.SetString(headerResource, resource)
}

func SetMessageActionDelete(m *Message, resource string) {
	m.Headers.SetInt64(headerAction, int64(ActionDelete))
	m.Headers.SetString(headerResource, resource)
}

func SetMessageActionGet(m *Message, resource string) {
	if m.Headers == nil {
		m.Headers = make(Header)
	}

	m.Headers.SetInt64(headerAction, int64(ActionGet))
	m.Headers.SetString(headerResource, resource)
}

func SetMessageActionUpdate(m *Message, resource string) {
	m.Headers.SetInt64(headerAction, int64(ActionUpdate))
	m.Headers.SetString(headerResource, resource)
}
