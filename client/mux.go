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
)

// ActionType is action type
type ActionType uint8

const (
	_ ActionType = iota

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
type HandlerFunc func(*Message)

func (hf HandlerFunc) ServeMessage(m *Message) {
	hf(m)
}

type Handler interface {
	ServeMessage(*Message)
}

// Middleware is function type to return Handler
type Middleware func(Handler) Handler

type ServeMux struct {
	*ExchangeClient

	reconn chan int8

	trees           map[ActionType]*node
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

// Update registers given resource name and handler function on action type ActionCreate
func (m *ServeMux) Update(resource string, handle HandlerFunc, md ...Middleware) {
	m.Handle(ActionUpdate, resource, wrapMiddlewares(handle, md))
}

// ServeMessage routes given Message to the handler function according toe the Message.Action and Message.Resource.
func (m *ServeMux) ServeMessage(msg *Message) {
	if m.PanicHandler != nil {
		defer m.recv(msg)
	}

	action, _ := msg.Headers.GetInt64(headerAction)
	resource := msg.Headers.GetString(headerResource)

	if n, ok := m.trees[ActionType(action)]; ok {
		leaf := n.children[resource]
		if leaf != nil {
			leaf.handle.ServeMessage(msg)
			return
		}
	}

	if m.NotFoundHandler != nil {
		m.NotFoundHandler.ServeMessage(msg)
	}
}

const (
	rotMask  int8 = (1 << 6) + (1 << 6) - 1
	lastBit  int8 = 1 << 6
	startBit int8 = 1
)

func rotatebit(i int8) int8 {
	if (i & rotMask) == 0 {
		return ((1 << 1) & rotMask) | (i & startBit)
	}
	return (((i & rotMask) << 1) & rotMask) | (i & startBit)
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

	for {
		select {
		case state := <-m.reconn:
			if (state & startBit) == 0 {
				return nil
			}

			if (state & lastBit) != 0 {
				// long wait
				time.Sleep(300 * time.Second)
			} else if (state & rotMask) != 0 {
				// short wait
				time.Sleep(10 * time.Second)
			}

			delivery, err = fn()
			if err != nil {
				if errors.Is(err, ErrUnauthenticated) {
					return err
				}

				m.reconn <- rotatebit(state)
				continue
			}

		case msg, ok := <-delivery:
			if !ok {
				// channel was lost, try to reconnect
				m.reconn <- 1
				continue
			}

			m.ServeMessage(msg)
		}
	}
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
	m.Headers.SetInt64(headerAction, int64(ActionGet))
	m.Headers.SetString(headerResource, resource)
}

func SetMessageActionUpdate(m *Message, resource string) {
	m.Headers.SetInt64(headerAction, int64(ActionUpdate))
	m.Headers.SetString(headerResource, resource)
}
