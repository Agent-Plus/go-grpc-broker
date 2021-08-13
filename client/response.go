package client

import (
	"sync"
	"sync/atomic"
	"time"
)

type rpcRegistry struct {
	m     sync.Mutex
	resps map[string]*rpcResponse
}

func (rr *rpcRegistry) add(id string, daedline time.Time) (<-chan *Message, func(), func() bool) {
	resp := newRPCResponse()

	rr.m.Lock()
	rr.resps[id] = resp
	rr.m.Unlock()

	stop, tmOut := setCancel(resp, daedline)
	return resp.rpc, stop, tmOut
}

func (rr *rpcRegistry) delete(id string) {
	if r := rr.get(id); r != nil {
		r.close()
	}

	rr.m.Lock()
	delete(rr.resps, id)
	rr.m.Unlock()
}

func (rr *rpcRegistry) get(id string) *rpcResponse {
	rr.m.Lock()
	resp := rr.resps[id]
	rr.m.Unlock()

	return resp
}

type rpcResponse struct {
	closed atomicBool
	rpc    chan *Message
}

func newRPCResponse() *rpcResponse {
	resp := new(rpcResponse)
	resp.rpc = make(chan *Message)

	return resp
}

func (r *rpcResponse) close() {
	if !r.closed.isSet() {
		close(r.rpc)
		r.closed.setTrue()
	}
}

func (r *rpcResponse) serve(msg *Message) {
	r.rpc <- msg
}

type atomicBool int32

func (b *atomicBool) isSet() bool { return atomic.LoadInt32((*int32)(b)) != 0 }
func (b *atomicBool) setTrue()    { atomic.StoreInt32((*int32)(b), 1) }
func (b *atomicBool) setFalse()   { atomic.StoreInt32((*int32)(b), 0) }

func setCancel(resp *rpcResponse, daedline time.Time) (func(), func() bool) {
	stopTimerCh := make(chan struct{})
	var once sync.Once

	stopTimer := func() {
		once.Do(func() {
			close(stopTimerCh)
		})
	}

	timer := time.NewTimer(time.Until(daedline))
	var timeOut atomicBool

	go func() {
		select {
		case <-timer.C:
			resp.close()
			timeOut.setTrue()

		case <-stopTimerCh:
			timer.Stop()
		}
	}()

	return stopTimer, timeOut.isSet
}

type ResponseWriter interface {
	// Header returns pointer to the reponse Message.Headers
	Header() Header

	// SetBody sets payload data to the reponse Message.Body
	SetBody([]byte)

	// SetError sets Message.Error flag `true`
	SetError(bool)

	// SetId sets identifier into the response message
	SetId(string)

	// Publish implements ExchangeClient.Publish
	Publish(string, *Message, []string) error

	// PublishResponse sends *Message to the topic where request was received
	PublishResponse() error
}

type response struct {
	mux   *ServeMux
	msg   *Message
	topic string
	sent  bool
}

func (rr *response) Header() Header {
	if rr.msg.Headers == nil {
		rr.msg.Headers = make(Header)
	}
	return rr.msg.Headers
}

func (rr *response) SetId(id string) {
	rr.msg.Id = id
}

func (rr *response) SetError(v bool) {
	rr.msg.Err = v
}

func (rr *response) SetBody(bd []byte) {
	rr.msg.Body = bd
}

// Publish implements ExchangeClient Publish method to make ResponseWriter
// in the HandlerFunc more flexible, it's posible to publish any message into any topic.
func (rr *response) Publish(topic string, msg *Message, tags []string) error {
	return rr.mux.Publish(topic, msg, tags)
}

// PublishResponse responses with prepared message to the publisher topic
func (rr *response) PublishResponse() error {
	return rr.mux.Publish(rr.topic, rr.msg, nil)
}
