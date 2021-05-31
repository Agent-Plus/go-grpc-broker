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
	Header() Header
	SetId(string)
	SetContentType(string)
	SetBody([]byte)
	Publish(string, []string) error
}

type response struct {
	mux *ServeMux
	msg *Message
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

func (rr *response) SetContentType(ct string) {
	rr.msg.ContentType = ct
}

func (rr *response) SetBody(bd []byte) {
	rr.msg.Body = bd
}

func (rr *response) Publish(topic string, tags []string) error {
	return rr.mux.Publish(topic, *rr.msg, tags)
}
