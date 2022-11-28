package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Agent-Plus/go-grpc-broker/api"
	uuid "github.com/google/uuid"
)

type delivery struct {
	id     uuid.UUID
	chId   uuid.UUID
	tpName string
	dlv    chan *api.Message
	tag    string
	ready  atomicBool
}

func (d *delivery) isTag(tags []string) bool {
	if len(tags) > 0 && len(d.tag) == 0 {
		return false
	}

	for _, tg := range tags {
		if tg == d.tag {
			return true
		}
	}

	return false
}

func (d *delivery) send(ctx context.Context, msg *api.Message, tm <-chan time.Time) (err error) {
	if !d.ready.isSet() {
		return ErrChannelNotConsumed
	}

	select {
	// delivery attempt timeout
	case <-tm:
		err = ErrDeliveryTimeout

	// publishing context was canceled by timeout or any other reason
	case <-ctx.Done():
		err = ctx.Err()

	// message picked up through channel
	case d.dlv <- msg:

	}

	return
}

// deliveryPool makes concurrent delivery queue
type deliveryPool struct {
	// max workers, queue lenth of the tasks, ttl to deliver (milliseconds)
	hMax, qLen, ttl uint8

	// errors buffer
	errs *CircuitErrors

	// errors queue
	errsQueue chan error

	// queue of the deliveries
	queue chan deliveryFn

	// semaphore which read tasks handlers and stops
	stop chan struct{}

	// tasks wait
	wait sync.WaitGroup

	// buffer to store count of the runing handler
	workers chan struct{}
}

func newDeliveryPool(hMax, qLen, ttl uint8) *deliveryPool {
	if hMax < 1 {
		hMax = 1
	}

	if qLen < 10 {
		qLen = 10
	}

	if ttl < 1 {
		ttl = 10
	}

	return &deliveryPool{hMax: hMax, qLen: qLen, ttl: ttl}
}

func (dp *deliveryPool) Close() error {
	for i := len(dp.workers); i > 0; i-- {
		dp.stop <- struct{}{}
	}
	close(dp.errsQueue)
	close(dp.stop)
	close(dp.queue)
	close(dp.workers)
	dp.errs = nil
	return nil
}

func (dp *deliveryPool) Err() error {
	if len(dp.errs.err) > 0 {
		return dp.errs
	}
	return nil
}

func (dp *deliveryPool) readErrs() {
	for {
		select {
		case e := <-dp.errsQueue:
			if e != nil {
				if ln, cp := len(dp.errs.err), cap(dp.errs.err); ln == cp {
					buf := make([]error, ln, ln+10)
					copy(buf, dp.errs.err)
					dp.errs.err = buf
				}

				dp.errs.err = append(dp.errs.err, e)
			}

		case <-dp.stop:
			return
		}
	}
}

func (dp *deliveryPool) process(task deliveryFn) {
	defer func() {
		// decrease workers counter buffer
		<-dp.workers
	}()

	ttl := time.Duration(dp.ttl) * time.Millisecond
	tm := time.NewTimer(ttl)
	if task != nil {
		if err := task(tm.C); err != nil {
			if err == ErrDeliveryTimeout {
				// schedule again
				dp.schedule(task)
			}
		}
	}
	tm.Stop()

	// pull queue
	for {
		select {
		case task := <-dp.queue:
			tm.Reset(ttl)
			err := task(tm.C)
			tm.Stop()

			if err != nil {
				if err == ErrDeliveryTimeout {
					// schedule again
					dp.schedule(task)
				}
			}

		case <-dp.stop:
			return
		}
	}
}

func (dp *deliveryPool) schedule(task deliveryFn) {
	select {
	// First try to push task to the queue
	case dp.queue <- task:

	// Create new go routine and increase workers by appending to the channel
	case dp.workers <- struct{}{}:
		go dp.process(task)
	}
}

func (dp *deliveryPool) start() {
	dp.errs = &CircuitErrors{}
	dp.errsQueue = make(chan error, dp.qLen)
	dp.stop = make(chan struct{})
	dp.queue = make(chan deliveryFn, dp.qLen)
	dp.workers = make(chan struct{}, dp.hMax+1)

	// spawn error reader
	dp.workers <- struct{}{}
	go dp.readErrs()

	// spawn first process
	dp.workers <- struct{}{}
	go dp.process(nil)

	if dp.hMax > 1 {
		dp.workers <- struct{}{}
		go dp.process(nil)
	}
}

type deliveryFn func(<-chan time.Time) error

func (dp *deliveryPool) wrapTask(ctx context.Context, pb *publisher, dlv *delivery) deliveryFn {
	var attempt uint8
	dp.wait.Add(1)

	return func(tm <-chan time.Time) (err error) {
		defer func() {
			if !errors.Is(err, ErrDeliveryTimeout) {
				dp.wait.Done()
			}
		}()

		if attempt > 3 {
			err = fmt.Errorf("attempt=(%d) to deliver: %w", attempt, ErrAttempExceeded)
			return
		}

		err = dlv.send(ctx, pb.request, tm)

		if err != nil {
			if err == ErrDeliveryTimeout {
				attempt++
			}
		} else {
			pb.ackAdd()
		}

		return
	}
}

func (dp *deliveryPool) WaitWithContext(ctx context.Context) {
	cc := make(chan struct{})
	go func() {
		defer close(cc)
		dp.wait.Wait()
	}()

	select {
	case <-ctx.Done():
	case <-cc:
	}
}

type deliveryStore struct {
	sync.RWMutex
	registry []*delivery
}

func newDeliveryStore() *deliveryStore {
	return &deliveryStore{
		// ready for exclusive
		registry: make([]*delivery, 0, 10),
	}
}

func (ds *deliveryStore) add(d *delivery) {
	if cp, ln := cap(ds.registry), len(ds.registry); cp == ln {
		buf := make([]*delivery, ln, cp+10)
		copy(buf, ds.registry)
		ds.registry = buf
	}

	ds.registry = append(ds.registry, d)
}

func (ds *deliveryStore) index(id uuid.UUID) int {
	for i, d := range ds.registry {
		if bytes.Equal(d.id[:], id[:]) {
			return i
		}
	}
	return -1
}

func (ds *deliveryStore) len() int {
	return len(ds.registry)
}

func (ds *deliveryStore) remove(id uuid.UUID) (d *delivery) {
	for i, ln := 0, len(ds.registry); i < ln; i++ {
		if bytes.Equal(ds.registry[i].id[:], id[:]) {
			d = ds.registry[i]

			ds.removeAt(i)
			break
		}
	}

	return
}

func (ds *deliveryStore) removeAt(idx int) {
	ln := len(ds.registry)

	if idx >= ln {
		return
	}

	ds.registry[idx] = ds.registry[ln-1]
	ds.registry[ln-1] = nil
	ds.registry = ds.registry[:ln-1]
}

func (ds *deliveryStore) walk(cb func(int, *delivery) error, skipId *uuid.UUID, tags []string) error {
	ds.Lock()
	rgs := ds.registry
	ds.Unlock()

	for idx, d := range rgs {
		if !d.ready.isSet() {
			// delivery container is not ready to consume messages
			continue
		}

		if skipId != nil && bytes.Equal(d.chId[:], skipId[:]) {
			continue
		}

		// exclude those are not in the passed tags list
		if len(tags) > 0 && !d.isTag(tags) {
			continue
		}

		if err := cb(idx, d); err != nil {
			return err
		}
	}

	return nil
}

// kvStore represents key-value store of the channels with manager to control race.
type waitStore struct {
	sync.RWMutex
	registry map[string]func(*api.Message)
}

func newWaitStore() *waitStore {
	return &waitStore{
		registry: make(map[string]func(*api.Message)),
	}
}

func (s *waitStore) add(id string, do func(*api.Message)) {
	s.Lock()
	if _, ok := s.registry[id]; !ok {
		s.registry[id] = do
	}
	s.Unlock()
}

func (s *waitStore) delete(id string) {
	s.Lock()
	delete(s.registry, id)
	s.Unlock()
}

func (s *waitStore) do(id string, msg *api.Message) (ack int32) {
	s.Lock()
	fn, ok := s.registry[id]
	s.Unlock()

	if ok {
		fn(msg)
		ack++
	}

	s.delete(id)
	return
}

func (s *waitStore) len() int {
	s.Lock()
	defer s.Unlock()

	return len(s.registry)
}
