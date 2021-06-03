package broker

import (
	"context"
	"sync"
	"time"

	"github.com/Agent-Plus/go-grpc-broker/api"
	uuid "github.com/satori/go.uuid"
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
	// message picked up through channel
	case d.dlv <- msg:

	// context was canceled by timeout or any other reason
	case <-ctx.Done():
		err = ctx.Err()

	// the circuit non breaking timeout per message
	case <-tm:
		err = ErrWaitTimeout
	}

	return
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
		if uuid.Equal(d.id, id) {
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
		if uuid.Equal(ds.registry[i].id, id) {
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
