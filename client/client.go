package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Agent-Plus/go-grpc-broker/api"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	ErrServerUnavailable = errors.New("server unavailable")
	ErrTimeout           = errors.New("timeout")
	ErrUnauthenticated   = errors.New("unauthenticated")
)

type ExchangeClient struct {
	api api.ExchangeClient

	// server address
	address string

	// authentication credentials
	// application identifier
	id string
	// authentication secret
	secret string

	// client is subscribed
	subscribed string

	// connection id
	tkm   sync.Mutex
	token string
}

func New() *ExchangeClient {
	return &ExchangeClient{}
}

func (ec *ExchangeClient) Dial(addr string) error {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}

	ec.address = addr
	ec.api = api.NewExchangeClient(conn)

	return nil
}

func (ec *ExchangeClient) metadata() context.Context {
	return metadata.AppendToOutgoingContext(
		context.Background(),
		"authorization", "Bearer "+ec.token,
	)
}

func (ec *ExchangeClient) Authenticate(name, secret string) error {
	ec.tkm.Lock()
	defer ec.tkm.Unlock()

	// check token value, prefilled value means authenticated otherwise try to send Authenticate request
	if len(ec.token) == 0 {
		tk, err := ec.api.Authenticate(context.Background(), &api.Identity{Id: name, Secret: secret})
		if err != nil {
			return ec.onError(err)
		}

		ec.token = tk.Key
	}

	return nil
}

func (ec *ExchangeClient) Subscribe(name, tag string, exc bool) error {
	ctx := ec.metadata()
	_, err := ec.api.Subscribe(ctx, &api.SubscribeRequest{
		Name:      name,
		Tag:       tag,
		Exclusive: exc,
	})
	if err != nil {
		return ec.onError(err)
	}

	ec.subscribed = name
	return nil
}

type Message struct {
	Body        []byte
	ContentType string
	//CorId       string
	Headers Header
	Id      string
}

type streamWorker struct {
	close   chan struct{}
	deliver chan *Message
}

func (ec *ExchangeClient) Consume() (<-chan *Message, error) {
	ctx := ec.metadata()
	stream, err := ec.api.Consume(ctx, &empty.Empty{})
	if err != nil {
		return nil, ec.onError(err)
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

			m := &Message{
				ContentType: msg.ContentType,
				//CorId:       msg.CorId,
				Id: msg.Id,
			}

			if ln := len(msg.Body); ln > 0 {
				m.Body = make([]byte, ln)
				copy(m.Body, msg.Body)
			} else {
				m.ContentType = ""
			}

			if ln := len(msg.Headers); ln > 0 {
				m.Headers = make(Header, ln)
				for k, v := range msg.Headers {
					m.Headers[k] = strings.Split(v, ",")
				}
			}

			worker.deliver <- m
		}
	}
}

func (ec *ExchangeClient) Publish(topic string, msg Message, tags []string) error {
	ctx := ec.metadata()

	m := &api.Message{
		ContentType: msg.ContentType,
		//CorId:       msg.CorId,
		Id: msg.Id,
	}

	if ln := len(msg.Body); ln > 0 {
		m.Body = make([]byte, ln)
		copy(m.Body, msg.Body)
	} else {
		m.ContentType = ""
	}

	if ln := len(msg.Headers); ln > 0 {
		m.Headers = make(map[string]string, ln)
		for k, v := range msg.Headers {
			m.Headers[k] = strings.Join(v, ",")
		}
	}

	_, err := ec.api.Publish(ctx, &api.PublishRequest{
		Topic:   topic,
		Tag:     tags,
		Message: m,
	})
	if err != nil {
		return ec.onError(err)
	}

	return nil
}

func (ec *ExchangeClient) onError(err error) error {
	if e, ok := status.FromError(err); ok {
		switch e.Code() {
		case codes.Unauthenticated:
			// drop down known token value on Unauthenticated server response
			ec.token = ""

			err = fmt.Errorf("%v: %w", err, ErrUnauthenticated)

		case codes.Unavailable:
			err = fmt.Errorf("%v: %w", err, ErrServerUnavailable)
		}
	}

	return err
}
