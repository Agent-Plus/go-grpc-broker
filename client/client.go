package client

import (
	"context"

	"github.com/Agent-Plus/go-grpc-broker/api"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ExchangeClient struct {
	address string
	api     api.ExchangeClient
	token   string
}

func New(addr string) (*ExchangeClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return &ExchangeClient{
		address: addr,
		api:     api.NewExchangeClient(conn),
	}, nil
}

func (ec *ExchangeClient) Authenticate(name, secret string) error {
	tk, err := ec.api.Authenticate(context.Background(), &api.Identity{Id: name, Secret: secret})
	if err != nil {
		return err
	}

	ec.token = tk.Key
	return nil
}

func (ec *ExchangeClient) Subscribe(name string, exc bool) error {
	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"authorization", "Bearer "+ec.token,
	)

	_, err := ec.api.Subscribe(ctx, &api.SubscribeRequest{
		Name:      name,
		Tag:       "",
		Exclusive: exc,
	})
	return err
}

type Deliverer interface {
	Body() []byte
}

type message struct {
	*api.Message
}

func (m *message) Body() []byte {
	m.GetBody()
}

func (ec *ExchangeClient) Consume(m chan<- Deliverer) error {
	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"authorization", "Bearer "+ec.token,
	)

	stream, err := ec.api.Consume(ctx, &empty.Empty{})
	if err != nil {
		return err
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}

		m <- &message{msg}
	}
}
