package broker

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/Agent-Plus/go-grpc-broker/api"
	"github.com/golang/protobuf/ptypes/empty"
	uuid "github.com/satori/go.uuid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// contextKey type is unexported to prevent collisions with context keys defined in other packages
type contextKey int

const (
	// connIdCtxKey is connection tag key
	connIdCtxKey contextKey = iota
)

// Authenticator describes credentials validator
type Authenticator interface {
	Validate(string, string) (bool, error)
}

// DummyAuthentication represents the authentication stub in the tests.
// Also this stub can be used for the gRPC service without authentication.
type DummyAuthentication struct {
	strict bool
	users  map[string]string
}

// NewDummyAuthentication creates new DummyAuthentication, can be prefiiled with given users store.
func NewDummyAuthentication(store map[string]string) *DummyAuthentication {
	auth := &DummyAuthentication{
		users: store,
	}
	auth.strict = len(auth.users) > 0
	return auth
}

// ExchangeServer wraps Exchange collection and implements api.ExchangeServer
type ExchangeServer struct {
	*Exchange
	api.UnimplementedExchangeServer

	auth Authenticator
	gs   *grpc.Server
}

type connStats struct {
	*Exchange
}

func (h *connStats) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return context.WithValue(ctx, connIdCtxKey, uuid.NewV4())
}

func (h *connStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h *connStats) HandleConn(ctx context.Context, s stats.ConnStats) {
	switch s.(type) {
	case *stats.ConnEnd:
		if id, ok := ctx.Value(connIdCtxKey).(uuid.UUID); ok {
			h.CloseChannel(id)
		}
		break
	}
}

func (h *connStats) HandleRPC(ctx context.Context, s stats.RPCStats) {}

// NewExchangeServer creates new Exchange Server with given authenticator and additional options for the grpc.Server.
func NewExchangeServer(auth Authenticator, opt ...grpc.ServerOption) (s *ExchangeServer) {
	s = &ExchangeServer{
		auth:     auth,
		Exchange: New(),
	}

	opts := make([]grpc.ServerOption, 1, len(opt)+1)
	opts[0] = grpc.StatsHandler(&connStats{s.Exchange})
	opts = append(opts, opt...)

	gs := grpc.NewServer(opts...)
	api.RegisterExchangeServer(gs, s)

	s.gs = gs

	return
}

// Authenticate implements api.ExchangeServer interface, authentication call.
func (s *ExchangeServer) Authenticate(ctx context.Context, identity *api.Identity) (*api.Token, error) {
	if ok, err := s.auth.Validate(identity.Id, identity.Secret); !ok {
		if errors.Is(err, ErrInvalidUserPass) {
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}
		return nil, err
	}

	ch := NewChannel()
	ch.token, _ = ctx.Value(connIdCtxKey).(uuid.UUID)
	ch.cid = identity.Id

	s.AddChannel(ch)

	return &api.Token{
		Key: ch.Token(),
	}, nil
}

func tkFromHeader(ctx context.Context) string {
	meta, _ := metadata.FromIncomingContext(ctx)

	if v, ok := meta["authorization"]; ok && len(v) > 0 {
		tok := v[0]
		if len(tok) > 6 && strings.ToLower(tok[:7]) == "bearer " {
			return tok[7:]
		}
	}

	return ""
}

// Consume implements api.ExchangeServer interface to start pulling messages from
// the topic which client was subscribed earlier.
func (m *ExchangeServer) Consume(_ *empty.Empty, stream api.Exchange_ConsumeServer) error {
	tk := tkFromHeader(stream.Context())
	if len(tk) == 0 {
		return status.Error(codes.Unauthenticated, ErrUknonwToken.Error())
	}

	ch, err := m.Channel(tk)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}

	queue := ch.Consume()
	// TODO: need trailer message
	defer ch.StopConsume()

	header := metadata.Pairs("x-state", "established")
	if err := stream.SendHeader(header); err != nil {
		return status.Errorf(codes.Internal, "unable to send 'x-state' header")
	}

	for msg := range queue {
		err = stream.Send(msg)

		if err != nil {
			break
		}
	}

	return err
}

// Publish implements api.ExchangeServer interface to send message to the topic.
func (m *ExchangeServer) Publish(ctx context.Context, pb *api.PublishRequest) (*api.PublishResponse, error) {
	tk := tkFromHeader(ctx)
	if len(tk) == 0 {
		return nil, status.Error(codes.Unauthenticated, ErrUknonwToken.Error())
	}

	ch, err := m.Channel(tk)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	_, err = ch.Publish(pb.Topic, pb.Message, pb.Tag)
	if err != nil {
		if _, ok := err.(*CircuitErrors); ok {
			// TODO; Dump these errors to debug
			err = nil
		} else {
			return nil, err
		}
	}

	return &api.PublishResponse{}, nil
}

// Subscribe implements api.ExchangeServer interface to subscribe client to the topic
func (m *ExchangeServer) Subscribe(ctx context.Context, sb *api.SubscribeRequest) (*api.SubscribeResponse, error) {
	tk := tkFromHeader(ctx)
	if len(tk) == 0 {
		return nil, status.Error(codes.Unauthenticated, ErrUknonwToken.Error())
	}

	ch, err := m.Channel(tk)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	err = ch.Subscribe(sb.Name, sb.Tag, sb.Exclusive)
	if err != nil {
		return nil, err
	}

	return &api.SubscribeResponse{}, nil
}

// Run starts networking
func (s *ExchangeServer) Run(address string) (err error) {
	var lis net.Listener

	lis, err = net.Listen("tcp", address)
	if err != nil {
		return
	}

	err = s.Serve(lis)

	return
}

func (s *ExchangeServer) Serve(lis net.Listener) error {
	return s.gs.Serve(lis)
}
