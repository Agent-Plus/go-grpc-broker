package broker

import (
	"context"
	"strconv"
	"time"

	"github.com/Agent-Plus/go-grpc-broker/api"
	uuid "github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// ChainStreamInterceptor is short hand for grpc.ChainStreamInterceptor
func ChainStreamInterceptor(interceptors ...grpc.StreamServerInterceptor) grpc.ServerOption {
	return grpc.ChainStreamInterceptor(interceptors...)
}

// ChainUnaryInterceptor is short hand for ChainUnaryInterceptor
func ChainUnaryInterceptor(interceptors ...grpc.UnaryServerInterceptor) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(interceptors...)
}

type Logger interface {
	Printf(string, ...interface{})
	Errorf(string, ...interface{})
}

func LogStreamInterceptor(il Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		startTime := time.Now()

		err := handler(srv, stream)

		conId, _ := stream.Context().Value(connIdCtxKey).(uuid.UUID)
		if err != nil {
			code := status.Code(err)
			il.Errorf(
				"finished streaming in %v, conn=(%s), code=(%s), method=(%s): %v",
				time.Now().Sub(startTime),
				conId,
				code.String(),
				info.FullMethod,
				err,
			)
		} else {
			il.Printf(
				"finished streaming in %v, conn=(%s), method=(%s)",
				time.Now().Sub(startTime),
				conId,
				info.FullMethod,
			)
		}

		return err
	}
}

func LogUnaryInterceptor(il Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		startTime := time.Now()

		resp, err := handler(ctx, req)

		conId, _ := ctx.Value(connIdCtxKey).(uuid.UUID)

		fm := "finished unary in %v, conn=(%s), method=(%s)"
		if v, ok := req.(*api.PublishRequest); ok && v != nil {
			fm += ", topic=(" + v.Topic + ")"
		}

		if v, ok := resp.(*api.PublishResponse); ok && v != nil {
			fm += ", consumed=(" + strconv.Itoa(int(v.Ack)) + ")"
		}

		il.Printf(
			fm,
			time.Now().Sub(startTime),
			conId,
			info.FullMethod,
		)

		if err != nil {
			if e, ok := err.(*CircuitErrors); ok {
				for _, ev := range e.err {
					il.Errorf(
						"conn=(%s), method=(%s): %v",
						conId,
						info.FullMethod,
						ev,
					)
				}

				err = nil
			} else {
				il.Errorf(
					"finished unary in %v, conn=(%s), method=(%s): %v",
					conId,
					info.FullMethod,
					err,
				)
			}
		}

		return resp, err
	}
}
