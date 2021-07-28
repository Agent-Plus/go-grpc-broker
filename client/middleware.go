package client

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// ChainStreamInterceptor is short hand for grpc.ChainStreamInterceptor
func ChainStreamInterceptor(interceptors ...grpc.StreamClientInterceptor) grpc.DialOption {
	return grpc.WithChainStreamInterceptor(interceptors...)
}

// ChainUnaryInterceptor is short hand for ChainUnaryInterceptor
func ChainUnaryInterceptor(interceptors ...grpc.UnaryClientInterceptor) grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(interceptors...)
}

type Logger interface {
	Printf(string, ...interface{})
	Errorf(string, ...interface{})
}

func LogStreamInterceptor(il Logger) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		startTime := time.Now()

		cstream, err := streamer(ctx, desc, cc, method, opts...)

		if err != nil {
			code := status.Code(err)
			il.Errorf(
				"finished streaming in %v, code=(%s): %v",
				time.Now().Sub(startTime),
				code.String(),
				err,
			)
		} else {
			il.Printf(
				"finished streaming in %v",
				time.Now().Sub(startTime),
			)
		}

		return cstream, err
	}
}

func LogUnaryInterceptor(il Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		startTime := time.Now()

		err := invoker(ctx, method, req, reply, cc, opts...)

		if err != nil {
			il.Errorf(
				"finished unary in %v, method=(%s): %v",
				time.Now().Sub(startTime),
				method,
				err,
			)
		} else {
			il.Printf(
				"finished unary in %v, method=(%s)",
				time.Now().Sub(startTime),
				method,
			)
		}

		return err
	}
}
