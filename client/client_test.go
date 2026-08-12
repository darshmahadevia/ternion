package client

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
)

func TestGetFallsBackAcrossConfiguredNodes(t *testing.T) {
	unavailable, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve unavailable address: %v", err)
	}
	unavailableAddress := unavailable.Addr().String()
	unavailable.Close()

	server := &getServer{value: []byte{0, 1, 255}}
	availableAddress, stop := serveClient(t, server)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	value, err := New(unavailableAddress, availableAddress).Get(ctx, "opaque")
	if err != nil {
		t.Fatalf("Get() through fallback Node: %v", err)
	}
	if !bytes.Equal(value, server.value) {
		t.Fatalf("Get() Value = %v, want %v", value, server.value)
	}
}

func TestSetDoesNotRetrySequenceErrors(t *testing.T) {
	tests := []struct {
		name          string
		sequenceError func(*status.Status) (*status.Status, error)
	}{
		{
			name: "stale",
			sequenceError: func(base *status.Status) (*status.Status, error) {
				return base.WithDetails(&quorumkvv1.StaleSequence{ReceivedSequence: 1, LastSequence: 2})
			},
		},
		{
			name: "out of order",
			sequenceError: func(base *status.Status) (*status.Status, error) {
				return base.WithDetails(&quorumkvv1.OutOfOrderSequence{ReceivedSequence: 4, NextSequence: 3})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := status.New(codes.FailedPrecondition, test.name)
			withDetails, err := test.sequenceError(base)
			if err != nil {
				t.Fatalf("attach typed sequence detail: %v", err)
			}
			server := &sequenceErrorServer{err: withDetails.Err()}
			address, stop := serveClient(t, server)
			defer stop()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err = New(address).Set(ctx, [16]byte{1}, 1, "key", []byte("value"))
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("Set() error = %v, want FailedPrecondition", err)
			}
			if attempts := server.attempts.Load(); attempts != 1 {
				t.Fatalf("Set() attempts = %d, want one for a non-retriable sequence error", attempts)
			}
		})
	}
}

func TestPermanentContractErrorsAreNeverRetried(t *testing.T) {
	withDetail := func(t *testing.T, code codes.Code, message string, detail protoadapt.MessageV1) error {
		t.Helper()
		result, err := status.New(code, message).WithDetails(detail)
		if err != nil {
			t.Fatalf("attach typed error detail: %v", err)
		}
		return result.Err()
	}

	tests := []struct {
		name   string
		err    func(*testing.T) error
		invoke func(context.Context, *Client) error
	}{
		{
			name: "validation",
			err: func(t *testing.T) error {
				return withDetail(t, codes.InvalidArgument, "invalid Key", &quorumkvv1.ValidationError{Field: "key"})
			},
			invoke: func(ctx context.Context, client *Client) error {
				return client.Set(ctx, [16]byte{1}, 1, "", nil)
			},
		},
		{
			name: "missing Key",
			err: func(t *testing.T) error {
				return withDetail(t, codes.NotFound, "missing Key", &quorumkvv1.KeyNotFound{Key: "missing"})
			},
			invoke: func(ctx context.Context, client *Client) error {
				_, err := client.Get(ctx, "missing")
				return err
			},
		},
		{
			name: "invalid Client Session",
			err: func(t *testing.T) error {
				return withDetail(t, codes.NotFound, "unknown Client Session", &quorumkvv1.InvalidSession{Reason: quorumkvv1.InvalidSessionReason_INVALID_SESSION_REASON_UNKNOWN})
			},
			invoke: func(ctx context.Context, client *Client) error {
				_, err := client.Delete(ctx, [16]byte{1}, 1, "key")
				return err
			},
		},
		{
			name: "stale sequence",
			err: func(t *testing.T) error {
				return withDetail(t, codes.FailedPrecondition, "stale", &quorumkvv1.StaleSequence{})
			},
			invoke: func(ctx context.Context, client *Client) error {
				return client.Set(ctx, [16]byte{1}, 1, "key", nil)
			},
		},
		{
			name: "out-of-order sequence",
			err: func(t *testing.T) error {
				return withDetail(t, codes.FailedPrecondition, "out of order", &quorumkvv1.OutOfOrderSequence{})
			},
			invoke: func(ctx context.Context, client *Client) error {
				_, err := client.Delete(ctx, [16]byte{1}, 3, "key")
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &contractErrorServer{err: test.err(t)}
			address, stop := serveClient(t, server)
			defer stop()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := test.invoke(ctx, New(address)); err == nil {
				t.Fatal("command succeeded, want permanent contract error")
			}
			if attempts := server.attempts.Load(); attempts != 1 {
				t.Fatalf("command attempts = %d, want one", attempts)
			}
		})
	}
}

type sequenceErrorServer struct {
	quorumkvv1.UnimplementedClientServiceServer
	err      error
	attempts atomic.Int32
}

type getServer struct {
	quorumkvv1.UnimplementedClientServiceServer
	value []byte
}

type contractErrorServer struct {
	quorumkvv1.UnimplementedClientServiceServer
	err      error
	attempts atomic.Int32
}

func (s *contractErrorServer) Set(context.Context, *quorumkvv1.SetRequest) (*quorumkvv1.SetResponse, error) {
	s.attempts.Add(1)
	return nil, s.err
}

func (s *contractErrorServer) Get(context.Context, *quorumkvv1.GetRequest) (*quorumkvv1.GetResponse, error) {
	s.attempts.Add(1)
	return nil, s.err
}

func (s *contractErrorServer) Delete(context.Context, *quorumkvv1.DeleteRequest) (*quorumkvv1.DeleteResponse, error) {
	s.attempts.Add(1)
	return nil, s.err
}

func (s *getServer) Get(context.Context, *quorumkvv1.GetRequest) (*quorumkvv1.GetResponse, error) {
	return &quorumkvv1.GetResponse{Value: append([]byte(nil), s.value...)}, nil
}

func (s *sequenceErrorServer) Set(context.Context, *quorumkvv1.SetRequest) (*quorumkvv1.SetResponse, error) {
	s.attempts.Add(1)
	return nil, s.err
}

func serveClient(t *testing.T, service quorumkvv1.ClientServiceServer) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	quorumkvv1.RegisterClientServiceServer(server, service)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	return listener.Addr().String(), func() {
		server.Stop()
		<-done
	}
}
