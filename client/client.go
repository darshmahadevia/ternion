// Package client implements the official QuorumKV v1 client behavior.
package client

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	maxAttempts    = 128
	initialBackoff = 25 * time.Millisecond
	maximumBackoff = 200 * time.Millisecond
)

// Client starts at one configured Node and follows typed Leader hints directly.
type Client struct {
	addresses []string
}

// New creates a Client that starts at the first address and falls back across
// the remaining configured Node addresses.
func New(addresses ...string) *Client {
	return &Client{addresses: append([]string(nil), addresses...)}
}

// OpenSession creates a replicated Client Session and returns its 128-bit identity.
func (c *Client) OpenSession(ctx context.Context) ([16]byte, error) {
	var sessionID [16]byte
	err := c.withLeader(ctx, func(client quorumkvv1.ClientServiceClient) error {
		response, err := client.OpenSession(ctx, &quorumkvv1.OpenSessionRequest{})
		if err != nil {
			return err
		}
		if len(response.SessionId) != len(sessionID) {
			return fmt.Errorf("leader returned a %d-byte Client Session identity, want 16", len(response.SessionId))
		}
		copy(sessionID[:], response.SessionId)
		return nil
	})
	return sessionID, err
}

// CloseSession permanently closes sessionID through consensus.
func (c *Client) CloseSession(ctx context.Context, sessionID [16]byte) error {
	return c.withLeader(ctx, func(client quorumkvv1.ClientServiceClient) error {
		_, err := client.CloseSession(ctx, &quorumkvv1.CloseSessionRequest{SessionId: sessionID[:]})
		return err
	})
}

// Set stores Value under key using the next sequence in sessionID.
func (c *Client) Set(ctx context.Context, sessionID [16]byte, sequence uint64, key string, value []byte) error {
	return c.withLeader(ctx, func(client quorumkvv1.ClientServiceClient) error {
		_, err := client.Set(ctx, &quorumkvv1.SetRequest{
			SessionId: sessionID[:],
			Sequence:  sequence,
			Key:       key,
			Value:     value,
		})
		return err
	})
}

// Delete removes key and reports whether a Value existed before the mutation.
func (c *Client) Delete(ctx context.Context, sessionID [16]byte, sequence uint64, key string) (bool, error) {
	var existed bool
	err := c.withLeader(ctx, func(client quorumkvv1.ClientServiceClient) error {
		response, err := client.Delete(ctx, &quorumkvv1.DeleteRequest{
			SessionId: sessionID[:],
			Sequence:  sequence,
			Key:       key,
		})
		if err != nil {
			return err
		}
		existed = response.Existed
		return nil
	})
	return existed, err
}

// Get returns the latest linearizable Value stored under key.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	var value []byte
	err := c.withLeader(ctx, func(client quorumkvv1.ClientServiceClient) error {
		response, err := client.Get(ctx, &quorumkvv1.GetRequest{Key: key})
		if err != nil {
			return err
		}
		value = append([]byte(nil), response.Value...)
		return nil
	})
	return value, err
}

func (c *Client) withLeader(ctx context.Context, call func(quorumkvv1.ClientServiceClient) error) error {
	if len(c.addresses) == 0 {
		return fmt.Errorf("at least one Node address is required")
	}
	configuredIndex := 0
	address := c.addresses[configuredIndex]
	backoff := initialBackoff
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("connect to Node at %q: %w", address, err)
		}
		err = call(quorumkvv1.NewClientServiceClient(connection))
		closeErr := connection.Close()
		if err == nil {
			if closeErr != nil {
				return fmt.Errorf("close connection to Node at %q: %w", address, closeErr)
			}
			return nil
		}
		hint, ok := leaderHint(err)
		if ok {
			address = hint
			continue
		}
		if status.Code(err) != codes.Unavailable {
			return err
		}
		configuredIndex = (configuredIndex + 1) % len(c.addresses)
		address = c.addresses[configuredIndex]
		jitter := time.Duration(rand.Int64N(int64(backoff)/2 + 1))
		timer := time.NewTimer(backoff/2 + jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, maximumBackoff)
	}
	return fmt.Errorf("command did not reach a stable Leader after %d attempts", maxAttempts)
}

func leaderHint(err error) (string, bool) {
	for _, detail := range status.Convert(err).Details() {
		notLeader, ok := detail.(*quorumkvv1.NotLeader)
		if ok && notLeader.LeaderAddress != "" {
			return notLeader.LeaderAddress, true
		}
	}
	return "", false
}
