package node

import (
	"unicode/utf8"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validateMutation(sessionID []byte, sequence uint64, key string) error {
	if len(sessionID) != len(raft.SessionID{}) {
		return validationError("session_id", "Client Session identity is %d bytes, want 16", len(sessionID))
	}
	if sequence == 0 {
		return validationError("sequence", "mutation sequence must begin at one")
	}
	return validateKey(key)
}

func validateKey(key string) error {
	if key == "" {
		return validationError("key", "Key must not be empty")
	}
	if !utf8.ValidString(key) {
		return validationError("key", "Key must be valid UTF-8")
	}
	if len(key) > maxKeyBytes {
		return validationError("key", "Key is %d bytes, limit is %d", len(key), maxKeyBytes)
	}
	return nil
}

func validationError(field, format string, args ...any) error {
	base := status.Newf(codes.InvalidArgument, format, args...)
	withDetails, err := base.WithDetails(&quorumkvv1.ValidationError{Field: field})
	if err != nil {
		return base.Err()
	}
	return withDetails.Err()
}
