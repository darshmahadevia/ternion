package node

import (
	"context"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/darshmahadevia/ternion/internal/raft"
	"github.com/darshmahadevia/ternion/internal/snapshot"
)

const (
	heartbeatInterval  = 100 * time.Millisecond
	electionTimeoutMin = 500 * time.Millisecond
	electionTimeoutMax = time.Second
	checkQuorumWindow  = time.Second
)

func (n *Node) runRaft(ctx context.Context, runtime *raftRuntime, transport *peerTransport) error {
	random := rand.New(rand.NewSource(time.Now().UnixNano() + nodeSeed(n.config.Node.ID))) // #nosec G404 -- election jitter is not security-sensitive.
	electionTimer := time.NewTimer(randomElectionTimeout(random))
	defer electionTimer.Stop()

	var heartbeatTimer, quorumTimer *time.Timer
	var heartbeatC, quorumC <-chan time.Time
	defer func() {
		stopTimer(heartbeatTimer)
		stopTimer(quorumTimer)
	}()

	n.publishRaftState(runtime.core.State())
	commands := newCommandCoordinator(n.config.ActiveSessionLimit, &n.metrics, n.observeMutation)
	if err := commands.restore(runtime.recoveredSnapshot); err != nil {
		return fmt.Errorf("restore replicated Snapshot state: %w", err)
	}
	recovery, err := runtime.step(raft.RecoverCommitted{})
	if err != nil {
		return err
	}
	if err := commands.recover(recovery); err != nil {
		return err
	}
	n.publishRaftState(runtime.core.State())

	type snapshotCompletion struct {
		index uint64
		term  uint64
		err   error
	}
	snapshotDone := make(chan snapshotCompletion, 1)
	snapshotRunning := false
	snapshotAutomatic := false
	var snapshotWaiters []chan error
	var lastSnapshotIndex uint64
	if runtime.recoveredSnapshot != nil {
		lastSnapshotIndex = runtime.recoveredSnapshot.IncludedIndex
	}
	startSnapshot := func(waiter chan error, automatic bool) {
		if waiter != nil {
			snapshotWaiters = append(snapshotWaiters, waiter)
		}
		if snapshotRunning {
			return
		}
		n.metrics.snapshots.Add(1)
		state := runtime.core.State()
		captured := commands.snapshot(snapshotIdentity(n.config), state.LastApplied, state.LastAppliedTerm)
		snapshotRunning = true
		snapshotAutomatic = automatic
		go func() {
			snapshotDone <- snapshotCompletion{
				index: captured.IncludedIndex,
				term:  captured.IncludedTerm,
				err:   installSnapshot(n.config.Node.DataDir, captured, runtime.wal),
			}
		}()
	}
	finishSnapshot := func(completion snapshotCompletion) {
		for _, waiter := range snapshotWaiters {
			waiter <- completion.err
		}
		snapshotWaiters = nil
		snapshotRunning = false
		if completion.err == nil && completion.index > lastSnapshotIndex {
			lastSnapshotIndex = completion.index
		}
	}
	defer func() {
		if snapshotRunning {
			finishSnapshot(<-snapshotDone)
		}
	}()

	type incomingSnapshot struct {
		index, term, length uint64
		checksum            uint32
		data                []byte
	}
	var incoming *incomingSnapshot
	for {
		commands.pruneCanceled()
		var event raft.Event
		var current runtimeInput
		select {
		case <-ctx.Done():
			if snapshotRunning {
				finishSnapshot(<-snapshotDone)
			}
			return nil
		case completion := <-snapshotDone:
			wasAutomatic := snapshotAutomatic
			finishSnapshot(completion)
			if completion.err != nil && wasAutomatic {
				return fmt.Errorf("automatic Snapshot: %w", completion.err)
			}
			if completion.err == nil {
				n.metrics.compactions.Add(1)
				if _, err := runtime.step(raft.SnapshotCompacted{Index: completion.index, Term: completion.term}); err != nil {
					return err
				}
				n.publishRaftState(runtime.core.State())
			}
			continue
		case input := <-n.inputs:
			current = input
			switch input := input.(type) {
			case raftEventInput:
				event = input.event
			case proposalInput:
				var shouldStep bool
				event, shouldStep = commands.beginProposal(input)
				if !shouldStep {
					continue
				}
			case readInput:
				event = commands.beginRead(input)
			case snapshotInput:
				startSnapshot(input.result, false)
				continue
			default:
				return fmt.Errorf("run Raft: unsupported runtime input %T", input)
			}
		case <-electionTimer.C:
			event = raft.ElectionTimeout{}
		case <-heartbeatC:
			event = raft.HeartbeatTimeout{}
		case <-quorumC:
			event = raft.CheckQuorumTimeout{}
		}
		if request, ok := event.(raft.InstallSnapshot); ok {
			if request.Offset == 0 {
				incoming = &incomingSnapshot{index: request.SnapshotIndex, term: request.SnapshotTerm, length: request.Length, checksum: request.Checksum}
			}
			if incoming == nil || incoming.index != request.SnapshotIndex || incoming.term != request.SnapshotTerm || incoming.length != request.Length || incoming.checksum != request.Checksum || request.Offset != uint64(len(incoming.data)) || request.Offset+uint64(len(request.Data)) > incoming.length {
				request.Success = false
				request.NextOffset = 0
			} else {
				incoming.data = append(incoming.data, request.Data...)
				request.Success = true
				request.NextOffset = uint64(len(incoming.data))
				if request.Done {
					valid := uint64(len(incoming.data)) == incoming.length && crc32.ChecksumIEEE(incoming.data) == incoming.checksum
					state, decodeErr := snapshot.Decode(incoming.data)
					valid = valid && decodeErr == nil && state.IncludedIndex == incoming.index && state.IncludedTerm == incoming.term && snapshotClusterMatches(state.Identity, snapshotIdentity(n.config))
					if valid {
						state.Identity = snapshotIdentity(n.config)
						if _, err := snapshot.Save(n.config.Node.DataDir, *state); err != nil {
							return fmt.Errorf("install received Snapshot file: %w", err)
						}
						if err := runtime.wal.InstallSnapshot(state.IncludedIndex, state.IncludedTerm); err != nil {
							return fmt.Errorf("persist received Snapshot: %w", err)
						}
						if err := commands.restore(state); err != nil {
							return fmt.Errorf("restore received Snapshot: %w", err)
						}
						n.metrics.snapshotInstalls.Add(1)
						request.Installed = true
					} else {
						request.Success = false
						request.NextOffset = 0
					}
					incoming = nil
				}
			}
			event = request
		}
		wasLeader := runtime.core.State().Role == raft.Leader
		actions, err := runtime.step(event)
		if err != nil {
			return err
		}
		for _, action := range actions {
			if commands.handle(action, current) {
				continue
			}
			switch action := action.(type) {
			case raft.SendPreVoteRequest, raft.SendPreVoteResponse, raft.SendVoteRequest,
				raft.SendVoteResponse, raft.SendAppendEntries, raft.SendAppendEntriesResponse,
				raft.SendInstallSnapshot, raft.SendInstallSnapshotResponse:
				n.metrics.raftRPCs.Add(1)
				// A missing peer is ordinary during startup, elections, and process loss.
				// The next timer or inbound message retries protocol progress.
				if err := transport.send(ctx, action); err != nil {
					if isPeerConfigurationError(err) {
						return err
					}
					var failedTo raft.NodeID
					var failedRequest uint64
					switch sent := action.(type) {
					case raft.SendAppendEntries:
						failedTo, failedRequest = sent.To, sent.Request.RequestID
					case raft.SendInstallSnapshot:
						failedTo, failedRequest = sent.To, sent.RequestID
					}
					if failedRequest != 0 {
						failed, stepErr := runtime.step(raft.AppendEntriesFailed{To: failedTo, RequestID: failedRequest})
						if stepErr != nil {
							return stepErr
						}
						if len(failed) != 0 {
							return fmt.Errorf("report failed replication delivery: unexpected Raft actions %T", failed[0])
						}
					}
				}
			case raft.ResetElectionTimer:
				resetTimer(electionTimer, randomElectionTimeout(random))
			case raft.ResetHeartbeatTimer:
				heartbeatTimer, heartbeatC = resetOptionalTimer(heartbeatTimer, heartbeatInterval)
			case raft.ResetCheckQuorumTimer:
				quorumTimer, quorumC = resetOptionalTimer(quorumTimer, checkQuorumWindow)
			case raft.BecameLeader:
				n.metrics.elections.Add(1)
			case raft.BecameReadReady, raft.LostLeadership:
				// Role and progress are read from the core after all actions finish.
			}
		}

		state := runtime.core.State()
		if wasLeader && state.Role != raft.Leader {
			commands.lostLeadership(state.LeaderID)
		}
		if state.Role != raft.Leader {
			stopTimer(heartbeatTimer)
			stopTimer(quorumTimer)
			heartbeatC, quorumC = nil, nil
		}
		n.publishRaftState(state)
		threshold := uint64(n.config.EffectiveSnapshotThresholdBytes())
		if !snapshotRunning && state.LastApplied > lastSnapshotIndex && runtime.wal.RetainedLogBytes(state.LastApplied) >= threshold {
			startSnapshot(nil, true)
		}
	}
}

func nodeSeed(id string) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(id))
	return int64(hash.Sum64())
}

func randomElectionTimeout(random *rand.Rand) time.Duration {
	span := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(random.Int63n(span+1))
}

func resetOptionalTimer(timer *time.Timer, delay time.Duration) (*time.Timer, <-chan time.Time) {
	if timer == nil {
		timer = time.NewTimer(delay)
	} else {
		resetTimer(timer, delay)
	}
	return timer, timer.C
}

func resetTimer(timer *time.Timer, delay time.Duration) {
	stopTimer(timer)
	timer.Reset(delay)
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
