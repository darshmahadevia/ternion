package node

import (
	"context"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/Het-Jethva/quorumkv/internal/raft"
	"github.com/Het-Jethva/quorumkv/internal/snapshot"
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
	sessions := newSessionMachine(n.config.ActiveSessionLimit)
	if err := sessions.restore(runtime.recoveredSnapshot); err != nil {
		return fmt.Errorf("restore replicated Snapshot state: %w", err)
	}
	recovery, err := runtime.step(raft.RecoverCommitted{})
	if err != nil {
		return err
	}
	for _, action := range recovery {
		apply, ok := action.(raft.ApplyEntry)
		if !ok {
			return fmt.Errorf("recover committed state: unexpected Raft action %T", action)
		}
		sessions.apply(apply.Entry)
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
		captured := sessions.snapshot(snapshotIdentity(n.config), state.LastApplied, state.LastAppliedTerm)
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

	type pendingProposal struct {
		result chan proposalResult
		ctx    context.Context
	}
	type inFlightMutation struct {
		sequence uint64
		index    uint64
	}
	type pendingRead struct {
		result chan readResult
		ctx    context.Context
		key    string
	}
	type incomingSnapshot struct {
		index, term, length uint64
		checksum            uint32
		data                []byte
	}
	var incoming *incomingSnapshot
	pending := make(map[uint64][]pendingProposal)
	inFlightMutations := make(map[raft.SessionID]inFlightMutation)
	pendingReads := make(map[raft.ReadID]pendingRead)
	for {
		for index, proposals := range pending {
			active := proposals[:0]
			for _, proposal := range proposals {
				if proposal.ctx.Err() == nil {
					active = append(active, proposal)
				}
			}
			if len(active) == 0 {
				delete(pending, index)
			} else {
				pending[index] = active
			}
		}
		for readID, read := range pendingReads {
			if read.ctx.Err() != nil {
				delete(pendingReads, readID)
			}
		}
		var event raft.Event
		var proposalResults chan proposalResult
		var proposalContext context.Context
		var readResults chan readResult
		var readKey string
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
		case input := <-n.events:
			if input.snapshotResult != nil {
				startSnapshot(input.snapshotResult, false)
				continue
			}
			event = input.event
			proposalResults = input.result
			proposalContext = input.requestContext
			readResults = input.readResult
			readKey = input.key
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
						if err := sessions.restore(state); err != nil {
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
		if read, ok := event.(raft.ConfirmRead); ok && readResults != nil {
			pendingReads[read.ReadID] = pendingRead{result: readResults, ctx: proposalContext, key: readKey}
		}

		mutationSession, mutationSequence, isMutation := proposedMutation(event)
		if isMutation {
			if mutation, exists := inFlightMutations[mutationSession]; exists && mutation.sequence == mutationSequence {
				pending[mutation.index] = append(pending[mutation.index], pendingProposal{result: proposalResults, ctx: proposalContext})
				continue
			}
			if result, shouldPropose := sessions.evaluateMutation(mutationSession, mutationSequence); !shouldPropose {
				proposalResults <- result
				continue
			}
			if n.observeMutation != nil {
				entry, _ := entryForMutation(event)
				n.observeMutation(mutationBeforeAppend, entry)
			}
		}

		wasLeader := runtime.core.State().Role == raft.Leader
		actions, err := runtime.step(event)
		if err != nil {
			return err
		}
		for _, action := range actions {
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
			case raft.ProposalAccepted:
				n.metrics.proposals.Add(1)
				if proposalResults != nil && proposalContext != nil {
					pending[action.Index] = append(pending[action.Index], pendingProposal{result: proposalResults, ctx: proposalContext})
				}
				if isMutation {
					inFlightMutations[mutationSession] = inFlightMutation{sequence: mutationSequence, index: action.Index}
				}
			case raft.ProposalRejected:
				if proposalResults != nil {
					proposalResults <- proposalResult{leaderID: action.LeaderID, rejected: true}
				}
			case raft.ApplyEntry:
				result := sessions.apply(action.Entry)
				if n.observeMutation != nil && (action.Entry.Type == raft.EntrySet || action.Entry.Type == raft.EntryDelete) {
					n.observeMutation(mutationAfterApplication, action.Entry)
				}
				if (action.Entry.Type == raft.EntrySet || action.Entry.Type == raft.EntryDelete) && inFlightMutations[action.Entry.SessionID].sequence == action.Entry.Sequence {
					delete(inFlightMutations, action.Entry.SessionID)
				}
				if proposals, ok := pending[action.Entry.Index]; ok {
					for _, proposal := range proposals {
						proposal.result <- result
					}
					delete(pending, action.Entry.Index)
				}
			case raft.BecameLeader:
				n.metrics.elections.Add(1)
			case raft.BecameReadReady, raft.LostLeadership:
				// Role and progress are read from the core after all actions finish.
			case raft.ReadConfirmed:
				read, ok := pendingReads[action.ReadID]
				if !ok {
					continue
				}
				value, found := sessions.get(read.key)
				read.result <- readResult{value: value, found: found}
				delete(pendingReads, action.ReadID)
			case raft.ReadRejected:
				read, ok := pendingReads[action.ReadID]
				if !ok {
					continue
				}
				read.result <- readResult{leaderID: action.LeaderID, rejected: true}
				delete(pendingReads, action.ReadID)
			}
		}

		state := runtime.core.State()
		if wasLeader && state.Role != raft.Leader {
			for index, proposals := range pending {
				for _, proposal := range proposals {
					proposal.result <- proposalResult{leaderID: state.LeaderID, rejected: true}
				}
				delete(pending, index)
			}
			clear(inFlightMutations)
			for readID, read := range pendingReads {
				read.result <- readResult{leaderID: state.LeaderID, rejected: true}
				delete(pendingReads, readID)
			}
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

func proposedMutation(event raft.Event) (raft.SessionID, uint64, bool) {
	switch event := event.(type) {
	case raft.ProposeSet:
		return event.SessionID, event.Sequence, true
	case raft.ProposeDelete:
		return event.SessionID, event.Sequence, true
	default:
		return raft.SessionID{}, 0, false
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
