# QuorumKV

QuorumKV is a distributed key-value database whose members cooperate to present one strongly consistent store to clients.

## Language

**Cluster**:
A group of QuorumKV nodes that together present one logical key-value database.

**Node**:
An independently running member of a cluster, with its own identity and durable state.
_Avoid_: Server, instance, replica

**Key**:
A non-empty UTF-8 identifier for a value within a cluster.
_Avoid_: Path, record ID

**Value**:
An opaque sequence of bytes associated with a key; QuorumKV assigns no structure or meaning to its contents.
_Avoid_: JSON document, object

**Command**:
A client instruction submitted to the cluster. Data commands retrieve, set, or delete values; session commands open or close client sessions.
_Avoid_: Query, operation

**Mutation**:
A `SET` or `DELETE` command that may change the key-value database.
_Avoid_: Write

## Participants and Consensus

**Client**:
An external caller that submits commands to a cluster but does not participate in consensus.
_Avoid_: Cluster member, node

**Client Session**:
A bounded client interaction during which the cluster retains the identity, sequence, and result needed to deduplicate that client's mutations. A closed session cannot be reused.
_Avoid_: Connection, transaction

**Leader**:
The node elected to coordinate a cluster during a particular term.
_Avoid_: Primary, master

**Follower**:
A node that recognizes a leader and receives replicated state from it.
_Avoid_: Secondary, slave

**Candidate**:
A node seeking election as the leader for a new term.

**Term**:
A monotonically numbered period of cluster leadership or attempted leadership.
_Avoid_: Epoch, generation

**Quorum**:
A majority of the cluster's voting nodes; in the fixed three-node cluster, any two nodes form a quorum.
_Avoid_: All nodes, consensus

**Cluster Identity**:
A stable identifier that distinguishes one cluster and its durable history from every other cluster.
_Avoid_: Cluster name

**Node Identity**:
A stable identifier for one node within a cluster that remains the same across restarts.
_Avoid_: Network address, process ID

**Snapshot**:
A point-in-time image of the replicated state through a specific position in the cluster's history.
_Avoid_: Backup, database dump
