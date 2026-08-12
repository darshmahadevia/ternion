# Implement the Raft core in QuorumKV

QuorumKV will implement its own Raft state machine instead of embedding an existing consensus library. This increases the testing and correctness burden, but makes leader election, log replication, commit safety, and crash recovery first-class parts of the project that can be demonstrated and explained in interviews; standard libraries may still provide networking, serialization, and disk I/O.
