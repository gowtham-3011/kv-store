# Distributed Key-Value Store with Replication

A small but *real* distributed key-value store: multiple nodes, automatic
leader election, synchronous replication, write-ahead logging for
durability, and automatic failover when the leader dies. No external
coordinator (no ZooKeeper/etcd) — leadership is derived independently by
each node from heartbeat data.

## Requirements

```
pip install flask requests --break-system-packages
```

## Running the cluster

Open 3 terminals (or 3 background processes) from this folder:

```bash
python3 node.py --id 0
python3 node.py --id 1
python3 node.py --id 2
```

The cluster is defined in `config.py` — 3 nodes on ports 5000/5001/5002.
Add a 4th node by adding an entry there and starting it the same way.

## Using the client

```bash
python3 client.py --node 0 status                 # see role/leader/alive peers
python3 client.py --node 0 put name nutanix        # write (works on ANY node)
python3 client.py --node 2 get name                # read
python3 client.py --node 1 delete name             # delete
```

You can `put`/`get`/`delete` against **any** node — if it isn't the
leader, it transparently forwards the write to whichever node currently
is the leader.

## Demoing failover (the interesting part)

1. Run `status` on any node — note who the leader is (lowest node ID
   currently alive, e.g. node 0).
2. `Ctrl+C` that node's process (or `pkill -f "node.py --id 0"`).
3. Within ~3 seconds (`HEARTBEAT_TIMEOUT_SEC` in `config.py`), the
   remaining nodes detect the leader is gone and every node's `/status`
   now reports the next-lowest alive node as leader.
4. Writes to any surviving node still succeed — they get forwarded to
   the new leader automatically.
5. Restart the dead node — it replays its write-ahead log from disk on
   startup, so it doesn't lose data it had written before dying.

## Architecture

```
        client
          │
          ▼
     ┌─────────┐   forwards write if not leader   ┌─────────┐
     │  node 2  │ ────────────────────────────────▶│  node 0  │◀── leader
     └─────────┘                                    └────┬────┘
          ▲                                              │ replicate
          │                                              ▼
          │                                         ┌─────────┐
          └─────────────── replicate ────────────────│  node 1  │
                                                       └─────────┘

  All 3 nodes heartbeat each other in the background to track liveness.
```

- **Leader election**: every node runs a background thread pinging every
  peer's `/health` endpoint once a second. Each node keeps its own view
  of who's alive (`last_seen` timestamps) and computes
  `leader = min(alive_node_ids)` independently. No node needs to be told
  it's the leader — it just is, as long as it has the lowest ID among
  everyone currently reachable.
- **Writes**: a `PUT`/`DELETE` on any node checks `is_leader()`. If true,
  it applies the write locally, appends it to its write-ahead log, then
  calls `/internal/replicate` on every alive peer. If false, it forwards
  the exact same request to whichever node it currently believes is the
  leader.
- **Reads**: served from local state on whichever node you ask — this is
  an eventual-consistency read. (A "strong read" variant would force the
  request to go to the leader — worth mentioning as an extension.)
- **Durability**: every applied write (whether local or replicated) is
  appended to `wal_node<id>.log` before being counted as done. On
  startup, a node replays its own WAL to rebuild in-memory state, so a
  restart doesn't lose previously-applied writes.

## Known limitations (good to bring up proactively in interviews — shows depth)

- **Replication is synchronous but best-effort**: if a replica is
  unreachable during a write, the leader logs a warning and moves on —
  it doesn't block or retry. There's no queue/retry mechanism, so a
  replica that was down during a write won't automatically catch up
  when it comes back (no anti-entropy / read-repair). A real system
  (this is essentially what Nutanix's own data path solves) would need
  a catch-up protocol for reconciling a rejoining replica.
- **No real consensus**: leader election here is a simplified
  "lowest-ID-alive" rule, not a quorum-based protocol like Raft or Paxos.
  It's vulnerable to split-brain in a network-partition scenario where
  two groups of nodes both believe they're isolated from a lower-ID
  node. Worth knowing what Raft would add here (log matching, term
  numbers, majority quorum commits) even though you didn't implement it.
- **No read quorum / strong consistency option**: reads can return stale
  data from a replica that hasn't received the latest replicated write
  yet.
- **Single-threaded WAL, no compaction**: the WAL only grows; a
  production system would periodically snapshot and truncate it.

These are exactly the right things to mention when an interviewer asks
"what would you change for production" — naming the gap and the
standard fix (Raft, quorum reads, anti-entropy) demonstrates you
understand the space beyond just what you built.

## REST API reference

| Method | Path                     | Description                                    |
|--------|--------------------------|-------------------------------------------------|
| GET    | `/keys/<key>`            | Read a key (local, eventually consistent)      |
| PUT    | `/keys/<key>`            | Write a key (forwarded to leader if needed)    |
| DELETE | `/keys/<key>`            | Delete a key (idempotent — repeat calls are safe) |
| GET    | `/status`                | Node role, current leader, alive peers, uptime |
| GET    | `/health`                | Liveness probe (used internally for heartbeats)|
| POST   | `/internal/replicate`    | Internal only — leader pushes writes to peers  |
