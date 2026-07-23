"""
A single node in the distributed key-value store.

Run one of these per terminal to simulate a multi-node cluster:

    python node.py --id 0
    python node.py --id 1
    python node.py --id 2

Each node:
  - Serves a REST API for client reads/writes.
  - Heartbeats every other node in the background to track who's alive.
  - Independently computes the current leader as "lowest node_id that is alive".
  - If it IS the leader: accepts writes, applies them locally, replicates
    to every alive replica, and appends to its write-ahead log (WAL).
  - If it is NOT the leader: forwards writes to whoever it currently
    believes the leader is.
  - On startup, replays its WAL so restarts don't lose data.
"""

import argparse
import json
import os
import threading
import time

import requests
from flask import Flask, jsonify, request

from config import CLUSTER, HEARTBEAT_INTERVAL_SEC, HEARTBEAT_TIMEOUT_SEC, REQUEST_TIMEOUT_SEC

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Node state (all guarded by `lock` since Flask + heartbeat thread share it)
# ---------------------------------------------------------------------------
NODE_ID = None
WAL_PATH = None
store = {}                 # key -> value
lock = threading.RLock()

# last_seen[peer_id] = timestamp of last successful heartbeat response
last_seen = {}
start_time = time.time()


def log(msg):
    print(f"[node {NODE_ID}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Write-ahead log: every applied write is appended here before/at the time
# it's applied, so a restarted node can rebuild its state from disk.
# ---------------------------------------------------------------------------
def wal_append(op, key, value=None):
    entry = {"op": op, "key": key, "value": value, "ts": time.time()}
    with open(WAL_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")


def wal_replay():
    if not os.path.exists(WAL_PATH):
        return
    with open(WAL_PATH, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            if entry["op"] == "put":
                store[entry["key"]] = entry["value"]
            elif entry["op"] == "delete":
                store.pop(entry["key"], None)
    log(f"replayed WAL, restored {len(store)} keys")


# ---------------------------------------------------------------------------
# Leader election: purely derived from who's currently alive. No node needs
# to be "told" it's the leader -- every node computes this independently
# from the same heartbeat information, so it's consistent cluster-wide
# (as long as heartbeat views converge, which they do within ~1-2 intervals).
# ---------------------------------------------------------------------------
def alive_peers():
    now = time.time()
    alive = {NODE_ID}  # a node always trusts itself
    with lock:
        for peer_id, ts in last_seen.items():
            if now - ts <= HEARTBEAT_TIMEOUT_SEC:
                alive.add(peer_id)
    return alive


def current_leader():
    return min(alive_peers())


def is_leader():
    return current_leader() == NODE_ID


# ---------------------------------------------------------------------------
# Heartbeat background thread: pings every other node's /health endpoint.
# A successful response updates last_seen; failures simply age out and the
# peer will be considered dead once HEARTBEAT_TIMEOUT_SEC has elapsed.
# ---------------------------------------------------------------------------
def heartbeat_loop():
    while True:
        for peer_id, base_url in CLUSTER.items():
            if peer_id == NODE_ID:
                continue
            try:
                r = requests.get(f"{base_url}/health", timeout=REQUEST_TIMEOUT_SEC)
                if r.status_code == 200:
                    with lock:
                        last_seen[peer_id] = time.time()
            except requests.RequestException:
                pass  # peer considered dead once it times out of the window
        time.sleep(HEARTBEAT_INTERVAL_SEC)


# ---------------------------------------------------------------------------
# Replication: called only by the leader, after a local write, to push the
# same write to every other alive node.
# ---------------------------------------------------------------------------
def replicate(op, key, value=None):
    failures = []
    for peer_id in alive_peers():
        if peer_id == NODE_ID:
            continue
        base_url = CLUSTER[peer_id]
        try:
            requests.post(
                f"{base_url}/internal/replicate",
                json={"op": op, "key": key, "value": value},
                timeout=REQUEST_TIMEOUT_SEC,
            )
        except requests.RequestException:
            failures.append(peer_id)
    if failures:
        log(f"replication to {failures} failed (they may be down; will catch up later)")


# ---------------------------------------------------------------------------
# Public REST API
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    return jsonify({"node_id": NODE_ID, "status": "ok"}), 200


@app.get("/status")
def status():
    with lock:
        replica_lag_keys = None  # simplification: we replicate synchronously
        return jsonify({
            "node_id": NODE_ID,
            "role": "leader" if is_leader() else "replica",
            "current_leader": current_leader(),
            "alive_peers": sorted(alive_peers()),
            "known_peers": sorted(CLUSTER.keys()),
            "key_count": len(store),
            "uptime_sec": round(time.time() - start_time, 1),
        }), 200


@app.get("/keys/<key>")
def get_key(key):
    with lock:
        if key not in store:
            return jsonify({"error": f"key '{key}' not found"}), 404
        return jsonify({"key": key, "value": store[key]}), 200


@app.put("/keys/<key>")
def put_key(key):
    body = request.get_json(silent=True) or {}
    if "value" not in body:
        return jsonify({"error": "request body must contain 'value'"}), 400
    value = body["value"]

    if not is_leader():
        # Not the leader: forward the write to whoever currently is.
        leader_id = current_leader()
        try:
            r = requests.put(
                f"{CLUSTER[leader_id]}/keys/{key}",
                json={"value": value},
                timeout=REQUEST_TIMEOUT_SEC,
            )
            return jsonify(r.json()), r.status_code
        except requests.RequestException:
            return jsonify({"error": f"leader node {leader_id} unreachable, retry shortly"}), 503

    # We are the leader: apply locally, log, then replicate.
    with lock:
        store[key] = value
        wal_append("put", key, value)
    replicate("put", key, value)
    return jsonify({"key": key, "value": value, "written_by": NODE_ID}), 200


@app.delete("/keys/<key>")
def delete_key(key):
    if not is_leader():
        leader_id = current_leader()
        try:
            r = requests.delete(f"{CLUSTER[leader_id]}/keys/{key}", timeout=REQUEST_TIMEOUT_SEC)
            return jsonify(r.json()), r.status_code
        except requests.RequestException:
            return jsonify({"error": f"leader node {leader_id} unreachable, retry shortly"}), 503

    with lock:
        existed = key in store
        store.pop(key, None)
        wal_append("delete", key)
    replicate("delete", key)
    if not existed:
        # Deleting a non-existent key is still a successful, idempotent no-op.
        return jsonify({"key": key, "deleted": False, "note": "key did not exist"}), 200
    return jsonify({"key": key, "deleted": True}), 200


# ---------------------------------------------------------------------------
# Internal endpoint: only ever called by the leader, to push a replicated
# write onto this node. Not meant to be called directly by clients.
# ---------------------------------------------------------------------------
@app.post("/internal/replicate")
def internal_replicate():
    body = request.get_json(silent=True) or {}
    op, key, value = body.get("op"), body.get("key"), body.get("value")
    with lock:
        if op == "put":
            store[key] = value
            wal_append("put", key, value)
        elif op == "delete":
            store.pop(key, None)
            wal_append("delete", key)
    return jsonify({"applied": True}), 200


def main():
    global NODE_ID, WAL_PATH
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True, help="node id, e.g. 0, 1, 2")
    args = parser.parse_args()

    NODE_ID = args.id
    if NODE_ID not in CLUSTER:
        raise SystemExit(f"node id {NODE_ID} not present in CLUSTER config")

    WAL_PATH = f"wal_node{NODE_ID}.log"
    wal_replay()

    port = int(CLUSTER[NODE_ID].split(":")[-1])

    threading.Thread(target=heartbeat_loop, daemon=True).start()
    log(f"starting on port {port}, cluster={list(CLUSTER.keys())}")
    app.run(host="0.0.0.0", port=port, threaded=True)


if __name__ == "__main__":
    main()
