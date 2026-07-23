"""
Cluster configuration.

Every node runs the exact same config, so each node independently knows
the full membership of the cluster. This is what lets leader election
be fully decentralized -- no external coordinator needed.
"""

# node_id -> base URL. node_id order matters: lowest ID among currently
# ALIVE nodes is always the leader.
CLUSTER = {
    0: "http://localhost:5000",
    1: "http://localhost:5001",
    2: "http://localhost:5002",
}

HEARTBEAT_INTERVAL_SEC = 1.0     # how often a node pings its peers
HEARTBEAT_TIMEOUT_SEC = 5.0      # how long before a silent peer is marked dead
REQUEST_TIMEOUT_SEC = 3.0        # timeout for internal HTTP calls between nodes
# Windows loopback / firewall can add latency to the first few requests
# between processes, so these are a bit more generous than on Linux/Mac.