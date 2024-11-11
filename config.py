# config.py

NODES = {
    'node1': {'ip': '192.168.1.2', 'port': 5000},
    'node2': {'ip': '192.168.1.3', 'port': 5001},
    'node3': {'ip': '192.168.1.4', 'port': 5002},
}

# Timeout settings (in seconds)
ELECTION_TIMEOUT = (5, 10)  # Range for randomized election timeout
HEARTBEAT_INTERVAL = 2  # Interval for leader to send heartbeats
