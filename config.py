# config.py

NODES = {
    'node1': {'ip': 'localhost', 'port': 5001},
    'node2': {'ip': 'localhost', 'port': 5002},
    'node3': {'ip': 'localhost', 'port': 5003},
}

# Timeout settings (in seconds)
ELECTION_TIMEOUT = (1.0, 2.0)  # Adjusted for faster testing
HEARTBEAT_INTERVAL = 0.5  # Interval for leader to send heartbeats
