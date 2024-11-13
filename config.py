# config.py

NODES = {
    'node1': {'ip': '10.128.0.4', 'port': 5000},
    'node2': {'ip': '10.128.0.5', 'port': 5000},
    'node3': {'ip': '10.128.0.6', 'port': 5000},
}

# Timeout settings (in seconds)
ELECTION_TIMEOUT = (1.0, 2.0)  # Adjusted for faster testing
HEARTBEAT_INTERVAL = 0.5  # Interval for leader to send heartbeats
