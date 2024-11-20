# Lab2: Raft Consensus Implementation
A Python implementation of the Raft consensus algorithm for distributed systems. This implementation maintains consistency across a cluster of three nodes and supports various scenarios including leader election, leader changes, and crash simulations (for both leader and follower/candidate states).

## Requirements ⬇️
- Python 3
- No external dependencies. All libraries are built-in in Python

## Project Structure 🎨
```
├── node.py         # Main Raft node implementation
├── client.py       # Client interface for interacting with the cluster
├── config.py       # Configuration file for node settings
└── README.md       # This file
```

## Running our system 🏃🏼‍♀️‍➡️
1. Set your multiple internal IP addresses in the `config.py`.
2. Start the three nodes in separate terminals.
3. Then execute each line within their corresponding terminals (nodes):
   ```
   python node.py node1
   python node.py node2
   python node.py node3
   ```
4. Utilize the client to interact with the cluster:
```
python client.py [command] [arguments]
```

### Available client commands 🧑🏼‍💻:
- Submit a value to the log:
```
python client.py submit [value]
```
- Trigger a leader change in perfect conditions:
```
python client.py leader_change
```
- Simulate a node crash:
```
python client.py simulate_crash [node_name]
```
- View logs from all nodes:
```
python client.py print_logs
```

## Simulating required scenarios ✌🏼
## Scenario 1: Basic leader election
1. Start all three nodes
2. The nodes will automatically elect a leader
3. Submit values to verify the leader is working. After submitting values, you will see that the nodes will interact and if the majority is reached, the value is committed:
```
python client.py submit "test_value"
```

## Scenario 2: Leader Change (Perfect situation)
1. Make sure that all nodes are running and a leader is elected
2. Trigger a leader change:
```
python client.py leader_change
```
3. The current leader will step down and a new election will occur

## Scenario 3: Leader Crash with Log Inconsistency:
1. Submit several values to build up the log
```
python client.py submit "value1"
python client.py submit "value2"
python client.py submit "value3"
```
2. Simulate a crash on the leader (Let's assume node1 is the leader):
```
python client.py simulate_crash node1  # assuming node1 is leader
```
3. After crashing the leader, you will see that there are new corrupted values given the simulated scenario.
4. While the former leader is off, we can submit new values and the newly election leader will handle them perfectly.
5. After the former leader rejoins the cluster as a follower, it will check the node's state and recover the log, first checking what values are committed and comparing it to what it has. Since it has some corrupted entries, the algorithm will delete these and iteratively correct its values.

### BONUS 🤑
1. Run any of the previous scenarios.
2. Instead of crashing the leader node, crash any follower (Either by utilizing the client's function `python client.py simulate_crash node1  # assuming node1 is a follower`, or using `CTRL + C`)
3. Submit new values to the cluster.
4. Check that the log (saved t

## Implementation Details
The system implements core Raft features:
- Leader Election
- Log Replication
- Safety Properties
- Crash Recovery
Each node maintains:
- A persistent log file (`{node_name}_lab2Raft.txt`)
- Current term number
- Voted-for information
- Leader state
- Commit index
## Error Handling
Our implementation includes:
- Network timeout handling
- Crash recovery mechanisms
-Log consistency checks
- Thread-safe operations
## Log Files
Each node maintains its log in:
- node1_lab2Raft.txt
- node2_lab2Raft.txt
- node3_lab2Raft.txt
These files persist across restarts and are used for crash recovery.

## Monitoring the system 🧑🏼‍💻
1. Watch the console output of each node
2. Check the log files in the working directory
3. Use the print_logs command to view cluster state
