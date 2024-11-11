# compute_node.py
import json
import time
import os
import random
from threading import Thread, Lock
from multiprocessing.connection import Listener, Client

class ComputeNode:
    """
    ComputeNode class implementing the Raft consensus algorithm.
    Each node can be in one of three states: follower, candidate, or leader.
    """
    
    def __init__(self, node_id, nodes_config):
        # Node identification
        self.node_id = node_id
        self.nodes_config = nodes_config
        self.connections = {}
        
        # Raft persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []  # List of dictionaries with 'term' and 'value'
        
        # Raft volatile state
        self.state = 'follower'
        self.leader_id = None
        self.votes_received = set()
        self.commit_index = -1
        self.last_applied = -1
        
        # Leader volatile state (reinitialized after election)
        self.next_index = {}    # For each server, index of next log entry to send
        self.match_index = {}   # For each server, index of highest log entry known to be replicated
        
        # Configuration
        self.election_timeout = random.uniform(1.5, 3.0)  # Random timeout between 1.5-3 seconds
        self.heartbeat_interval = 0.5  # Heartbeat every 0.5 seconds
        self.last_heartbeat = time.time()
        
        # Synchronization
        self.state_lock = Lock()
        self.log_lock = Lock()
        
        # Initialize log file
        self.log_file = "CISC6935"  # As per requirements
        self.create_log_file()
        
        # Initialize connections to other nodes
        self.initialize_connections()

# In ComputeNode class, update create_log_file:
    def create_log_file(self):
        """Create the initial log file as required"""
        try:
            with open(self.log_file, 'w') as f:
                f.write('')
            print(f"Node {self.node_id}: Created log file {self.log_file}")
        except Exception as e:
            print(f"Node {self.node_id}: Error creating log file: {e}")
            raise e  # Re-raise to catch initialization failures

    def initialize_connections(self):
        """Initialize connections to other nodes in the cluster with retry mechanism"""
        def try_connect(other_id, addr):
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    print(f"Node {self.node_id}: Attempting to connect to node {other_id} (attempt {attempt + 1})")
                    connection = Client(addr, authkey=b'raft')
                    self.connections[other_id] = connection
                    print(f"Node {self.node_id}: Successfully connected to node {other_id}")
                    return True
                except Exception as e:
                    print(f"Node {self.node_id}: Failed to connect to node {other_id} (attempt {attempt + 1}): {e}")
                    time.sleep(2)  # Wait longer between retries
            return False

        # Initial connection attempt
        for other_id, addr in self.nodes_config.items():
            if other_id != self.node_id:
                try_connect(other_id, addr)

        # Start a background thread for continuous connection attempts
        def connection_monitor():
            while True:
                for other_id, addr in self.nodes_config.items():
                    if other_id != self.node_id and other_id not in self.connections:
                        try_connect(other_id, addr)
                time.sleep(5)  # Wait before next retry cycle

        monitor_thread = Thread(target=connection_monitor, daemon=True)
        monitor_thread.start()

    def start(self):
        """Start the node's operation"""
        print(f"\nNode {self.node_id}: Starting as follower")
        self.state = 'follower'
        self.last_heartbeat = time.time()
        
        # Start RPC listener first
        self.listener_thread = Thread(target=self.start_listener)
        self.listener_thread.daemon = True
        self.listener_thread.start()
        
        # Give some time for listeners to start
        time.sleep(2)
        
        # Start heartbeat checker (which will trigger election)
        self.heartbeat_thread = Thread(target=self.check_heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        print(f"Node {self.node_id}: Started and waiting for election timeout")

    def start_listener(self):
        """Start listening for RPC calls"""
        address = self.nodes_config[self.node_id]
        try:
            print(f"Node {self.node_id}: Starting listener on {address}")
            listener = Listener(address, authkey=b'raft')
            while True:
                try:
                    conn = listener.accept()
                    print(f"Node {self.node_id}: Accepted new connection")
                    Thread(target=self.handle_connection, args=(conn,), daemon=True).start()
                except Exception as e:
                    print(f"Node {self.node_id}: Error accepting connection: {e}")
        except Exception as e:
            print(f"Node {self.node_id}: Failed to start listener: {e}")

    def handle_connection(self, conn):
        """Handle incoming RPC connections"""
        try:
            while True:
                msg = conn.recv()
                request = json.loads(msg)
                response = self.handle_request(request)
                time.sleep(random.uniform(0.05, 0.15))  # Simulate network delay
                conn.send(json.dumps(response))
        except EOFError:
            pass
        except Exception as e:
            print(f"Node {self.node_id}: Error handling connection: {e}")

    def handle_request(self, request):
        """Handle different types of RPC requests"""
        request_type = request.get('type')
        
        if request_type == 'request_vote':
            return self.handle_vote_request(request)
        elif request_type == 'append_entries':
            return self.handle_append_entries(request)
        elif request_type == 'submit_value':
            return self.handle_submit_value(request)
        elif request_type == 'force_leader_change':
            return self.handle_force_leader_change()
        elif request_type == 'simulate_crash':
            return self.handle_simulate_crash()
        elif request_type == 'sync_request':
            return self.handle_sync_request(request)
        
        return {'success': False, 'message': 'Unknown request type'}

    def check_heartbeat(self):
        """Check for heartbeat timeout and start election if needed"""
        while True:
            time.sleep(0.1)  # Check every 100ms
            with self.state_lock:
                if self.state != 'leader':
                    current_time = time.time()
                    time_since_last_heartbeat = current_time - self.last_heartbeat
                    
                    if time_since_last_heartbeat > self.election_timeout:
                        print(f"\nNode {self.node_id}: Starting election due to timeout")
                        print(f"Last heartbeat: {time_since_last_heartbeat:.2f} seconds ago")
                        self.last_heartbeat = current_time
                        
                        # Start election in a new thread
                        election_thread = Thread(target=self.start_election)
                        election_thread.daemon = True
                        election_thread.start()

    def start_election(self):
        """Start a new election"""
        with self.state_lock:
            self.state = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes_received = {self.node_id}  # Vote for self
            print(f"\nNode {self.node_id}: Starting election for term {self.current_term}")
            print(f"Node {self.node_id}: Voted for self")

        # Request votes from all other nodes
        votes_received = 1  # Count self-vote
        for other_id in self.nodes_config:
            if other_id != self.node_id:
                try:
                    print(f"Node {self.node_id}: Requesting vote from node {other_id}")
                    if self.request_vote(other_id):  # Request vote from other nodes
                        votes_received += 1
                        print(f"Node {self.node_id}: Received vote from node {other_id}")
                        if votes_received > len(self.nodes_config) // 2:  # Majority achieved
                            self.become_leader()
                            return
                except Exception as e:
                    print(f"Node {self.node_id}: Error requesting vote from node {other_id}: {e}")

        print(f"Node {self.node_id}: Election ended with {votes_received} votes")

    def start_election(self):
        """Start a new election"""
        with self.state_lock:
            self.state = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes_received = {self.node_id}
            print(f"\nNode {self.node_id}: Starting election for term {self.current_term}")
            print(f"Node {self.node_id}: Voted for self")

        # Request votes from all other nodes
        votes_received = 1  # Count self-vote
        for other_id in self.nodes_config:
            if other_id != self.node_id:
                try:
                    print(f"Node {self.node_id}: Requesting vote from node {other_id}")
                    if self.request_vote(other_id):
                        votes_received += 1
                        print(f"Node {self.node_id}: Received vote from node {other_id}")
                        if votes_received > len(self.nodes_config) // 2:  # Majority achieved
                            self.become_leader()
                            return
                except Exception as e:
                    print(f"Node {self.node_id}: Error requesting vote from node {other_id}: {e}")

        print(f"Node {self.node_id}: Election ended with {votes_received} votes")
    
    def request_vote(self, target_id):
        """Send RequestVote RPC to a target node"""
        with self.state_lock:
            # Prepare the RequestVote message
            request = {
                'type': 'request_vote',
                'term': self.current_term,
                'candidate_id': self.node_id,
                'last_log_index': len(self.log) - 1,
                'last_log_term': self.log[-1]['term'] if self.log else 0
            }
        
        try:
            # Get the connection to the target node
            conn = self.connections.get(target_id)
            if conn:
                print(f"Node {self.node_id}: Sending vote request to node {target_id}")
                
                # Send the RequestVote message
                conn.send(json.dumps(request))
                
                # Wait for and process the response
                response = json.loads(conn.recv())
                
                # If the vote was granted, return True
                if response.get('vote_granted'):
                    return True
                
                # If the target node has a higher term, update our term and step down to follower
                if response.get('term', 0) > self.current_term:
                    with self.state_lock:
                        print(f"Node {self.node_id}: Stepping down due to higher term from node {target_id}")
                        self.current_term = response['term']
                        self.state = 'follower'
                        self.voted_for = None
                return False

        except Exception as e:
            print(f"Node {self.node_id}: Error requesting vote from node {target_id}: {e}")
            return False
        
    def handle_vote_request(self, request):
        """Handle incoming vote requests"""
        with self.state_lock:
            print(f"Node {self.node_id}: Received vote request from node {request['candidate_id']}")

            # If the term in the request is lower than the current term, deny the vote.
            if request['term'] < self.current_term:
                print(f"Node {self.node_id}: Denying vote - lower term")
                return {'vote_granted': False, 'term': self.current_term}

            # If the term in the request is higher, update our current term and become a follower.
            if request['term'] > self.current_term:
                print(f"Node {self.node_id}: Updating term and becoming follower")
                self.current_term = request['term']
                self.state = 'follower'
                self.voted_for = None

            # Grant the vote if we haven't voted yet or voted for this candidate in this term.
            if (self.voted_for is None or self.voted_for == request['candidate_id']):
                print(f"Node {self.node_id}: Granting vote to node {request['candidate_id']}")
                self.voted_for = request['candidate_id']
                return {'vote_granted': True, 'term': self.current_term}

            print(f"Node {self.node_id}: Denying vote - already voted for another candidate")
            return {'vote_granted': False, 'term': self.current_term}

    def become_leader(self):
        """Transition to leader state"""
        with self.state_lock:
            if self.state == 'candidate':
                self.state = 'leader'
                self.leader_id = self.node_id
                # Initialize leader state
                self.next_index = {node_id: len(self.log) for node_id in self.nodes_config if node_id != self.node_id}
                self.match_index = {node_id: -1 for node_id in self.nodes_config if node_id != self.node_id}
                print(f"\n{'='*50}")
                print(f"Node {self.node_id}: BECAME LEADER for term {self.current_term}")
                print(f"{'='*50}")
                
                # Start sending heartbeats immediately
                self.send_heartbeats()
                
                # Start heartbeat thread
                heartbeat_thread = Thread(target=self.send_heartbeat_periodically, daemon=True)
                heartbeat_thread.start()

    def send_heartbeat_periodically(self):
        """Send periodic heartbeats"""
        while True:
            with self.state_lock:
                if self.state != 'leader':
                    break
                print(f"Node {self.node_id}: Sending heartbeats as leader")
                for other_node in list(self.nodes_config.keys()):
                    if other_node != self.node_id: 
                        success = self.send_append_entries(other_node)
                        if success: 
                            print(f"Heartbeat sent to Node-{other_node}")
            
            time.sleep(self.heartbeat_interval)
    
    def send_heartbeats(self):
        """Send heartbeats to all followers"""
        with self.state_lock:
            if self.state != 'leader':
                return
            
            print(f"Node {self.node_id}: Sending heartbeats as leader")
            for other_id in self.nodes_config:
                if other_id != self.node_id:
                    try:
                        # Send empty append entries as heartbeat
                        request = {
                            'type': 'append_entries',
                            'term': self.current_term,
                            'leader_id': self.node_id,
                            'prev_log_index': len(self.log) - 1,
                            'prev_log_term': self.log[-1]['term'] if self.log else 0,
                            'entries': [],
                            'leader_commit': self.commit_index
                        }
                        
                        conn = self.connections.get(other_id)
                        if conn:
                            conn.send(json.dumps(request))
                            response = json.loads(conn.recv())
                            if not response.get('success'):
                                print(f"Node {self.node_id}: Heartbeat rejected by node {other_id}")
                    except Exception as e:
                        print(f"Node {self.node_id}: Failed to send heartbeat to node {other_id}: {e}")
    
    def retry_connection(self, target_id):
        """Retry connection to a specific node"""
        try:
            addr = self.nodes_config[target_id]
            self.connections[target_id] = Client(addr, authkey=b'raft')
            print(f"Node {self.node_id}: Reconnected to node {target_id}")
            return True
        except Exception as e:
            print(f"Node {self.node_id}: Failed to reconnect to node {target_id}: {e}")
            return False
    def send_append_entries(self, target_id):
        """Send AppendEntries RPC to a target node"""
        with self.state_lock:
            if self.state != 'leader':
                return False
            
            next_idx = self.next_index.get(target_id, len(self.log))
            prev_log_index = next_idx - 1
            prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0
            entries = self.log[next_idx:] if next_idx < len(self.log) else []
            
            request = {
                'type': 'append_entries',
                'term': self.current_term,
                'leader_id': self.node_id,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term,
                'entries': entries,
                'leader_commit': self.commit_index
            }
        
        try:
            conn = self.connections.get(target_id)
            if conn:
                time.sleep(random.uniform(0.05, 0.15))  # Simulate network delay
                conn.send(json.dumps(request))
                response = json.loads(conn.recv())
                
                if response.get('success'):
                    with self.state_lock:
                        self.match_index[target_id] = prev_log_index + len(entries)
                        self.next_index[target_id] = self.match_index[target_id] + 1
                    return True
                else:
                    with self.state_lock:
                        self.next_index[target_id] = max(0, self.next_index[target_id] - 1)
                    return False
        except Exception as e:
            print(f"Node {self.node_id}: Error sending append entries to node {target_id}: {e}")
            return False

    def handle_append_entries(self, request):
        """Handle append entries requests (including heartbeats)"""
        with self.state_lock:
            # First check the term
            if request['term'] < self.current_term:
                return {'success': False, 'term': self.current_term}
            
            # Update term if needed and convert to follower if necessary
            if request['term'] >= self.current_term:
                if request['term'] > self.current_term:
                    print(f"Node {self.node_id}: Updating term from {self.current_term} to {request['term']}")
                    self.current_term = request['term']
                
                # Always become follower when receiving valid append entries
                if self.state != 'follower':
                    print(f"Node {self.node_id}: Converting to follower")
                    self.state = 'follower'
                
                self.leader_id = request['leader_id']
                self.voted_for = None
            
            # Reset election timeout as we received valid append entries
            self.last_heartbeat = time.time()
            
            # Check previous log entry for consistency
            if request['prev_log_index'] >= len(self.log) or \
            (request['prev_log_index'] >= 0 and \
                self.log[request['prev_log_index']]['term'] != request['prev_log_term']):
                print(f"Node {self.node_id}: Log inconsistency detected")
                return {'success': False, 'term': self.current_term}
            
            # Handle log entries
            if request['entries']:
                print(f"Node {self.node_id}: Received {len(request['entries'])} new entries")
                with self.log_lock:
                    # Delete conflicting entries
                    if request['prev_log_index'] + 1 < len(self.log):
                        self.log = self.log[:request['prev_log_index'] + 1]
                        print(f"Node {self.node_id}: Deleted conflicting entries")
                    
                    # Append new entries
                    self.log.extend(request['entries'])
                    
                    # Update log file
                    with open(self.log_file, 'w') as f:
                        for entry in self.log:
                            f.write(f"{entry['value']}\n")
                    print(f"Node {self.node_id}: Updated log file with new entries")
            else:
                print(f"Node {self.node_id}: Received heartbeat from leader {request['leader_id']}")
            
            # Update commit index
            if request['leader_commit'] > self.commit_index:
                self.commit_index = min(request['leader_commit'], len(self.log) - 1)
                print(f"Node {self.node_id}: Updated commit index to {self.commit_index}")
            
            return {'success': True, 'term': self.current_term}

    def handle_submit_value(self, request):
        """Handle client value submissions"""
        with self.state_lock:
            if self.state != 'leader':
                print(f"Node {self.node_id}: Redirecting client to leader (node {self.leader_id})")
                return {'success': False, 'leader_id': self.leader_id}
            
            print(f"\nNode {self.node_id}: Handling submit value request: {request['value']}")
            
            # Append to log
            entry = {
                'term': self.current_term,
                'value': request['value']
            }
            
            with self.log_lock:
                self.log.append(entry)
                log_index = len(self.log) - 1
                print(f"Node {self.node_id}: Appended value to log at index {log_index}")
            
            # Replicate to followers
            replication_count = 1  # Count self
            print(f"Node {self.node_id}: Replicating to followers...")
            
            for other_id in self.nodes_config:
                if other_id != self.node_id:
                    if self.send_append_entries(other_id):
                        replication_count += 1
                        print(f"Node {self.node_id}: Successfully replicated to node {other_id}")
                    else:
                        print(f"Node {self.node_id}: Failed to replicate to node {other_id}")
            
            # Check if majority achieved
            if replication_count > len(self.nodes_config) // 2:
                with self.log_lock:
                    with open(self.log_file, 'a') as f:
                        f.write(f"{entry['value']}\n")
                print(f"Node {self.node_id}: Successfully committed value {entry['value']}")
                return {'success': True}
            else:
                # Rollback if not replicated to majority
                with self.log_lock:
                    self.log.pop()
                print(f"Node {self.node_id}: Failed to achieve majority consensus, rolling back")
                return {'success': False, 'message': 'Failed to replicate to majority'}

    def handle_force_leader_change(self):
        """Handle forced leader change request"""
        with self.state_lock:
            if self.state == 'leader':
                self.state = 'follower'
                self.current_term += 1
                self.voted_for = None
                print(f"Node {self.node_id}: Forced to step down")
                return {'success': True}
        return {'success': False}

    def handle_simulate_crash(self):
        with self.state_lock:
            if self.state == 'leader':
                # Remove last entry to simulate inconsistency
                if self.log:
                    removed_entry = self.log.pop()
                    print(f"Node {self.node_id}: Simulating crash by removing entry: {removed_entry}")
                
                # Update log file to reflect inconsistency
                with open(self.log_file, 'w') as f:
                    for entry in self.log:
                        f.write(f"{entry['value']}\n")
                
                self.state = 'follower'
                self.current_term = 0
                self.voted_for = None
                print(f"Node {self.node_id}: Simulated crash with log inconsistency")
                return {'success': True}
        return {'success': False}

    def handle_sync_request(self, request):
        """Handle synchronization request from a rejoining node"""
        with self.state_lock:
            if self.state != 'leader':
                return {'success': False, 'message': 'Not leader'}
            
            last_log_index = request.get('last_log_index', -1)
            entries_to_sync = self.log[last_log_index + 1:]
            
            return {
                'success': True,
                'term': self.current_term,
                'entries': entries_to_sync
            }

    def rejoin_cluster(self):
        """Rejoin the cluster after crash (Bonus feature)"""
        print(f"Node {self.node_id}: Attempting to rejoin cluster")
        self.state = 'follower'
        self.current_term = 0
        self.voted_for = None
        self.last_heartbeat = time.time()
        
        # Request log synchronization from current leader
        for node_id, conn in self.connections.items():
            try:
                request = {
                    'type': 'sync_request',
                    'last_log_index': len(self.log) - 1
                }
                conn.send(json.dumps(request))
                response = json.loads(conn.recv())
                
                if response.get('success'):
                    # Update log with received entries
                    with self.log_lock:
                        self.log.extend(response.get('entries', []))
                        with open(self.log_file, 'w') as f:
                            for entry in self.log:
                                f.write(f"{entry['value']}\n")
                    print(f"Node {self.node_id}: Successfully rejoined cluster")
                    return True
            except Exception as e:
                print(f"Node {self.node_id}: Error during rejoin attempt with node {node_id}: {e}")
                continue
        
        print(f"Node {self.node_id}: Failed to rejoin cluster")
        return False