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

    def create_log_file(self):
        """Create the initial log file as required"""
        try:
            with open(self.log_file, 'w') as f:
                f.write('')
            print(f"Node {self.node_id}: Created log file CISC6935")
        except Exception as e:
            print(f"Node {self.node_id}: Error creating log file: {e}")

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
        # Start heartbeat checker
        self.heartbeat_thread = Thread(target=self.check_heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        # Start RPC listener
        self.listener_thread = Thread(target=self.start_listener)
        self.listener_thread.daemon = True
        self.listener_thread.start()
        
        print(f"Node {self.node_id}: Started")

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
                    if time.time() - self.last_heartbeat > self.election_timeout:
                        self.start_election()

    def start_election(self):
        """Start a new election"""
        with self.state_lock:
            self.state = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes_received = {self.node_id}
            print(f"Node {self.node_id}: Starting election for term {self.current_term}")

        # Request votes from all other nodes
        for other_id in self.nodes_config:
            if other_id != self.node_id:
                self.request_vote(other_id)

    def request_vote(self, target_id):
        """Send RequestVote RPC to a target node"""
        with self.state_lock:
            request = {
                'type': 'request_vote',
                'term': self.current_term,
                'candidate_id': self.node_id,
                'last_log_index': len(self.log) - 1,
                'last_log_term': self.log[-1]['term'] if self.log else 0
            }
        
        try:
            conn = self.connections.get(target_id)
            if conn:
                time.sleep(random.uniform(0.05, 0.15))  # Simulate network delay
                conn.send(json.dumps(request))
                response = json.loads(conn.recv())
                
                if response.get('vote_granted'):
                    with self.state_lock:
                        if self.state == 'candidate':
                            self.votes_received.add(target_id)
                            if len(self.votes_received) > len(self.nodes_config) // 2:
                                self.become_leader()
        except Exception as e:
            print(f"Node {self.node_id}: Error requesting vote from node {target_id}: {e}")

    def handle_vote_request(self, request):
        """Handle incoming vote requests"""
        with self.state_lock:
            if request['term'] < self.current_term:
                return {'vote_granted': False, 'term': self.current_term}
            
            if request['term'] > self.current_term:
                self.current_term = request['term']
                self.state = 'follower'
                self.voted_for = None
            
            last_log_index = len(self.log) - 1
            last_log_term = self.log[-1]['term'] if self.log else 0
            
            if (self.voted_for is None or self.voted_for == request['candidate_id']) and \
               (request['last_log_term'] > last_log_term or \
                (request['last_log_term'] == last_log_term and \
                 request['last_log_index'] >= last_log_index)):
                self.voted_for = request['candidate_id']
                return {'vote_granted': True, 'term': self.current_term}
            
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
                print(f"Node {self.node_id}: Became leader for term {self.current_term}")
                # Start sending heartbeats
                Thread(target=self.send_heartbeats, daemon=True).start()

    def send_heartbeats(self):
        """Send periodic heartbeats to all followers"""
        while True:
            with self.state_lock:
                if self.state != 'leader':
                    break
                
                for other_id in self.nodes_config:
                    if other_id != self.node_id:
                        self.send_append_entries(other_id)
            
            time.sleep(self.heartbeat_interval)
    
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
            if request['term'] < self.current_term:
                return {'success': False, 'term': self.current_term}
            
            # Update term if needed
            if request['term'] > self.current_term:
                self.current_term = request['term']
                self.state = 'follower'
                self.voted_for = None
            
            self.last_heartbeat = time.time()
            self.leader_id = request['leader_id']
            
            # Check previous log entry
            if request['prev_log_index'] >= len(self.log) or \
               (request['prev_log_index'] >= 0 and \
                self.log[request['prev_log_index']]['term'] != request['prev_log_term']):
                return {'success': False, 'term': self.current_term}
            
            # Handle log entries
            if request['entries']:
                with self.log_lock:
                    # Delete conflicting entries
                    if request['prev_log_index'] + 1 < len(self.log):
                        self.log = self.log[:request['prev_log_index'] + 1]
                    
                    # Append new entries
                    self.log.extend(request['entries'])
                    
                    # Update log file
                    with open(self.log_file, 'w') as f:
                        for entry in self.log:
                            f.write(f"{entry['value']}\n")
            
            # Update commit index
            if request['leader_commit'] > self.commit_index:
                self.commit_index = min(request['leader_commit'], len(self.log) - 1)
            
            return {'success': True, 'term': self.current_term}

    def handle_submit_value(self, request):
        """Handle client value submissions"""
        with self.state_lock:
            if self.state != 'leader':
                return {'success': False, 'leader_id': self.leader_id}
            
            # Append to log
            entry = {
                'term': self.current_term,
                'value': request['value']
            }
            
            with self.log_lock:
                self.log.append(entry)
                log_index = len(self.log) - 1
            
            # Replicate to followers
            replication_count = 1  # Count self
            for other_id in self.nodes_config:
                if other_id != self.node_id:
                    if self.send_append_entries(other_id):
                        replication_count += 1
            
            # Check if majority achieved
            if replication_count > len(self.nodes_config) // 2:
                with self.log_lock:
                    with open(self.log_file, 'a') as f:
                        f.write(f"{entry['value']}\n")
                return {'success': True}
            else:
                # Rollback if not replicated to majority
                with self.log_lock:
                    self.log.pop()
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