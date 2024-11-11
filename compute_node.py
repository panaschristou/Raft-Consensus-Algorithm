# compute_node.py
import json
import time
import random
from threading import Thread, Lock
from multiprocessing.connection import Listener, Client

class ComputeNode:
    def __init__(self, node_id, nodes_config):
        # Basic configuration
        self.node_id = node_id
        self.nodes_config = nodes_config
        self.connections = {}
        
        # Raft state
        self.current_term = 0
        self.voted_for = None
        self.state = 'follower'
        self.leader_id = None
        self.log = []  # List of dictionaries with 'term' and 'value'
        
        # Timing configuration
        self.election_timeout = random.uniform(2, 4)  # Longer timeout for stability
        self.heartbeat_interval = 1.0
        self.last_heartbeat = time.time()
        
        # Locks
        self.state_lock = Lock()
        self.log_lock = Lock()
        
        # Initialize log file
        self.log_file = f"CISC6935_{self.node_id}"
        self.create_log_file()

    def create_log_file(self):
        with open(self.log_file, 'w') as f:
            f.write('')
        print(f"Node {self.node_id}: Created log file {self.log_file}")

    def start(self):
        # Start listener for incoming connections
        listener_thread = Thread(target=self.start_listener)
        listener_thread.daemon = True
        listener_thread.start()

        # Give time for all nodes to start
        time.sleep(2)

        # Connect to other nodes
        self.connect_to_peers()

        # Start election timeout checker
        timeout_thread = Thread(target=self.check_election_timeout)
        timeout_thread.daemon = True
        timeout_thread.start()

        print(f"Node {self.node_id}: Started as follower")

    def connect_to_peers(self):
        for other_id, addr in self.nodes_config.items():
            if other_id != self.node_id:
                try:
                    self.connections[other_id] = Client(addr, authkey=b'raft')
                    print(f"Node {self.node_id}: Connected to node {other_id}")
                except Exception as e:
                    print(f"Node {self.node_id}: Failed to connect to node {other_id}: {e}")

    def start_listener(self):
        address = self.nodes_config[self.node_id]
        listener = Listener(address, authkey=b'raft')
        while True:
            try:
                conn = listener.accept()
                Thread(target=self.handle_connection, args=(conn,), daemon=True).start()
            except Exception as e:
                print(f"Node {self.node_id}: Listener error: {e}")

    def handle_connection(self, conn):
        while True:
            try:
                msg = conn.recv()
                request = json.loads(msg)
                response = self.handle_request(request)
                conn.send(json.dumps(response))
            except EOFError:
                break
            except Exception as e:
                print(f"Node {self.node_id}: Connection handler error: {e}")
                break

    def check_election_timeout(self):
        while True:
            time.sleep(0.1)
            with self.state_lock:
                if self.state != 'leader':
                    if time.time() - self.last_heartbeat > self.election_timeout:
                        self.start_election()

    def start_election(self):
        with self.state_lock:
            self.state = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.last_heartbeat = time.time()
            
            print(f"\nNode {self.node_id}: Starting election for term {self.current_term}")
            
            votes = 1  # Vote for self
            
            # Request votes from all other nodes
            for other_id in self.nodes_config:
                if other_id != self.node_id:
                    try:
                        conn = self.connections.get(other_id)
                        if not conn:
                            continue
                            
                        request = {
                            'type': 'request_vote',
                            'term': self.current_term,
                            'candidate_id': self.node_id
                        }
                        
                        conn.send(json.dumps(request))
                        response = json.loads(conn.recv())
                        
                        if response.get('vote_granted'):
                            votes += 1
                            
                        if response.get('term', 0) > self.current_term:
                            self.state = 'follower'
                            self.current_term = response['term']
                            return
                            
                    except Exception as e:
                        print(f"Node {self.node_id}: Vote request error: {e}")
                        
            # If we got majority votes, become leader
            if votes > len(self.nodes_config) // 2:
                self.become_leader()

    def become_leader(self):
        with self.state_lock:
            self.state = 'leader'
            self.leader_id = self.node_id
            print(f"\nNode {self.node_id}: Became leader for term {self.current_term}")
            
            # Start sending heartbeats
            heartbeat_thread = Thread(target=self.send_heartbeats)
            heartbeat_thread.daemon = True
            heartbeat_thread.start()

    def send_heartbeats(self):
        while True:
            with self.state_lock:
                if self.state != 'leader':
                    break
                    
                for other_id in self.nodes_config:
                    if other_id != self.node_id:
                        try:
                            conn = self.connections.get(other_id)
                            if conn:
                                request = {
                                    'type': 'heartbeat',
                                    'term': self.current_term,
                                    'leader_id': self.node_id
                                }
                                conn.send(json.dumps(request))
                                
                        except Exception as e:
                            print(f"Node {self.node_id}: Heartbeat error: {e}")
                            
            time.sleep(self.heartbeat_interval)

    def handle_request(self, request):
        """Handle different types of RPC requests"""
        try:
            request_type = request.get('type')
            
            if request_type == 'request_vote':
                return self.handle_vote_request(request)
                
            elif request_type == 'append_entries':
                return self.handle_append_entries(request)
                
            elif request_type == 'submit_value':
                return self.handle_submit_value(request)
                
            elif request_type == 'force_leader_change':
                return self.handle_force_leader_change()
                
            else:
                return {'success': False, 'message': 'Unknown request type'}
                
        except Exception as e:
            print(f"Node {self.node_id}: Error handling request: {e}")
            return {'success': False, 'error': str(e)}

    def handle_vote_request(self, request):
        """Handle incoming vote requests"""
        with self.state_lock:
            print(f"Node {self.node_id}: Received vote request from node {request['candidate_id']}")
            
            # If the candidate's term is less than ours, reject
            if request['term'] < self.current_term:
                return {
                    'vote_granted': False,
                    'term': self.current_term
                }
                
            # If we see a higher term, update our term and become follower
            if request['term'] > self.current_term:
                self.current_term = request['term']
                self.state = 'follower'
                self.voted_for = None
                self.leader_id = None
                
            # Grant vote if we haven't voted in this term and log is at least as up-to-date
            if (self.voted_for is None or self.voted_for == request['candidate_id']):
                self.voted_for = request['candidate_id']
                self.last_heartbeat = time.time()  # Reset election timeout
                return {
                    'vote_granted': True,
                    'term': self.current_term
                }
                
            return {
                'vote_granted': False,
                'term': self.current_term
            }

    def handle_append_entries(self, request):
        """Handle append entries requests (including heartbeats)"""
        with self.state_lock:
            # If message from old term
            if request['term'] < self.current_term:
                return {
                    'success': False,
                    'term': self.current_term
                }
                
            # If we see a valid leader with same or higher term
            if request['term'] >= self.current_term:
                # Update term if necessary
                self.current_term = request['term']
                # Always become follower when receiving valid append entries
                self.state = 'follower'
                self.leader_id = request['leader_id']
                self.last_heartbeat = time.time()
                
            # Process entries
            if len(request['entries']) > 0:
                with self.log_lock:
                    # Append new entries
                    self.log.extend(request['entries'])
                    # Update log file
                    with open(self.log_file, 'a') as f:
                        for entry in request['entries']:
                            f.write(f"{entry['value']}\n")
                            
            return {
                'success': True,
                'term': self.current_term
            }

    def handle_submit_value(self, request):
        """Handle client value submissions"""
        with self.state_lock:
            # If not leader, redirect to leader
            if self.state != 'leader':
                return {
                    'success': False,
                    'leader_id': self.leader_id
                }
                
            # Add to log
            entry = {
                'term': self.current_term,
                'value': request['value']
            }
            
            with self.log_lock:
                self.log.append(entry)
                
            # Try to replicate to followers
            replicated = 1  # Count self
            for other_id in self.nodes_config:
                if other_id != self.node_id:
                    if self.send_append_entries(other_id):
                        replicated += 1
                        
            # If majority achieved
            if replicated > len(self.nodes_config) // 2:
                # Commit to log file
                with open(self.log_file, 'a') as f:
                    f.write(f"{entry['value']}\n")
                return {'success': True}
            else:
                # Roll back if couldn't replicate
                with self.log_lock:
                    self.log.pop()
                return {
                    'success': False,
                    'message': 'Failed to replicate to majority'
                }

    def handle_force_leader_change(self):
        """Handle forced leader change request"""
        with self.state_lock:
            if self.state == 'leader':
                self.state = 'follower'
                self.voted_for = None
                self.current_term += 1
                self.last_heartbeat = 0  # Force election timeout
                return {'success': True}
            return {'success': False}