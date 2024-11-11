# compute_node.py
import json
import time
import random
import threading
from threading import Thread, Lock
from multiprocessing.connection import Listener, Client

class ComputeNode:
    def __init__(self, node_id, nodes_config):
        # Node identity
        self.node_id = node_id
        self.nodes_config = nodes_config
        self.peers = {nid: addr for nid, addr in nodes_config.items() if nid != self.node_id}

        # Persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []  # Each entry is a dict with 'term' and 'value'

        # Volatile state
        self.commit_index = -1
        self.last_applied = -1

        # Leader state
        self.next_index = {}
        self.match_index = {}

        # Node state
        self.state = 'follower'  # Possible states: follower, candidate, leader
        self.leader_id = None

        # Timing
        self.election_timeout = random.uniform(1.5, 3.0)
        self.heartbeat_interval = 0.5
        self.last_heartbeat = time.time()

        # Synchronization
        self.state_lock = Lock()
        self.log_lock = Lock()

        # Networking
        self.connections = {}
        self.listener = None

        # Initialize log file
        self.log_file = f"CISC6935_node_{self.node_id}.txt"
        self.initialize_log_file()

        # Start listener and background threads
        self.start_listener()
        self.start_background_tasks()

    def initialize_log_file(self):
        """Create or clear the log file."""
        with open(self.log_file, 'w') as f:
            pass  # Just create or clear the file

    def start_listener(self):
        """Start listening for incoming connections."""
        host, port = self.nodes_config[self.node_id]
        self.listener_thread = Thread(target=self.listen_for_connections, args=(host, port))
        self.listener_thread.daemon = True
        self.listener_thread.start()

    def listen_for_connections(self, host, port):
        """Accept incoming connections and handle them."""
        try:
            self.listener = Listener(('', port), authkey=b'raft')  # Bind to all interfaces
            print(f"Node {self.node_id}: Listening on port {port}")
            while True:
                conn = self.listener.accept()
                Thread(target=self.handle_connection, args=(conn,), daemon=True).start()
        except Exception as e:
            print(f"Node {self.node_id}: Listener error: {e}")

    def handle_connection(self, conn):
        """Handle incoming messages."""
        try:
            while True:
                msg = conn.recv()
                response = self.handle_message(msg)
                conn.send(response)
        except EOFError:
            pass
        except Exception as e:
            print(f"Node {self.node_id}: Connection error: {e}")
        finally:
            conn.close()

    def handle_message(self, msg):
        """Process incoming messages and route them to the appropriate handler."""
        try:
            data = json.loads(msg)
            msg_type = data.get('type')
            if msg_type == 'request_vote':
                return json.dumps(self.handle_request_vote(data))
            elif msg_type == 'append_entries':
                return json.dumps(self.handle_append_entries(data))
            elif msg_type == 'client_request':
                return json.dumps(self.handle_client_request(data))
            elif msg_type == 'simulate_crash':
                return json.dumps(self.handle_simulate_crash())
            elif msg_type == 'simulate_rejoin':
                return json.dumps(self.handle_simulate_rejoin())
            else:
                return json.dumps({'error': 'Unknown message type'})
        except Exception as e:
            print(f"Node {self.node_id}: Error handling message: {e}")
            return json.dumps({'error': 'Exception occurred'})

    def start_background_tasks(self):
        """Start threads for election timeout and heartbeat."""
        Thread(target=self.election_timeout_check, daemon=True).start()

    def election_timeout_check(self):
        """Check for election timeout and start election if necessary."""
        while True:
            time.sleep(0.1)
            with self.state_lock:
                if self.state == 'leader':
                    continue
                if time.time() - self.last_heartbeat >= self.election_timeout:
                    self.start_election()

    def start_election(self):
        """Start a new election."""
        with self.state_lock:
            self.state = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.last_heartbeat = time.time()
            votes_received = 1  # Vote for self
        print(f"Node {self.node_id}: Starting election for term {self.current_term}")

        def request_votes():
            nonlocal votes_received
            for peer_id, peer_addr in self.peers.items():
                try:
                    conn = Client(peer_addr, authkey=b'raft')  # Removed timeout
                    request = {
                        'type': 'request_vote',
                        'term': self.current_term,
                        'candidate_id': self.node_id,
                        'last_log_index': len(self.log) - 1,
                        'last_log_term': self.log[-1]['term'] if self.log else 0
                    }
                    conn.send(json.dumps(request))
                    response = json.loads(conn.recv())
                    conn.close()
                    if response.get('vote_granted'):
                        votes_received += 1
                        if votes_received > len(self.nodes_config) // 2:
                            self.become_leader()
                            return
                    elif response.get('term', 0) > self.current_term:
                        with self.state_lock:
                            self.current_term = response['term']
                            self.state = 'follower'
                            self.voted_for = None
                            return
                except Exception as e:
                    print(f"Node {self.node_id}: Failed to get vote from Node {peer_id}: {e}")
            # Election failed, stay as candidate
            with self.state_lock:
                self.state = 'follower'
                self.voted_for = None

        Thread(target=request_votes, daemon=True).start()

    def become_leader(self):
        """Transition to leader state."""
        with self.state_lock:
            self.state = 'leader'
            self.leader_id = self.node_id
            self.next_index = {peer_id: len(self.log) for peer_id in self.peers}
            self.match_index = {peer_id: -1 for peer_id in self.peers}
        print(f"Node {self.node_id}: Became leader for term {self.current_term}")
        Thread(target=self.send_heartbeats, daemon=True).start()

    def send_heartbeats(self):
        """Send heartbeats to followers."""
        while True:
            with self.state_lock:
                if self.state != 'leader':
                    break
            for peer_id, peer_addr in self.peers.items():
                Thread(target=self.send_append_entries, args=(peer_id, peer_addr), daemon=True).start()
            time.sleep(self.heartbeat_interval)

    def send_append_entries(self, peer_id, peer_addr):
        """Send AppendEntries RPC to a follower."""
        with self.state_lock:
            prev_log_index = self.next_index.get(peer_id, 0) - 1
            prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 and len(self.log) > 0 else 0
            entries = self.log[self.next_index.get(peer_id, 0):]
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
            conn = Client(peer_addr, authkey=b'raft')  # Removed timeout
            conn.send(json.dumps(request))
            response = json.loads(conn.recv())
            conn.close()
            if response.get('success'):
                with self.state_lock:
                    self.next_index[peer_id] = self.next_index.get(peer_id, 0) + len(entries)
                    self.match_index[peer_id] = self.next_index[peer_id] - 1
            else:
                with self.state_lock:
                    self.next_index[peer_id] = max(0, self.next_index.get(peer_id, 0) - 1)
                    if response.get('term', 0) > self.current_term:
                        self.current_term = response['term']
                        self.state = 'follower'
                        self.voted_for = None
        except Exception as e:
            print(f"Node {self.node_id}: Failed to send AppendEntries to Node {peer_id}: {e}")


    def handle_request_vote(self, request):
        """Handle RequestVote RPC."""
        with self.state_lock:
            term = request['term']
            candidate_id = request['candidate_id']
            last_log_index = request['last_log_index']
            last_log_term = request['last_log_term']
            if term < self.current_term:
                return {'term': self.current_term, 'vote_granted': False}
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = 'follower'
            if (self.voted_for is None or self.voted_for == candidate_id) and \
               (last_log_term > self.log[-1]['term'] if self.log else True or \
                (last_log_term == (self.log[-1]['term'] if self.log else 0) and \
                 last_log_index >= len(self.log) - 1)):
                self.voted_for = candidate_id
                return {'term': self.current_term, 'vote_granted': True}
            else:
                return {'term': self.current_term, 'vote_granted': False}

    def handle_append_entries(self, request):
        """Handle AppendEntries RPC."""
        with self.state_lock:
            term = request['term']
            leader_id = request['leader_id']
            prev_log_index = request['prev_log_index']
            prev_log_term = request['prev_log_term']
            entries = request['entries']
            leader_commit = request['leader_commit']

            if term < self.current_term:
                return {'term': self.current_term, 'success': False}
            self.current_term = term
            self.leader_id = leader_id
            self.state = 'follower'
            self.last_heartbeat = time.time()

            # Check if log is consistent
            if prev_log_index >= 0:
                if prev_log_index >= len(self.log) or self.log[prev_log_index]['term'] != prev_log_term:
                    return {'term': self.current_term, 'success': False}

            # Append any new entries
            with self.log_lock:
                self.log = self.log[:prev_log_index + 1]  # Delete conflicting entries
                self.log.extend(entries)
                # Write to log file
                with open(self.log_file, 'w') as f:
                    for entry in self.log:
                        f.write(f"{entry['value']}\n")

            # Update commit index
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)
            return {'term': self.current_term, 'success': True}

    def handle_client_request(self, request):
        """Handle client requests (only the leader should handle this)."""
        with self.state_lock:
            if self.state != 'leader':
                return {'success': False, 'leader_id': self.leader_id}
            # Append entry to log
            with self.log_lock:
                entry = {'term': self.current_term, 'value': request['value']}
                self.log.append(entry)
                index = len(self.log) - 1

            # Replicate entry to followers
            replicated = 1
            acked = threading.Event()

            def replicate_to_peer(peer_id, peer_addr):
                nonlocal replicated
                while True:
                    with self.state_lock:
                        if self.state != 'leader':
                            return
                    self.send_append_entries(peer_id, peer_addr)
                    with self.state_lock:
                        if self.match_index.get(peer_id, -1) >= index:
                            replicated += 1
                            if replicated > len(self.nodes_config) // 2:
                                acked.set()
                            return
                    time.sleep(0.1)

            for peer_id, peer_addr in self.peers.items():
                Thread(target=replicate_to_peer, args=(peer_id, peer_addr), daemon=True).start()

            # Wait for majority replication
            acked.wait(timeout=5)
            with self.state_lock:
                if replicated > len(self.nodes_config) // 2:
                    self.commit_index = index
                    # Apply to state machine
                    with open(self.log_file, 'a') as f:
                        f.write(f"{entry['value']}\n")
                    return {'success': True}
                else:
                    # Failed to replicate, remove the entry
                    with self.log_lock:
                        self.log.pop()
                    return {'success': False, 'message': 'Failed to replicate to majority'}

    def handle_simulate_crash(self):
        """Simulate a node crash by clearing the log."""
        with self.state_lock:
            self.state = 'crashed'
        with self.log_lock:
            if self.log:
                self.log.pop()
            # Clear the log file
            with open(self.log_file, 'w') as f:
                for entry in self.log:
                    f.write(f"{entry['value']}\n")
        print(f"Node {self.node_id}: Simulated crash")
        return {'success': True}

    def handle_simulate_rejoin(self):
        """Simulate node rejoining the cluster."""
        with self.state_lock:
            self.state = 'follower'
            self.current_term = 0
            self.voted_for = None
            self.last_heartbeat = time.time()
        print(f"Node {self.node_id}: Rejoined the cluster")
        return {'success': True}

    def shutdown(self):
        """Clean up resources."""
        self.listener.close()
        print(f"Node {self.node_id}: Shutdown complete")
