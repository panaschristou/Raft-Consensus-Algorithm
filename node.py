# node.py
import socket
import threading
import time
import json
import sys
import random
import os
from config import NODES, ELECTION_TIMEOUT, HEARTBEAT_INTERVAL

class Node:
    def __init__(self, name):
        self.name = name
        self.ip = NODES[self.name]['ip']
        self.port = NODES[self.name]['port']
        self.state = 'Follower'
        self.current_term = 0 # Initialized to 0 as per the Raft paper
        self.voted_for = None # Initialized to None as per the Raft paper
        self.log = []  # [{'term': term, 'value': value, 'index': index (Note: First index is 1)}]
        self.commit_index = -1 # Initialized to 0 as per the Raft paper
        self.last_applied = -1 # Initialized to 0 as per the Raft paper
        self.next_index = {}
        self.match_index = {}
        self.leader_id = None # No leader yet
        self.election_timer = None # Initialized in start method
        self.heartbeat_timer = None # Initialized in start method
        self.server_socket = None # Initialized in run_server method
        self.running = True # Flag to control the main loop
        self.lock = threading.Lock() # Lock to synchronize access to shared variables
        
        # Create or clear log file
        self.log_filename = f"{self.name}_CISC6935.txt"
        open(self.log_filename, 'w').close()

    def start(self):
        print(f"[{self.name}] Starting node...")
        server_thread = threading.Thread(target=self.run_server) # Start the server thread
        server_thread.daemon = True # Set the server thread as a daemon
        server_thread.start() # Start the server thread
        
        self.reset_election_timer()
        print(f"[{self.name}] Initial election timeout: {self.election_timer}")

        while self.running:
            with self.lock:
                current_state = self.state # Get the current state
                current_timer = self.election_timer # Get the current election timer

            if current_state == 'Leader': # If the current state is leader
                self.send_heartbeats() # Send heartbeats to followers
                time.sleep(HEARTBEAT_INTERVAL) # Sleep for the heartbeat interval
            else:
                if current_timer <= 0: # If the election timer is less than or equal to 0
                    with self.lock: # Lock the shared variables
                        if self.state != 'Leader':  # Double-check state
                            self.start_election() # Start the election
                time.sleep(0.1)
                with self.lock:
                    self.election_timer -= 0.1 # Decrement the election timer

    def run_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Create a TCP socket
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Set the socket option to reuse the address
        self.server_socket.bind((self.ip, self.port)) # Bind the socket to the address
        self.server_socket.listen(5) # Listen for incoming connections
        print(f"[{self.name}] Server listening at {self.ip}:{self.port}")

        while self.running:
            try:
                client_socket, addr = self.server_socket.accept() # Accept incoming connections
                client_thread = threading.Thread( # Create a new thread to handle the client connection
                    target=self.handle_client_connection, # Target method to handle the client connection
                    args=(client_socket,) # Arguments to pass to the target method
                )
                client_thread.daemon = True # Set the client thread as a daemon
                client_thread.start() # Start the client thread
            except Exception as e:
                if self.running:
                    print(f"[{self.name}] Server error: {e}")

    def handle_client_connection(self, client_socket): # Handle client connection
        try:
            data = client_socket.recv(4096).decode() # Receive data from the client
            if data:
                request = json.loads(data) # Parse the JSON data
                rpc_type = request['rpc_type'] # Get the RPC type
                response = {} # Initialize the response

                with self.lock:
                    if rpc_type == 'RequestVote': # If the RPC type is RequestVote handle the vote request
                        response = self.handle_request_vote(request['data']) 
                    elif rpc_type == 'AppendEntries': # If the RPC type is AppendEntries handle the append entries request
                        response = self.handle_append_entries(request['data'])
                    elif rpc_type == 'SubmitValue': # If the RPC type is SubmitValue handle the client submit request
                        response = self.handle_client_submit(request['data'])
                    elif rpc_type == 'TriggerLeaderChange': # If the RPC type is TriggerLeaderChange handle the leader change request
                        response = self.trigger_leader_change()
                    elif rpc_type == 'SimulateCrashLeader': # If the RPC type is SimulateCrashLeader handle the leader crash request
                        response = self.simulate_crash_leader()
                    elif rpc_type == 'SimulateCrashNode': # If the RPC type is SimulateCrashNode handle the node crash request
                        response = self.simulate_crash_node() 
                    else:
                        response = {'error': 'Unknown RPC type'} # If the RPC type is unknown, return an error

                client_socket.sendall(json.dumps(response).encode()) # Send the response to the client
        except Exception as e:
            print(f"[{self.name}] Error handling client connection: {e}")
        finally:
            client_socket.close() # Close the client socket

    def reset_election_timer(self):
        self.election_timer = random.uniform(*ELECTION_TIMEOUT) # Randomize the election timeout

    def handle_request_vote(self, data):
        # Parse data. Get candidate term, candidate id, candidate last log index, and candidate last log term
        candidate_term = data['term']
        candidate_id = data['candidate_name']
        candidate_last_log_index = data['last_log_index']
        candidate_last_log_term = data['last_log_term']

        # Reply false if term < currentTerm
        if candidate_term < self.current_term:
            return {
                'term': self.current_term,
                'vote_granted': False
            }

        # Update term if candidateTerm > currentTerm, convert to follower, and reset votedFor and leaderId
        if candidate_term > self.current_term:
            self.current_term = candidate_term
            self.state = 'Follower'
            self.voted_for = None
            self.leader_id = None

        # Check if we can vote for this candidate
        can_vote = (self.voted_for is None or self.voted_for == candidate_id)
        
        # Check if candidate's log is at least as up-to-date as ours
        # If the logs have last entries with different terms, then the log with the later term is more up-to-date
        last_log_index = len(self.log) - 1
        # Get the term of the last log entry or 0 if the log is empty
        last_log_term = self.log[last_log_index]['term'] if self.log else 0

        # If the candidate's last log term is greater than the last log term or if the terms are the same and the candidate's last log index is greater than or equal to the last log index, then the candidate's log is up-to-date
        log_is_up_to_date = (
            candidate_last_log_term > last_log_term or
            (candidate_last_log_term == last_log_term and
             candidate_last_log_index >= last_log_index)
        )

        # If we can vote and the candidate's log is up-to-date, vote for the candidate
        if can_vote and log_is_up_to_date:
            self.voted_for = candidate_id
            self.reset_election_timer()
            print(f"[{self.name}] Voted for {candidate_id} in term {self.current_term}")
            return {
                'term': self.current_term,
                'vote_granted': True
            }

        # Otherwise, don't vote
        return {
            'term': self.current_term,
            'vote_granted': False
        }
    
    def handle_append_entries(self, data):
        # Parse data. Get leader term, leader id, previous log index, previous log term, entries, and leader commit index
        leader_term = data['term']
        leader_id = data['leader_name']
        prev_log_index = data['prev_log_index']
        prev_log_term = data['prev_log_term']
        entries = data['entries']
        leader_commit = data['leader_commit']
        
        # Log receipt of AppendEntries
        if entries or self.current_term != leader_term:
            print(f"[{self.name}] Received AppendEntries from Leader {leader_id}. Term: {leader_term}, Current Term: {self.current_term}")
            print(f"[{self.name}] PrevLogIndex: {prev_log_index}, PrevLogTerm: {prev_log_term}, Entries: {entries}, LeaderCommit: {leader_commit}")

        # Reply false if term < currentTerm
        if leader_term < self.current_term:
            print(f"[{self.name}] AppendEntries rejected: Leader term {leader_term} is less than current term {self.current_term}.")
            return {'term': self.current_term, 'success': False}

        # Update term if leaderTerm > currentTerm
        if leader_term > self.current_term:
            print(f"[{self.name}] Updating term to {leader_term} and converting to Follower.")
            self.current_term = leader_term
            self.voted_for = None

        # Reset election timer, convert to follower, and set leader id
        self.reset_election_timer()
        self.state = 'Follower'
        self.leader_id = leader_id

        # Check log consistency. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if prev_log_index >= len(self.log):
            print(f"[{self.name}] AppendEntries failed: PrevLogIndex {prev_log_index} out of bounds (Log Length: {len(self.log)}).")
            return {'term': self.current_term, 'success': False}
        
        # If an existing entry conflicts with a new one, delete the existing entry and all that follow it
        if prev_log_index >= 0 and (
            prev_log_index >= len(self.log) or
            self.log[prev_log_index]['term'] != prev_log_term
        ):
            print(f"[{self.name}] Log inconsistency detected at PrevLogIndex {prev_log_index}. Term mismatch.")
            return {'term': self.current_term, 'success': False}

        # Process new entries
        if entries:
            print(f"[{self.name}] Appending new entries to the log.")
            # Delete conflicting entries by using the previous log index
            self.log = self.log[:prev_log_index + 1]
            # Append new entries to ensure consistency
            self.log.extend(entries)
            print(f"[{self.name}] Appended {len(entries)} entries to log")

        # Update commit index
        if leader_commit > self.commit_index:
            # Set commit index to the minimum of leader commit and the last index in the log
            # `commitIndex` is the index of the highest log entry known to be committed
            self.commit_index = min(leader_commit, len(self.log) - 1)
            # print(f"[{self.name}] Commit index updated to {self.commit_index}.")
            # Apply committed entries to the state machine
            self.apply_committed_entries()

        return {'term': self.current_term, 'success': True}

    def apply_committed_entries(self): # Apply committed entries to the state machine
        while self.last_applied < self.commit_index: # Apply all committed entries
            self.last_applied += 1 # Increment the last applied index
            entry = self.log[self.last_applied] # Get the entry to apply
            self.apply_entry_to_state_machine(entry) # Apply the entry to the state machine

    def apply_entry_to_state_machine(self, entry): # Apply the entry to the state machine (i.e. persistent memory)
        with open(self.log_filename, 'a') as f:
            f.write(f"{entry['value']}\n")
        print(f"[{self.name}] Applied entry to log: {entry['value']}")

    def start_election(self):
        self.state = 'Candidate' # Start of the election convert to candidate
        self.current_term += 1 # Increment the term
        self.voted_for = self.name # Vote for self
        self.leader_id = None # No leader yet
        votes_received = 1  # 1 vote for self

        print(f"[{self.name}] Starting election for term {self.current_term}")
        self.reset_election_timer() 

        # Prepare RequestVote arguments
        last_log_index = len(self.log) - 1 # Last index in the log
        last_log_term = self.log[last_log_index]['term'] if self.log else 0 # Term of the last log entry

        # Send RequestVote RPCs to all other nodes
        for node_name in NODES:
            if node_name != self.name:
                try: # Send a request to vote with the current term, candidate name, last log index and last log term
                    response = self.send_rpc( 
                        NODES[node_name]['ip'],
                        NODES[node_name]['port'],
                        'RequestVote',
                        {
                            'term': self.current_term,
                            'candidate_name': self.name,
                            'last_log_index': last_log_index,
                            'last_log_term': last_log_term
                        }
                    )

                    if response and response.get('vote_granted'): # If the vote is granted
                        votes_received += 1 # Increment the vote count
                        if (votes_received > len(NODES) // 2 and # If majority of the votes are received
                            self.state == 'Candidate'):  # Check if still candidate
                            self.become_leader() # Become leader
                            break
                    elif response and response['term'] > self.current_term: # If the term is greater than the current term
                        self.current_term = response['term'] # Update the term
                        self.state = 'Follower' # Convert to follower
                        self.voted_for = None # Reset the vote
                        break
                except Exception as e:
                    print(f"[{self.name}] Error requesting vote from {node_name}: {e}")

    def check_cluster_health(self):
        """Check how many nodes are reachable in the cluster"""
        reachable_nodes = 1  # Count self
        for node_name in NODES:
            if node_name != self.name:
                try: # Send an AppendEntries RPC to all other nodes
                    response = self.send_rpc(
                        NODES[node_name]['ip'],
                        NODES[node_name]['port'],
                        'AppendEntries',  # Use as heartbeat. Send empty entries with current term and commit index to check if node is reachable
                        {
                            'term': self.current_term,
                            'leader_name': self.name,
                            'prev_log_index': len(self.log) - 1,
                            'prev_log_term': self.log[-1]['term'] if self.log else 0,
                            'entries': [],
                            'leader_commit': self.commit_index
                        }
                    )
                    if response is not None: # If response received, node is reachable
                        reachable_nodes += 1
                except Exception:
                    continue
        return reachable_nodes

    def become_leader(self):
        # Check cluster health before becoming leader
        reachable_nodes = self.check_cluster_health()
        # If majority nodes are reachable, become leader
        if reachable_nodes <= len(NODES) // 2:
            print(f"[{self.name}] Cannot become leader: only {reachable_nodes}/{len(NODES)} nodes reachable")
            self.state = 'Follower'
            return

        print(f"[{self.name}] Becoming leader for term {self.current_term}")
        self.state = 'Leader' # Convert to leader
        self.leader_id = self.name # Set leader id to self
        
        # Initialize leader state by setting next_index and match_index for each node
        # next_index is the index of the next log entry to send to that node
        # match_index is the index of the highest log entry known to be replicated on that node
        self.next_index = {node: len(self.log) for node in NODES if node != self.name} # Next index for each node
        self.match_index = {node: -1 for node in NODES if node != self.name} # Match index for each node
        
        # Send immediate heartbeat
        self.send_heartbeats()

    def send_heartbeats(self):
        # Send empty AppendEntries RPCs as heartbeats to all other nodes
        for node_name in NODES:
            if node_name != self.name:
                entries = [] # Empty entries
                next_idx = self.next_index.get(node_name, len(self.log)) # Next index for the node
                
                if next_idx < len(self.log): # If there are entries to send
                    entries = self.log[next_idx:] # Get the entries to send
                
                self.send_append_entries(node_name, entries) # Send the AppendEntries RPC

    def handle_client_submit(self, data):
        # If not leader, redirect to leader
        if self.state != 'Leader':
            return {
                'redirect': True,
                'leader_name': self.leader_id
            }

        # If leader, append entry to log
        # The entry is not yet committed, but it will be replicated to followers
        # It contains the term, value, and index of the entry
        entry = {
            'term': self.current_term,
            'value': data['value'],
            'index': len(self.log)
        }
        self.log.append(entry)
        print(f"[{self.name}] New entry added to log: {entry}")

        # Replicate to followers
        success_count = 1  # Count self
        for node_name in NODES:
            if node_name != self.name:
                if self.replicate_log_to_follower(node_name): # Replicate log to follower. If successful, increment success count
                    success_count += 1

        # If majority successful, commit and apply
        if success_count > len(NODES) // 2:
            self.commit_index = len(self.log) - 1 
            self.apply_committed_entries()
            return {'success': True}
        else:
            # Roll back if replication failed, FIFO
            self.log.pop()
            return {'success': False}

    def replicate_log_to_follower(self, follower_name):
        # Get entries to replicate
        next_idx = self.next_index[follower_name]
        entries = self.log[next_idx:]
        
        # Send AppendEntries RPC to follower
        response = self.send_append_entries(follower_name, entries)
        # If successful, update next_index and match_index
        # next_index is the index of the next log entry to send to that node
        # upon success, next_index of the follower is the same as the length of the leader's log
        if response and response.get('success'):
            self.next_index[follower_name] = len(self.log)
            self.match_index[follower_name] = len(self.log) - 1
            return True
        elif response:
            # If follower is behind, decrement next_index and retry
            # We decrement because the follower may have skipped entries so we need to retry from the previous index
            self.next_index[follower_name] = max(0, self.next_index[follower_name] - 1) # If failed, decrement next_index and retry
        else:
            # No response from follower (e.g., node failure)
            print(f"[{self.name}] Failed to contact {follower_name}.")
            return False

    def send_append_entries(self, follower_name, entries):
        # Get previous log index and term
        prev_log_index = self.next_index[follower_name] - 1
        prev_log_term = (
            self.log[prev_log_index]['term'] 
            if prev_log_index >= 0 and self.log 
            else 0
        )

        # Send AppendEntries RPC to follower
        return self.send_rpc(
            # Get follower IP and port from the configuration
            NODES[follower_name]['ip'],
            NODES[follower_name]['port'],
            'AppendEntries',
            {
                # Send term, leader name, previous log index and term, entries, and leader commit index
                'term': self.current_term,
                'leader_name': self.name,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term,
                'entries': entries,
                'leader_commit': self.commit_index
            }
        )

    def trigger_leader_change(self):
        # Simulating a leader change by stepping down
        if self.state == 'Leader':
            # Reset leader state by setting next_index and match_index for each node to None to force a refresh and reset the election timer
            print(f"[{self.name}] Triggering leader change")
            self.state = 'Follower'
            self.voted_for = None
            self.leader_id = None
            self.reset_election_timer()
            return {'status': 'Leader stepping down'}
        return {'status': 'Not a leader'}
    
    def simulate_crash_leader(self):
        if self.state == 'Leader':
            print(f"[{self.name}] Simulating leader crash...")

            # Clear state to simulate a crash
            self.log = []
            self.commit_index = -1
            self.last_applied = -1
            self.current_term = 0
            self.voted_for = None
            self.state = 'Follower'
            print(f"[{self.name}] Cleared state and transitioned to FOLLOWER after crash.")

            # Notify followers to trigger a new leader election
            for peer_name in NODES:
                if peer_name != self.name:
                    try:
                        self.send_rpc(
                            NODES[peer_name]['ip'],
                            NODES[peer_name]['port'],
                            'TriggerLeaderChange',
                            {}
                        )
                        print(f"[{self.name}] Notified follower {peer_name} to start an election.")
                    except Exception as e:
                        print(f"[{self.name}] Error notifying follower {peer_name}: {e}")

            # Simulate downtime
            time.sleep(5)
            print(f"[{self.name}] Waking up from simulated crash.")

            return {'status': 'Leader crashed and rejoined the cluster'}

        print(f"[{self.name}] This node is not a leader; cannot simulate leader crash.")
        return {'status': 'Not a leader'}
    
    def simulate_crash_node(self):
        print(f"[{self.name}] Simulating node crash...")

        print(f"[{self.name}] Log before crash: {self.log}")

        # Truncate the log to simulate missing entries
        if len(self.log) > 1:
            self.log = self.log[:len(self.log) // 2]
            print(f"[{self.name}] Truncated log to simulate inconsistency: {self.log}")

        # Clear state to simulate crash
        self.commit_index = -1
        self.last_applied = -1
        self.current_term = 0
        self.voted_for = None
        self.state = 'Follower'
        print(f"[{self.name}] Cleared state and transitioned to FOLLOWER after crash.")

        # Simulate downtime
        time.sleep(5)
        print(f"[{self.name}] Waking up from simulated crash.")

        return {'status': 'Node rejoined after crash'}


    def send_rpc(self, ip, port, rpc_type, data, timeout=2.0):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: # Create a new socket
                s.settimeout(timeout) # Set the socket timeout
                s.connect((ip, port)) # Connect to the server
                message = json.dumps({'rpc_type': rpc_type, 'data': data}) # Create the message using the RPC type and data
                s.sendall(message.encode()) # Send the message
                response = s.recv(4096).decode() # Receive the response and decode it
                return json.loads(response) # Parse the JSON response
        except socket.timeout:
            print(f"[{self.name}] RPC to {ip}:{port} timed out")
            return None
        except ConnectionRefusedError:
            print(f"[{self.name}] RPC to {ip}:{port} failed: Connection refused")
            return None
        except Exception as e:
            print(f"[{self.name}] RPC to {ip}:{port} failed: {e}")
            return None

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python node.py [node_name]") # Check if the correct number of arguments is provided
        sys.exit(1)

    node_name = sys.argv[1] # Get the node name from the command line arguments
    if node_name not in NODES: # Check if the node name is valid
        print(f"Invalid node name. Available nodes: {list(NODES.keys())}")
        sys.exit(1)

    node = Node(node_name) # Create a new node instance
    try:
        node.start() # Start the node
    except KeyboardInterrupt:
        print(f"[{node.name}] Shutting down...")
        node.running = False # Set the running flag to False if the program is interrupted
        if node.server_socket:
            node.server_socket.close() # Close the server socket