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
        self.current_term = 0
        self.voted_for = None
        self.log = []  # [{'term': term, 'value': value, 'index': index}]
        self.commit_index = -1
        self.last_applied = -1
        self.next_index = {}
        self.match_index = {}
        self.leader_id = None
        self.election_timer = None
        self.heartbeat_timer = None
        self.server_socket = None
        self.running = True
        self.lock = threading.Lock()
        
        # Create or clear log file
        self.log_filename = f"{self.name}_CISC6935"
        open(self.log_filename, 'w').close()

    def start(self):
        print(f"[{self.name}] Starting node...")
        server_thread = threading.Thread(target=self.run_server)
        server_thread.daemon = True
        server_thread.start()
        
        self.reset_election_timer()
        print(f"[{self.name}] Initial election timeout: {self.election_timer}")

        while self.running:
            with self.lock:
                current_state = self.state
                current_timer = self.election_timer

            if current_state == 'Leader':
                self.send_heartbeats()
                time.sleep(HEARTBEAT_INTERVAL)
            else:
                if current_timer <= 0:
                    with self.lock:
                        if self.state != 'Leader':  # Double-check state
                            self.start_election()
                time.sleep(0.1)
                with self.lock:
                    self.election_timer -= 0.1

    def run_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(5)
        print(f"[{self.name}] Server listening at {self.ip}:{self.port}")

        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                client_thread = threading.Thread(
                    target=self.handle_client_connection,
                    args=(client_socket,)
                )
                client_thread.daemon = True
                client_thread.start()
            except Exception as e:
                if self.running:
                    print(f"[{self.name}] Server error: {e}")

    def handle_client_connection(self, client_socket):
        try:
            data = client_socket.recv(4096).decode()
            if data:
                request = json.loads(data)
                rpc_type = request['rpc_type']
                response = {}

                with self.lock:
                    if rpc_type == 'RequestVote':
                        response = self.handle_request_vote(request['data'])
                    elif rpc_type == 'AppendEntries':
                        response = self.handle_append_entries(request['data'])
                    elif rpc_type == 'SubmitValue':
                        response = self.handle_client_submit(request['data'])
                    elif rpc_type == 'TriggerLeaderChange':
                        response = self.trigger_leader_change()
                    elif rpc_type == 'SimulateCrash':
                        response = self.simulate_crash()
                    else:
                        response = {'error': 'Unknown RPC type'}

                client_socket.sendall(json.dumps(response).encode())
        except Exception as e:
            print(f"[{self.name}] Error handling client connection: {e}")
        finally:
            client_socket.close()

    def reset_election_timer(self):
        self.election_timer = random.uniform(*ELECTION_TIMEOUT)

    def handle_request_vote(self, data):
        candidate_term = data['term']
        candidate_id = data['candidate_name']
        candidate_last_log_index = data['last_log_index']
        candidate_last_log_term = data['last_log_term']

        if candidate_term < self.current_term:
            return {
                'term': self.current_term,
                'vote_granted': False
            }

        if candidate_term > self.current_term:
            self.current_term = candidate_term
            self.state = 'Follower'
            self.voted_for = None
            self.leader_id = None

        # Check if we can vote for this candidate
        can_vote = (self.voted_for is None or self.voted_for == candidate_id)
        
        # Check if candidate's log is at least as up-to-date as ours
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]['term'] if self.log else 0

        log_is_up_to_date = (
            candidate_last_log_term > last_log_term or
            (candidate_last_log_term == last_log_term and
             candidate_last_log_index >= last_log_index)
        )

        if can_vote and log_is_up_to_date:
            self.voted_for = candidate_id
            self.reset_election_timer()
            print(f"[{self.name}] Voted for {candidate_id} in term {self.current_term}")
            return {
                'term': self.current_term,
                'vote_granted': True
            }

        return {
            'term': self.current_term,
            'vote_granted': False
        }
    
    def handle_append_entries(self, data):
        leader_term = data['term']
        leader_id = data['leader_name']
        prev_log_index = data['prev_log_index']
        prev_log_term = data['prev_log_term']
        entries = data['entries']
        leader_commit = data['leader_commit']

        # Reply false if term < currentTerm
        if leader_term < self.current_term:
            return {'term': self.current_term, 'success': False}

        # Update term if needed
        if leader_term > self.current_term:
            self.current_term = leader_term
            self.voted_for = None

        # Reset election timer and update leader
        self.reset_election_timer()
        self.state = 'Follower'
        self.leader_id = leader_id

        # Check log consistency
        if prev_log_index >= len(self.log):
            return {'term': self.current_term, 'success': False}
        
        if prev_log_index >= 0 and (
            prev_log_index >= len(self.log) or
            self.log[prev_log_index]['term'] != prev_log_term
        ):
            return {'term': self.current_term, 'success': False}

        # Process new entries
        if entries:
            # Delete conflicting entries
            self.log = self.log[:prev_log_index + 1]
            # Append new entries
            self.log.extend(entries)
            print(f"[{self.name}] Appended {len(entries)} entries to log")

        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            self.apply_committed_entries()

        return {'term': self.current_term, 'success': True}

    def apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            self.apply_entry_to_state_machine(entry)

    def apply_entry_to_state_machine(self, entry):
        with open(self.log_filename, 'a') as f:
            f.write(f"{entry['value']}\n")
        print(f"[{self.name}] Applied entry to log: {entry['value']}")

    def start_election(self):
        self.state = 'Candidate'
        self.current_term += 1
        self.voted_for = self.name
        self.leader_id = None
        votes_received = 1  # Vote for self

        print(f"[{self.name}] Starting election for term {self.current_term}")
        self.reset_election_timer()

        # Prepare RequestVote arguments
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]['term'] if self.log else 0

        # Send RequestVote RPCs to all other nodes
        for node_name in NODES:
            if node_name != self.name:
                try:
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

                    if response and response.get('vote_granted'):
                        votes_received += 1
                        if (votes_received > len(NODES) // 2 and 
                            self.state == 'Candidate'):  # Check if still candidate
                            self.become_leader()
                            break
                    elif response and response['term'] > self.current_term:
                        self.current_term = response['term']
                        self.state = 'Follower'
                        self.voted_for = None
                        break
                except Exception as e:
                    print(f"[{self.name}] Error requesting vote from {node_name}: {e}")

    def check_cluster_health(self):
        """Check how many nodes are reachable in the cluster"""
        reachable_nodes = 1  # Count self
        for node_name in NODES:
            if node_name != self.name:
                try:
                    response = self.send_rpc(
                        NODES[node_name]['ip'],
                        NODES[node_name]['port'],
                        'AppendEntries',  # Use as heartbeat
                        {
                            'term': self.current_term,
                            'leader_name': self.name,
                            'prev_log_index': len(self.log) - 1,
                            'prev_log_term': self.log[-1]['term'] if self.log else 0,
                            'entries': [],
                            'leader_commit': self.commit_index
                        }
                    )
                    if response is not None:
                        reachable_nodes += 1
                except Exception:
                    continue
        return reachable_nodes

    def become_leader(self):
        # Check cluster health before becoming leader
        reachable_nodes = self.check_cluster_health()
        if reachable_nodes <= len(NODES) // 2:
            print(f"[{self.name}] Cannot become leader: only {reachable_nodes}/{len(NODES)} nodes reachable")
            self.state = 'Follower'
            return

        print(f"[{self.name}] Becoming leader for term {self.current_term}")
        self.state = 'Leader'
        self.leader_id = self.name
        
        # Initialize leader state
        self.next_index = {node: len(self.log) for node in NODES if node != self.name}
        self.match_index = {node: -1 for node in NODES if node != self.name}
        
        # Send immediate heartbeat
        self.send_heartbeats()

    # def become_leader(self):
    #     print(f"[{self.name}] Becoming leader for term {self.current_term}")
    #     self.state = 'Leader'
    #     self.leader_id = self.name
        
    #     # Initialize leader state
    #     self.next_index = {
    #         node: len(self.log) 
    #         for node in NODES if node != self.name
    #     }
    #     self.match_index = {
    #         node: -1 
    #         for node in NODES if node != self.name
    #     }
        
    #     # Send immediate heartbeat
    #     self.send_heartbeats()

    def send_heartbeats(self):
        for node_name in NODES:
            if node_name != self.name:
                entries = []
                next_idx = self.next_index.get(node_name, len(self.log))
                
                if next_idx < len(self.log):
                    entries = self.log[next_idx:]
                
                self.send_append_entries(node_name, entries)

    def handle_client_submit(self, data):
        if self.state != 'Leader':
            return {
                'redirect': True,
                'leader_name': self.leader_id
            }

        # Append new entry to log
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
                if self.replicate_log_to_follower(node_name):
                    success_count += 1

        # If majority successful, commit and apply
        if success_count > len(NODES) // 2:
            self.commit_index = len(self.log) - 1
            self.apply_committed_entries()
            return {'success': True}
        else:
            # Roll back if replication failed
            self.log.pop()
            return {'success': False}

    def replicate_log_to_follower(self, follower_name):
        next_idx = self.next_index[follower_name]
        entries = self.log[next_idx:]
        
        response = self.send_append_entries(follower_name, entries)
        if response and response.get('success'):
            self.next_index[follower_name] = len(self.log)
            self.match_index[follower_name] = len(self.log) - 1
            return True
        elif response:
            # If failed, decrement next_index and retry
            self.next_index[follower_name] = max(0, self.next_index[follower_name] - 1)
        return False

    def send_append_entries(self, follower_name, entries):
        prev_log_index = self.next_index[follower_name] - 1
        prev_log_term = (
            self.log[prev_log_index]['term'] 
            if prev_log_index >= 0 and self.log 
            else 0
        )

        return self.send_rpc(
            NODES[follower_name]['ip'],
            NODES[follower_name]['port'],
            'AppendEntries',
            {
                'term': self.current_term,
                'leader_name': self.name,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term,
                'entries': entries,
                'leader_commit': self.commit_index
            }
        )

    def trigger_leader_change(self):
        if self.state == 'Leader':
            print(f"[{self.name}] Triggering leader change")
            self.state = 'Follower'
            self.voted_for = None
            self.leader_id = None
            self.reset_election_timer()
            return {'status': 'Leader stepping down'}
        return {'status': 'Not a leader'}

    def simulate_crash(self):
        print(f"[{self.name}] Simulating crash")
        self.log = []
        self.commit_index = -1
        self.last_applied = -1
        self.current_term = 0
        self.voted_for = None
        self.state = 'Follower'
        
        # Clear log file
        open(self.log_filename, 'w').close()
        return {'status': 'Node crashed'}

    def send_rpc(self, ip, port, rpc_type, data, timeout=2.0):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((ip, port))
                message = json.dumps({'rpc_type': rpc_type, 'data': data})
                s.sendall(message.encode())
                response = s.recv(4096).decode()
                return json.loads(response)
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
        print("Usage: python node.py [node_name]")
        sys.exit(1)

    node_name = sys.argv[1]
    if node_name not in NODES:
        print(f"Invalid node name. Available nodes: {list(NODES.keys())}")
        sys.exit(1)

    node = Node(node_name)
    try:
        node.start()
    except KeyboardInterrupt:
        print(f"[{node.name}] Shutting down...")
        node.running = False
        if node.server_socket:
            node.server_socket.close()