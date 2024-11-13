# node.py

import socket
import threading
import time
import json
import sys
import random
from config import NODES, ELECTION_TIMEOUT, HEARTBEAT_INTERVAL

class Node:
    def __init__(self, name):
        self.name = name
        self.ip = NODES[self.name]['ip']
        self.port = NODES[self.name]['port']
        self.state = 'Follower'  # Possible states: Follower, Candidate, Leader
        self.current_term = 0
        self.voted_for = None
        self.log = []  # List of log entries: [{'term': term, 'value': value}]
        self.commit_index = -1
        self.last_applied = -1
        self.next_index = {}
        self.match_index = {}
        self.votes_received = 0
        self.leader_name = None
        self.election_timer = None
        self.heartbeat_timer = None
        self.server_socket = None
        self.running = True
        self.lock = threading.Lock()

    def start(self):
        server_thread = threading.Thread(target=self.run_server)
        server_thread.daemon = True
        server_thread.start()

        self.reset_election_timer()

        while self.running:
            time.sleep(0.1)
            with self.lock:
                if self.state == 'Leader':
                    if self.heartbeat_timer <= 0:
                        self.send_heartbeats()
                        self.heartbeat_timer = HEARTBEAT_INTERVAL
                    else:
                        self.heartbeat_timer -= 0.1
                else:
                    if self.election_timer <= 0:
                        print(f"[{self.name}] Election timeout. Starting election.")
                        self.start_election()
                    else:
                        self.election_timer -= 0.1

    def run_server(self):
        # Create a TCP server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(5)
        print(f"[{self.name}] Server started at {self.ip}:{self.port}")

        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                client_thread = threading.Thread(target=self.handle_client_connection, args=(client_socket,))
                client_thread.daemon = True
                client_thread.start()
            except Exception as e:
                print(f"[{self.name}] Server error: {e}")

    def handle_client_connection(self, client_socket):
        try:
            data = client_socket.recv(4096).decode()
            if data:
                request = json.loads(data)
                rpc_type = request['rpc_type']
                response = {}
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
        self.election_timeout = random.uniform(*ELECTION_TIMEOUT)
        self.election_timer = self.election_timeout

    def handle_request_vote(self, data):
        with self.lock:
            term = data['term']
            candidate_name = data['candidate_name']
            last_log_index = data['last_log_index']
            last_log_term = data['last_log_term']
            vote_granted = False

            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = 'Follower'

            if term == self.current_term and (self.voted_for is None or self.voted_for == candidate_name):
                if self.is_candidate_log_up_to_date(last_log_index, last_log_term):
                    self.voted_for = candidate_name
                    vote_granted = True
                    self.reset_election_timer()
                    print(f"[{self.name}] Voted for {candidate_name} in term {self.current_term}")

            response = {'term': self.current_term, 'vote_granted': vote_granted}
            return response

    def is_candidate_log_up_to_date(self, candidate_last_log_index, candidate_last_log_term):
        # A candidate's log is at least as up-to-date as a voter’s log if:
        # 1. The candidate's last log term is greater than the voter’s last log term.
        # 2. If the last log terms are equal, then the candidate's last log index is at least as great as the voter’s.

        if len(self.log) == 0:
            # Voter has no entries
            return True

        last_log_index = len(self.log) - 1
        last_log_term = self.log[-1]['term']

        if candidate_last_log_term > last_log_term:
            return True
        elif candidate_last_log_term == last_log_term:
            return candidate_last_log_index >= last_log_index
        else:
            return False

    def handle_append_entries(self, data):
        with self.lock:
            term = data['term']
            leader_name = data['leader_name']
            prev_log_index = data['prev_log_index']
            prev_log_term = data['prev_log_term']
            entries = data['entries']
            leader_commit = data['leader_commit']

            success = False

            if term >= self.current_term:
                if term > self.current_term:
                    self.current_term = term
                    self.voted_for = None

                self.leader_name = leader_name
                self.state = 'Follower'
                self.reset_election_timer()

                # Check if log contains an entry at prevLogIndex whose term matches prevLogTerm
                if prev_log_index == -1 or (prev_log_index < len(self.log) and self.log[prev_log_index]['term'] == prev_log_term):
                    # Matching log, append any new entries
                    # Remove any conflicting entries
                    self.log = self.log[:prev_log_index + 1]
                    self.log.extend(entries)
                    success = True

                    # Update commit index
                    if leader_commit > self.commit_index:
                        self.commit_index = min(leader_commit, len(self.log) - 1)
                        self.apply_committed_entries()
                else:
                    success = False
            else:
                success = False

            response = {'term': self.current_term, 'success': success}
            return response

    def apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            self.apply_entry_to_state_machine(entry)

    def apply_entry_to_state_machine(self, entry):
        # Write the value to the log file
        filename = f"{self.name}_CISC6935"
        with open(filename, 'a') as f:
            f.write(f"{entry['value']}\n")
        print(f"[{self.name}] Applied entry to state machine: {entry['value']}")

    def send_heartbeats(self):
        for node_name in NODES:
            if node_name != self.name:
                threading.Thread(target=self.replicate_log_entries, args=(node_name,)).start()

    def replicate_log_entries(self, node_name):
        while self.running:
            with self.lock:
                next_index = self.next_index[node_name]
                prev_log_index = next_index - 1
                prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0

                entries = self.log[next_index:]  # Entries to send

                data = {
                    'term': self.current_term,
                    'leader_name': self.name,
                    'prev_log_index': prev_log_index,
                    'prev_log_term': prev_log_term,
                    'entries': entries,
                    'leader_commit': self.commit_index,
                }

            response = self.send_rpc(NODES[node_name]['ip'], NODES[node_name]['port'], 'AppendEntries', data)
            if response:
                with self.lock:
                    if response['term'] > self.current_term:
                        self.current_term = response['term']
                        self.state = 'Follower'
                        self.voted_for = None
                        self.reset_election_timer()
                        return
                    elif response['success']:
                        # Update nextIndex and matchIndex
                        self.match_index[node_name] = len(self.log) - 1
                        self.next_index[node_name] = len(self.log)
                        # Update commit index if necessary
                        self.update_commit_index()
                        return  # Entries replicated successfully
                    else:
                        # Decrement nextIndex and retry
                        self.next_index[node_name] = max(0, self.next_index[node_name] - 1)
            else:
                # No response, stop trying
                return

    def update_commit_index(self):
        # Leader considers whether any new entries can be committed
        N = self.commit_index + 1
        while N < len(self.log):
            count = 1  # Leader has this entry
            for node in self.match_index:
                if self.match_index[node] >= N:
                    count += 1
            if count > len(NODES) // 2 and self.log[N]['term'] == self.current_term:
                self.commit_index = N
                self.apply_committed_entries()
                N += 1
            else:
                break

    def handle_client_submit(self, data):
        value = data['value']
        with self.lock:
            if self.state != 'Leader':
                response = {'redirect': True, 'leader_name': self.leader_name}
                return response
            else:
                # Append to own log
                entry = {'term': self.current_term, 'value': value}
                self.log.append(entry)
                # Initialize nextIndex and matchIndex if not already
                for node_name in NODES:
                    if node_name != self.name:
                        if node_name not in self.next_index:
                            self.next_index[node_name] = len(self.log)
                        if node_name not in self.match_index:
                            self.match_index[node_name] = -1
                # Start replication to followers
                for node_name in NODES:
                    if node_name != self.name:
                        threading.Thread(target=self.replicate_log_entries, args=(node_name,)).start()
                response = {'success': True}
                return response

    def start_election(self):
        with self.lock:
            self.state = 'Candidate'
            self.current_term += 1
            self.voted_for = self.name
            self.votes_received = 1  # Vote for self
            self.reset_election_timer()
            self.leader_name = None

            print(f"[{self.name}] Starting election for term {self.current_term}")

            last_log_index = len(self.log) - 1
            last_log_term = self.log[last_log_index]['term'] if last_log_index >= 0 else 0

            for node_name in NODES:
                if node_name != self.name:
                    threading.Thread(target=self.send_request_vote, args=(node_name, last_log_index, last_log_term)).start()

    def send_request_vote(self, node_name, last_log_index, last_log_term):
        node_info = NODES[node_name]
        data = {
            'term': self.current_term,
            'candidate_name': self.name,
            'last_log_index': last_log_index,
            'last_log_term': last_log_term,
        }
        response = self.send_rpc(node_info['ip'], node_info['port'], 'RequestVote', data)
        if response:
            with self.lock:
                if self.state != 'Candidate':
                    return
                if response['term'] > self.current_term:
                    self.current_term = response['term']
                    self.state = 'Follower'
                    self.voted_for = None
                    self.reset_election_timer()
                elif response['vote_granted']:
                    self.votes_received += 1
                    print(f"[{self.name}] Received vote from {node_name} for term {self.current_term}")
                    if self.votes_received > len(NODES) // 2 and self.state == 'Candidate':
                        self.become_leader()
        else:
            # No response from node
            pass

    def become_leader(self):
        with self.lock:
            self.state = 'Leader'
            self.leader_name = self.name
            # Initialize nextIndex and matchIndex for each follower
            next_index = len(self.log)
            for node_name in NODES:
                if node_name != self.name:
                    self.next_index[node_name] = next_index
                    self.match_index[node_name] = -1
            print(f"[{self.name}] Became leader in term {self.current_term}")
            self.heartbeat_timer = 0  # Send immediate heartbeat

    def send_rpc(self, ip, port, rpc_type, data):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                s.connect((ip, port))
                message = json.dumps({'rpc_type': rpc_type, 'data': data})
                s.sendall(message.encode())
                response = s.recv(4096).decode()
                return json.loads(response)
        except Exception as e:
            # print(f"[{self.name}] RPC to {ip}:{port} failed: {e}")
            return None

    def trigger_leader_change(self):
        with self.lock:
            if self.state == 'Leader':
                print(f"[{self.name}] Leader change triggered")
                self.state = 'Follower'
                self.voted_for = None
                self.leader_name = None
                self.reset_election_timer()
                return {'status': 'Leader stepping down'}
            else:
                return {'status': 'Not a leader'}

    def simulate_crash(self):
        with self.lock:
            print(f"[{self.name}] Simulating crash")
            self.log = []
            self.commit_index = -1
            self.last_applied = -1
            # Optionally delete the log file
            filename = f"{self.name}_CISC6935"
            open(filename, 'w').close()
            return {'status': 'Node crashed'}

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python node.py [node_name]")
        sys.exit(1)

    node_name = sys.argv[1]
    if node_name not in NODES:
        print("Invalid node name. Available nodes are:", list(NODES.keys()))
        sys.exit(1)

    node = Node(node_name)
    try:
        node.start()
    except KeyboardInterrupt:
        print(f"[{node.name}] Shutting down...")
        node.running = False
        if node.server_socket:
            node.server_socket.close()
