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
        self.next_index = {}  # For leader to keep track of next log index to send to each follower
        self.match_index = {}  # For leader to keep track of highest log index known to be replicated
        self.votes_received = 0
        self.leader_name = None
        self.election_timer = None
        self.heartbeat_timer = None
        self.server_socket = None
        self.running = True

    def start(self):
        # Start the server thread to listen for incoming RPCs
        server_thread = threading.Thread(target=self.run_server)
        server_thread.start()

        # Start the election timer
        self.reset_election_timer()

        # Main loop
        while self.running:
            time.sleep(0.1)
            if self.state == 'Leader':
                # Leader sends heartbeats at regular intervals
                if self.heartbeat_timer <= 0:
                    self.send_heartbeats()
                    self.heartbeat_timer = HEARTBEAT_INTERVAL
                else:
                    self.heartbeat_timer -= 0.1

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
                elif rpc_type == 'LeaderRedirect':
                    response = {'leader_ip': NODES[self.leader_name]['ip'], 'leader_port': NODES[self.leader_name]['port']}
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

        response = {'term': self.current_term, 'vote_granted': vote_granted}
        return response

    def is_candidate_log_up_to_date(self, last_log_index, last_log_term):
        if not self.log:
            return True
        last_entry = self.log[-1]
        if last_log_term != last_entry['term']:
            return last_log_term > last_entry['term']
        else:
            return last_log_index >= len(self.log) - 1

    def handle_append_entries(self, data):
        term = data['term']
        leader_name = data['leader_name']
        prev_log_index = data['prev_log_index']
        prev_log_term = data['prev_log_term']
        entries = data['entries']
        leader_commit = data['leader_commit']

        success = False

        if term >= self.current_term:
            self.current_term = term
            self.leader_name = leader_name
            self.state = 'Follower'
            self.reset_election_timer()

            if prev_log_index == -1 or (prev_log_index < len(self.log) and self.log[prev_log_index]['term'] == prev_log_term):
                # Matching log, append any new entries
                self.log = self.log[:prev_log_index + 1] + entries
                success = True

                # Update commit index
                if leader_commit > self.commit_index:
                    self.commit_index = min(leader_commit, len(self.log) - 1)
                    self.apply_committed_entries()
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
                threading.Thread(target=self.send_append_entries, args=(node_name, [])).start()

    def send_append_entries(self, node_name, entries):
        node_info = NODES[node_name]
        prev_log_index = len(self.log) - 1
        prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0

        data = {
            'term': self.current_term,
            'leader_name': self.name,
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'entries': entries,
            'leader_commit': self.commit_index,
        }
        response = self.send_rpc(node_info['ip'], node_info['port'], 'AppendEntries', data)
        if response:
            if not response['success']:
                # If AppendEntries fails, decrement next_index and retry (not implemented here)
                pass

    def handle_client_submit(self, data):
        value = data['value']
        if self.state != 'Leader':
            response = {'redirect': True, 'leader_name': self.leader_name}
            return response
        else:
            # Append to own log
            entry = {'term': self.current_term, 'value': value}
            self.log.append(entry)
            self.commit_index = len(self.log) - 1  # For simplicity, commit immediately
            self.apply_committed_entries()
            # Replicate to followers
            for node_name in NODES:
                if node_name != self.name:
                    threading.Thread(target=self.send_append_entries, args=(node_name, [entry])).start()
            response = {'success': True}
            return response

    def start_election(self):
        self.state = 'Candidate'
        self.current_term += 1
        self.voted_for = self.name
        self.votes_received = 1  # Vote for self
        self.reset_election_timer()

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
            if response['vote_granted']:
                self.votes_received += 1
                if self.votes_received > len(NODES) // 2 and self.state == 'Candidate':
                    self.become_leader()
            elif response['term'] > self.current_term:
                self.current_term = response['term']
                self.state = 'Follower'
                self.voted_for = None

    def become_leader(self):
        self.state = 'Leader'
        self.leader_name = self.name
        self.next_index = {node_name: len(self.log) for node_name in NODES if node_name != self.name}
        self.match_index = {node_name: -1 for node_name in NODES if node_name != self.name}
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
            print(f"[{self.name}] RPC to {ip}:{port} failed: {e}")
            return None

    def wait_for_leader(self):
        # Wait for election timeout
        if self.election_timer <= 0:
            self.start_election()
        else:
            self.election_timer -= 0.1

    def trigger_leader_change(self):
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
