import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import threading
import time
import random
import os
import sys

# Raft States
FOLLOWER = "Follower"
CANDIDATE = "Candidate"
LEADER = "Leader"

class RaftNode:
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers  # List of other node IPs
        self.state = FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.leader_id = None
        self.last_heartbeat = time.time()
        self.election_in_progress = False
        self.lock = threading.Lock()  # For thread-safe log updates

        # Log file for each node
        self.log_file = f"node_{self.node_id}_log.txt"
        if os.path.exists(self.log_file):
            os.remove(self.log_file)  # Clear existing log file

    def write_log_to_file(self):
        with open(self.log_file, "w") as file:
            for entry in self.log:
                file.write(f"{entry}\n")

    def submit_value(self, value):
        if self.state == LEADER:
            with self.lock:
                self.log.append(value)
                self.write_log_to_file()  # Write log to file after appending
                print(f"Leader {self.node_id} appended value '{value}' to log.")
                # Propagate value to other nodes (simulate through RPC)
                for peer in self.peers:
                    try:
                        with xmlrpc.client.ServerProxy(peer) as client:
                            client.append_entry(self.node_id, value, self.current_term)
                    except:
                        print(f"Error: Could not reach peer {peer}")
            return "Value added successfully."
        return "Not a leader, cannot add value."

    def append_entry(self, leader_id, value, term):
        with self.lock:
            if term >= self.current_term:
                self.current_term = term
                self.leader_id = leader_id
                self.state = FOLLOWER
                self.election_in_progress = False  # Reset election state on receiving heartbeat
                if value and value not in self.log:
                    self.log.append(value)
                    self.write_log_to_file()  # Write log to file after appending
                self.last_heartbeat = time.time()  # Reset election timer
                print(f"Node {self.node_id} received append entry from Leader {leader_id}.")
                return "Entry appended."
            else:
                return "Term mismatch. Entry not appended."

    def start_election(self):
        with self.lock:
            self.current_term += 1
            self.state = CANDIDATE
            self.voted_for = self.node_id
            self.last_heartbeat = time.time()  # Reset own timer to avoid re-election immediately
            self.election_in_progress = True
        votes = 1  # Vote for self

        print(f"Node {self.node_id} starting election for term {self.current_term}.")
        
        # Notify peers to suspend their timers
        for peer in self.peers:
            try:
                with xmlrpc.client.ServerProxy(peer) as client:
                    client.suspend_election_timer(self.current_term)
            except:
                print(f"Error: Could not reach peer {peer} to suspend election timer.")

        # Collect votes
        for peer in self.peers:
            try:
                with xmlrpc.client.ServerProxy(peer) as client:
                    vote_granted = client.request_vote(self.node_id, self.current_term)
                    if vote_granted:
                        votes += 1
            except:
                print(f"Error: Could not reach peer {peer}")

        # Check if majority votes received
        if votes > len(self.peers) // 2:
            with self.lock:
                self.state = LEADER
                self.leader_id = self.node_id
                self.election_in_progress = False
            print(f"Node {self.node_id} became the leader for term {self.current_term}.")
            threading.Thread(target=self.send_heartbeats).start()  # Start sending heartbeats
        else:
            # Election failed, notify followers to resume their election timers
            print(f"Node {self.node_id} failed to become leader.")
            for peer in self.peers:
                try:
                    with xmlrpc.client.ServerProxy(peer) as client:
                        client.resume_election_timer()
                except:
                    print(f"Error: Could not reach peer {peer} to resume election timer.")
            time.sleep(random.uniform(10, 30))  # Wait before potentially starting another election
            self.state = FOLLOWER
            self.election_in_progress = False

    def suspend_election_timer(self, term):
        with self.lock:
            if term >= self.current_term:
                self.election_in_progress = True  # Suspend election timer until the election completes
                print(f"Node {self.node_id} suspends election timer for term {term}.")

    def resume_election_timer(self):
        with self.lock:
            self.election_in_progress = False  # Allow election timer to resume
            print(f"Node {self.node_id} resumes election timer.")

    def request_vote(self, candidate_id, term):
        with self.lock:
            if term > self.current_term and (self.voted_for is None or self.voted_for == candidate_id):
                self.voted_for = candidate_id
                self.current_term = term
                self.state = FOLLOWER
                print(f"Node {self.node_id} voted for {candidate_id} for term {term}.")
                return True
            return False

    def send_heartbeats(self):
        """Send heartbeats to peers to maintain leadership and prevent elections."""
        while self.state == LEADER:
            time.sleep(1)  # Send heartbeat every second
            for peer in self.peers:
                try:
                    with xmlrpc.client.ServerProxy(peer) as client:
                        client.append_entry(self.node_id, None, self.current_term)
                except:
                    print(f"Error: Could not send heartbeat to {peer}")
    
    def check_heartbeat(self):
        """Check if a heartbeat is missed; if so, start an election."""
        while True:
            election_timeout = random.uniform(10, 30)
            time.sleep(election_timeout)
            with self.lock:
                if self.state == FOLLOWER and not self.election_in_progress and time.time() - self.last_heartbeat > election_timeout:
                    print(f"Node {self.node_id} did not receive heartbeat, starting election.")
                    self.start_election()

def run_node(node_id, peers):
    node = RaftNode(node_id, peers)
    with SimpleXMLRPCServer(("localhost", 8000 + node_id), allow_none=True) as server:
        server.register_instance(node)
        print(f"Node {node_id} running on port {8000 + node_id}...")
        threading.Thread(target=node.check_heartbeat).start()  # Start heartbeat checking
        server.serve_forever()

if __name__ == "__main__":
    peers = ["http://localhost:8001", "http://localhost:8002", "http://localhost:8003"]

    # Start each node as a separate process
    node_id = int(sys.argv[1])  # Node ID passed as a command-line argument
    run_node(node_id, peers[:node_id-1] + peers[node_id:])
