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
        self.peers = peers
        self.state = FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.leader_id = None
        self.last_heartbeat = time.time()
        self.election_in_progress = False
        self.lock = threading.Lock()

        # Log file
        self.log_file = f"node_{self.node_id}_log.txt"
        if os.path.exists(self.log_file):
            os.remove(self.log_file)

    def write_log(self, value):
        with open(self.log_file, "a") as file:
            file.write(f"{value}\n")
        print(f"Node {self.node_id}: Wrote to log: {value}")

    def submit_value(self, value):
        if self.state == LEADER:
            self.append_to_log(value)
            self.propagate_log_to_peers(value)
            return "Value added successfully."
        return "Not a leader, cannot add value."

    def append_to_log(self, value):
        with self.lock:
            self.log.append(value)
            self.write_log(value)

    def propagate_log_to_peers(self, value):
        for peer in self.peers:
            try:
                with xmlrpc.client.ServerProxy(peer) as client:
                    client.append_entry(self.node_id, value, self.current_term)
            except:
                print(f"Error: Could not reach peer {peer}")

    def append_entry(self, leader_id, value, term):
        with self.lock:
            if term >= self.current_term:
                self.current_term = term
                self.leader_id = leader_id
                self.state = FOLLOWER
                self.election_in_progress = False
                self.last_heartbeat = time.time()
                if value and value not in self.log:
                    self.append_to_log(value)
                print(f"Node {self.node_id} received entry from Leader {leader_id}.")
                return "Entry appended."
            return "Term mismatch. Entry not appended."

    def start_election(self):
        if not self.check_election_in_progress():
            self.initiate_election()
            self.lock_peers_election_timers()
            votes = self.collect_votes()
            self.process_election_results(votes)
    
    def check_election_in_progress(self):
        if self.election_in_progress:
            print(f"Node {self.node_id}: Election already in progress.")
            return True
        return False

    def initiate_election(self):
        with self.lock:
            self.current_term += 1
            self.state = CANDIDATE
            self.voted_for = self.node_id
            self.last_heartbeat = time.time()
            self.election_in_progress = True
        print(f"Node {self.node_id} initiating election for term {self.current_term}.")

        # Notify peers to set their election lock
        self.notify_peers_of_election()

    def notify_peers_of_election(self):
        """Notify peers to set their election lock to prevent simultaneous elections."""
        for peer in self.peers:
            try:
                with xmlrpc.client.ServerProxy(peer) as client:
                    client.lock_election_timer(self.current_term)
                    print(f"Node {self.node_id} notified peer {peer} to lock election timer.")
            except Exception as e:
                print(f"Node {self.node_id}: Error notifying peer {peer} to lock election - {e}")

    def lock_election_timer(self, term):
        """RPC method to allow a peer to lock election timers."""
        with self.lock:
            print(term, self.current_term)
            if term >= self.current_term:
                self.election_in_progress = True
                print(f"Node {self.node_id} locked election timer for term {term}.")
                return True
            return False


    def collect_votes(self):
        votes = 1  # Vote for self
        for peer in self.peers:
            try:
                with xmlrpc.client.ServerProxy(peer) as client:
                    if client.request_vote(self.node_id, self.current_term):
                        votes += 1
            except:
                print(f"Node {self.node_id}: Failed to collect vote from {peer}.")
        print(f"Node {self.node_id} received {votes} votes.")
        return votes

    def process_election_results(self, votes):
        if votes > len(self.peers) // 2:
            self.become_leader()
        else:
            self.election_failed()

    def become_leader(self):
        with self.lock:
            self.state = LEADER
            self.leader_id = self.node_id
            self.election_in_progress = False
        print(f"Node {self.node_id} became the leader.")
        threading.Thread(target=self.send_heartbeats).start()

    def election_failed(self):
        print(f"Node {self.node_id} failed to become leader.")
        self.unlock_peers_election_timers()
        time.sleep(random.uniform(10, 30))
        self.state = FOLLOWER
        self.election_in_progress = False

    def lock_election_timer(self, term):
        with self.lock:
            if term >= self.current_term:
                self.election_in_progress = True
                print(f"Node {self.node_id} locked election timer.")
                return True
            return False

    def unlock_peers_election_timers(self):
        for peer in self.peers:
            try:
                with xmlrpc.client.ServerProxy(peer) as client:
                    client.unlock_election_timer()
            except:
                print(f"Node {self.node_id}: Failed to unlock timer on {peer}.")

    def unlock_election_timer(self):
        with self.lock:
            self.election_in_progress = False
            print(f"Node {self.node_id} unlocked election timer.")

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
        while self.state == LEADER:
            time.sleep(1)
            for peer in self.peers:
                try:
                    with xmlrpc.client.ServerProxy(peer) as client:
                        client.append_entry(self.node_id, None, self.current_term)
                except:
                    print(f"Node {self.node_id}: Error sending heartbeat to {peer}")

    def check_heartbeat(self):
        while True:
            election_timeout = self.node_id * 5
            time.sleep(election_timeout)
            with self.lock:
                if self.state == FOLLOWER and not self.election_in_progress and time.time() - self.last_heartbeat > election_timeout:
                    print(f"Node {self.node_id} starting election due to missed heartbeat.")
                    self.start_election()

def run_node(node_id, peers):
    node = RaftNode(node_id, peers)
    with SimpleXMLRPCServer(("localhost", 8000 + node_id), allow_none=True) as server:
        server.register_instance(node)
        print(f"Node {node_id} running on port {8000 + node_id}...")
        threading.Thread(target=node.check_heartbeat).start()
        server.serve_forever()

if __name__ == "__main__":
    peers = ["http://localhost:8001", "http://localhost:8002", "http://localhost:8003"]
    node_id = int(sys.argv[1])
    run_node(node_id, peers[:node_id-1] + peers[node_id:])
