# client.py
import json
import time
from multiprocessing.connection import Client
from nodes_config import nodes_config

class RaftClient:
    def __init__(self, nodes_config):
        """Initialize the client with cluster configuration"""
        self.nodes_config = nodes_config
        self.connections = {}
        self.leader_id = None
        self.connect_to_nodes()

    def connect_to_nodes(self):
        """Establish connections to all nodes in the cluster"""
        print("\nConnecting to nodes...")
        for node_id, address in self.nodes_config.items():
            try:
                self.connections[node_id] = Client(address, authkey=b'raft')
                print(f"✓ Connected to node {node_id}")
            except Exception as e:
                print(f"✗ Failed to connect to node {node_id}: {e}")

    def find_leader(self):
        """Attempt to find the current leader"""
        print("\nSearching for cluster leader...")
        for node_id in self.nodes_config:
            try:
                conn = self.connections.get(node_id)
                if not conn:
                    continue

                # Send a test value to identify leader
                request = {
                    'type': 'submit_value',
                    'value': '_test'  # This won't actually be committed
                }
                conn.send(json.dumps(request))
                response = json.loads(conn.recv())

                if response.get('success'):
                    self.leader_id = node_id
                    print(f"Found leader: Node {node_id}")
                    return True
                elif 'leader_id' in response:
                    self.leader_id = response['leader_id']
                    print(f"Redirected to leader: Node {response['leader_id']}")
                    return True

            except Exception as e:
                print(f"Error communicating with node {node_id}: {e}")
                
        print("No leader found")
        return False

    def submit_value(self, value):
        """Submit a value to be written to the distributed log"""
        print(f"\nAttempting to submit value: {value}")

        # If we don't know the leader, try to find it
        if self.leader_id is None:
            if not self.find_leader():
                print("Failed to find leader. Try again later.")
                return False

        # Prepare request
        request = {
            'type': 'submit_value',
            'value': value
        }

        # Try submitting to leader
        try:
            conn = self.connections.get(self.leader_id)
            if not conn:
                print(f"No connection to leader (Node {self.leader_id})")
                self.leader_id = None
                return False

            print(f"Submitting value to leader (Node {self.leader_id})")
            conn.send(json.dumps(request))
            response = json.loads(conn.recv())

            if response.get('success'):
                print(f"Success! Value '{value}' has been committed to the log")
                return True
            elif 'leader_id' in response:
                print(f"Leader has changed to Node {response['leader_id']}")
                self.leader_id = response['leader_id']
                # Retry with new leader
                return self.submit_value(value)
            else:
                print("Failed to submit value")
                return False

        except Exception as e:
            print(f"Error communicating with leader: {e}")
            self.leader_id = None
            return False

    def force_leader_change(self):
        """Force a leader change in the cluster"""
        print("\nAttempting to force leader change...")
        
        if self.leader_id is None:
            if not self.find_leader():
                print("Cannot force leader change: No leader found")
                return False

        try:
            conn = self.connections.get(self.leader_id)
            if not conn:
                print(f"No connection to leader (Node {self.leader_id})")
                self.leader_id = None
                return False

            request = {'type': 'force_leader_change'}
            conn.send(json.dumps(request))
            response = json.loads(conn.recv())

            if response.get('success'):
                print(f"Successfully forced Node {self.leader_id} to step down")
                self.leader_id = None
                return True
            else:
                print("Failed to force leader change")
                return False

        except Exception as e:
            print(f"Error during leader change: {e}")
            self.leader_id = None
            return False

    def simulate_crash(self, node_id):
        """Simulate a crash on a specific node"""
        if node_id not in self.nodes_config:
            print(f"Invalid node ID: {node_id}")
            return False

        print(f"\nSimulating crash on Node {node_id}...")
        try:
            conn = self.connections.get(node_id)
            if not conn:
                print(f"No connection to Node {node_id}")
                return False

            request = {'type': 'simulate_crash'}
            conn.send(json.dumps(request))
            response = json.loads(conn.recv())

            if response.get('success'):
                print(f"Successfully simulated crash on Node {node_id}")
                if node_id == self.leader_id:
                    self.leader_id = None
                return True
            else:
                print(f"Failed to simulate crash on Node {node_id}")
                return False

        except Exception as e:
            print(f"Error simulating crash: {e}")
            if node_id == self.leader_id:
                self.leader_id = None
            return False

def main():
    """Main function to run the client interface"""
    client = RaftClient(nodes_config)
    
    while True:
        print("\nAvailable commands:")
        print("1. submit <value>  - Submit a value to the log")
        print("2. change         - Force a leader change")
        print("3. crash <node_id> - Simulate a node crash")
        print("4. quit           - Exit the program")
        
        try:
            command = input("\nEnter command: ").strip().split()
            
            if not command:
                continue
                
            if command[0] == 'quit':
                break
                
            elif command[0] == 'submit':
                if len(command) != 2:
                    print("Usage: submit <value>")
                    continue
                client.submit_value(command[1])
                
            elif command[0] == 'change':
                client.force_leader_change()
                
            elif command[0] == 'crash':
                if len(command) != 2:
                    print("Usage: crash <node_id>")
                    continue
                try:
                    node_id = int(command[1])
                    client.simulate_crash(node_id)
                except ValueError:
                    print("Node ID must be a number")
                    
            else:
                print("Unknown command")
                
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()