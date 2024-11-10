import json
import time
from multiprocessing.connection import Client

class RaftClient:
    """
    Client implementation for interacting with the Raft cluster.
    Provides methods to submit values, force leader changes, and simulate crashes.
    """
    
    def __init__(self, nodes_config):
        """
        Initialize the client with the cluster configuration.
        
        Args:
            nodes_config: Dictionary mapping node IDs to their (host, port) addresses
        """
        self.nodes_config = nodes_config
        self.connections = {}
        self.leader_id = None
        self.connect_to_nodes()

    def connect_to_nodes(self):
        """Establish connections to all nodes in the cluster"""
        for node_id, address in self.nodes_config.items():
            try:
                self.connections[node_id] = Client(address, authkey=b'raft')
                print(f"Connected to node {node_id}")
            except Exception as e:
                print(f"Failed to connect to node {node_id}: {e}")

    def submit_value(self, value):
        """
        Submit a value to be written to the distributed log.
        
        Args:
            value: The value to be written to the log
            
        Returns:
            bool: True if successful, False otherwise
        """
        request = {
            'type': 'submit_value',
            'value': value
        }

        # If we know the leader, try it first
        if self.leader_id is not None:
            success = self._try_submit_to_node(self.leader_id, request)
            if success:
                return True

        # Try all nodes until we find the leader
        for node_id in self.nodes_config:
            if node_id != self.leader_id:  # Skip if we already tried this node as leader
                success = self._try_submit_to_node(node_id, request)
                if success:
                    return True

        print("Failed to submit value: No leader found")
        return False

    def _try_submit_to_node(self, node_id, request):
        """
        Try to submit a request to a specific node.
        
        Args:
            node_id: The ID of the node to try
            request: The request to submit
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            conn = self.connections.get(node_id)
            if not conn:
                return False

            conn.send(json.dumps(request))
            response = json.loads(conn.recv())

            if response.get('success'):
                self.leader_id = node_id
                print(f"Value successfully committed through node {node_id}")
                return True
            elif response.get('leader_id') is not None:
                self.leader_id = response['leader_id']
                print(f"Redirecting to leader node {self.leader_id}")
                return False
            
            return False

        except Exception as e:
            print(f"Error communicating with node {node_id}: {e}")
            return False

    def force_leader_change(self):
        """
        Force a leader change in the cluster.
        
        Returns:
            bool: True if leader change was initiated, False otherwise
        """
        request = {
            'type': 'force_leader_change'
        }
        
        if self.leader_id is not None:
            try:
                conn = self.connections.get(self.leader_id)
                if conn:
                    conn.send(json.dumps(request))
                    response = json.loads(conn.recv())
                    if response.get('success'):
                        print(f"Leader change initiated on node {self.leader_id}")
                        self.leader_id = None
                        return True
            except Exception as e:
                print(f"Error forcing leader change: {e}")

        print("Failed to force leader change: No leader found")
        return False

    def simulate_crash(self, node_id):
        """
        Simulate a crash on a specific node.
        
        Args:
            node_id: The ID of the node to crash
            
        Returns:
            bool: True if crash was simulated successfully, False otherwise
        """
        if node_id not in self.connections:
            print(f"Invalid node ID: {node_id}")
            return False

        request = {
            'type': 'simulate_crash'
        }

        try:
            conn = self.connections[node_id]
            conn.send(json.dumps(request))
            response = json.loads(conn.recv())
            
            if response.get('success'):
                print(f"Node {node_id} crash simulated successfully")
                if node_id == self.leader_id:
                    self.leader_id = None
                return True
            
            return False

        except Exception as e:
            print(f"Error simulating crash on node {node_id}: {e}")
            return False

    def rejoin_node(self, node_id):
        """
        Trigger a node to rejoin the cluster (bonus feature).
        
        Args:
            node_id: The ID of the node to rejoin
            
        Returns:
            bool: True if rejoin was successful, False otherwise
        """
        if node_id not in self.connections:
            print(f"Invalid node ID: {node_id}")
            return False

        request = {
            'type': 'rejoin_cluster'
        }

        try:
            conn = self.connections[node_id]
            conn.send(json.dumps(request))
            response = json.loads(conn.recv())
            
            if response.get('success'):
                print(f"Node {node_id} successfully rejoined the cluster")
                return True
            
            return False

        except Exception as e:
            print(f"Error rejoining node {node_id}: {e}")
            return False

def main():
    """Main function to run the client interface"""
    # Node configuration
    nodes_config = {
        0: ('localhost', 5000),
        1: ('localhost', 5001),
        2: ('localhost', 5002)
    }

    # Create client
    client = RaftClient(nodes_config)
    
    # Command loop
    while True:
        print("\nAvailable commands:")
        print("1. submit <value> - Submit a value to the log")
        print("2. change - Force a leader change")
        print("3. crash <node_id> - Simulate a node crash")
        print("4. rejoin <node_id> - Rejoin a crashed node")
        print("5. quit - Exit the program")
        
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
                
        elif command[0] == 'rejoin':
            if len(command) != 2:
                print("Usage: rejoin <node_id>")
                continue
            try:
                node_id = int(command[1])
                client.rejoin_node(node_id)
            except ValueError:
                print("Node ID must be a number")
                
        else:
            print("Unknown command")

if __name__ == "__main__":
    main()