import sys
import time
from compute_node import ComputeNode
from threading import Thread

def start_node(node_id, nodes_config):
    """Start a single node in the Raft cluster"""
    try:
        node = ComputeNode(node_id, nodes_config)
        node.start()
        return node
    except Exception as e:
        print(f"Error starting node {node_id}: {e}")
        return None

def main():
    """Main function to start the Raft cluster"""
    # Node configuration
    nodes_config = {
        0: ('localhost', 5001),
        1: ('localhost', 5002),
        2: ('localhost', 5003)
    }

    # Check command line arguments for specific node to start
    if len(sys.argv) > 1:
        try:
            node_id = int(sys.argv[1])
            if node_id not in nodes_config:
                print(f"Invalid node ID: {node_id}")
                return
            
            print(f"Starting node {node_id}...")
            node = start_node(node_id, nodes_config)
            if node:
                # Keep the main thread running
                while True:
                    time.sleep(1)
        except ValueError:
            print("Node ID must be a number")
            return
    else:
        # Start all nodes (for testing purposes)
        print("Starting all nodes...")
        nodes = []
        
        # Start nodes with a slight delay between them
        for node_id in nodes_config:
            node = start_node(node_id, nodes_config)
            if node:
                nodes.append(node)
            time.sleep(1)  # Delay between node starts
        
        if not nodes:
            print("Failed to start any nodes")
            return
        
        print("All nodes started successfully")
        print("Use client.py to interact with the cluster")
        
        # Keep the main thread running
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down nodes...")
            sys.exit(0)

if __name__ == "__main__":
    main()