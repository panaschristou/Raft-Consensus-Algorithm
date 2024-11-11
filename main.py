# main.py
import sys
import time
from compute_node import ComputeNode
from nodes_config import nodes_config
from threading import Thread
import socket

def check_network():
    """Check network connectivity between nodes"""
    print("\nChecking network connectivity...")
    for node_id, (ip, port) in nodes_config.items():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((ip, port))
            if result == 0:
                print(f"✓ Node {node_id}: Port {port} is open on {ip}")
            else:
                print(f"✗ Node {node_id}: Port {port} is closed on {ip}")
            sock.close()
        except Exception as e:
            print(f"✗ Node {node_id}: Error checking {ip}:{port} - {e}")
    print("")

def start_node(node_id, nodes_config):
    """Start a single node in the Raft cluster"""
    try:
        print(f"\nInitializing node {node_id}...")
        node = ComputeNode(node_id, nodes_config)
        
        # Give nodes time to initialize before starting
        time.sleep(1)
        
        print(f"Starting node {node_id}...")
        node.start()
        return node
    except Exception as e:
        print(f"Error starting node {node_id}: {e}")
        return None

def main():
    """Main function to start the Raft cluster"""
    # Check command line arguments for specific node to start
    if len(sys.argv) > 1:
        try:
            node_id = int(sys.argv[1])
            if node_id not in nodes_config:
                print(f"Invalid node ID: {node_id}")
                return
            
            # Check network connectivity first
            check_network()
            
            print(f"Starting node {node_id}...")
            node = start_node(node_id, nodes_config)
            
            if node:
                print(f"Node {node_id} started successfully")
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    print(f"\nShutting down node {node_id}...")
                    sys.exit(0)
        except ValueError:
            print("Node ID must be a number")
            return
    else:
        print("Please specify a node ID to start (0, 1, or 2)")
        print("Usage: python main.py <node_id>")
        return

if __name__ == "__main__":
    main()