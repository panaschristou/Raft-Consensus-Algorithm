# main.py
import sys
import time
from compute_node import ComputeNode
from nodes_config import nodes_config

def main():
    if len(sys.argv) != 2:
        print("Usage: python main.py <node_id>")
        sys.exit(1)
    node_id = int(sys.argv[1])
    if node_id not in nodes_config:
        print(f"Invalid node_id: {node_id}")
        sys.exit(1)
    node = ComputeNode(node_id, nodes_config)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.shutdown()
        sys.exit(0)

if __name__ == "__main__":
    main()
