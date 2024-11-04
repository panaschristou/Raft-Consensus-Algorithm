import xmlrpc.client
import random
import time

def submit_to_random_node(value):
    # Connect to a random compute node
    node_port = random.choice([8001, 8002, 8003])
    with xmlrpc.client.ServerProxy(f"http://localhost:{node_port}") as node:
        result = node.submit_value(value)
        print(f"Client submitted '{value}' to node on port {node_port}: {result}")

if __name__ == "__main__":
    value = "write 1"
    submit_to_random_node(value)
