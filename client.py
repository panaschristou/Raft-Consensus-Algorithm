import socket
import json
import sys
from config import NODES

def send_rpc(ip, port, rpc_type, data):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: # Create a TCP/IP socket
            s.settimeout(3) # Set a timeout for the connection
            s.connect((ip, port)) # Connect to the server
            message = json.dumps({'rpc_type': rpc_type, 'data': data}) # Create a JSON message
            s.sendall(message.encode()) # Send the message
            response = s.recv(4096).decode() # Receive the response
            return json.loads(response) # Return the response as a dictionary parsed from JSON
    except Exception as e:
        return None

def submit_value(value):
    tried_nodes = set() # Set to keep track of nodes that have been tried
    while True:
        for node_name in NODES:
            if node_name in tried_nodes:
                continue
            node_info = NODES[node_name] # Get the node information
            response = send_rpc(node_info['ip'], node_info['port'], 'SubmitValue', {'value': value}) # Send the RPC with the value to be submitted to the node
            if response: # Check if a response was received
                if response.get('redirect'): # Check if the response is a redirect
                    leader_name = response['leader_name'] # Get the leader name from the response
                    if leader_name and leader_name in NODES: # Check if the leader name is known
                        tried_nodes.add(node_name) # Add the current node to the set of tried nodes to avoid retrying if the leader is unknown
                        print(f"Redirected to leader: {leader_name}")
                        leader_info = NODES[leader_name] # Get the leader information
                        response = send_rpc(leader_info['ip'], leader_info['port'], 'SubmitValue', {'value': value}) # Send the RPC to the leader
                        if response and response.get('success'): # Check if the response is successful
                            print("Value submitted successfully to the leader.")
                            return
                        else:
                            print("Failed to submit value to the leader.")
                    else:
                        print("Leader is unknown. Retrying...")
                        tried_nodes.add(node_name) # Add the current node to the set of tried nodes
                elif response.get('success'): # Check if the response is successful without a redirect
                    print("Value submitted successfully.")
                    return
                else:
                    print("Failed to submit value.")
            else:
                print(f"No response from {node_name}.") # Print a message if no response was received from the node
                tried_nodes.add(node_name) # Add the current node to the set of tried nodes
        if len(tried_nodes) == len(NODES): # Check if all nodes have been tried
            print("Unable to submit value to the cluster.")
            return

def trigger_leader_change():
    # Trigger leader change by sending a TriggerLeaderChange RPC to all nodes
    for node_name in NODES:
        node_info = NODES[node_name]
        response = send_rpc(node_info['ip'], node_info['port'], 'TriggerLeaderChange', {}) # Send the RPC to trigger a leader change
        if response and response.get('status') == 'Leader stepping down': # Check if the response indicates that the leader is stepping down
            print(f"Leader {node_name} is stepping down.")
            return
    print("No leader found to step down.") # Print a message if no leader was found to step down

def simulate_crash_leader():
    # Simulate a leader crash by sending a SimulateCrashLeader RPC to all nodes
    for node_name in NODES:
        node_info = NODES[node_name]
        response = send_rpc(node_info['ip'], node_info['port'], 'SimulateCrashLeader', {}) # Send the RPC to simulate a crash
        if response and response.get('status') == 'Leader crashed': # Check if the response indicates that the leader has crashed
            print(f"Node {node_name} has simulated a crash.")
            return
    print("No node could simulate a crash.") # Print a message ifthe leader could not be crashed
    
def simulate_crash_node(node_name):
    # Simulate a node crash by sending a SimulateCrashNode RPC to the specified node
    node_info = NODES[node_name]
    response = send_rpc(node_info['ip'], node_info['port'], 'SimulateCrashNode', {}) # Send the RPC to simulate a crash
    if response and response.get('status') == 'Node crashed': # Check if the response indicates that the node has crashed
        print(f"Node {node_name} has simulated a crash.")
        return
    print("No node could simulate a crash.") # Print a message if the node could not be crashed

if __name__ == '__main__':
    # Check the command-line arguments
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python client.py submit [value]") # If the command is 'submit', submit the value to the cluster
        print("  python client.py leader_change")  # If the command is 'leader_change', trigger a leader change in the cluster
        print("  python client.py simulate_crash_leader")  # If the command is 'simulate_crash_leader', simulate a leader crash in the cluster
        print("  python client.py simulate_crash_node [node_name]")  # If the command is 'simulate_crash_node', simulate a node crash in the cluster
        sys.exit(1)

    command = sys.argv[1]

    # Execute the command
    # If the command is 'submit', submit the value to the cluster otherwise trigger a leader change or simulate crashes
    if command == 'submit':
        if len(sys.argv) != 3:
            print("Usage: python client.py submit [value]")
            sys.exit(1)
        value = sys.argv[2]
        submit_value(value)
    elif command == 'leader_change':
        trigger_leader_change()
    elif command == 'simulate_crash_leader':
        simulate_crash_leader()
    elif command == 'simulate_crash_node':
        simulate_crash_node(sys.argv[2])
    else:
        print("Unknown command.")