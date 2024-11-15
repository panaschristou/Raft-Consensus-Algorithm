import socket
import json
import sys
from config import NODES

def send_rpc(ip, port, rpc_type, data):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(3)
            s.connect((ip, port))
            message = json.dumps({'rpc_type': rpc_type, 'data': data})
            s.sendall(message.encode())
            response = s.recv(4096).decode()
            return json.loads(response)
    except Exception as e:
        return None

def submit_value(value):
    tried_nodes = set()
    while True:
        for node_name in NODES:
            if node_name in tried_nodes:
                continue
            node_info = NODES[node_name]
            response = send_rpc(node_info['ip'], node_info['port'], 'SubmitValue', {'value': value})
            if response:
                if response.get('redirect'):
                    leader_name = response['leader_name']
                    if leader_name and leader_name in NODES:
                        tried_nodes.add(node_name)
                        print(f"Redirected to leader: {leader_name}")
                        leader_info = NODES[leader_name]
                        response = send_rpc(leader_info['ip'], leader_info['port'], 'SubmitValue', {'value': value})
                        if response and response.get('success'):
                            print("Value submitted successfully to the leader.")
                            return
                        else:
                            print("Failed to submit value to the leader.")
                    else:
                        print("Leader is unknown. Retrying...")
                        tried_nodes.add(node_name)
                elif response.get('success'):
                    print("Value submitted successfully.")
                    return
                else:
                    print("Failed to submit value.")
            else:
                print(f"No response from {node_name}.")
                tried_nodes.add(node_name)
        if len(tried_nodes) == len(NODES):
            print("Unable to submit value to the cluster.")
            return

def trigger_leader_change():
    for node_name in NODES:
        node_info = NODES[node_name]
        response = send_rpc(node_info['ip'], node_info['port'], 'TriggerLeaderChange', {})
        if response and response.get('status') == 'Leader stepping down':
            print(f"Leader {node_name} is stepping down.")
            return
    print("No leader found to step down.")

def simulate_crash_leader():
    for node_name in NODES:
        node_info = NODES[node_name]
        response = send_rpc(node_info['ip'], node_info['port'], 'SimulateCrashLeader', {})
        if response and response.get('status') == 'Leader crashed':
            print(f"Node {node_name} has simulated a crash.")
            return
    print("No node could simulate a crash.")
    
def simulate_crash_node(node_name):
    node_info = NODES[node_name]
    response = send_rpc(node_info['ip'], node_info['port'], 'SimulateCrashNode', {})
    if response and response.get('status') == 'Node crashed':
        print(f"Node {node_name} has simulated a crash.")
        return
    print("No node could simulate a crash.")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python client.py submit [value]")
        print("  python client.py leader_change")
        print("  python client.py simulate_crash_leader")
        print("  python client.py simulate_crash_node [node_name]")
        sys.exit(1)

    command = sys.argv[1]

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