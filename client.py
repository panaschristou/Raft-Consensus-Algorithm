# client.py

import socket
import json
import sys
from config import NODES

def send_rpc(ip, port, rpc_type, data):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((ip, port))
            message = json.dumps({'rpc_type': rpc_type, 'data': data})
            s.sendall(message.encode())
            response = s.recv(4096).decode()
            return json.loads(response)
    except Exception as e:
        print(f"RPC to {ip}:{port} failed: {e}")
        return None

def submit_value(node_name, value):
    node_info = NODES[node_name]
    data = {'value': value}
    response = send_rpc(node_info['ip'], node_info['port'], 'SubmitValue', data)
    if response:
        if response.get('redirect'):
            leader_name = response['leader_name']
            print(f"Redirecting to leader: {leader_name}")
            leader_info = NODES[leader_name]
            response = send_rpc(leader_info['ip'], leader_info['port'], 'SubmitValue', data)
            if response.get('success'):
                print("Value submitted successfully to the leader.")
            else:
                print("Failed to submit value to the leader.")
        elif response.get('success'):
            print("Value submitted successfully.")
        else:
            print("Failed to submit value.")
    else:
        print("No response from the node.")

def trigger_leader_change(node_name):
    node_info = NODES[node_name]
    response = send_rpc(node_info['ip'], node_info['port'], 'TriggerLeaderChange', {})
    if response:
        print(response['status'])
    else:
        print("Failed to trigger leader change.")

def simulate_crash(node_name):
    node_info = NODES[node_name]
    response = send_rpc(node_info['ip'], node_info['port'], 'SimulateCrash', {})
    if response:
        print(response['status'])
    else:
        print("Failed to simulate crash.")

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage:")
        print("  python client.py submit [node_name] [value]")
        print("  python client.py leader_change [node_name]")
        print("  python client.py simulate_crash [node_name]")
        sys.exit(1)

    command = sys.argv[1]
    node_name = sys.argv[2]

    if node_name not in NODES:
        print("Invalid node name. Available nodes are:", list(NODES.keys()))
        sys.exit(1)

    if command == 'submit':
        if len(sys.argv) != 4:
            print("Usage: python client.py submit [node_name] [value]")
            sys.exit(1)
        value = sys.argv[3]
        submit_value(node_name, value)
    elif command == 'leader_change':
        trigger_leader_change(node_name)
    elif command == 'simulate_crash':
        simulate_crash(node_name)
    else:
        print("Unknown command.")
