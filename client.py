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
    """Submit a value to the Raft cluster, discovering the leader automatically"""
    for node_name in NODES:
        node_info = NODES[node_name]
        print(f"Attempting to submit value through node {node_name}")
        
        response = send_rpc(node_info['ip'], node_info['port'], 'SubmitValue', {'value': value})
        
        if not response:
            print(f"Node {node_name} is unreachable")
            continue
            
        if response.get('success'):
            print(f"Value successfully committed to the cluster")
            return True
            
        if response.get('redirect') and response.get('leader_name'):
            leader_name = response['leader_name']
            if leader_name in NODES:
                leader_info = NODES[leader_name]
                print(f"Redirecting to leader {leader_name}")
                response = send_rpc(leader_info['ip'], leader_info['port'], 
                                  'SubmitValue', {'value': value})
                if response and response.get('success'):
                    print(f"Value successfully committed to the cluster")
                    return True
                
    print("Failed to submit value to the cluster - no leader available")
    return False

def trigger_leader_change():
    for node_name in NODES:
        node_info = NODES[node_name]
        response = send_rpc(node_info['ip'], node_info['port'], 'TriggerLeaderChange', {})
        if response and response.get('status') == 'Leader stepping down':
            print(f"Leader {node_name} is stepping down.")
            return
    print("No leader found to step down.")

def simulate_crash(node_name):
    if node_name not in NODES:
        print(f"Invalid node name: {node_name}")
        return
    
    node_info = NODES[node_name]
    response = send_rpc(node_info['ip'], node_info['port'], 'SimulateCrash', {})
    if response and response.get('status') == 'Node crashed':
        print(f"Node {node_name} has simulated a crash")
    else:
        print(f"Failed to crash node {node_name}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python client.py submit [value]")
        print("  python client.py leader_change")
        print("  python client.py simulate_crash")
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
    elif command == 'simulate_crash':
        node_name = sys.argv[2]
        simulate_crash(node_name)
    else:
        print("Unknown command.")