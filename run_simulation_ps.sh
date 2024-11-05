#!/bin/bash

# Function to start a node in a new PowerShell window
start_node() {
    local node_id=$1
    echo "Starting node $node_id in a new PowerShell window..."
    powershell.exe -NoExit -Command "Start-Process powershell -ArgumentList '-NoExit','-Command','echo Starting node $node_id; python3 compute_node.py $node_id'"
}

# Function to start the client in a new PowerShell window
start_client() {
    echo "Starting client in a new PowerShell window..."
    powershell.exe -NoExit -Command "Start-Process powershell -ArgumentList '-NoExit','-Command','echo Starting client; python3 client.py'"
}

# Function to handle script termination and kill all background processes
cleanup() {
    echo "Terminating compute nodes and client..."
    # Kill all child processes of this script
    pkill -P $$
    exit 0
}

# Trap Ctrl+C (SIGINT) and other termination signals to run the cleanup function
trap cleanup SIGINT SIGTERM

# Start the three compute nodes, each in a new PowerShell window
echo "Starting compute nodes..."
start_node 1
start_node 2
start_node 3

# Give the nodes a moment to initialize
sleep 3

# Start the client in a new PowerShell window
echo "Starting client..."
start_client

echo "All processes started in separate PowerShell windows."
