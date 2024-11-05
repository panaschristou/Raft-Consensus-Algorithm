#!/bin/bash

# Function to start a node in a new Windows Terminal tab
start_node() {
    local node_id=$1
    echo "Starting node $node_id in a new terminal tab..."
    wt -w 0 nt -p "Ubuntu" bash -c "echo 'Starting node $node_id...'; python3 compute_node.py $node_id; read -n 1"
}

# Function to start the client in a new Windows Terminal tab
start_client() {
    echo "Starting client in a new terminal tab..."
    wt -w 0 nt -p "Ubuntu" bash -c "echo 'Starting client...'; python3 client.py; read -n 1"
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

# Start the three compute nodes, each in a new terminal tab
echo "Starting compute nodes..."
start_node 1
start_node 2
start_node 3

# Give the nodes a moment to initialize
sleep 3

# Start the client in a new terminal tab
echo "Starting client..."
start_client

echo "All processes started in separate Windows Terminal tabs."
