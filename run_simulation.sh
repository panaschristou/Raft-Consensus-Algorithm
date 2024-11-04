#!/bin/bash

# Function to handle script termination and kill all background processes
cleanup() {
    echo "Terminating compute nodes..."
    # Kill all child processes of this script
    pkill -P $$
    exit 0
}

# Trap Ctrl+C (SIGINT) and other termination signals to run the cleanup function
trap cleanup SIGINT SIGTERM

# Start the three compute nodes in the background
echo "Starting compute nodes..."
python3 compute_node.py 1 &
python3 compute_node.py 2 &
python3 compute_node.py 3 &

# Give the nodes a moment to initialize
sleep 2

# Start the client
echo "Starting client..."
python3 client.py

# Wait for all background processes to finish
wait
