#!/bin/bash

fuser -k 8011/tcp

set -e

# Define the directories you want to delete

echo "Deleting test dbs..."
find . -type d -name '*consumer_state*' -exec rm -r {} +
find . -type d -name '*test-topic*' -exec rm -r {} +

echo "running binary..."
cargo run --release &

# wait for process to start up
sleep 5s

echo "Running tests..."
cargo test

echo "Done."
