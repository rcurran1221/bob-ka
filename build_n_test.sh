#!/bin/bash

fuser -k 8011/tcp

set -e

echo "Deleting test dbs..."
rm -rf test-topic*/
rm -rf consumer_state*/

echo "running binary..."
cargo run &

sleep 5s

echo "Running tests..."
cargo test $1

echo "Done."
