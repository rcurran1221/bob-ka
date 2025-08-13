#!/bin/bash

fuser -k 8011/tcp
fuser -k 8012/tcp

set -e

echo "Deleting test dbs..."
rm -rf test-topic*/
rm -rf consumer_state*/
rm -rf mothership_db*/

echo "running binary..."
cargo run -- test_config.toml &
cargo run -- example_mothership_config.toml &

sleep 5s

echo "Running tests..."
cargo test $1

echo "Done."
