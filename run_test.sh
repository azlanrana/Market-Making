#!/bin/bash
# Simple test script to verify compilation

set -e

echo "Building workspace..."
cargo build --workspace 2>&1 | head -100

echo ""
echo "Running unit tests..."
cargo test --lib 2>&1 | tail -50

echo ""
echo "Tests completed!"
