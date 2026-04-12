# Test Suite

## Overview

This test suite verifies that the Rust market making system works correctly and matches Hummingbot's behavior.

## Test Files

### `integration_test.rs`
Core functionality tests:
- Order simulator operations
- Portfolio tracking
- Fill simulation

### `simple_backtest.rs`
End-to-end backtest tests:
- Strategy creation
- Backtest execution
- Results validation

## Running Tests

```bash
# All tests
cargo test

# Specific test file
cargo test --test integration_test

# With output
cargo test -- --nocapture
```

## Test Coverage

- ✅ Order book simulator
- ✅ Queue position tracking
- ✅ Portfolio P&L calculation
- ✅ Fill simulation
- ✅ Strategy interface
- ⏳ CSV data loading (requires data file)
- ⏳ Full backtest run (requires data file)
