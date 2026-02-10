# Phase 3: Pipelined Execution Optimizations

This document describes the Phase 3 optimizations applied to the store layer for
the perpx-chain protocol's Block-STM pipeline.

## Summary

- **Optimistic Execution (OE)**: Already implemented in baseapp. When
  `ProcessProposal` accepts a block, OE starts `FinalizeBlock` in a goroutine so
  the result is ready when CometBFT actually calls `FinalizeBlock`. Saves ~200ms
  latency when the proposal is accepted.

- **Parallel Store Commit**: `commitStores` in rootmulti now commits each IAVL
  store in parallel via goroutines. Each store's `Commit()` is independent, so
  total commit latency is reduced when many stores are mounted.

- **Parallel WorkingHash**: `WorkingHash` in rootmulti now computes each store's
  hash in parallel. This reduces the time to compute the app hash before commit,
  which is used by `FinalizeBlock` to return `AppHash` to CometBFT.

## Protocol Integration

To use the Phase 3 store optimizations, update the protocol's go.mod replace:

```go
cosmossdk.io/store => github.com/1119-Labs/cosmos-sdk/store v0.0.0-YYYYMMDDHHMMSS-<commit>
```

Use the commit hash from the `feature/phase3-pipelined-execution` branch.

## Compatibility

- Backward compatible: no API changes
- Deterministic: results are identical to sequential execution
- Thread-safe: each store is committed/hashed independently
