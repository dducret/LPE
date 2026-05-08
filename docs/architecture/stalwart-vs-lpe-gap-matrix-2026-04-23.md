# Benchmark Gap Focus

## Current State/Functionality Overview

Benchmarking must measure `LPE` against concrete operational and protocol outcomes. Benchmark results do not override the architecture and licensing constraints in `LICENSE.md` and `AGENTS.md`.

## Implementation/Usage

- Measure protocol correctness before adding new protocol families.
- Measure canonical-state consistency across adapters.
- Measure edge-mail custody, quarantine, retry, bounce, and `DSN` behavior in `LPE-CT`.
- Measure administrator workflows for mail flow, quarantine, routing, and diagnostics.
- Measure restore and node-replacement behavior separately for `LPE` and `LPE-CT`.
- Measure performance with reproducible scenarios.
- Treat comparison systems as benchmarks only.

## Reference Table/List

| Metric | Requirement |
| --- | --- |
| submission latency | canonical `Sent` before relay |
| inbound delivery latency | `LPE-CT -> LPE` final delivery |
| queue recovery | no duplicate or regressed terminal state |
| JMAP push recovery | reconnect from canonical state |
| IMAP refresh behavior | stable UID and flag behavior |
| quarantine operation | operator can identify, inspect, release, or reject |
| restore time | core and `LPE-CT` measured separately |
| accepted-message custody | no loss across `LPE-CT` restart or replacement |
