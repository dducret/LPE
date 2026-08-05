---
type: Rust Function
title: uptime_seconds
resource: LPE-CT/src/system_metrics.rs#L92-L97
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/read_trimmed
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/LPE-CT/src/system_metrics/collect
---

# Signature

`fn uptime_seconds() -> Option<u64>`

# Calls

- [read_trimmed](../../../../functions/LPE-CT/src/system_metrics/read_trimmed.md)
- [next](../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [collect](../../../../functions/LPE-CT/src/system_metrics/collect.md)