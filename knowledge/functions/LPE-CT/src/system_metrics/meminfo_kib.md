---
type: Rust Function
title: meminfo_kib
resource: LPE-CT/src/system_metrics.rs#L161-L168
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/key_value_file
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/LPE-CT/src/system_metrics/memory_total_bytes
  - functions/LPE-CT/src/system_metrics/memory_used_percent
---

# Signature

`fn meminfo_kib(name: &str) -> Option<u64>`

# Calls

- [key_value_file](../../../../functions/LPE-CT/src/system_metrics/key_value_file.md)
- [next](../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [memory_total_bytes](../../../../functions/LPE-CT/src/system_metrics/memory_total_bytes.md)
- [memory_used_percent](../../../../functions/LPE-CT/src/system_metrics/memory_used_percent.md)