---
type: Rust Function
title: configured_ntp_servers
resource: LPE-CT/src/system_metrics.rs#L388-L409
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/LPE-CT/src/system_metrics/split_words
  called_by:
  - functions/LPE-CT/src/system_metrics/ntp_metric
---

# Signature

`fn configured_ntp_servers() -> Vec<String>`

# Calls

- [next](../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [split_words](../../../../functions/LPE-CT/src/system_metrics/split_words.md)

# Called by

- [ntp_metric](../../../../functions/LPE-CT/src/system_metrics/ntp_metric.md)