---
type: Rust Function
title: ntp_metric
resource: LPE-CT/src/system_metrics.rs#L343-L375
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/command_stdout
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/LPE-CT/src/system_metrics/configured_ntp_servers
---

# Signature

`fn ntp_metric() -> NtpMetric`

# Calls

- [command_stdout](../../../../functions/LPE-CT/src/system_metrics/command_stdout.md)
- [next](../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [configured_ntp_servers](../../../../functions/LPE-CT/src/system_metrics/configured_ntp_servers.md)