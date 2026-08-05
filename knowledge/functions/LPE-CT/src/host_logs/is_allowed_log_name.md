---
type: Rust Function
title: is_allowed_log_name
resource: LPE-CT/src/host_logs.rs#L272-L277
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/host_logs/resolve_log
---

# Signature

`fn is_allowed_log_name(current_name: &str, name: &str) -> bool`

# Called by

- [resolve_log](../../../../functions/LPE-CT/src/host_logs/resolve_log.md)