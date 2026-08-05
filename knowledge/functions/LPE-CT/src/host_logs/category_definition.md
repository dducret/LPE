---
type: Rust Function
title: category_definition
resource: LPE-CT/src/host_logs.rs#L166-L185
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/host_logs/list
  - functions/LPE-CT/src/host_logs/resolve_log
---

# Signature

`fn category_definition(category: &str) -> Result<LogCategory, HostLogError>`

# Called by

- [list](../../../../functions/LPE-CT/src/host_logs/list.md)
- [resolve_log](../../../../functions/LPE-CT/src/host_logs/resolve_log.md)