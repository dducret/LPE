---
type: Rust Function
title: ipv4_from_proc_hex
resource: LPE-CT/src/system_diagnostics.rs#L199-L203
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/system_diagnostics/format_proc_ipv4_route
---

# Signature

`fn ipv4_from_proc_hex(value: &str) -> Option<Ipv4Addr>`

# Called by

- [format_proc_ipv4_route](../../../../functions/LPE-CT/src/system_diagnostics/format_proc_ipv4_route.md)