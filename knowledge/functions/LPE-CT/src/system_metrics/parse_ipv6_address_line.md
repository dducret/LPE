---
type: Rust Function
title: parse_ipv6_address_line
resource: LPE-CT/src/system_metrics.rs#L325-L340
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
---

# Signature

`fn parse_ipv6_address_line(line: &str) -> Option<NetworkAddressMetric>`

# Calls

- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [position](../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)