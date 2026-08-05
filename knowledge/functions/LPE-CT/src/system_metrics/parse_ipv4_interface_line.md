---
type: Rust Function
title: parse_ipv4_interface_line
resource: LPE-CT/src/system_metrics.rs#L457-L477
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  called_by:
  - functions/LPE-CT/src/system_metrics/network_interfaces
---

# Signature

`fn parse_ipv4_interface_line( line: &str, gateways: &std::collections::BTreeMap<String, String>, ) -> Option<NetworkInterfaceMetric>`

# Calls

- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [position](../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)

# Called by

- [network_interfaces](../../../../functions/LPE-CT/src/system_metrics/network_interfaces.md)