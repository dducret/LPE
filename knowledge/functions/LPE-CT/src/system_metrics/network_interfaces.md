---
type: Rust Function
title: network_interfaces
resource: LPE-CT/src/system_metrics.rs#L215-L234
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/default_gateways
  - functions/LPE-CT/src/system_metrics/parse_ipv4_interface_line
---

# Signature

`fn network_interfaces() -> Vec<NetworkInterfaceMetric>`

# Calls

- [default_gateways](../../../../functions/LPE-CT/src/system_metrics/default_gateways.md)
- [parse_ipv4_interface_line](../../../../functions/LPE-CT/src/system_metrics/parse_ipv4_interface_line.md)