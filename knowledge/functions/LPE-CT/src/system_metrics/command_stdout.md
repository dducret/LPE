---
type: Rust Function
title: command_stdout
resource: LPE-CT/src/system_metrics.rs#L412-L424
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/system_metrics/read_dns_servers_from_resolvectl
  - functions/LPE-CT/src/system_metrics/ip_route_lines
  - functions/LPE-CT/src/system_metrics/ipv6_addresses
  - functions/LPE-CT/src/system_metrics/ntp_metric
---

# Signature

`fn command_stdout(program: &str, args: &[&str]) -> Option<String>`

# Called by

- [read_dns_servers_from_resolvectl](../../../../functions/LPE-CT/src/system_metrics/read_dns_servers_from_resolvectl.md)
- [ip_route_lines](../../../../functions/LPE-CT/src/system_metrics/ip_route_lines.md)
- [ipv6_addresses](../../../../functions/LPE-CT/src/system_metrics/ipv6_addresses.md)
- [ntp_metric](../../../../functions/LPE-CT/src/system_metrics/ntp_metric.md)