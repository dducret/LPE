---
type: Rust Function
title: dns_servers
resource: LPE-CT/src/system_metrics.rs#L242-L250
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/read_dns_servers_from_resolv_conf
  - functions/LPE-CT/src/system_metrics/read_dns_servers_from_resolvectl
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn dns_servers() -> Vec<String>`

# Calls

- [read_dns_servers_from_resolv_conf](../../../../functions/LPE-CT/src/system_metrics/read_dns_servers_from_resolv_conf.md)
- [read_dns_servers_from_resolvectl](../../../../functions/LPE-CT/src/system_metrics/read_dns_servers_from_resolvectl.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)