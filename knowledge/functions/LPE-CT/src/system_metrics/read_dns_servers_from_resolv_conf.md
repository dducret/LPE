---
type: Rust Function
title: read_dns_servers_from_resolv_conf
resource: LPE-CT/src/system_metrics.rs#L258-L274
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/LPE-CT/src/system_metrics/dns_servers
---

# Signature

`fn read_dns_servers_from_resolv_conf() -> Vec<String>`

# Calls

- [next](../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [dns_servers](../../../../functions/LPE-CT/src/system_metrics/dns_servers.md)