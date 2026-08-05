---
type: Rust Function
title: read_dns_servers_from_resolvectl
resource: LPE-CT/src/system_metrics.rs#L277-L291
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/system_metrics/command_stdout
  called_by:
  - functions/LPE-CT/src/system_metrics/dns_servers
---

# Signature

`fn read_dns_servers_from_resolvectl() -> Vec<String>`

# Calls

- [command_stdout](../../../../functions/LPE-CT/src/system_metrics/command_stdout.md)

# Called by

- [dns_servers](../../../../functions/LPE-CT/src/system_metrics/dns_servers.md)