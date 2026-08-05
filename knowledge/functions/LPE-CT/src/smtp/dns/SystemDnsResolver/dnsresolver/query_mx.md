---
type: Rust Method
title: query_mx
resource: LPE-CT/src/smtp/dns.rs#L48-L57
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/direct_mx_targets
---

# Signature

`async fn query_mx(&self, name: &str) -> Result<Vec<MxRecord>, DnsError>`

# Called by

- [direct_mx_targets](../../../../../../../functions/LPE-CT/src/smtp/outbound_delivery/direct_mx_targets.md)