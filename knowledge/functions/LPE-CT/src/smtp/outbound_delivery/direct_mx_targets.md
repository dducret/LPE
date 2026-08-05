---
type: Rust Function
title: direct_mx_targets
resource: LPE-CT/src/smtp/outbound_delivery.rs#L345-L367
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/dns/SystemDnsResolver/dnsresolver/query_mx
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx
---

# Signature

`async fn direct_mx_targets(resolver: &SystemDnsResolver, domain: &str) -> Result<Vec<String>>`

# Calls

- [query_mx](../../../../../functions/LPE-CT/src/smtp/dns/SystemDnsResolver/dnsresolver/query_mx.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [relay_message_direct_mx](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx.md)