---
type: Rust Function
title: query_dnsbl
resource: LPE-CT/src/smtp/anti_abuse.rs#L16-L36
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/anti_abuse/dnsbl_query_name
  - functions/LPE-CT/src/smtp/dns/SystemDnsResolver/dnsresolver/query_exists
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy
---

# Signature

`pub(in crate::smtp) async fn query_dnsbl(ip: IpAddr, zones: &[String]) -> DnsblOutcome`

# Calls

- [dnsbl_query_name](../../../../../functions/LPE-CT/src/smtp/anti_abuse/dnsbl_query_name.md)
- [query_exists](../../../../../functions/LPE-CT/src/smtp/dns/SystemDnsResolver/dnsresolver/query_exists.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [evaluate_inbound_policy](../../../../../functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy.md)