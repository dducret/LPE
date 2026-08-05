---
type: Rust Function
title: dnsbl_query_name
resource: LPE-CT/src/smtp/anti_abuse.rs#L38-L61
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/anti_abuse/query_dnsbl
---

# Signature

`pub(in crate::smtp) fn dnsbl_query_name(ip: IpAddr, zone: &str) -> String`

# Called by

- [query_dnsbl](../../../../../functions/LPE-CT/src/smtp/anti_abuse/query_dnsbl.md)