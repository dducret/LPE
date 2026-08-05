---
type: Rust Method
title: query_exists
resource: LPE-CT/src/smtp/dns.rs#L68-L71
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/LPE-CT/src/smtp/anti_abuse/query_dnsbl
---

# Signature

`async fn query_exists(&self, name: &str) -> Result<bool, DnsError>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [query_dnsbl](../../../../../../../functions/LPE-CT/src/smtp/anti_abuse/query_dnsbl.md)