---
type: Rust Module
title: dns
resource: LPE-CT/src/smtp/dns.rs#L1-L83
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-context-result
  - external/email-auth-common-dns-dnserror-dnsresolver-mxrecord
  - external/hickory-resolver-proto-rr-recordtype-tokioresolver
  - external/std-net-ipaddr-ipv4addr-ipv6addr
  member_of:
  - packages/LPE-CT
---

# Contains

- [SystemDnsResolver](../../../../classes/LPE-CT/src/smtp/dns/SystemDnsResolver.md)
- [new](../../../../functions/LPE-CT/src/smtp/dns/SystemDnsResolver/new.md)
- [query_txt](../../../../functions/LPE-CT/src/smtp/dns/SystemDnsResolver/dnsresolver/query_txt.md)
- [query_a](../../../../functions/LPE-CT/src/smtp/dns/SystemDnsResolver/dnsresolver/query_a.md)
- [query_aaaa](../../../../functions/LPE-CT/src/smtp/dns/SystemDnsResolver/dnsresolver/query_aaaa.md)
- [query_mx](../../../../functions/LPE-CT/src/smtp/dns/SystemDnsResolver/dnsresolver/query_mx.md)
- [query_ptr](../../../../functions/LPE-CT/src/smtp/dns/SystemDnsResolver/dnsresolver/query_ptr.md)
- [query_exists](../../../../functions/LPE-CT/src/smtp/dns/SystemDnsResolver/dnsresolver/query_exists.md)
- [map_dns_error](../../../../functions/LPE-CT/src/smtp/dns/map_dns_error.md)

# Imports

- `anyhow::{Context, Result}`
- `email_auth::common::dns::{DnsError, DnsResolver, MxRecord}`
- `hickory_resolver::{proto::rr::RecordType, TokioResolver}`
- `std::net::{IpAddr, Ipv4Addr, Ipv6Addr}`

# Member of

- [lpe-ct](../../../../packages/LPE-CT.md)