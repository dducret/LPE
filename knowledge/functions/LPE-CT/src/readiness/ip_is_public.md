---
type: Rust Function
title: ip_is_public
resource: LPE-CT/src/readiness.rs#L249-L254
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/readiness/address_binds_publicly
---

# Signature

`fn ip_is_public(ip: std::net::IpAddr) -> bool`

# Called by

- [address_binds_publicly](../../../../functions/LPE-CT/src/readiness/address_binds_publicly.md)