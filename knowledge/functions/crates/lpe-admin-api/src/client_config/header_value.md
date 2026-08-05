---
type: Rust Function
title: header_value
resource: crates/lpe-admin-api/src/client_config.rs#L949-L956
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
---

# Signature

`fn header_value(headers: &HeaderMap, name: &str) -> Option<String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)