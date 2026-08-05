---
type: Rust Function
title: forwarded_header
resource: crates/lpe-admin-api/src/http.rs#L32-L40
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-admin-api/src/http/public_origin
---

# Signature

`pub(crate) fn forwarded_header(headers: &HeaderMap, name: &str) -> Option<String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [public_origin](../../../../../functions/crates/lpe-admin-api/src/http/public_origin.md)