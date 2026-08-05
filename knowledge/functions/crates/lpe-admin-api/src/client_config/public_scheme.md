---
type: Rust Function
title: public_scheme
resource: crates/lpe-admin-api/src/client_config.rs#L933-L947
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/from_headers
---

# Signature

`fn public_scheme(headers: &HeaderMap) -> String`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [from_headers](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/from_headers.md)