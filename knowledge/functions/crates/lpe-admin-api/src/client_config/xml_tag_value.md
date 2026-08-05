---
type: Rust Function
title: xml_tag_value
resource: crates/lpe-admin-api/src/client_config.rs#L870-L921
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/parse_autodiscover_email
---

# Signature

`fn xml_tag_value(body: &str, tag: &str) -> Option<String>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [parse_autodiscover_email](../../../../../functions/crates/lpe-admin-api/src/client_config/parse_autodiscover_email.md)