---
type: Rust Function
title: strip_content_type_parameters
resource: crates/lpe-magika/src/validator.rs#L230-L237
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-magika/src/validator/normalize_mime
---

# Signature

`fn strip_content_type_parameters(value: &str) -> String`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [normalize_mime](../../../../../functions/crates/lpe-magika/src/validator/normalize_mime.md)