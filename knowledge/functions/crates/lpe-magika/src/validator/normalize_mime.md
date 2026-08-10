---
type: Rust Function
title: normalize_mime
resource: crates/lpe-magika/src/validator.rs#L226-L228
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/validator/strip_content_type_parameters
  called_by:
  - functions/crates/lpe-magika/src/validator/decide_policy
  - functions/crates/lpe-magika/src/validator/matches_expected_kind
  - functions/crates/lpe-magika/src/validator/mime_matches
---

# Signature

`fn normalize_mime(value: &str) -> String`

# Calls

- [strip_content_type_parameters](../../../../../functions/crates/lpe-magika/src/validator/strip_content_type_parameters.md)

# Called by

- [decide_policy](../../../../../functions/crates/lpe-magika/src/validator/decide_policy.md)
- [matches_expected_kind](../../../../../functions/crates/lpe-magika/src/validator/matches_expected_kind.md)
- [mime_matches](../../../../../functions/crates/lpe-magika/src/validator/mime_matches.md)