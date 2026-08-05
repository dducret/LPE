---
type: Rust Function
title: mime_matches
resource: crates/lpe-magika/src/validator.rs#L204-L224
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/validator/normalize_mime
  called_by:
  - functions/crates/lpe-magika/src/validator/decide_policy
  - functions/crates/lpe-magika/src/validator/matches_expected_kind
---

# Signature

`fn mime_matches(left: &str, right: &str) -> bool`

# Calls

- [normalize_mime](../../../../../functions/crates/lpe-magika/src/validator/normalize_mime.md)

# Called by

- [decide_policy](../../../../../functions/crates/lpe-magika/src/validator/decide_policy.md)
- [matches_expected_kind](../../../../../functions/crates/lpe-magika/src/validator/matches_expected_kind.md)