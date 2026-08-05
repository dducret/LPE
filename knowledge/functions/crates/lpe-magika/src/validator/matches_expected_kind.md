---
type: Rust Function
title: matches_expected_kind
resource: crates/lpe-magika/src/validator.rs#L176-L202
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/validator/mime_matches
  - functions/crates/lpe-magika/src/validator/normalize_mime
  called_by:
  - functions/crates/lpe-magika/src/validator/decide_policy
---

# Signature

`fn matches_expected_kind(expected_kind: ExpectedKind, detection: &MagikaDetection) -> bool`

# Calls

- [mime_matches](../../../../../functions/crates/lpe-magika/src/validator/mime_matches.md)
- [normalize_mime](../../../../../functions/crates/lpe-magika/src/validator/normalize_mime.md)

# Called by

- [decide_policy](../../../../../functions/crates/lpe-magika/src/validator/decide_policy.md)