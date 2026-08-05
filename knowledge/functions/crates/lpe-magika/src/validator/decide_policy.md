---
type: Rust Function
title: decide_policy
resource: crates/lpe-magika/src/validator.rs#L64-L174
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/validator/normalize_mime
  - functions/crates/lpe-magika/src/validator/mime_matches
  - functions/crates/lpe-magika/src/validator/matches_expected_kind
  called_by:
  - functions/crates/lpe-magika/src/validator/Validator/validate
---

# Signature

`fn decide_policy( request: &ValidationRequest, detection: &MagikaDetection, min_score: f32, ) -> ValidationOutcome`

# Calls

- [normalize_mime](../../../../../functions/crates/lpe-magika/src/validator/normalize_mime.md)
- [mime_matches](../../../../../functions/crates/lpe-magika/src/validator/mime_matches.md)
- [matches_expected_kind](../../../../../functions/crates/lpe-magika/src/validator/matches_expected_kind.md)

# Called by

- [validate](../../../../../functions/crates/lpe-magika/src/validator/Validator/validate.md)