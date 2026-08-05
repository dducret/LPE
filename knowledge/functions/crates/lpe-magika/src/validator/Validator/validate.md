---
type: Rust Method
title: validate
resource: crates/lpe-magika/src/validator.rs#L38-L45
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/validator/decide_policy
---

# Signature

`pub fn validate( &self, request: ValidationRequest, source: DetectionSource<'_>, ) -> Result<ValidationOutcome>`

# Calls

- [decide_policy](../../../../../../functions/crates/lpe-magika/src/validator/decide_policy.md)