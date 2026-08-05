---
type: Rust Module
title: validator
resource: crates/lpe-magika/src/validator.rs#L1-L244
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/std-path-path
  - external/crate-constants-unknown-labels-system-systemdetector-types-detectionsource-detector-expectedkind-ingresscontext-magikadetection-policydecision-validationoutcome-validationrequest
  member_of:
  - packages/crates/lpe-magika
---

# Contains

- [Validator](../../../../classes/crates/lpe-magika/src/validator/Validator.md)
- [from_env](../../../../functions/crates/lpe-magika/src/validator/Validator/from_env.md)
- [new](../../../../functions/crates/lpe-magika/src/validator/Validator/new.md)
- [validate](../../../../functions/crates/lpe-magika/src/validator/Validator/validate.md)
- [validate_path](../../../../functions/crates/lpe-magika/src/validator/Validator/validate_path.md)
- [validate_bytes](../../../../functions/crates/lpe-magika/src/validator/Validator/validate_bytes.md)
- [decide_policy](../../../../functions/crates/lpe-magika/src/validator/decide_policy.md)
- [matches_expected_kind](../../../../functions/crates/lpe-magika/src/validator/matches_expected_kind.md)
- [mime_matches](../../../../functions/crates/lpe-magika/src/validator/mime_matches.md)
- [normalize_mime](../../../../functions/crates/lpe-magika/src/validator/normalize_mime.md)
- [strip_content_type_parameters](../../../../functions/crates/lpe-magika/src/validator/strip_content_type_parameters.md)
- [file_extension](../../../../functions/crates/lpe-magika/src/validator/file_extension.md)

# Imports

- `anyhow::Result`
- `std::path::Path`
- `crate::{
    constants::UNKNOWN_LABELS,
    system::SystemDetector,
    types::{
        DetectionSource, Detector, ExpectedKind, IngressContext, MagikaDetection, PolicyDecision,
        ValidationOutcome, ValidationRequest,
    },
}`

# Member of

- [lpe-magika](../../../../packages/crates/lpe-magika.md)