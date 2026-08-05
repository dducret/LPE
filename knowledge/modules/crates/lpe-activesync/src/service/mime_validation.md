---
type: Rust Module
title: mime_validation
resource: crates/lpe-activesync/src/service/mime_validation.rs#L1-L85
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/lpe-magika-collect-mime-attachment-parts-detector-expectedkind-ingresscontext-policydecision-validationrequest-validator
  - external/super-validate-mime-attachments-with-validator
  - external/lpe-magika-detectionsource-detector-magikadetection-validator
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [validate_mime_attachments](../../../../../functions/crates/lpe-activesync/src/service/mime_validation/validate_mime_attachments.md)
- [validate_mime_attachments_with_validator](../../../../../functions/crates/lpe-activesync/src/service/mime_validation/validate_mime_attachments_with_validator.md)
- [FakeDetector](../../../../../classes/crates/lpe-activesync/src/service/mime_validation/FakeDetector.md)
- [detect](../../../../../functions/crates/lpe-activesync/src/service/mime_validation/FakeDetector/detector/detect.md)
- [activesync_sendmail_blocks_mismatched_attachment_payloads](../../../../../functions/crates/lpe-activesync/src/service/mime_validation/activesync_sendmail_blocks_mismatched_attachment_payloads.md)

# Imports

- `anyhow::{bail, Result}`
- `lpe_magika::{
    collect_mime_attachment_parts, Detector, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
}`
- `super::validate_mime_attachments_with_validator`
- `lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator}`

# Member of

- [lpe-activesync](../../../../../packages/crates/lpe-activesync.md)