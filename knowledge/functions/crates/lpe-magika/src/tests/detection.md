---
type: Rust Function
title: detection
resource: crates/lpe-magika/src/tests.rs#L15-L24
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-magika/src/tests/supported_attachment_kind_is_accepted
  - functions/crates/lpe-magika/src/tests/smtp_mismatch_is_rejected
---

# Signature

`fn detection(mime_type: &str, extension: &str, score: f32) -> MagikaDetection`

# Called by

- [supported_attachment_kind_is_accepted](../../../../../functions/crates/lpe-magika/src/tests/supported_attachment_kind_is_accepted.md)
- [smtp_mismatch_is_rejected](../../../../../functions/crates/lpe-magika/src/tests/smtp_mismatch_is_rejected.md)