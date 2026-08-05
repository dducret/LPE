---
type: Rust Module
title: tests
resource: crates/lpe-magika/src/tests.rs#L1-L198
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/anyhow-result
  member_of:
  - packages/crates/lpe-magika
---

# Contains

- [FakeDetector](../../../../classes/crates/lpe-magika/src/tests/FakeDetector.md)
- [detect](../../../../functions/crates/lpe-magika/src/tests/FakeDetector/detector/detect.md)
- [detection](../../../../functions/crates/lpe-magika/src/tests/detection.md)
- [supported_attachment_kind_is_accepted](../../../../functions/crates/lpe-magika/src/tests/supported_attachment_kind_is_accepted.md)
- [smtp_mismatch_is_rejected](../../../../functions/crates/lpe-magika/src/tests/smtp_mismatch_is_rejected.md)
- [smtp_client_submission_unknown_file_is_restricted](../../../../functions/crates/lpe-magika/src/tests/smtp_client_submission_unknown_file_is_restricted.md)
- [collect_mime_attachment_parts_extracts_attachment_payloads](../../../../functions/crates/lpe-magika/src/tests/collect_mime_attachment_parts_extracts_attachment_payloads.md)
- [extract_visible_text_prefers_plaintext_from_multipart_alternative](../../../../functions/crates/lpe-magika/src/tests/extract_visible_text_prefers_plaintext_from_multipart_alternative.md)
- [extract_visible_text_decodes_quoted_printable_iso_8859_1](../../../../functions/crates/lpe-magika/src/tests/extract_visible_text_decodes_quoted_printable_iso_8859_1.md)
- [extract_visible_text_uses_html_when_plaintext_is_missing](../../../../functions/crates/lpe-magika/src/tests/extract_visible_text_uses_html_when_plaintext_is_missing.md)
- [collect_mime_attachment_parts_handles_non_utf8_body_bytes](../../../../functions/crates/lpe-magika/src/tests/collect_mime_attachment_parts_handles_non_utf8_body_bytes.md)

# Imports

- `super::*`
- `anyhow::Result`

# Member of

- [lpe-magika](../../../../packages/crates/lpe-magika.md)