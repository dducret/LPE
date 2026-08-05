---
type: Rust Module
title: src
resource: crates/lpe-magika/src/lib.rs#L1-L23
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/pub-use-crate-mime-collect-mime-attachment-parts-extract-visible-body-parts-extract-visible-text-parse-rfc822-header-value
  - external/pub-use-crate-record-read-validation-record-validation-sidecar-path-write-validation-record
  - external/pub-use-crate-system-systemdetector
  - external/pub-use-crate-types-detectionsource-detector-expectedkind-ingresscontext-magikadetection-mimeattachmentpart-persistedvalidationrecord-policydecision-validationoutcome-validationrequest-visiblebodyparts
  - external/pub-use-crate-validator-validator
  member_of:
  - packages/crates/lpe-magika
---

# Imports

- `pub use crate::mime::{
    collect_mime_attachment_parts, extract_visible_body_parts, extract_visible_text,
    parse_rfc822_header_value,
}`
- `pub use crate::record::{read_validation_record, validation_sidecar_path, write_validation_record}`
- `pub use crate::system::SystemDetector`
- `pub use crate::types::{
    DetectionSource, Detector, ExpectedKind, IngressContext, MagikaDetection, MimeAttachmentPart,
    PersistedValidationRecord, PolicyDecision, ValidationOutcome, ValidationRequest,
    VisibleBodyParts,
}`
- `pub use crate::validator::Validator`

# Member of

- [lpe-magika](../../../packages/crates/lpe-magika.md)