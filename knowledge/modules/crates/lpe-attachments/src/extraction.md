---
type: Rust Module
title: extraction
resource: crates/lpe-attachments/src/extraction.rs#L1-L246
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-context-result
  - external/lpe-magika-expectedkind-ingresscontext-policydecision-validationrequest-validator
  - external/quick-xml-events-event
  - external/quick-xml-reader
  - external/std-fs-self
  - external/std-io-read
  - external/std-path-path
  - external/std-time-systemtime-unix-epoch
  - external/zip-ziparchive
  - external/super-attachmentformat
  member_of:
  - packages/crates/lpe-attachments
---

# Contains

- [AttachmentFormat](../../../../classes/crates/lpe-attachments/src/extraction/AttachmentFormat.md)
- [from_detected_mime](../../../../functions/crates/lpe-attachments/src/extraction/AttachmentFormat/from_detected_mime.md)
- [extract_text_from_path](../../../../functions/crates/lpe-attachments/src/extraction/extract_text_from_path.md)
- [extract_text_from_bytes](../../../../functions/crates/lpe-attachments/src/extraction/extract_text_from_bytes.md)
- [extract_pdf_text](../../../../functions/crates/lpe-attachments/src/extraction/extract_pdf_text.md)
- [extract_pdf_text_from_bytes](../../../../functions/crates/lpe-attachments/src/extraction/extract_pdf_text_from_bytes.md)
- [extract_docx_text](../../../../functions/crates/lpe-attachments/src/extraction/extract_docx_text.md)
- [extract_docx_text_from_bytes](../../../../functions/crates/lpe-attachments/src/extraction/extract_docx_text_from_bytes.md)
- [extract_odt_text](../../../../functions/crates/lpe-attachments/src/extraction/extract_odt_text.md)
- [extract_odt_text_from_bytes](../../../../functions/crates/lpe-attachments/src/extraction/extract_odt_text_from_bytes.md)
- [extract_odt_content_xml](../../../../functions/crates/lpe-attachments/src/extraction/extract_odt_content_xml.md)
- [local_name](../../../../functions/crates/lpe-attachments/src/extraction/local_name.md)
- [append_text](../../../../functions/crates/lpe-attachments/src/extraction/append_text.md)
- [ensure_paragraph_break](../../../../functions/crates/lpe-attachments/src/extraction/ensure_paragraph_break.md)
- [normalize_whitespace](../../../../functions/crates/lpe-attachments/src/extraction/normalize_whitespace.md)
- [validated_attachment_format_matches_supported_v1_scope](../../../../functions/crates/lpe-attachments/src/extraction/validated_attachment_format_matches_supported_v1_scope.md)

# Imports

- `anyhow::{anyhow, bail, Context, Result}`
- `lpe_magika::{ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator}`
- `quick_xml::events::Event`
- `quick_xml::Reader`
- `std::fs::{self}`
- `std::io::Read`
- `std::path::Path`
- `std::time::{SystemTime, UNIX_EPOCH}`
- `zip::ZipArchive`
- `super::AttachmentFormat`

# Member of

- [lpe-attachments](../../../../packages/crates/lpe-attachments.md)