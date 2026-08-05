---
type: Rust Method
title: from_detected_mime
resource: crates/lpe-attachments/src/extraction.rs#L19-L28
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-attachments/src/extraction/extract_text_from_path
  - functions/crates/lpe-attachments/src/extraction/extract_text_from_bytes
---

# Signature

`fn from_detected_mime(mime_type: &str) -> Result<Self>`

# Called by

- [extract_text_from_path](../../../../../../functions/crates/lpe-attachments/src/extraction/extract_text_from_path.md)
- [extract_text_from_bytes](../../../../../../functions/crates/lpe-attachments/src/extraction/extract_text_from_bytes.md)