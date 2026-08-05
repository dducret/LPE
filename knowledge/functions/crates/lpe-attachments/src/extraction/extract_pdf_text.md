---
type: Rust Function
title: extract_pdf_text
resource: crates/lpe-attachments/src/extraction.rs#L87-L103
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/test_rca_outlook_trace_summary/FakePath/open
  - functions/crates/lpe-attachments/src/extraction/normalize_whitespace
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-attachments/src/extraction/extract_text_from_path
  - functions/crates/lpe-attachments/src/extraction/extract_pdf_text_from_bytes
---

# Signature

`fn extract_pdf_text(path: &Path) -> Result<String>`

# Calls

- [open](../../../../../functions/tools/test_rca_outlook_trace_summary/FakePath/open.md)
- [normalize_whitespace](../../../../../functions/crates/lpe-attachments/src/extraction/normalize_whitespace.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [extract_text_from_path](../../../../../functions/crates/lpe-attachments/src/extraction/extract_text_from_path.md)
- [extract_pdf_text_from_bytes](../../../../../functions/crates/lpe-attachments/src/extraction/extract_pdf_text_from_bytes.md)