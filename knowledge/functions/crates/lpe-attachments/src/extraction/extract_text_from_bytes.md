---
type: Rust Function
title: extract_text_from_bytes
resource: crates/lpe-attachments/src/extraction.rs#L59-L85
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-attachments/src/extraction/AttachmentFormat/from_detected_mime
  - functions/crates/lpe-attachments/src/extraction/extract_pdf_text_from_bytes
  - functions/crates/lpe-attachments/src/extraction/extract_docx_text_from_bytes
  - functions/crates/lpe-attachments/src/extraction/extract_odt_text_from_bytes
  called_by:
  - functions/crates/lpe-attachments/src/extraction/extract_docx_text_from_bytes
---

# Signature

`pub fn extract_text_from_bytes( bytes: &[u8], declared_mime: Option<&str>, filename: Option<&str>, ) -> Result<String>`

# Calls

- [from_detected_mime](../../../../../functions/crates/lpe-attachments/src/extraction/AttachmentFormat/from_detected_mime.md)
- [extract_pdf_text_from_bytes](../../../../../functions/crates/lpe-attachments/src/extraction/extract_pdf_text_from_bytes.md)
- [extract_docx_text_from_bytes](../../../../../functions/crates/lpe-attachments/src/extraction/extract_docx_text_from_bytes.md)
- [extract_odt_text_from_bytes](../../../../../functions/crates/lpe-attachments/src/extraction/extract_odt_text_from_bytes.md)

# Called by

- [extract_docx_text_from_bytes](../../../../../functions/crates/lpe-attachments/src/extraction/extract_docx_text_from_bytes.md)