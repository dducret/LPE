---
type: Rust Function
title: extract_text_from_path
resource: crates/lpe-attachments/src/extraction.rs#L31-L57
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/validator/Validator/validate_path
  - functions/crates/lpe-attachments/src/extraction/AttachmentFormat/from_detected_mime
  - functions/crates/lpe-attachments/src/extraction/extract_pdf_text
  - functions/crates/lpe-attachments/src/extraction/extract_docx_text
  - functions/crates/lpe-attachments/src/extraction/extract_odt_text
---

# Signature

`pub fn extract_text_from_path(path: impl AsRef<Path>) -> Result<String>`

# Calls

- [validate_path](../../../../../functions/crates/lpe-magika/src/validator/Validator/validate_path.md)
- [from_detected_mime](../../../../../functions/crates/lpe-attachments/src/extraction/AttachmentFormat/from_detected_mime.md)
- [extract_pdf_text](../../../../../functions/crates/lpe-attachments/src/extraction/extract_pdf_text.md)
- [extract_docx_text](../../../../../functions/crates/lpe-attachments/src/extraction/extract_docx_text.md)
- [extract_odt_text](../../../../../functions/crates/lpe-attachments/src/extraction/extract_odt_text.md)