---
type: Rust Function
title: normalize_whitespace
resource: crates/lpe-attachments/src/extraction.rs#L213-L220
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-attachments/src/extraction/extract_pdf_text
  - functions/crates/lpe-attachments/src/extraction/extract_docx_text_from_bytes
  - functions/crates/lpe-attachments/src/extraction/extract_odt_content_xml
---

# Signature

`fn normalize_whitespace(input: &str) -> String`

# Called by

- [extract_pdf_text](../../../../../functions/crates/lpe-attachments/src/extraction/extract_pdf_text.md)
- [extract_docx_text_from_bytes](../../../../../functions/crates/lpe-attachments/src/extraction/extract_docx_text_from_bytes.md)
- [extract_odt_content_xml](../../../../../functions/crates/lpe-attachments/src/extraction/extract_odt_content_xml.md)