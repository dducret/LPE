---
type: Rust Function
title: extract_pdf_text_from_bytes
resource: crates/lpe-attachments/src/extraction.rs#L105-L116
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-attachments/src/extraction/extract_pdf_text
  called_by:
  - functions/crates/lpe-attachments/src/extraction/extract_text_from_bytes
---

# Signature

`fn extract_pdf_text_from_bytes(bytes: &[u8]) -> Result<String>`

# Calls

- [extract_pdf_text](../../../../../functions/crates/lpe-attachments/src/extraction/extract_pdf_text.md)

# Called by

- [extract_text_from_bytes](../../../../../functions/crates/lpe-attachments/src/extraction/extract_text_from_bytes.md)