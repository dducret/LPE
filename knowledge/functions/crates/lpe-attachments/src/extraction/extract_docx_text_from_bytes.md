---
type: Rust Function
title: extract_docx_text_from_bytes
resource: crates/lpe-attachments/src/extraction.rs#L124-L127
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-attachments/src/extraction/extract_text_from_bytes
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-attachments/src/extraction/normalize_whitespace
  called_by:
  - functions/crates/lpe-attachments/src/extraction/extract_text_from_bytes
  - functions/crates/lpe-attachments/src/extraction/extract_docx_text
---

# Signature

`fn extract_docx_text_from_bytes(bytes: &[u8]) -> Result<String>`

# Calls

- [extract_text_from_bytes](../../../../../functions/crates/lpe-attachments/src/extraction/extract_text_from_bytes.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [normalize_whitespace](../../../../../functions/crates/lpe-attachments/src/extraction/normalize_whitespace.md)

# Called by

- [extract_text_from_bytes](../../../../../functions/crates/lpe-attachments/src/extraction/extract_text_from_bytes.md)
- [extract_docx_text](../../../../../functions/crates/lpe-attachments/src/extraction/extract_docx_text.md)