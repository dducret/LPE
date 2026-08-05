---
type: Rust Function
title: extract_odt_content_xml
resource: crates/lpe-attachments/src/extraction.rs#L149-L180
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-attachments/src/extraction/append_text
  - functions/crates/lpe-attachments/src/extraction/ensure_paragraph_break
  - functions/crates/lpe-attachments/src/extraction/normalize_whitespace
  called_by:
  - functions/crates/lpe-attachments/src/extraction/extract_odt_text_from_bytes
---

# Signature

`fn extract_odt_content_xml(xml: &str) -> Result<String>`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [append_text](../../../../../functions/crates/lpe-attachments/src/extraction/append_text.md)
- [ensure_paragraph_break](../../../../../functions/crates/lpe-attachments/src/extraction/ensure_paragraph_break.md)
- [normalize_whitespace](../../../../../functions/crates/lpe-attachments/src/extraction/normalize_whitespace.md)

# Called by

- [extract_odt_text_from_bytes](../../../../../functions/crates/lpe-attachments/src/extraction/extract_odt_text_from_bytes.md)