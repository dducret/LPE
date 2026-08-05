---
type: Rust Function
title: extract_odt_text_from_bytes
resource: crates/lpe-attachments/src/extraction.rs#L135-L147
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-attachments/src/extraction/extract_odt_content_xml
  called_by:
  - functions/crates/lpe-attachments/src/extraction/extract_text_from_bytes
  - functions/crates/lpe-attachments/src/extraction/extract_odt_text
---

# Signature

`fn extract_odt_text_from_bytes(bytes: &[u8]) -> Result<String>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [extract_odt_content_xml](../../../../../functions/crates/lpe-attachments/src/extraction/extract_odt_content_xml.md)

# Called by

- [extract_text_from_bytes](../../../../../functions/crates/lpe-attachments/src/extraction/extract_text_from_bytes.md)
- [extract_odt_text](../../../../../functions/crates/lpe-attachments/src/extraction/extract_odt_text.md)