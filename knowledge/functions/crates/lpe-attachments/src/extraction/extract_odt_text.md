---
type: Rust Function
title: extract_odt_text
resource: crates/lpe-attachments/src/extraction.rs#L129-L133
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-attachments/src/extraction/extract_odt_text_from_bytes
  called_by:
  - functions/crates/lpe-attachments/src/extraction/extract_text_from_path
---

# Signature

`fn extract_odt_text(path: &Path) -> Result<String>`

# Calls

- [extract_odt_text_from_bytes](../../../../../functions/crates/lpe-attachments/src/extraction/extract_odt_text_from_bytes.md)

# Called by

- [extract_text_from_path](../../../../../functions/crates/lpe-attachments/src/extraction/extract_text_from_path.md)