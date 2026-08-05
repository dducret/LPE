---
type: Rust Function
title: ensure_paragraph_break
resource: crates/lpe-attachments/src/extraction.rs#L199-L211
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-attachments/src/extraction/extract_odt_content_xml
---

# Signature

`fn ensure_paragraph_break(output: &mut String)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [extract_odt_content_xml](../../../../../functions/crates/lpe-attachments/src/extraction/extract_odt_content_xml.md)