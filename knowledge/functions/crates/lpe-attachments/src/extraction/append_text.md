---
type: Rust Function
title: append_text
resource: crates/lpe-attachments/src/extraction.rs#L186-L197
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

`fn append_text(output: &mut String, text: &str)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [extract_odt_content_xml](../../../../../functions/crates/lpe-attachments/src/extraction/extract_odt_content_xml.md)