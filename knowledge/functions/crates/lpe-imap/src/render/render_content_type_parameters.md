---
type: Rust Function
title: render_content_type_parameters
resource: crates/lpe-imap/src/render.rs#L844-L872
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-imap/src/render/render_attachment_bodystructure
---

# Signature

`fn render_content_type_parameters(part: &ImapMimePart) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [render_attachment_bodystructure](../../../../../functions/crates/lpe-imap/src/render/render_attachment_bodystructure.md)