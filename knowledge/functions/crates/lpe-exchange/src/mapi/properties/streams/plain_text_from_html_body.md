---
type: Rust Function
title: plain_text_from_html_body
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L916-L942
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/html_tag_is_line_break
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/streams/decode_basic_html_entities
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property
---

# Signature

`fn plain_text_from_html_body(html: &str) -> String`

# Calls

- [html_tag_is_line_break](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/html_tag_is_line_break.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [decode_basic_html_entities](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/decode_basic_html_entities.md)

# Called by

- [pending_body_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property.md)