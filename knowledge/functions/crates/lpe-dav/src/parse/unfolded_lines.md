---
type: Rust Function
title: unfolded_lines
resource: crates/lpe-dav/src/parse.rs#L324-L337
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-dav/src/parse/parse_vcard
  - functions/crates/lpe-dav/src/parse/parse_ical
  - functions/crates/lpe-dav/src/parse/parse_vtodo
---

# Signature

`fn unfolded_lines(content: &str) -> Vec<String>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse_vcard](../../../../../functions/crates/lpe-dav/src/parse/parse_vcard.md)
- [parse_ical](../../../../../functions/crates/lpe-dav/src/parse/parse_ical.md)
- [parse_vtodo](../../../../../functions/crates/lpe-dav/src/parse/parse_vtodo.md)