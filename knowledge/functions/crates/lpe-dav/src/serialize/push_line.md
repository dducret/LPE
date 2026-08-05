---
type: Rust Function
title: push_line
resource: crates/lpe-dav/src/serialize.rs#L85-L89
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-dav/src/serialize/serialize_vcard
  - functions/crates/lpe-dav/src/serialize/serialize_ical
  - functions/crates/lpe-dav/src/serialize/serialize_vtodo
---

# Signature

`fn push_line(lines: &mut Vec<String>, name: &str, value: &str)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [serialize_vcard](../../../../../functions/crates/lpe-dav/src/serialize/serialize_vcard.md)
- [serialize_ical](../../../../../functions/crates/lpe-dav/src/serialize/serialize_ical.md)
- [serialize_vtodo](../../../../../functions/crates/lpe-dav/src/serialize/serialize_vtodo.md)