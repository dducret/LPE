---
type: Rust Function
title: serialize_vcard
resource: crates/lpe-dav/src/serialize.rs#L6-L20
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/serialize/push_line
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-dav/src/paths/etag_for_contact
  - functions/crates/lpe-dav/src/propfind/contact_resource_entry
  - functions/crates/lpe-dav/src/propfind/contact_report_entry
  - functions/crates/lpe-dav/src/service/DavService/handle_get
---

# Signature

`pub(crate) fn serialize_vcard(contact: &AccessibleContact) -> String`

# Calls

- [push_line](../../../../../functions/crates/lpe-dav/src/serialize/push_line.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [etag_for_contact](../../../../../functions/crates/lpe-dav/src/paths/etag_for_contact.md)
- [contact_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/contact_resource_entry.md)
- [contact_report_entry](../../../../../functions/crates/lpe-dav/src/propfind/contact_report_entry.md)
- [handle_get](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_get.md)