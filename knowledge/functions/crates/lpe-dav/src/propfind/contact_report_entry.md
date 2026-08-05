---
type: Rust Function
title: contact_report_entry
resource: crates/lpe-dav/src/propfind.rs#L168-L178
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/serialize/serialize_vcard
  - functions/crates/lpe-dav/src/responses/response_entry
  - functions/crates/lpe-dav/src/paths/contact_href
---

# Signature

`pub(crate) fn contact_report_entry(contact: AccessibleContact) -> String`

# Calls

- [serialize_vcard](../../../../../functions/crates/lpe-dav/src/serialize/serialize_vcard.md)
- [response_entry](../../../../../functions/crates/lpe-dav/src/responses/response_entry.md)
- [contact_href](../../../../../functions/crates/lpe-dav/src/paths/contact_href.md)