---
type: Rust Function
title: etag_for_contact
resource: crates/lpe-dav/src/paths.rs#L129-L131
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/etag
  - functions/crates/lpe-dav/src/serialize/serialize_vcard
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_get
  - functions/crates/lpe-dav/src/service/DavService/handle_put
---

# Signature

`pub(crate) fn etag_for_contact(contact: &AccessibleContact) -> String`

# Calls

- [etag](../../../../../functions/crates/lpe-dav/src/paths/etag.md)
- [serialize_vcard](../../../../../functions/crates/lpe-dav/src/serialize/serialize_vcard.md)

# Called by

- [handle_get](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_get.md)
- [handle_put](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_put.md)