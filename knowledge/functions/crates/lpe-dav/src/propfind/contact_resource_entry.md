---
type: Rust Function
title: contact_resource_entry
resource: crates/lpe-dav/src/propfind.rs#L111-L128
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/serialize/serialize_vcard
  - functions/crates/lpe-dav/src/responses/response_entry
  - functions/crates/lpe-dav/src/paths/contact_href
  - functions/crates/lpe-dav/src/propfind/collection_props
  - functions/crates/lpe-dav/src/paths/etag
  - functions/crates/lpe-dav/src/propfind/collection_metadata
---

# Signature

`pub(crate) fn contact_resource_entry(contact: AccessibleContact) -> String`

# Calls

- [serialize_vcard](../../../../../functions/crates/lpe-dav/src/serialize/serialize_vcard.md)
- [response_entry](../../../../../functions/crates/lpe-dav/src/responses/response_entry.md)
- [contact_href](../../../../../functions/crates/lpe-dav/src/paths/contact_href.md)
- [collection_props](../../../../../functions/crates/lpe-dav/src/propfind/collection_props.md)
- [etag](../../../../../functions/crates/lpe-dav/src/paths/etag.md)
- [collection_metadata](../../../../../functions/crates/lpe-dav/src/propfind/collection_metadata.md)