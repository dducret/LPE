---
type: Rust Function
title: event_resource_entry
resource: crates/lpe-dav/src/propfind.rs#L130-L147
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/serialize/serialize_ical
  - functions/crates/lpe-dav/src/responses/response_entry
  - functions/crates/lpe-dav/src/paths/event_href
  - functions/crates/lpe-dav/src/propfind/collection_props
  - functions/crates/lpe-dav/src/paths/etag
  - functions/crates/lpe-dav/src/propfind/collection_metadata
---

# Signature

`pub(crate) fn event_resource_entry(event: AccessibleEvent) -> String`

# Calls

- [serialize_ical](../../../../../functions/crates/lpe-dav/src/serialize/serialize_ical.md)
- [response_entry](../../../../../functions/crates/lpe-dav/src/responses/response_entry.md)
- [event_href](../../../../../functions/crates/lpe-dav/src/paths/event_href.md)
- [collection_props](../../../../../functions/crates/lpe-dav/src/propfind/collection_props.md)
- [etag](../../../../../functions/crates/lpe-dav/src/paths/etag.md)
- [collection_metadata](../../../../../functions/crates/lpe-dav/src/propfind/collection_metadata.md)