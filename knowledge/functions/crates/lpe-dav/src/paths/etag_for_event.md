---
type: Rust Function
title: etag_for_event
resource: crates/lpe-dav/src/paths.rs#L133-L135
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/etag
  - functions/crates/lpe-dav/src/serialize/serialize_ical
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_get
  - functions/crates/lpe-dav/src/service/DavService/handle_put
  - functions/crates/lpe-dav/src/tests/get_returns_not_modified_when_if_none_match_matches
---

# Signature

`pub(crate) fn etag_for_event(event: &AccessibleEvent) -> String`

# Calls

- [etag](../../../../../functions/crates/lpe-dav/src/paths/etag.md)
- [serialize_ical](../../../../../functions/crates/lpe-dav/src/serialize/serialize_ical.md)

# Called by

- [handle_get](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_get.md)
- [handle_put](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_put.md)
- [get_returns_not_modified_when_if_none_match_matches](../../../../../functions/crates/lpe-dav/src/tests/get_returns_not_modified_when_if_none_match_matches.md)