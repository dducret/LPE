---
type: Rust Function
title: get_returns_not_modified_when_if_none_match_matches
resource: crates/lpe-dav/src/tests.rs#L893-L942
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-dav/src/paths/etag_for_event
---

# Signature

`async fn get_returns_not_modified_when_if_none_match_matches()`

# Calls

- [serialize_calendar_participants_metadata](../../../../../functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [etag_for_event](../../../../../functions/crates/lpe-dav/src/paths/etag_for_event.md)