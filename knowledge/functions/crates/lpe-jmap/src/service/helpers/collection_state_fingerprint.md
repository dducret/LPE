---
type: Rust Function
title: collection_state_fingerprint
resource: crates/lpe-jmap/src/service/helpers.rs#L691-L705
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_changes
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_changes
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries
---

# Signature

`pub(crate) fn collection_state_fingerprint(collection: &CollaborationCollection) -> String`

# Calls

- [opaque_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)

# Called by

- [handle_calendar_changes](../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_changes.md)
- [handle_address_book_changes](../../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_changes.md)
- [object_state_entries](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries.md)