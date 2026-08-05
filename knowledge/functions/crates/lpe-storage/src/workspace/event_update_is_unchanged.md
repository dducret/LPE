---
type: Rust Function
title: event_update_is_unchanged
resource: crates/lpe-storage/src/workspace.rs#L1209-L1245
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/workspace/json_text_matches
  called_by:
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar
---

# Signature

`fn event_update_is_unchanged( existing: &ClientEvent, input: &UpsertClientEventInput, event_id: Uuid, ) -> bool`

# Calls

- [json_text_matches](../../../../../functions/crates/lpe-storage/src/workspace/json_text_matches.md)

# Called by

- [upsert_client_event_in_calendar](../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar.md)