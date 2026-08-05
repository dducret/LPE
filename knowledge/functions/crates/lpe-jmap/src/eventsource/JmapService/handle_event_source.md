---
type: Rust Method
title: handle_event_source
resource: crates/lpe-jmap/src/eventsource.rs#L35-L130
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/websocket/normalize_push_data_types
  - functions/crates/lpe-jmap/src/eventsource/event_source_data_types
  - functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states
  - functions/crates/lpe-jmap/src/state/encode_push_state
  - functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change
  - functions/crates/lpe-jmap/src/eventsource/event_source_close_after
  - functions/crates/lpe-jmap/src/eventsource/send_state_change_event
  - functions/crates/lpe-jmap/src/websocket/JmapService/push_categories
  - functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes
  - functions/crates/lpe-jmap/src/websocket/finalize_push_change
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/journal_cursor
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
  called_by:
  - functions/crates/lpe-jmap/src/service/event_source_handler
---

# Signature

`pub(crate) async fn handle_event_source( &self, account: AuthenticatedAccount, query: EventSourceQuery, last_event_id: Option<String>, ) -> Result<EventSourceStream>`

# Calls

- [normalize_push_data_types](../../../../../../functions/crates/lpe-jmap/src/websocket/normalize_push_data_types.md)
- [event_source_data_types](../../../../../../functions/crates/lpe-jmap/src/eventsource/event_source_data_types.md)
- [current_push_states](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states.md)
- [encode_push_state](../../../../../../functions/crates/lpe-jmap/src/state/encode_push_state.md)
- [recover_push_enable_change](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change.md)
- [event_source_close_after](../../../../../../functions/crates/lpe-jmap/src/eventsource/event_source_close_after.md)
- [send_state_change_event](../../../../../../functions/crates/lpe-jmap/src/eventsource/send_state_change_event.md)
- [push_categories](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/push_categories.md)
- [compute_push_changes](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes.md)
- [finalize_push_change](../../../../../../functions/crates/lpe-jmap/src/websocket/finalize_push_change.md)
- [journal_cursor](../../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/journal_cursor.md)
- [into_response](../../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)

# Called by

- [event_source_handler](../../../../../../functions/crates/lpe-jmap/src/service/event_source_handler.md)