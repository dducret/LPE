---
type: Rust Method
title: record_post_visible_inbox_release_create_save_batch
resource: crates/lpe-exchange/src/mapi/session.rs#L1188-L1218
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/execute_has_create_save_batch
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_execute_after_hierarchy_completion
---

# Signature

`fn record_post_visible_inbox_release_create_save_batch( &mut self, rop_ids: &[u8], rop_names: &str, )`

# Calls

- [execute_has_create_save_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/execute_has_create_save_batch.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)

# Called by

- [record_execute_after_hierarchy_completion](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_execute_after_hierarchy_completion.md)