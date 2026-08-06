---
type: Rust Method
title: string_object_changes_response
resource: crates/lpe-jmap/src/service/canonical.rs#L298-L345
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/state_cursor
  - functions/crates/lpe-jmap/src/state/changes_response_from_durable_with_cursor
  - functions/crates/lpe-jmap/src/state/changes_response_with_cursor
  called_by:
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_changes
---

# Signature

`pub(crate) async fn string_object_changes_response( &self, account_id: Uuid, data_type: &str, since_state: &str, max_changes: Option<u64>, entries: Vec<StateEntry>, ) -> Result<Value>`

# Calls

- [state_cursor](../../../../../../../functions/crates/lpe-jmap/src/state/state_cursor.md)
- [changes_response_from_durable_with_cursor](../../../../../../../functions/crates/lpe-jmap/src/state/changes_response_from_durable_with_cursor.md)
- [changes_response_with_cursor](../../../../../../../functions/crates/lpe-jmap/src/state/changes_response_with_cursor.md)

# Called by

- [handle_canonical_changes](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_changes.md)