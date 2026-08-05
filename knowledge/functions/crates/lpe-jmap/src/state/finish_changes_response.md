---
type: Rust Function
title: finish_changes_response
resource: crates/lpe-jmap/src/state.rs#L239-L292
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/apply_state_changes
  - functions/crates/lpe-jmap/src/state/encode_state_with_cursor
  called_by:
  - functions/crates/lpe-jmap/src/state/changes_response_from_durable_with_cursor
  - functions/crates/lpe-jmap/src/state/changes_response_with_cursor
---

# Signature

`fn finish_changes_response( account_id: Uuid, kind: &str, since_state: &str, max_changes: usize, previous_entries: Vec<StateEntry>, previous_cursor: Option<i64>, current_entries: Vec<StateEntry>, current_cursor: Option<i64>, current_map: &HashMap<String, String>, mut created: Vec<String>, mut updated: Vec<String>, mut destroyed: Vec<String>, ) -> Result<Value>`

# Calls

- [apply_state_changes](../../../../../functions/crates/lpe-jmap/src/state/apply_state_changes.md)
- [encode_state_with_cursor](../../../../../functions/crates/lpe-jmap/src/state/encode_state_with_cursor.md)

# Called by

- [changes_response_from_durable_with_cursor](../../../../../functions/crates/lpe-jmap/src/state/changes_response_from_durable_with_cursor.md)
- [changes_response_with_cursor](../../../../../functions/crates/lpe-jmap/src/state/changes_response_with_cursor.md)