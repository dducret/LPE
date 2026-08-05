---
type: Rust Function
title: changes_response
resource: crates/lpe-jmap/src/state.rs#L59-L74
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/changes_response_with_cursor
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_changes
  - functions/crates/lpe-jmap/src/state/changes_response_returns_intermediate_state_when_truncated
  - functions/crates/lpe-jmap/src/state/changes_response_rejects_invalid_or_mismatched_state_tokens
---

# Signature

`pub(crate) fn changes_response( account_id: Uuid, kind: &str, since_state: &str, max_changes: Option<u64>, current_entries: Vec<StateEntry>, ) -> Result<Value>`

# Calls

- [changes_response_with_cursor](../../../../../functions/crates/lpe-jmap/src/state/changes_response_with_cursor.md)

# Called by

- [handle_identity_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_identity_changes.md)
- [changes_response_returns_intermediate_state_when_truncated](../../../../../functions/crates/lpe-jmap/src/state/changes_response_returns_intermediate_state_when_truncated.md)
- [changes_response_rejects_invalid_or_mismatched_state_tokens](../../../../../functions/crates/lpe-jmap/src/state/changes_response_rejects_invalid_or_mismatched_state_tokens.md)