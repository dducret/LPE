---
type: Rust Function
title: decode_query_state
resource: crates/lpe-jmap/src/state.rs#L479-L489
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes
  - functions/crates/lpe-jmap/src/state/query_changes_response
  - functions/crates/lpe-jmap/src/tests/stored_email_query_state_keeps_snapshot_out_of_token_and_paginates_changes
  - functions/crates/lpe-jmap/src/tests/stored_mailbox_query_state_keeps_snapshot_out_of_token_and_paginates_changes
  - functions/crates/lpe-jmap/src/tests/thread_query_state_keeps_full_snapshot_when_page_is_limited
  - functions/crates/lpe-jmap/src/tests/canonical_private_query_changes_use_persisted_query_snapshots
---

# Signature

`pub(crate) fn decode_query_state(value: &str) -> Result<QueryStateToken>`

# Called by

- [handle_email_query_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes.md)
- [handle_mailbox_query_changes](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes.md)
- [handle_canonical_query_changes](../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes.md)
- [query_changes_response](../../../../../functions/crates/lpe-jmap/src/state/query_changes_response.md)
- [stored_email_query_state_keeps_snapshot_out_of_token_and_paginates_changes](../../../../../functions/crates/lpe-jmap/src/tests/stored_email_query_state_keeps_snapshot_out_of_token_and_paginates_changes.md)
- [stored_mailbox_query_state_keeps_snapshot_out_of_token_and_paginates_changes](../../../../../functions/crates/lpe-jmap/src/tests/stored_mailbox_query_state_keeps_snapshot_out_of_token_and_paginates_changes.md)
- [thread_query_state_keeps_full_snapshot_when_page_is_limited](../../../../../functions/crates/lpe-jmap/src/tests/thread_query_state_keeps_full_snapshot_when_page_is_limited.md)
- [canonical_private_query_changes_use_persisted_query_snapshots](../../../../../functions/crates/lpe-jmap/src/tests/canonical_private_query_changes_use_persisted_query_snapshots.md)