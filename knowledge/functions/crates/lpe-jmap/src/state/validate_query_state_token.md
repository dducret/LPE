---
type: Rust Function
title: validate_query_state_token
resource: crates/lpe-jmap/src/state.rs#L548-L565
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes
  - functions/crates/lpe-jmap/src/state/query_changes_response_from_diff
---

# Signature

`pub(crate) fn validate_query_state_token( account_id: Uuid, kind: &str, filter: Option<&Value>, sort: Option<&Vec<Value>>, previous: &QueryStateToken, ) -> Result<()>`

# Called by

- [handle_email_query_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes.md)
- [handle_mailbox_query_changes](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes.md)
- [handle_canonical_query_changes](../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes.md)
- [query_changes_response_from_diff](../../../../../functions/crates/lpe-jmap/src/state/query_changes_response_from_diff.md)