---
type: Rust Function
title: query_changes_response_from_diff
resource: crates/lpe-jmap/src/state.rs#L524-L546
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/validate_query_state_token
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes
  - functions/crates/lpe-jmap/src/state/query_changes_response
---

# Signature

`pub(crate) fn query_changes_response_from_diff( account_id: Uuid, kind: &str, since_query_state: String, filter: Option<Value>, sort: Option<Vec<Value>>, previous: QueryStateToken, next_query_state: String, total: u64, diff: QueryDiff, ) -> Result<Value>`

# Calls

- [validate_query_state_token](../../../../../functions/crates/lpe-jmap/src/state/validate_query_state_token.md)

# Called by

- [handle_email_query_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes.md)
- [handle_mailbox_query_changes](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes.md)
- [handle_canonical_query_changes](../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes.md)
- [query_changes_response](../../../../../functions/crates/lpe-jmap/src/state/query_changes_response.md)