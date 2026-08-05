---
type: Rust Function
title: query_diff_for_kind
resource: crates/lpe-jmap/src/state.rs#L567-L591
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/compute_query_diff_with_reorders
  - functions/crates/lpe-jmap/src/state/compute_query_diff
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes
  - functions/crates/lpe-jmap/src/state/query_changes_response
---

# Signature

`pub(crate) fn query_diff_for_kind( kind: &str, previous_ids: &[String], current_ids: &[String], max_changes: Option<u64>, ) -> QueryDiff`

# Calls

- [compute_query_diff_with_reorders](../../../../../functions/crates/lpe-jmap/src/state/compute_query_diff_with_reorders.md)
- [compute_query_diff](../../../../../functions/crates/lpe-jmap/src/state/compute_query_diff.md)

# Called by

- [handle_email_query_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes.md)
- [handle_mailbox_query_changes](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes.md)
- [handle_canonical_query_changes](../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes.md)
- [query_changes_response](../../../../../functions/crates/lpe-jmap/src/state/query_changes_response.md)