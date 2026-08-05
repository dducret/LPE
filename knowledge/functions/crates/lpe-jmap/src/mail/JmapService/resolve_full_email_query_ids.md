---
type: Rust Method
title: resolve_full_email_query_ids
resource: crates/lpe-jmap/src/mail.rs#L1257-L1280
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/mail/values/full_query_limit
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes
---

# Signature

`pub(crate) async fn resolve_full_email_query_ids( &self, account_id: Uuid, mailbox_id: Option<Uuid>, search_text: Option<&str>, query: &lpe_storage::JmapEmailQuery, ) -> Result<Vec<String>>`

# Calls

- [full_query_limit](../../../../../../functions/crates/lpe-jmap/src/mail/values/full_query_limit.md)

# Called by

- [handle_email_query](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query.md)
- [handle_email_query_changes](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes.md)