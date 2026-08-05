---
type: Rust Method
title: handle_thread_query_changes
resource: crates/lpe-jmap/src/mail.rs#L1051-L1099
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/validation/validate_query_sort
  - functions/crates/lpe-jmap/src/mail/JmapService/resolve_full_thread_query_ids
  - functions/crates/lpe-jmap/src/state/query_changes_response
  - functions/crates/lpe-jmap/src/mail/values/serialize_email_query_sort
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_thread_query_changes( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [validate_query_sort](../../../../../../functions/crates/lpe-jmap/src/validation/validate_query_sort.md)
- [resolve_full_thread_query_ids](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/resolve_full_thread_query_ids.md)
- [query_changes_response](../../../../../../functions/crates/lpe-jmap/src/state/query_changes_response.md)
- [serialize_email_query_sort](../../../../../../functions/crates/lpe-jmap/src/mail/values/serialize_email_query_sort.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)