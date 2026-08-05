---
type: Rust Method
title: handle_email_query_changes
resource: crates/lpe-jmap/src/mail.rs#L147-L269
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/validation/validate_query_sort
  - functions/crates/lpe-jmap/src/mail/values/serialize_email_query_sort
  - functions/crates/lpe-jmap/src/state/decode_query_state
  - functions/crates/lpe-jmap/src/state/validate_query_state_token
  - functions/crates/lpe-jmap/src/mail/JmapService/resolve_full_email_query_ids
  - functions/crates/lpe-jmap/src/state/query_diff_for_kind
  - functions/crates/lpe-jmap/src/state/encode_query_state_reference
  - functions/crates/lpe-jmap/src/state/encode_query_state
  - functions/crates/lpe-jmap/src/state/query_changes_response_from_diff
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_email_query_changes( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [validate_query_sort](../../../../../../functions/crates/lpe-jmap/src/validation/validate_query_sort.md)
- [serialize_email_query_sort](../../../../../../functions/crates/lpe-jmap/src/mail/values/serialize_email_query_sort.md)
- [decode_query_state](../../../../../../functions/crates/lpe-jmap/src/state/decode_query_state.md)
- [validate_query_state_token](../../../../../../functions/crates/lpe-jmap/src/state/validate_query_state_token.md)
- [resolve_full_email_query_ids](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/resolve_full_email_query_ids.md)
- [query_diff_for_kind](../../../../../../functions/crates/lpe-jmap/src/state/query_diff_for_kind.md)
- [encode_query_state_reference](../../../../../../functions/crates/lpe-jmap/src/state/encode_query_state_reference.md)
- [encode_query_state](../../../../../../functions/crates/lpe-jmap/src/state/encode_query_state.md)
- [query_changes_response_from_diff](../../../../../../functions/crates/lpe-jmap/src/state/query_changes_response_from_diff.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)