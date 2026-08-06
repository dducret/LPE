---
type: Rust Method
title: handle_canonical_query_changes
resource: crates/lpe-jmap/src/service/canonical.rs#L136-L257
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_query_ids
  - functions/crates/lpe-jmap/src/service/helpers/canonical_query_state_method
  - functions/crates/lpe-jmap/src/service/helpers/canonical_query_filter
  - functions/crates/lpe-jmap/src/state/decode_query_state
  - functions/crates/lpe-jmap/src/state/validate_query_state_token
  - functions/crates/lpe-jmap/src/state/query_diff_for_kind
  - functions/crates/lpe-jmap/src/state/encode_query_state_reference
  - functions/crates/lpe-jmap/src/state/encode_query_state
  - functions/crates/lpe-jmap/src/state/query_changes_response_from_diff
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_canonical_query_changes( &self, account: &AuthenticatedAccount, arguments: Value, data_type: &str, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [requested_account_id_from_arguments](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments.md)
- [mailbox_account_may_submit](../../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit.md)
- [canonical_query_ids](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_query_ids.md)
- [canonical_query_state_method](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/canonical_query_state_method.md)
- [canonical_query_filter](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/canonical_query_filter.md)
- [decode_query_state](../../../../../../../functions/crates/lpe-jmap/src/state/decode_query_state.md)
- [validate_query_state_token](../../../../../../../functions/crates/lpe-jmap/src/state/validate_query_state_token.md)
- [query_diff_for_kind](../../../../../../../functions/crates/lpe-jmap/src/state/query_diff_for_kind.md)
- [encode_query_state_reference](../../../../../../../functions/crates/lpe-jmap/src/state/encode_query_state_reference.md)
- [encode_query_state](../../../../../../../functions/crates/lpe-jmap/src/state/encode_query_state.md)
- [query_changes_response_from_diff](../../../../../../../functions/crates/lpe-jmap/src/state/query_changes_response_from_diff.md)

# Called by

- [handle_api_request_for_account](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)