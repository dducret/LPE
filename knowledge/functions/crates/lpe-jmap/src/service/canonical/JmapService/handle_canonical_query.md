---
type: Rust Method
title: handle_canonical_query
resource: crates/lpe-jmap/src/service/canonical.rs#L52-L134
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_objects
  - functions/crates/lpe-jmap/src/state/query_position
  - functions/crates/lpe-jmap/src/state/encode_query_state_reference
  - functions/crates/lpe-jmap/src/state/encode_query_state
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_canonical_query( &self, account: &AuthenticatedAccount, arguments: Value, data_type: &str, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [requested_account_id_from_arguments](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments.md)
- [mailbox_account_may_submit](../../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit.md)
- [canonical_objects](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_objects.md)
- [query_position](../../../../../../../functions/crates/lpe-jmap/src/state/query_position.md)
- [encode_query_state_reference](../../../../../../../functions/crates/lpe-jmap/src/state/encode_query_state_reference.md)
- [encode_query_state](../../../../../../../functions/crates/lpe-jmap/src/state/encode_query_state.md)

# Called by

- [handle_api_request_for_account](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)