---
type: Rust Method
title: handle_identity_get
resource: crates/lpe-jmap/src/mail.rs#L897-L945
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/mail/values/identity_properties
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit
  - functions/crates/lpe-jmap/src/mail/values/identity_to_value
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state
  - functions/crates/lpe-jmap/src/state/encode_state
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_identity_get( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [identity_properties](../../../../../../functions/crates/lpe-jmap/src/mail/values/identity_properties.md)
- [mailbox_account_may_submit](../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit.md)
- [identity_to_value](../../../../../../functions/crates/lpe-jmap/src/mail/values/identity_to_value.md)
- [identity_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state.md)
- [encode_state](../../../../../../functions/crates/lpe-jmap/src/state/encode_state.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)