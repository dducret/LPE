---
type: Rust Method
title: mail_push_type_state
resource: crates/lpe-jmap/src/websocket.rs#L620-L647
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/email_delivery_object_state
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state
  - functions/crates/lpe-jmap/src/state/encode_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes
  - functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states
---

# Signature

`async fn mail_push_type_state( &self, principal_account_id: Uuid, access: &lpe_storage::MailboxAccountAccess, data_type: &str, ) -> Result<String>`

# Calls

- [mailbox_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state.md)
- [mail_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state.md)
- [email_delivery_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/email_delivery_object_state.md)
- [mailbox_account_may_submit](../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit.md)
- [identity_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state.md)
- [encode_state](../../../../../../functions/crates/lpe-jmap/src/state/encode_state.md)
- [email_submission_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state.md)
- [object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state.md)

# Called by

- [compute_push_changes](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes.md)
- [current_push_states](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states.md)