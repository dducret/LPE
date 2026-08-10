---
type: Rust Method
title: mirror_jmap_email_into_mailbox
resource: crates/lpe-exchange/src/tests/mod.rs#L11733-L11776
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response
---

# Signature

`fn mirror_jmap_email_into_mailbox<'a>( &'a self, _account_id: Uuid, message_id: Uuid, target_mailbox_id: Uuid, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [append_submit_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)