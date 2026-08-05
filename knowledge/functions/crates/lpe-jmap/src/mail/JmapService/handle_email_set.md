---
type: Rust Method
title: handle_email_set
resource: crates/lpe-jmap/src/mail.rs#L530-L639
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state
  - functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_draft_write
  - functions/crates/lpe-jmap/src/mail/JmapService/create_draft
  - functions/crates/lpe-jmap/src/error/set_error
  - functions/crates/lpe-jmap/src/mail/JmapService/update_draft
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_email_set( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &mut HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [mail_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state.md)
- [ensure_mailbox_draft_write](../../../../../../functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_draft_write.md)
- [create_draft](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/create_draft.md)
- [set_error](../../../../../../functions/crates/lpe-jmap/src/error/set_error.md)
- [update_draft](../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/update_draft.md)
- [parse_uuid](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)