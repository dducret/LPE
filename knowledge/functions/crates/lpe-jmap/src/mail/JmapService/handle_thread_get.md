---
type: Rust Method
title: handle_thread_get
resource: crates/lpe-jmap/src/mail.rs#L1101-L1149
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/mail/values/thread_properties
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-jmap/src/mail/values/thread_to_value
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_thread_get( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [thread_properties](../../../../../../functions/crates/lpe-jmap/src/mail/values/thread_properties.md)
- [parse_uuid](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [thread_to_value](../../../../../../functions/crates/lpe-jmap/src/mail/values/thread_to_value.md)
- [mail_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)