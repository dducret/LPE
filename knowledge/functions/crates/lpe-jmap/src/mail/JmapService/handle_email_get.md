---
type: Rust Method
title: handle_email_get
resource: crates/lpe-jmap/src/mail.rs#L271-L328
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/mail/values/EmailBodyOptions/from_arguments
  - functions/crates/lpe-jmap/src/mail/values/email_properties
  - functions/crates/lpe-jmap/src/parse/parse_uuid_list
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_email_get( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [from_arguments](../../../../../../functions/crates/lpe-jmap/src/mail/values/EmailBodyOptions/from_arguments.md)
- [email_properties](../../../../../../functions/crates/lpe-jmap/src/mail/values/email_properties.md)
- [parse_uuid_list](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid_list.md)
- [mail_object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)