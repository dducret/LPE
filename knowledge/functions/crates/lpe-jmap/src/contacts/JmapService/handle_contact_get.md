---
type: Rust Method
title: handle_contact_get
resource: crates/lpe-jmap/src/contacts.rs#L176-L216
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/contacts/contact_properties
  - functions/crates/lpe-jmap/src/parse/parse_uuid_list
  - functions/crates/lpe-jmap/src/contacts/contact_to_value
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_contact_get( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [contact_properties](../../../../../../functions/crates/lpe-jmap/src/contacts/contact_properties.md)
- [parse_uuid_list](../../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid_list.md)
- [contact_to_value](../../../../../../functions/crates/lpe-jmap/src/contacts/contact_to_value.md)
- [object_state](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)