---
type: Rust Function
title: resolve_client_sender_fields
resource: crates/lpe-admin-api/src/workspace.rs#L1467-L1490
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/map_submit_message_request
  - functions/crates/lpe-admin-api/src/workspace/tests/delegated_send_on_behalf_defaults_sender_to_authenticated_account
  - functions/crates/lpe-admin-api/src/workspace/tests/delegated_send_as_without_explicit_sender_keeps_sender_empty
  - functions/crates/lpe-admin-api/src/workspace/tests/explicit_sender_fields_are_preserved
---

# Signature

`fn resolve_client_sender_fields( authenticated_account: &AuthenticatedAccount, mailbox_access: &MailboxAccountAccess, request: &SubmitMessageRequest, ) -> (Option<String>, Option<String>)`

# Called by

- [map_submit_message_request](../../../../../functions/crates/lpe-admin-api/src/workspace/map_submit_message_request.md)
- [delegated_send_on_behalf_defaults_sender_to_authenticated_account](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/delegated_send_on_behalf_defaults_sender_to_authenticated_account.md)
- [delegated_send_as_without_explicit_sender_keeps_sender_empty](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/delegated_send_as_without_explicit_sender_keeps_sender_empty.md)
- [explicit_sender_fields_are_preserved](../../../../../functions/crates/lpe-admin-api/src/workspace/tests/explicit_sender_fields_are_preserved.md)