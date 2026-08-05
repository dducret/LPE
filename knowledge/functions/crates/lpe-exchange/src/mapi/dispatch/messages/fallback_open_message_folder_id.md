---
type: Rust Function
title: fallback_open_message_folder_id
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L28-L38
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/email_matches_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/canonical_message_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
---

# Signature

`pub(super) fn fallback_open_message_folder_id( requested_folder_id: u64, email: &JmapEmail, mailboxes: &[JmapMailbox], ) -> u64`

# Calls

- [email_matches_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/email_matches_folder.md)
- [canonical_message_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/canonical_message_folder_id.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)