---
type: Rust Function
title: canonical_message_folder_id
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L3-L26
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/fallback_open_message_folder_id
---

# Signature

`pub(super) fn canonical_message_folder_id(email: &JmapEmail, mailboxes: &[JmapMailbox]) -> u64`

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [fallback_open_message_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/fallback_open_message_folder_id.md)