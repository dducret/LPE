---
type: Rust Function
title: pending_message_is_sync_metadata_only
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L793-L808
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(super) fn pending_message_is_sync_metadata_only( properties: &HashMap<u32, MapiValue>, recipients: &[PendingRecipient], ) -> bool`

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)