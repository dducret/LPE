---
type: Rust Function
title: delegate_freebusy_message_for_open
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L104-L113
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_message_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/freebusy_open_prefers_delegate_message_over_stale_associated_config_identity
---

# Signature

`pub(super) fn delegate_freebusy_message_for_open<'a>( snapshot: &'a MapiMailStoreSnapshot, folder_id: u64, message_id: u64, ) -> Option<&'a crate::mapi_store::MapiDelegateFreeBusyMessage>`

# Calls

- [delegate_freebusy_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_message_for_id.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [freebusy_open_prefers_delegate_message_over_stale_associated_config_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/freebusy_open_prefers_delegate_message_over_stale_associated_config_identity.md)