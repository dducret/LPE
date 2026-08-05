---
type: Rust Function
title: freebusy_open_prefers_delegate_message_over_stale_associated_config_identity
resource: crates/lpe-exchange/src/mapi/dispatch/tests/associated_config.rs#L16-L57
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_delegate_freebusy_messages
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delegate_freebusy_message_for_open
---

# Signature

`fn freebusy_open_prefers_delegate_message_over_stale_associated_config_identity()`

# Calls

- [remember_mapi_identity](../../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [empty](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_delegate_freebusy_messages](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_delegate_freebusy_messages.md)
- [with_associated_configs](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)
- [delegate_freebusy_message_for_open](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delegate_freebusy_message_for_open.md)