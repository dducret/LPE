---
type: Rust Function
title: delegate_freebusy_getprops_rejects_message_from_wrong_folder
resource: crates/lpe-exchange/src/mapi/rop/tests.rs#L2669-L2729
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_delegate_freebusy_messages
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_messages
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response
---

# Signature

`fn delegate_freebusy_getprops_rejects_message_from_wrong_folder()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_delegate_freebusy_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_delegate_freebusy_messages.md)
- [delegate_freebusy_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/delegate_freebusy_messages.md)
- [rop_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response.md)