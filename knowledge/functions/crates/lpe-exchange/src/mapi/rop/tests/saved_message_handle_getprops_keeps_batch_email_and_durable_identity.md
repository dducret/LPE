---
type: Rust Function
title: saved_message_handle_getprops_keeps_batch_email_and_durable_identity
resource: crates/lpe-exchange/src/mapi/rop/tests.rs#L2953-L3162
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/new_with_scoped_calendar_identities
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
---

# Signature

`fn saved_message_handle_getprops_keeps_batch_email_and_durable_identity()`

# Calls

- [remember_mapi_identity_with_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)
- [legacy_for_tests](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests.md)
- [new_with_scoped_calendar_identities](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/new_with_scoped_calendar_identities.md)
- [rop_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response.md)
- [parse_mapi_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)