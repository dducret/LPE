---
type: Rust Function
title: microsoft_oxcfxics_content_sync_uses_recipient_markers
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L1091-L1172
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_tag_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_i32_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property_present
---

# Signature

`fn microsoft_oxcfxics_content_sync_uses_recipient_markers()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [sync_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments.md)
- [assert_tag_order](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_tag_order.md)
- [assert_i32_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_i32_property.md)
- [assert_variable_property_present](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property_present.md)