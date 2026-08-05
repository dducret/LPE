---
type: Rust Function
title: assert_i64_property
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L4244-L4254
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/fast_transfer_copy_properties_filters_message_identity_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/direct_fast_transfer_uses_persisted_normal_message_identity_properties
---

# Signature

`fn assert_i64_property(buffer: &[u8], property_tag: u32, value: i64)`

# Calls

- [position](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [sync_manifest_serializes_content_message_header_in_fixed_order](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order.md)
- [microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties.md)
- [microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers.md)
- [fast_transfer_copy_properties_filters_message_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/fast_transfer_copy_properties_filters_message_identity_properties.md)
- [direct_fast_transfer_uses_persisted_normal_message_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/direct_fast_transfer_uses_persisted_normal_message_identity_properties.md)