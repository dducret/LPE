---
type: Rust Function
title: assert_i32_property
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L4232-L4242
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_variable_strings_with_fast_transfer_lengths
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_recipient_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_attachment_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_embedded_message_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag
---

# Signature

`fn assert_i32_property(buffer: &[u8], property_tag: u32, value: i32)`

# Calls

- [position](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [sync_manifest_serializes_variable_strings_with_fast_transfer_lengths](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_variable_strings_with_fast_transfer_lengths.md)
- [sync_manifest_serializes_content_message_header_in_fixed_order](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order.md)
- [microsoft_oxcfxics_content_sync_uses_recipient_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_recipient_markers.md)
- [microsoft_oxcfxics_content_sync_uses_attachment_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_attachment_markers.md)
- [microsoft_oxcfxics_content_sync_uses_embedded_message_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_embedded_message_markers.md)
- [microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root.md)
- [content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag.md)