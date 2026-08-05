---
type: Rust Function
title: assert_tag_order
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L4283-L4296
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_recipient_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_attachment_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_progress_markers_follow_progress_flag_example
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_starts_fai_message_before_item_properties
---

# Signature

`fn assert_tag_order(buffer: &[u8], tags: &[u32])`

# Calls

- [position](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [sync_manifest_serializes_content_message_header_in_fixed_order](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order.md)
- [microsoft_oxcfxics_content_sync_uses_recipient_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_recipient_markers.md)
- [microsoft_oxcfxics_content_sync_uses_attachment_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_attachment_markers.md)
- [microsoft_oxcfxics_content_sync_progress_markers_follow_progress_flag_example](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_progress_markers_follow_progress_flag_example.md)
- [content_sync_manifest_starts_fai_message_before_item_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_starts_fai_message_before_item_properties.md)