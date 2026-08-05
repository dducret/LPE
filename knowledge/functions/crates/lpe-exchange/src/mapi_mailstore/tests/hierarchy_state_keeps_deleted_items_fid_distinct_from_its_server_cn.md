---
type: Rust Function
title: hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L207-L232
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
---

# Signature

`fn hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn()`

# Calls

- [virtual_special_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [sync_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments.md)
- [decode_hierarchy_transfer_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)