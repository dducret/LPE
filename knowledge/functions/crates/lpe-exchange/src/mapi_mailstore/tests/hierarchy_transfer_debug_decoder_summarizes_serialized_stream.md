---
type: Rust Function
title: hierarchy_transfer_debug_decoder_summarizes_serialized_stream
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L1874-L1997
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation
---

# Signature

`fn hierarchy_transfer_debug_decoder_summarizes_serialized_stream()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [sync_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments.md)
- [decode_hierarchy_transfer_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)
- [hierarchy_semantic_validation](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation.md)