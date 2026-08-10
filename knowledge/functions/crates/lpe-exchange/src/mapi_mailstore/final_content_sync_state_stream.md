---
type: Rust Function
title: final_content_sync_state_stream
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L597-L610
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream_with_cnsets
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_state_keeps_normal_and_fai_cnsets_separate
---

# Signature

`fn final_content_sync_state_stream( object_ids: &[u64], normal_change_numbers: &[u64], fai_change_numbers: &[u64], read_change_numbers: &[u64], ) -> Vec<u8>`

# Calls

- [final_sync_state_stream_with_cnsets](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream_with_cnsets.md)

# Called by

- [final_sync_state_stream](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream.md)
- [content_sync_state_keeps_normal_and_fai_cnsets_separate](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_state_keeps_normal_and_fai_cnsets_separate.md)