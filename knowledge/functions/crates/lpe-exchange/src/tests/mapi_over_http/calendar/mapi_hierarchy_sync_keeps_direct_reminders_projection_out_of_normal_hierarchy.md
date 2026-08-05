---
type: Rust Function
title: mapi_hierarchy_sync_keeps_direct_reminders_projection_out_of_normal_hierarchy
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar.rs#L8040-L8085
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_final_state
  - functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream
---

# Signature

`fn mapi_hierarchy_sync_keeps_direct_reminders_projection_out_of_normal_hierarchy()`

# Calls

- [virtual_special_mailbox](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [sync_manifest_buffer_with_final_state](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_final_state.md)
- [strict_decode_hierarchy_sync_stream](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream.md)