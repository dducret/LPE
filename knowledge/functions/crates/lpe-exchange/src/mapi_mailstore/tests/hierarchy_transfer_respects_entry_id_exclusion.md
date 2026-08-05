---
type: Rust Function
title: hierarchy_transfer_respects_entry_id_exclusion
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L2812-L2837
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_final_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_absent_property
---

# Signature

`fn hierarchy_transfer_respects_entry_id_exclusion()`

# Calls

- [virtual_special_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [sync_manifest_buffer_with_final_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_final_state.md)
- [assert_absent_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_absent_property.md)