---
type: Rust Function
title: hierarchy_sync_excluded_properties_are_not_reintroduced_as_stable_folder_facts
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L3813-L3865
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_final_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn hierarchy_sync_excluded_properties_are_not_reintroduced_as_stable_folder_facts()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [sync_manifest_buffer_with_final_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_final_state.md)
- [decode_hierarchy_transfer_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)