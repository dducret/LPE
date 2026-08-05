---
type: Rust Function
title: associated_config_mutation_prefers_cumulative_saved_handle_over_stale_snapshot
resource: crates/lpe-exchange/src/mapi/dispatch/tests/associated_config.rs#L364-L400
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn associated_config_mutation_prefers_cumulative_saved_handle_over_stale_snapshot()`

# Calls

- [remember_mapi_identity](../../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [empty](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_associated_configs](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)
- [associated_config_message_for_mutation](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation.md)
- [expect](../../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)