---
type: Rust Function
title: associated_config_mutation_uses_saved_handle_when_snapshot_misses_row
resource: crates/lpe-exchange/src/mapi/dispatch/tests/associated_config.rs#L332-L361
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn associated_config_mutation_uses_saved_handle_when_snapshot_misses_row()`

# Calls

- [associated_config_message_for_mutation](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation.md)
- [empty](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [expect](../../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)