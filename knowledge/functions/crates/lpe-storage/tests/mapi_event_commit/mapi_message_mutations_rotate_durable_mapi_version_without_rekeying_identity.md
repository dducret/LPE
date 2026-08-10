---
type: Rust Function
title: mapi_message_mutations_rotate_durable_mapi_version_without_rekeying_identity
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L554-L1278
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/tests/mapi_event_commit/change_key
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn mapi_message_mutations_rotate_durable_mapi_version_without_rekeying_identity() -> Result<()>`

# Calls

- [event_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [change_key](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/change_key.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)