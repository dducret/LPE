---
type: Rust Function
title: delegated_mapi_event_create_uses_owner_scope_for_event_and_custom_properties
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L2304-L2399
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn delegated_mapi_event_create_uses_owner_scope_for_event_and_custom_properties() -> Result<()>`

# Calls

- [event_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)