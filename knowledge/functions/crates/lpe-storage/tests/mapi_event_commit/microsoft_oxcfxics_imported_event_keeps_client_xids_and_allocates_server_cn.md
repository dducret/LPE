---
type: Rust Function
title: microsoft_oxcfxics_imported_event_keeps_client_xids_and_allocates_server_cn
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L1719-L1790
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture
  - functions/crates/lpe-storage/tests/mapi_event_commit/reserve_imported_event_range
  - functions/crates/lpe-storage/tests/mapi_event_commit/change_key
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn microsoft_oxcfxics_imported_event_keeps_client_xids_and_allocates_server_cn() -> Result<()>`

# Calls

- [event_fixture](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [reserve_imported_event_range](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/reserve_imported_event_range.md)
- [change_key](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/change_key.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)