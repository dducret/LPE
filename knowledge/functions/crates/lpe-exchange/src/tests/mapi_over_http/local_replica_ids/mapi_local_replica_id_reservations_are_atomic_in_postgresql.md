---
type: Rust Function
title: mapi_local_replica_id_reservations_are_atomic_in_postgresql
resource: crates/lpe-exchange/src/tests/mapi_over_http/local_replica_ids.rs#L139-L171
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-storage/src/core/Storage/pool
---

# Signature

`async fn mapi_local_replica_id_reservations_are_atomic_in_postgresql() -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)