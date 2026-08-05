---
type: Rust Module
title: mapi_store_identity
resource: crates/lpe-storage/src/mapi_store_identity.rs#L1-L174
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/sqlx-postgres-transaction
  - external/uuid-uuid
  - external/crate-jmapemail-storage
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [MapiStoreIdentity](../../../../classes/crates/lpe-storage/src/mapi_store_identity/MapiStoreIdentity.md)
- [MapiMessageImportedMoveIdentity](../../../../classes/crates/lpe-storage/src/mapi_store_identity/MapiMessageImportedMoveIdentity.md)
- [MapiMessageIdentityMove](../../../../classes/crates/lpe-storage/src/mapi_store_identity/MapiMessageIdentityMove.md)
- [MapiMessageMoveResult](../../../../classes/crates/lpe-storage/src/mapi_store_identity/MapiMessageMoveResult.md)
- [fetch_mapi_store_identity](../../../../functions/crates/lpe-storage/src/mapi_store_identity/Storage/fetch_mapi_store_identity.md)
- [ensure_mapi_store_identity_in_tx](../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_store_identity_in_tx.md)
- [ensure_mapi_mailbox_replica_in_tx](../../../../functions/crates/lpe-storage/src/mapi_store_identity/ensure_mapi_mailbox_replica_in_tx.md)
- [allocate_mapi_store_global_counter_in_tx](../../../../functions/crates/lpe-storage/src/mapi_store_identity/allocate_mapi_store_global_counter_in_tx.md)
- [reserve_mapi_store_global_counter_range_in_tx](../../../../functions/crates/lpe-storage/src/mapi_store_identity/reserve_mapi_store_global_counter_range_in_tx.md)
- [mapi_store_id](../../../../functions/crates/lpe-storage/src/mapi_store_identity/mapi_store_id.md)
- [mapi_xid](../../../../functions/crates/lpe-storage/src/mapi_store_identity/mapi_xid.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `sqlx::{Postgres, Transaction}`
- `uuid::Uuid`
- `crate::{JmapEmail, Storage}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)