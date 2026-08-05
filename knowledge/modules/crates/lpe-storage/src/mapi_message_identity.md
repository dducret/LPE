---
type: Rust Module
title: mapi_message_identity
resource: crates/lpe-storage/src/mapi_message_identity.rs#L1-L191
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-mapi-events-mapi-change-key-merge-predecessor-change-list-mapi-store-identity-allocate-mapi-store-global-counter-in-tx-ensure-mapi-mailbox-replica-in-tx-mapi-store-id-mapi-xid-mapimessageidentitymove-mapi-first-global-counter-mapi-first-reserved-high-global-counter-mapi-max-global-counter
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [rotate_active_mapi_message_identity_in_tx](../../../../functions/crates/lpe-storage/src/mapi_message_identity/rotate_active_mapi_message_identity_in_tx.md)
- [rekey_active_mapi_message_identity_for_server_move_in_tx](../../../../functions/crates/lpe-storage/src/mapi_message_identity/rekey_active_mapi_message_identity_for_server_move_in_tx.md)

# Imports

- `anyhow::{bail, Result}`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{
    mapi_events::{mapi_change_key, merge_predecessor_change_list},
    mapi_store_identity::{
        allocate_mapi_store_global_counter_in_tx, ensure_mapi_mailbox_replica_in_tx, mapi_store_id,
        mapi_xid, MapiMessageIdentityMove, MAPI_FIRST_GLOBAL_COUNTER,
        MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER, MAPI_MAX_GLOBAL_COUNTER,
    },
}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)