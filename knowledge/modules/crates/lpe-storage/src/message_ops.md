---
type: Rust Module
title: message_ops
resource: crates/lpe-storage/src/message_ops.rs#L1-L1525
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-mapi-events-merge-predecessor-change-list-mapi-message-identity-rekey-active-mapi-message-identity-for-server-move-in-tx-rotate-active-mapi-message-identity-in-tx-mapi-store-identity-allocate-mapi-store-global-counter-in-tx-ensure-mapi-mailbox-replica-in-tx-ensure-mapi-store-identity-in-tx-mapi-store-id-mapimessageidentitymove-mapimessageimportedmoveidentity-mapimessagemoveresult-mapi-first-global-counter-mapi-first-reserved-high-global-counter-mapi-max-global-counter-sha256-hex-submission-activesyncsyncstate-activesyncsyncstaterow-auditentryinput-canonicalchangecategory-jmapemail-jmapimportedemailinput-storage
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [delete_client_contact](../../../../functions/crates/lpe-storage/src/message_ops/Storage/delete_client_contact.md)
- [delete_client_event](../../../../functions/crates/lpe-storage/src/message_ops/Storage/delete_client_event.md)
- [copy_jmap_email](../../../../functions/crates/lpe-storage/src/message_ops/Storage/copy_jmap_email.md)
- [copy_jmap_email_between_accounts](../../../../functions/crates/lpe-storage/src/message_ops/Storage/copy_jmap_email_between_accounts.md)
- [move_jmap_email](../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email.md)
- [move_jmap_email_from_mailbox](../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_from_mailbox.md)
- [move_jmap_email_from_mailbox_with_mapi_identity](../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_from_mailbox_with_mapi_identity.md)
- [move_jmap_email_membership](../../../../functions/crates/lpe-storage/src/message_ops/Storage/move_jmap_email_membership.md)
- [update_jmap_email_flags](../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_flags.md)
- [update_jmap_email_followup_flags](../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_followup_flags.md)
- [update_jmap_email_content](../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_content.md)
- [import_jmap_email](../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)
- [fetch_latest_activesync_sync_state](../../../../functions/crates/lpe-storage/src/message_ops/Storage/fetch_latest_activesync_sync_state.md)
- [rekey_mapi_message_identity_in_tx](../../../../functions/crates/lpe-storage/src/message_ops/rekey_mapi_message_identity_in_tx.md)
- [imported_message_move_destination_global_counter](../../../../functions/crates/lpe-storage/src/message_ops/imported_message_move_destination_global_counter.md)
- [normalize_mail_categories](../../../../functions/crates/lpe-storage/src/message_ops/normalize_mail_categories.md)

# Imports

- `anyhow::{bail, Result}`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{
    mapi_events::merge_predecessor_change_list,
    mapi_message_identity::{
        rekey_active_mapi_message_identity_for_server_move_in_tx,
        rotate_active_mapi_message_identity_in_tx,
    },
    mapi_store_identity::{
        allocate_mapi_store_global_counter_in_tx, ensure_mapi_mailbox_replica_in_tx,
        ensure_mapi_store_identity_in_tx, mapi_store_id, MapiMessageIdentityMove,
        MapiMessageImportedMoveIdentity, MapiMessageMoveResult, MAPI_FIRST_GLOBAL_COUNTER,
        MAPI_FIRST_RESERVED_HIGH_GLOBAL_COUNTER, MAPI_MAX_GLOBAL_COUNTER,
    },
    sha256_hex, submission, ActiveSyncSyncState, ActiveSyncSyncStateRow, AuditEntryInput,
    CanonicalChangeCategory, JmapEmail, JmapImportedEmailInput, Storage,
}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)