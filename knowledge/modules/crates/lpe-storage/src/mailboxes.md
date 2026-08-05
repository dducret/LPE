---
type: Rust Module
title: mailboxes
resource: crates/lpe-storage/src/mailboxes.rs#L1-L1374
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-domain-mailboxdisplayname-mailboxnamepolicy-mailboxpath
  - external/serde-serialize
  - external/sqlx-postgres-row
  - external/uuid-uuid
  - external/crate-imap-imapmailboxstate-shared-allocate-uid-validity-util-canonical-system-mailbox-display-name-system-mailbox-role-for-display-name-auditentryinput-jmapmailboxrow-storage
  - external/super-is-system-mailbox-role
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [JmapMailbox](../../../../classes/crates/lpe-storage/src/mailboxes/JmapMailbox.md)
- [JmapMailboxCreateInput](../../../../classes/crates/lpe-storage/src/mailboxes/JmapMailboxCreateInput.md)
- [JmapMailboxUpdateInput](../../../../classes/crates/lpe-storage/src/mailboxes/JmapMailboxUpdateInput.md)
- [ManagedRetentionFolderCreateInput](../../../../classes/crates/lpe-storage/src/mailboxes/ManagedRetentionFolderCreateInput.md)
- [fetch_jmap_mailboxes](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/fetch_jmap_mailboxes.md)
- [ensure_imap_mailboxes](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/ensure_imap_mailboxes.md)
- [fetch_imap_highest_modseq](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/fetch_imap_highest_modseq.md)
- [fetch_imap_mailbox_state](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/fetch_imap_mailbox_state.md)
- [fetch_jmap_mailbox_ids](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/fetch_jmap_mailbox_ids.md)
- [ensure_mailbox_name_available_in_tx](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_name_available_in_tx.md)
- [find_mailbox_by_name_in_tx](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/find_mailbox_by_name_in_tx.md)
- [insert_imap_custom_mailbox_in_tx](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/insert_imap_custom_mailbox_in_tx.md)
- [ensure_mailbox_parent_valid_in_tx](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_parent_valid_in_tx.md)
- [set_mailbox_subscription_in_tx](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription_in_tx.md)
- [create_jmap_mailbox](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox.md)
- [create_managed_retention_folder](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_managed_retention_folder.md)
- [create_imap_mailbox](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox.md)
- [update_jmap_mailbox](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/update_jmap_mailbox.md)
- [rename_imap_mailbox](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox.md)
- [set_mailbox_subscription](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/set_mailbox_subscription.md)
- [destroy_jmap_mailbox](../../../../functions/crates/lpe-storage/src/mailboxes/Storage/destroy_jmap_mailbox.md)
- [is_system_mailbox_role](../../../../functions/crates/lpe-storage/src/mailboxes/is_system_mailbox_role.md)
- [custom_mailbox_role_is_user_managed](../../../../functions/crates/lpe-storage/src/mailboxes/custom_mailbox_role_is_user_managed.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_domain::{MailboxDisplayName, MailboxNamePolicy, MailboxPath}`
- `serde::Serialize`
- `sqlx::{Postgres, Row}`
- `uuid::Uuid`
- `crate::{
    imap::ImapMailboxState,
    shared::allocate_uid_validity,
    util::{canonical_system_mailbox_display_name, system_mailbox_role_for_display_name},
    AuditEntryInput, JmapMailboxRow, Storage,
}`
- `super::is_system_mailbox_role`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)