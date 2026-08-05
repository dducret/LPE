---
type: Rust Module
title: provisioning
resource: crates/lpe-storage/src/admin/provisioning.rs#L1-L396
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-domain-mailboxdisplayname
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-shared-allocate-uid-validity
  - external/crate-normalize-directory-kind-normalize-domain-name-normalize-email-normalize-gal-visibility-auditentryinput-newaccount-newalias-newdomain-newmailbox-newpsttransferjob-storage-updateaccount-updatedomain-platform-tenant-id
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [create_account](../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_account.md)
- [update_account](../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/update_account.md)
- [create_mailbox](../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_mailbox.md)
- [create_pst_transfer_job](../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_pst_transfer_job.md)
- [create_domain](../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_domain.md)
- [update_domain](../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/update_domain.md)
- [create_alias](../../../../../functions/crates/lpe-storage/src/admin/provisioning/Storage/create_alias.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_domain::MailboxDisplayName`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::shared::allocate_uid_validity`
- `crate::{
    normalize_directory_kind, normalize_domain_name, normalize_email, normalize_gal_visibility,
    AuditEntryInput, NewAccount, NewAlias, NewDomain, NewMailbox, NewPstTransferJob, Storage,
    UpdateAccount, UpdateDomain, PLATFORM_TENANT_ID,
}`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)