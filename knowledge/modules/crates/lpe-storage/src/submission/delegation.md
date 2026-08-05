---
type: Rust Module
title: delegation
resource: crates/lpe-storage/src/submission/delegation.rs#L1-L809
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-normalize-email-auditentryinput-mailboxaccountaccessrow-mailboxdelegationgrantrow-senderdelegationgrantrow-storage
  - external/super-types-map-mailbox-delegation-grant-map-sender-delegation-grant-sender-identity-id-validate-mailbox-delegation-rights-mailboxaccountaccess-mailboxdelegationgrant-mailboxdelegationgrantinput-mailboxfolderdelegationgrantinput-senderauthorizationkind-senderdelegationgrant-senderdelegationgrantinput-senderdelegationright-senderidentity
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [fetch_account_identity](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_account_identity.md)
- [upsert_mailbox_delegation_grant](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_mailbox_delegation_grant.md)
- [set_mailbox_folder_delegation_grant](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/set_mailbox_folder_delegation_grant.md)
- [delete_mailbox_delegation_grant](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/delete_mailbox_delegation_grant.md)
- [default_mailbox_delegation_mailbox_id](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/default_mailbox_delegation_mailbox_id.md)
- [fetch_mailbox_delegation_grant](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_mailbox_delegation_grant.md)
- [upsert_sender_delegation_grant](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_sender_delegation_grant.md)
- [delete_sender_delegation_grant](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/delete_sender_delegation_grant.md)
- [fetch_sender_delegation_grant](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_sender_delegation_grant.md)
- [fetch_outgoing_mailbox_delegation_grants](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_outgoing_mailbox_delegation_grants.md)
- [fetch_outgoing_sender_delegation_grants](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_outgoing_sender_delegation_grants.md)
- [fetch_accessible_mailbox_accounts](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_accessible_mailbox_accounts.md)
- [require_mailbox_account_access](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/require_mailbox_account_access.md)
- [fetch_sender_identities](../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_sender_identities.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{
    normalize_email, AuditEntryInput, MailboxAccountAccessRow, MailboxDelegationGrantRow,
    SenderDelegationGrantRow, Storage,
}`
- `super::types::{
    map_mailbox_delegation_grant, map_sender_delegation_grant, sender_identity_id,
    validate_mailbox_delegation_rights, MailboxAccountAccess, MailboxDelegationGrant,
    MailboxDelegationGrantInput, MailboxFolderDelegationGrantInput, SenderAuthorizationKind,
    SenderDelegationGrant, SenderDelegationGrantInput, SenderDelegationRight, SenderIdentity,
}`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)