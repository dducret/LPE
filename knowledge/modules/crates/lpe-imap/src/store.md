---
type: Rust Module
title: store
resource: crates/lpe-imap/src/store.rs#L1-L403
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-mail-auth-accountauthstore
  - external/lpe-storage-auditentryinput-imapemail-imapmailboxstate-jmapemailquery-jmapimportedemailinput-jmapmailbox-mailboxaccountaccess-mailboxdelegationgrant-mailboxdelegationgrantinput-saveddraftmessage-senderdelegationgrant-senderdelegationgrantinput-senderdelegationright-storage-submitmessageinput
  - external/std-future-future-pin-pin
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-imap
---

# Contains

- [ImapStore](../../../../interfaces/crates/lpe-imap/src/store/ImapStore.md)
- [ensure_imap_mailboxes](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/ensure_imap_mailboxes.md)
- [fetch_imap_highest_modseq](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/fetch_imap_highest_modseq.md)
- [fetch_imap_mailbox_state](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/fetch_imap_mailbox_state.md)
- [fetch_imap_emails](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/fetch_imap_emails.md)
- [update_imap_flags](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/update_imap_flags.md)
- [expunge_imap_deleted](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/expunge_imap_deleted.md)
- [query_jmap_email_ids](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/query_jmap_email_ids.md)
- [create_imap_mailbox](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/create_imap_mailbox.md)
- [rename_imap_mailbox](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/rename_imap_mailbox.md)
- [delete_imap_mailbox](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/delete_imap_mailbox.md)
- [set_mailbox_subscription](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/set_mailbox_subscription.md)
- [copy_imap_email](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/copy_imap_email.md)
- [move_imap_email](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/move_imap_email.md)
- [save_draft_message](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/save_draft_message.md)
- [import_imap_email](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/import_imap_email.md)
- [fetch_account_identity](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/fetch_account_identity.md)
- [fetch_outgoing_mailbox_delegation_grants](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/fetch_outgoing_mailbox_delegation_grants.md)
- [fetch_outgoing_sender_delegation_grants](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/fetch_outgoing_sender_delegation_grants.md)
- [upsert_mailbox_delegation_grant](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/upsert_mailbox_delegation_grant.md)
- [delete_mailbox_delegation_grant](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/delete_mailbox_delegation_grant.md)
- [upsert_sender_delegation_grant](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/upsert_sender_delegation_grant.md)
- [delete_sender_delegation_grant](../../../../functions/crates/lpe-imap/src/store/Storage/imapstore/delete_sender_delegation_grant.md)

# Imports

- `anyhow::Result`
- `lpe_mail_auth::AccountAuthStore`
- `lpe_storage::{
    AuditEntryInput, ImapEmail, ImapMailboxState, JmapEmailQuery, JmapImportedEmailInput,
    JmapMailbox, MailboxAccountAccess, MailboxDelegationGrant, MailboxDelegationGrantInput,
    SavedDraftMessage, SenderDelegationGrant, SenderDelegationGrantInput, SenderDelegationRight,
    Storage, SubmitMessageInput,
}`
- `std::{future::Future, pin::Pin}`
- `uuid::Uuid`

# Member of

- [lpe-imap](../../../../packages/crates/lpe-imap.md)