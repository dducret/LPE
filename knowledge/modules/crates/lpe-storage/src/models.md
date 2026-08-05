---
type: Rust Module
title: models
resource: crates/lpe-storage/src/models.rs#L1-L861
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/serde-json-value
  - external/sqlx-fromrow
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [AccountRow](../../../../classes/crates/lpe-storage/src/models/AccountRow.md)
- [MailboxRow](../../../../classes/crates/lpe-storage/src/models/MailboxRow.md)
- [DomainRow](../../../../classes/crates/lpe-storage/src/models/DomainRow.md)
- [AliasRow](../../../../classes/crates/lpe-storage/src/models/AliasRow.md)
- [AuditRow](../../../../classes/crates/lpe-storage/src/models/AuditRow.md)
- [JmapMailboxRow](../../../../classes/crates/lpe-storage/src/models/JmapMailboxRow.md)
- [JmapEmailRow](../../../../classes/crates/lpe-storage/src/models/JmapEmailRow.md)
- [SearchFolderRow](../../../../classes/crates/lpe-storage/src/models/SearchFolderRow.md)
- [ImapEmailRow](../../../../classes/crates/lpe-storage/src/models/ImapEmailRow.md)
- [JmapEmailRecipientRow](../../../../classes/crates/lpe-storage/src/models/JmapEmailRecipientRow.md)
- [JmapEmailSubmissionRow](../../../../classes/crates/lpe-storage/src/models/JmapEmailSubmissionRow.md)
- [MailboxAccountAccessRow](../../../../classes/crates/lpe-storage/src/models/MailboxAccountAccessRow.md)
- [MailboxDelegationGrantRow](../../../../classes/crates/lpe-storage/src/models/MailboxDelegationGrantRow.md)
- [SenderDelegationGrantRow](../../../../classes/crates/lpe-storage/src/models/SenderDelegationGrantRow.md)
- [PendingOutboundQueueRow](../../../../classes/crates/lpe-storage/src/models/PendingOutboundQueueRow.md)
- [OutboundQueueStateRow](../../../../classes/crates/lpe-storage/src/models/OutboundQueueStateRow.md)
- [MessageBccRecipientRow](../../../../classes/crates/lpe-storage/src/models/MessageBccRecipientRow.md)
- [MessageBccRecipientRecordRow](../../../../classes/crates/lpe-storage/src/models/MessageBccRecipientRecordRow.md)
- [AccountQuotaRow](../../../../classes/crates/lpe-storage/src/models/AccountQuotaRow.md)
- [JmapUploadBlobRow](../../../../classes/crates/lpe-storage/src/models/JmapUploadBlobRow.md)
- [ServerAdministratorRow](../../../../classes/crates/lpe-storage/src/models/ServerAdministratorRow.md)
- [AdminLoginRow](../../../../classes/crates/lpe-storage/src/models/AdminLoginRow.md)
- [AccountLoginRow](../../../../classes/crates/lpe-storage/src/models/AccountLoginRow.md)
- [AuthenticatedAdminRow](../../../../classes/crates/lpe-storage/src/models/AuthenticatedAdminRow.md)
- [AdminAuthFactorRow](../../../../classes/crates/lpe-storage/src/models/AdminAuthFactorRow.md)
- [AccountAuthFactorRow](../../../../classes/crates/lpe-storage/src/models/AccountAuthFactorRow.md)
- [AccountAppPasswordRow](../../../../classes/crates/lpe-storage/src/models/AccountAppPasswordRow.md)
- [AuthenticatedAccountRow](../../../../classes/crates/lpe-storage/src/models/AuthenticatedAccountRow.md)
- [ActiveSyncSyncStateRow](../../../../classes/crates/lpe-storage/src/models/ActiveSyncSyncStateRow.md)
- [ActiveSyncDeviceRow](../../../../classes/crates/lpe-storage/src/models/ActiveSyncDeviceRow.md)
- [ClientMessageRow](../../../../classes/crates/lpe-storage/src/models/ClientMessageRow.md)
- [ClientAttachmentRow](../../../../classes/crates/lpe-storage/src/models/ClientAttachmentRow.md)
- [ClientEventRow](../../../../classes/crates/lpe-storage/src/models/ClientEventRow.md)
- [ClientContactRow](../../../../classes/crates/lpe-storage/src/models/ClientContactRow.md)
- [CollaborationCollectionRow](../../../../classes/crates/lpe-storage/src/models/CollaborationCollectionRow.md)
- [CollaborationGrantRow](../../../../classes/crates/lpe-storage/src/models/CollaborationGrantRow.md)
- [AccessibleContactRow](../../../../classes/crates/lpe-storage/src/models/AccessibleContactRow.md)
- [AccessibleEventRow](../../../../classes/crates/lpe-storage/src/models/AccessibleEventRow.md)
- [FreeBusyEventRow](../../../../classes/crates/lpe-storage/src/models/FreeBusyEventRow.md)
- [ClientTaskListRow](../../../../classes/crates/lpe-storage/src/models/ClientTaskListRow.md)
- [ClientTaskRow](../../../../classes/crates/lpe-storage/src/models/ClientTaskRow.md)
- [ClientNoteRow](../../../../classes/crates/lpe-storage/src/models/ClientNoteRow.md)
- [JournalEntryRow](../../../../classes/crates/lpe-storage/src/models/JournalEntryRow.md)
- [ClientReminderRow](../../../../classes/crates/lpe-storage/src/models/ClientReminderRow.md)
- [PublicFolderTreeRow](../../../../classes/crates/lpe-storage/src/models/PublicFolderTreeRow.md)
- [PublicFolderRow](../../../../classes/crates/lpe-storage/src/models/PublicFolderRow.md)
- [PublicFolderItemRow](../../../../classes/crates/lpe-storage/src/models/PublicFolderItemRow.md)
- [PublicFolderPermissionRow](../../../../classes/crates/lpe-storage/src/models/PublicFolderPermissionRow.md)
- [PublicFolderReplicaRow](../../../../classes/crates/lpe-storage/src/models/PublicFolderReplicaRow.md)
- [PublicFolderPerUserStateRow](../../../../classes/crates/lpe-storage/src/models/PublicFolderPerUserStateRow.md)
- [DavTaskRow](../../../../classes/crates/lpe-storage/src/models/DavTaskRow.md)
- [TaskListGrantRow](../../../../classes/crates/lpe-storage/src/models/TaskListGrantRow.md)
- [EmailTraceRow](../../../../classes/crates/lpe-storage/src/models/EmailTraceRow.md)
- [MailFlowRow](../../../../classes/crates/lpe-storage/src/models/MailFlowRow.md)

# Imports

- `serde_json::Value`
- `sqlx::FromRow`
- `uuid::Uuid`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)