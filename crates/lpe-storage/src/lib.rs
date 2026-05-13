pub mod admin;
pub mod attachments;
pub mod auth;
mod blob_store;
pub mod calendar;
pub mod change;
pub mod collaboration;
pub mod core;
pub mod inbound;
pub mod mail;
mod message_ops;
pub mod models;
mod outbound;
pub mod protocols;
pub mod pst;
mod shared;
pub mod storage_policy;
pub mod storage_visibility;
pub mod submission;
pub mod tasks;
pub mod types;
pub mod util;
pub mod workspace;

#[cfg(test)]
mod schema_contract;

pub use crate::attachments::ClientAttachment;
pub use crate::auth::{
    AccountAppPassword, AccountAuthFactor, AccountCredentialInput, AccountLogin, AccountOidcClaims,
    AdminAuthFactor, AdminCredentialInput, AdminLogin, AdminOidcClaims, AuthenticatedAccount,
    AuthenticatedAdmin, NewAccountAuthFactor, NewAdminAuthFactor, StoredAccountAppPassword,
};
pub use crate::calendar::{
    calendar_attendee_labels, calendar_participant_label, normalize_calendar_email,
    normalize_calendar_participation_status, parse_calendar_participants_metadata,
    serialize_calendar_participants_metadata, CalendarOrganizerMetadata,
    CalendarParticipantMetadata, CalendarParticipantsMetadata,
};
pub use crate::change::{
    CanonicalChangeCategory, CanonicalChangeListener, CanonicalChangeReplay, CanonicalPushChangeSet,
};
pub use crate::collaboration::{
    AccessibleContact, AccessibleEvent, CollaborationCollection, CollaborationGrant,
    CollaborationGrantInput, CollaborationResourceKind, CollaborationRights,
};
pub use crate::core::Storage;
pub use crate::protocols::{
    ActiveSyncAttachment, ActiveSyncAttachmentContent, ActiveSyncItemState, ActiveSyncSyncState,
    ImapEmail, ImapMailboxState, ImapMimePart, JmapEmail, JmapEmailAddress, JmapEmailQuery,
    JmapEmailSubmission, JmapImportedEmailInput, JmapMailObjectChange, JmapMailbox,
    JmapMailboxCreateInput, JmapMailboxUpdateInput, JmapQuota, JmapStoredQueryState,
    JmapThreadQuery, JmapUploadBlob,
};
pub use crate::pst::{NewPstTransferJob, PstJobExecutionSummary, PstTransferJobRecord};
pub use crate::submission::{
    AttachmentUploadInput, MailboxAccountAccess, MailboxDelegationGrant,
    MailboxDelegationGrantInput, MailboxDelegationOverview, SavedDraftMessage,
    SenderAuthorizationKind, SenderDelegationGrant, SenderDelegationGrantInput,
    SenderDelegationRight, SenderIdentity, SubmissionAccountIdentity, SubmitMessageInput,
    SubmittedMessage, SubmittedRecipientInput,
};
pub use crate::tasks::{
    ClientTask, ClientTaskList, CreateTaskListInput, DavTask, TaskListGrant, TaskListGrantInput,
    UpdateTaskListInput, UpsertClientTaskInput,
};
pub use crate::types::{
    AccountRecord, AdminDashboard, AliasRecord, AntispamSettings, AuditEntryInput, AuditEvent,
    DashboardUpdate, DomainRecord, EmailTraceResult, EmailTraceSearchInput, FilterRule,
    HealthResponse, LocalAiSettings, MailFlowEntry, MailboxRecord, NewAccount, NewAlias, NewDomain,
    NewFilterRule, NewMailbox, NewServerAdministrator, NewStoragePool, OutboundQueueStatusUpdate,
    OverviewStats, ProtocolStatus, QuarantineItem, SecuritySettings, ServerAdministrator,
    ServerSettings, SieveScriptDocument, SieveScriptSummary, StorageCleanupCounts,
    StorageCleanupPlacementSummary, StorageCleanupVisibilityResponse, StorageHealthResponse,
    StorageMetadataDiagnostics, StorageMigrationCounts, StorageMigrationJobSummary,
    StorageMigrationVisibilityResponse, StorageOverview, StoragePlacementCounts,
    StoragePolicyOverview, StoragePolicyScope, StoragePolicySummary, StoragePolicyUpdate,
    StoragePoolHealth, StoragePoolReference, StoragePoolSummary, UpdateAccount, UpdateDomain,
    UpdateStoragePool,
};
pub use crate::workspace::{
    ClientContact, ClientEvent, ClientMessage, ClientWorkspace, UpsertClientContactInput,
    UpsertClientEventInput,
};

pub(crate) use crate::models::*;
pub(crate) use crate::pst::PstTransferJobRow;
pub(crate) use crate::shared::{
    CANONICAL_CHANGE_CHANNEL, DEFAULT_COLLECTION_ID, DEFAULT_TASK_LIST_NAME,
    DEFAULT_TASK_LIST_ROLE, EXPECTED_SCHEMA_VERSION, MAX_SIEVE_SCRIPTS_PER_ACCOUNT,
    MAX_SIEVE_SCRIPT_BYTES, PLATFORM_TENANT_ID,
};
pub(crate) use crate::tasks::{map_dav_task, map_task, map_task_list, map_task_list_grant};
pub(crate) use crate::util::*;
