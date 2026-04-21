use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_attachments::extract_text_from_bytes;
use lpe_core::sieve::{
    evaluate_script, parse_script, ExecutionOutcome as SieveExecutionOutcome,
    MessageContext as SieveMessageContext, VacationAction,
};
use lpe_domain::{
    InboundDeliveryRequest, InboundDeliveryResponse, OutboundMessageHandoffRequest,
    OutboundMessageHandoffResponse, TransportDeliveryStatus, TransportRecipient,
};
use lpe_magika::{
    read_validation_record, ExpectedKind, IngressContext, PolicyDecision, ValidationRequest,
    Validator,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgListener, Executor, FromRow, Pool, Postgres, Row};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use uuid::Uuid;

pub mod mail;

use crate::mail::{parse_header_recipients, parse_headers_map, parse_message_attachments};

const PLATFORM_TENANT_ID: &str = "__platform__";
const MAX_SIEVE_SCRIPT_BYTES: usize = 64 * 1024;
const MAX_SIEVE_SCRIPTS_PER_ACCOUNT: i64 = 16;
const MAX_SIEVE_REDIRECTS_PER_MESSAGE: usize = 4;
const DEFAULT_SIEVE_MAILBOX_RETENTION_DAYS: i32 = 365;
const DEFAULT_COLLECTION_ID: &str = "default";
const DEFAULT_TASK_LIST_NAME: &str = "Tasks";
const DEFAULT_TASK_LIST_ROLE: &str = "inbox";
const CANONICAL_CHANGE_CHANNEL: &str = "lpe_canonical_changes";
const EXPECTED_SCHEMA_VERSION: &str = "0.1.5";

#[derive(Clone)]
pub struct Storage {
    pool: Pool<Postgres>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CanonicalChangeCategory {
    Mail,
    Contacts,
    Calendar,
    Tasks,
}

impl CanonicalChangeCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mail => "mail",
            Self::Contacts => "contacts",
            Self::Calendar => "calendar",
            Self::Tasks => "tasks",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "mail" => Some(Self::Mail),
            "contacts" => Some(Self::Contacts),
            "calendar" => Some(Self::Calendar),
            "tasks" => Some(Self::Tasks),
            _ => None,
        }
    }
}

pub struct CanonicalChangeListener {
    principal_account_id: Uuid,
    tenant_id: String,
    listener: PgListener,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CanonicalPushChangeSet {
    scoped_accounts: HashMap<CanonicalChangeCategory, HashSet<Uuid>>,
}

impl CanonicalPushChangeSet {
    pub fn is_empty(&self) -> bool {
        self.scoped_accounts.values().all(HashSet::is_empty)
    }

    pub fn insert_accounts<I>(&mut self, category: CanonicalChangeCategory, account_ids: I)
    where
        I: IntoIterator<Item = Uuid>,
    {
        self.scoped_accounts
            .entry(category)
            .or_default()
            .extend(account_ids);
    }

    pub fn accounts_for(&self, category: CanonicalChangeCategory) -> HashSet<Uuid> {
        self.scoped_accounts
            .get(&category)
            .cloned()
            .unwrap_or_default()
    }

    pub fn contains_category(&self, category: CanonicalChangeCategory) -> bool {
        self.scoped_accounts
            .get(&category)
            .is_some_and(|accounts| !accounts.is_empty())
    }
}

impl CanonicalChangeListener {
    pub async fn wait_for_change(
        &mut self,
        categories: &[CanonicalChangeCategory],
    ) -> Result<CanonicalPushChangeSet> {
        let categories = categories.iter().copied().collect::<HashSet<_>>();
        if categories.is_empty() {
            return Ok(CanonicalPushChangeSet::default());
        }

        loop {
            let notification = self.listener.recv().await?;
            let Ok(payload) =
                serde_json::from_str::<CanonicalChangeNotification>(notification.payload())
            else {
                continue;
            };
            if payload.tenant_id != self.tenant_id {
                continue;
            }

            let Some(category) = CanonicalChangeCategory::from_str(&payload.category) else {
                continue;
            };
            if !categories.contains(&category) {
                continue;
            }
            if !payload
                .principal_account_ids
                .iter()
                .any(|value| value == &self.principal_account_id.to_string())
            {
                continue;
            }

            let mut changes = CanonicalPushChangeSet::default();
            changes.insert_accounts(
                category,
                payload
                    .account_ids
                    .iter()
                    .filter_map(|value| Uuid::parse_str(value).ok()),
            );
            if !changes.is_empty() {
                return Ok(changes);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminDashboard {
    pub health: HealthResponse,
    pub overview: OverviewStats,
    pub protocols: Vec<ProtocolStatus>,
    pub accounts: Vec<AccountRecord>,
    pub domains: Vec<DomainRecord>,
    pub aliases: Vec<AliasRecord>,
    pub server_admins: Vec<ServerAdministrator>,
    pub server_settings: ServerSettings,
    pub security_settings: SecuritySettings,
    pub local_ai_settings: LocalAiSettings,
    pub antispam_settings: AntispamSettings,
    pub antispam_rules: Vec<FilterRule>,
    pub quarantine_items: Vec<QuarantineItem>,
    pub storage: StorageOverview,
    pub audit_log: Vec<AuditEvent>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub service: &'static str,
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct OverviewStats {
    pub total_accounts: usize,
    pub total_mailboxes: usize,
    pub total_domains: usize,
    pub total_aliases: usize,
    pub pending_queue_items: u32,
    pub attachment_formats: usize,
    pub local_ai_enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProtocolStatus {
    pub name: String,
    pub enabled: bool,
    pub bind_address: String,
    pub state: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MailboxRecord {
    pub id: Uuid,
    pub display_name: String,
    pub role: String,
    pub message_count: u32,
    pub retention_days: u16,
    pub pst_jobs: Vec<PstTransferJobRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PstTransferJobRecord {
    pub id: Uuid,
    pub direction: String,
    pub server_path: String,
    pub status: String,
    pub requested_by: String,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub processed_messages: u32,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountRecord {
    pub id: Uuid,
    pub email: String,
    pub display_name: String,
    pub quota_mb: u32,
    pub used_mb: u32,
    pub status: String,
    pub gal_visibility: String,
    pub directory_kind: String,
    pub mailboxes: Vec<MailboxRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DomainRecord {
    pub id: Uuid,
    pub name: String,
    pub status: String,
    pub inbound_enabled: bool,
    pub outbound_enabled: bool,
    pub default_quota_mb: u32,
    pub default_sieve_script: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AliasRecord {
    pub id: Uuid,
    pub source: String,
    pub target: String,
    pub kind: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerAdministrator {
    pub id: Uuid,
    pub domain_id: Option<Uuid>,
    pub domain_name: String,
    pub email: String,
    pub display_name: String,
    pub role: String,
    pub rights_summary: String,
    pub permissions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthenticatedAdmin {
    pub tenant_id: String,
    pub email: String,
    pub display_name: String,
    pub role: String,
    pub domain_id: Option<Uuid>,
    pub domain_name: String,
    pub rights_summary: String,
    pub permissions: Vec<String>,
    pub auth_method: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminAuthFactor {
    pub id: Uuid,
    pub factor_type: String,
    pub status: String,
    pub created_at: String,
    pub verified_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerSettings {
    pub primary_hostname: String,
    pub admin_bind_address: String,
    pub smtp_bind_address: String,
    pub imap_bind_address: String,
    pub jmap_bind_address: String,
    pub default_locale: String,
    pub max_message_size_mb: u32,
    pub tls_mode: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SecuritySettings {
    pub password_login_enabled: bool,
    pub mfa_required_for_admins: bool,
    pub session_timeout_minutes: u32,
    pub audit_retention_days: u32,
    pub oidc_login_enabled: bool,
    pub oidc_provider_label: String,
    pub oidc_auto_link_by_email: bool,
    pub oidc_issuer_url: String,
    pub oidc_authorization_endpoint: String,
    pub oidc_token_endpoint: String,
    pub oidc_userinfo_endpoint: String,
    pub oidc_client_id: String,
    pub oidc_client_secret: String,
    pub oidc_scopes: String,
    pub oidc_claim_email: String,
    pub oidc_claim_display_name: String,
    pub oidc_claim_subject: String,
    pub mailbox_password_login_enabled: bool,
    pub mailbox_oidc_login_enabled: bool,
    pub mailbox_oidc_provider_label: String,
    pub mailbox_oidc_auto_link_by_email: bool,
    pub mailbox_oidc_issuer_url: String,
    pub mailbox_oidc_authorization_endpoint: String,
    pub mailbox_oidc_token_endpoint: String,
    pub mailbox_oidc_userinfo_endpoint: String,
    pub mailbox_oidc_client_id: String,
    pub mailbox_oidc_client_secret: String,
    pub mailbox_oidc_scopes: String,
    pub mailbox_oidc_claim_email: String,
    pub mailbox_oidc_claim_display_name: String,
    pub mailbox_oidc_claim_subject: String,
    pub mailbox_app_passwords_enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct LocalAiSettings {
    pub enabled: bool,
    pub provider: String,
    pub model: String,
    pub offline_only: bool,
    pub indexing_enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AntispamSettings {
    pub content_filtering_enabled: bool,
    pub spam_engine: String,
    pub quarantine_enabled: bool,
    pub quarantine_retention_days: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilterRule {
    pub id: Uuid,
    pub name: String,
    pub scope: String,
    pub action: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuarantineItem {
    pub id: Uuid,
    pub message_ref: String,
    pub sender: String,
    pub recipient: String,
    pub reason: String,
    pub status: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageOverview {
    pub primary_store: String,
    pub search_engine: String,
    pub attachment_formats: Vec<String>,
    pub replication_mode: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditEvent {
    pub id: Uuid,
    pub timestamp: String,
    pub actor: String,
    pub action: String,
    pub subject: String,
}

#[derive(Debug, Clone)]
pub struct NewAccount {
    pub email: String,
    pub display_name: String,
    pub quota_mb: u32,
    pub gal_visibility: String,
    pub directory_kind: String,
}

#[derive(Debug, Clone)]
pub struct UpdateAccount {
    pub account_id: Uuid,
    pub display_name: String,
    pub quota_mb: u32,
    pub status: String,
    pub gal_visibility: String,
    pub directory_kind: String,
    pub password_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewMailbox {
    pub account_id: Uuid,
    pub display_name: String,
    pub role: String,
    pub retention_days: u16,
}

#[derive(Debug, Clone)]
pub struct NewPstTransferJob {
    pub mailbox_id: Uuid,
    pub direction: String,
    pub server_path: String,
    pub requested_by: String,
}

#[derive(Debug, Clone)]
pub struct NewDomain {
    pub name: String,
    pub default_quota_mb: u32,
    pub inbound_enabled: bool,
    pub outbound_enabled: bool,
    pub default_sieve_script: String,
}

#[derive(Debug, Clone)]
pub struct UpdateDomain {
    pub domain_id: Uuid,
    pub default_quota_mb: u32,
    pub inbound_enabled: bool,
    pub outbound_enabled: bool,
    pub default_sieve_script: String,
}

#[derive(Debug, Clone)]
pub struct NewAlias {
    pub source: String,
    pub target: String,
    pub kind: String,
}

#[derive(Debug, Clone)]
pub struct AuditEntryInput {
    pub actor: String,
    pub action: String,
    pub subject: String,
}

#[derive(Debug, Clone)]
pub struct DashboardUpdate {
    pub server_settings: ServerSettings,
    pub security_settings: SecuritySettings,
    pub local_ai_settings: LocalAiSettings,
    pub antispam_settings: AntispamSettings,
}

#[derive(Debug, Clone)]
pub struct NewServerAdministrator {
    pub domain_id: Option<Uuid>,
    pub email: String,
    pub display_name: String,
    pub role: String,
    pub rights_summary: String,
    pub permissions: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AdminCredentialInput {
    pub email: String,
    pub password_hash: String,
}

#[derive(Debug, Clone)]
pub struct AccountCredentialInput {
    pub email: String,
    pub password_hash: String,
}

#[derive(Debug, Clone)]
pub struct AdminLogin {
    pub tenant_id: String,
    pub email: String,
    pub password_hash: String,
    pub status: String,
    pub display_name: String,
    pub role: String,
    pub domain_id: Option<Uuid>,
    pub domain_name: String,
    pub rights_summary: String,
    pub permissions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminOidcClaims {
    pub issuer_url: String,
    pub subject: String,
    pub email: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountOidcClaims {
    pub issuer_url: String,
    pub subject: String,
    pub email: String,
    pub display_name: String,
}

#[derive(Debug, Clone)]
pub struct NewAdminAuthFactor {
    pub admin_email: String,
    pub factor_type: String,
    pub secret_ciphertext: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountAuthFactor {
    pub id: Uuid,
    pub factor_type: String,
    pub status: String,
    pub created_at: String,
    pub verified_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewAccountAuthFactor {
    pub account_email: String,
    pub factor_type: String,
    pub secret_ciphertext: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountAppPassword {
    pub id: Uuid,
    pub label: String,
    pub status: String,
    pub created_at: String,
    pub last_used_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StoredAccountAppPassword {
    pub id: Uuid,
    pub password_hash: String,
}

#[derive(Debug, Clone)]
pub struct AccountLogin {
    pub tenant_id: String,
    pub account_id: Uuid,
    pub email: String,
    pub password_hash: String,
    pub status: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthenticatedAccount {
    pub tenant_id: String,
    pub account_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncSyncState {
    pub sync_key: String,
    pub snapshot_json: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncItemState {
    pub id: Uuid,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PstJobExecutionSummary {
    pub processed_jobs: u32,
    pub completed_jobs: u32,
    pub failed_jobs: u32,
}

#[derive(Debug, Clone)]
pub struct NewFilterRule {
    pub name: String,
    pub scope: String,
    pub action: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct EmailTraceSearchInput {
    pub query: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct EmailTraceResult {
    pub message_id: Uuid,
    pub internet_message_id: Option<String>,
    pub subject: String,
    pub sender: String,
    pub account_email: String,
    pub mailbox: String,
    pub received_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientWorkspace {
    pub messages: Vec<ClientMessage>,
    pub events: Vec<ClientEvent>,
    pub contacts: Vec<ClientContact>,
    pub tasks: Vec<ClientTask>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientMessage {
    pub id: Uuid,
    pub folder: String,
    pub from: String,
    pub from_address: String,
    pub to: String,
    pub cc: String,
    pub subject: String,
    pub preview: String,
    pub received_at: String,
    pub time_label: String,
    pub unread: bool,
    pub flagged: bool,
    pub tags: Vec<String>,
    pub attachments: Vec<ClientAttachment>,
    pub body: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientAttachment {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub size: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncAttachment {
    pub id: Uuid,
    pub message_id: Uuid,
    pub file_name: String,
    pub media_type: String,
    pub size_octets: u64,
    pub file_reference: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSyncAttachmentContent {
    pub file_reference: String,
    pub file_name: String,
    pub media_type: String,
    pub blob_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientEvent {
    pub id: Uuid,
    pub date: String,
    pub time: String,
    pub time_zone: String,
    pub duration_minutes: i32,
    pub recurrence_rule: String,
    pub title: String,
    pub location: String,
    pub attendees: String,
    pub attendees_json: String,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CalendarParticipantMetadata {
    pub email: String,
    pub common_name: String,
    pub role: String,
    pub partstat: String,
    pub rsvp: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CalendarOrganizerMetadata {
    pub email: String,
    pub common_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CalendarParticipantsMetadata {
    pub organizer: Option<CalendarOrganizerMetadata>,
    pub attendees: Vec<CalendarParticipantMetadata>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientContact {
    pub id: Uuid,
    pub name: String,
    pub role: String,
    pub email: String,
    pub phone: String,
    pub team: String,
    pub notes: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CollaborationResourceKind {
    Contacts,
    Calendar,
    Tasks,
}

impl CollaborationResourceKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Contacts => "contacts",
            Self::Calendar => "calendar",
            Self::Tasks => "tasks",
        }
    }

    pub fn collection_label(&self) -> &'static str {
        match self {
            Self::Contacts => "Contacts",
            Self::Calendar => "Calendar",
            Self::Tasks => "Task List",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaborationRights {
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaborationCollection {
    pub id: String,
    pub kind: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub display_name: String,
    pub is_owned: bool,
    pub rights: CollaborationRights,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessibleContact {
    pub id: Uuid,
    pub collection_id: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub rights: CollaborationRights,
    pub name: String,
    pub role: String,
    pub email: String,
    pub phone: String,
    pub team: String,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessibleEvent {
    pub id: Uuid,
    pub collection_id: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub rights: CollaborationRights,
    pub date: String,
    pub time: String,
    pub time_zone: String,
    pub duration_minutes: i32,
    pub recurrence_rule: String,
    pub title: String,
    pub location: String,
    pub attendees: String,
    pub attendees_json: String,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaborationGrant {
    pub id: Uuid,
    pub kind: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub grantee_account_id: Uuid,
    pub grantee_email: String,
    pub grantee_display_name: String,
    pub rights: CollaborationRights,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct CollaborationGrantInput {
    pub kind: CollaborationResourceKind,
    pub owner_account_id: Uuid,
    pub grantee_email: String,
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskListGrant {
    pub id: Uuid,
    pub task_list_id: Uuid,
    pub task_list_name: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub grantee_account_id: Uuid,
    pub grantee_email: String,
    pub grantee_display_name: String,
    pub rights: CollaborationRights,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct TaskListGrantInput {
    pub owner_account_id: Uuid,
    pub task_list_id: Uuid,
    pub grantee_email: String,
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

#[derive(Debug, Clone)]
pub struct UpsertClientContactInput {
    pub id: Option<Uuid>,
    pub account_id: Uuid,
    pub name: String,
    pub role: String,
    pub email: String,
    pub phone: String,
    pub team: String,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientTaskList {
    pub id: Uuid,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub is_owned: bool,
    pub rights: CollaborationRights,
    pub name: String,
    pub role: Option<String>,
    pub sort_order: i32,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientTask {
    pub id: Uuid,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub is_owned: bool,
    pub rights: CollaborationRights,
    pub task_list_id: Uuid,
    pub task_list_sort_order: i32,
    pub title: String,
    pub description: String,
    pub status: String,
    pub due_at: Option<String>,
    pub completed_at: Option<String>,
    pub sort_order: i32,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DavTask {
    pub id: Uuid,
    pub collection_id: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub rights: CollaborationRights,
    pub task_list_id: Uuid,
    pub task_list_name: String,
    pub title: String,
    pub description: String,
    pub status: String,
    pub due_at: Option<String>,
    pub completed_at: Option<String>,
    pub sort_order: i32,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct UpsertClientEventInput {
    pub id: Option<Uuid>,
    pub account_id: Uuid,
    pub date: String,
    pub time: String,
    pub time_zone: String,
    pub duration_minutes: i32,
    pub recurrence_rule: String,
    pub title: String,
    pub location: String,
    pub attendees: String,
    pub attendees_json: String,
    pub notes: String,
}

pub fn parse_calendar_participants_metadata(raw: &str) -> CalendarParticipantsMetadata {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return CalendarParticipantsMetadata::default();
    }

    if let Ok(metadata) = serde_json::from_str::<CalendarParticipantsMetadata>(trimmed) {
        return normalize_calendar_participants_metadata(metadata);
    }

    if let Ok(attendees) = serde_json::from_str::<Vec<CalendarParticipantMetadata>>(trimmed) {
        return normalize_calendar_participants_metadata(CalendarParticipantsMetadata {
            organizer: None,
            attendees,
        });
    }

    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        if let Some(object) = value.as_object() {
            let mut metadata = CalendarParticipantsMetadata::default();
            for participant in object.values().filter_map(Value::as_object) {
                let email = participant
                    .get("email")
                    .and_then(Value::as_str)
                    .map(normalize_calendar_email)
                    .or_else(|| {
                        participant
                            .get("sendTo")
                            .and_then(Value::as_object)
                            .and_then(|send_to| send_to.get("imip"))
                            .and_then(Value::as_str)
                            .map(normalize_calendar_email)
                    })
                    .unwrap_or_default();
                let common_name = participant
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                let roles = participant.get("roles").and_then(Value::as_object);
                let is_owner = roles
                    .and_then(|roles| roles.get("owner"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if is_owner && metadata.organizer.is_none() {
                    metadata.organizer = Some(CalendarOrganizerMetadata { email, common_name });
                    continue;
                }
                metadata.attendees.push(CalendarParticipantMetadata {
                    email,
                    common_name,
                    role: if roles
                        .and_then(|roles| roles.get("optional"))
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                    {
                        "OPT-PARTICIPANT".to_string()
                    } else {
                        "REQ-PARTICIPANT".to_string()
                    },
                    partstat: normalize_calendar_participation_status(
                        participant
                            .get("participationStatus")
                            .and_then(Value::as_str)
                            .or_else(|| participant.get("partstat").and_then(Value::as_str))
                            .unwrap_or("needs-action"),
                    ),
                    rsvp: participant
                        .get("expectReply")
                        .and_then(Value::as_bool)
                        .or_else(|| participant.get("rsvp").and_then(Value::as_bool))
                        .unwrap_or(false),
                });
            }
            return normalize_calendar_participants_metadata(metadata);
        }
    }

    CalendarParticipantsMetadata::default()
}

pub fn serialize_calendar_participants_metadata(metadata: &CalendarParticipantsMetadata) -> String {
    serde_json::to_string(&normalize_calendar_participants_metadata(metadata.clone()))
        .unwrap_or_else(|_| "{}".to_string())
}

pub fn calendar_attendee_labels(metadata: &CalendarParticipantsMetadata) -> String {
    metadata
        .attendees
        .iter()
        .map(calendar_participant_label)
        .filter(|label| !label.is_empty())
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn calendar_participant_label(participant: &CalendarParticipantMetadata) -> String {
    if !participant.common_name.trim().is_empty() {
        participant.common_name.trim().to_string()
    } else {
        participant.email.trim().to_string()
    }
}

pub fn normalize_calendar_email(value: &str) -> String {
    value
        .trim()
        .strip_prefix("mailto:")
        .unwrap_or(value.trim())
        .trim()
        .to_ascii_lowercase()
}

pub fn normalize_calendar_participation_status(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "accepted" => "accepted".to_string(),
        "declined" => "declined".to_string(),
        "tentative" => "tentative".to_string(),
        "delegated" => "delegated".to_string(),
        _ => "needs-action".to_string(),
    }
}

fn normalize_calendar_participants_metadata(
    mut metadata: CalendarParticipantsMetadata,
) -> CalendarParticipantsMetadata {
    metadata.organizer = metadata.organizer.and_then(|organizer| {
        let email = normalize_calendar_email(&organizer.email);
        let common_name = organizer.common_name.trim().to_string();
        if email.is_empty() && common_name.is_empty() {
            None
        } else {
            Some(CalendarOrganizerMetadata { email, common_name })
        }
    });
    metadata.attendees = metadata
        .attendees
        .into_iter()
        .filter_map(|attendee| {
            let email = normalize_calendar_email(&attendee.email);
            let common_name = attendee.common_name.trim().to_string();
            if email.is_empty() && common_name.is_empty() {
                return None;
            }
            Some(CalendarParticipantMetadata {
                email,
                common_name,
                role: if attendee.role.trim().is_empty() {
                    "REQ-PARTICIPANT".to_string()
                } else {
                    attendee.role.trim().to_ascii_uppercase()
                },
                partstat: normalize_calendar_participation_status(&attendee.partstat),
                rsvp: attendee.rsvp,
            })
        })
        .collect();
    metadata
}

#[derive(Debug, Clone)]
pub struct CreateTaskListInput {
    pub account_id: Uuid,
    pub name: String,
    pub sort_order: i32,
}

#[derive(Debug, Clone)]
pub struct UpdateTaskListInput {
    pub account_id: Uuid,
    pub task_list_id: Uuid,
    pub name: Option<String>,
    pub sort_order: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct UpsertClientTaskInput {
    pub id: Option<Uuid>,
    pub principal_account_id: Uuid,
    pub account_id: Uuid,
    pub task_list_id: Option<Uuid>,
    pub title: String,
    pub description: String,
    pub status: String,
    pub due_at: Option<String>,
    pub completed_at: Option<String>,
    pub sort_order: i32,
}

#[derive(Debug, Clone)]
pub struct SubmitMessageInput {
    pub draft_message_id: Option<Uuid>,
    pub account_id: Uuid,
    pub submitted_by_account_id: Uuid,
    pub source: String,
    pub from_display: Option<String>,
    pub from_address: String,
    pub sender_display: Option<String>,
    pub sender_address: Option<String>,
    pub to: Vec<SubmittedRecipientInput>,
    pub cc: Vec<SubmittedRecipientInput>,
    pub bcc: Vec<SubmittedRecipientInput>,
    pub subject: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub internet_message_id: Option<String>,
    pub mime_blob_ref: Option<String>,
    pub size_octets: i64,
    pub unread: Option<bool>,
    pub flagged: Option<bool>,
    pub attachments: Vec<AttachmentUploadInput>,
}

#[derive(Debug, Clone)]
pub struct SubmittedRecipientInput {
    pub address: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AttachmentUploadInput {
    pub file_name: String,
    pub media_type: String,
    pub blob_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct SubmissionAccountIdentity {
    pub account_id: Uuid,
    pub email: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SubmittedMessage {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    pub account_id: Uuid,
    pub submitted_by_account_id: Uuid,
    pub sent_mailbox_id: Uuid,
    pub outbound_queue_id: Uuid,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SieveScriptSummary {
    pub name: String,
    pub is_active: bool,
    pub size_octets: u64,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SieveScriptDocument {
    pub name: String,
    pub content: String,
    pub is_active: bool,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SavedDraftMessage {
    pub message_id: Uuid,
    pub account_id: Uuid,
    pub submitted_by_account_id: Uuid,
    pub draft_mailbox_id: Uuid,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SenderAuthorizationKind {
    SelfSend,
    SendAs,
    SendOnBehalf,
}

impl SenderAuthorizationKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SelfSend => "self",
            Self::SendAs => "send-as",
            Self::SendOnBehalf => "send-on-behalf",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SenderDelegationRight {
    SendAs,
    SendOnBehalf,
}

impl SenderDelegationRight {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SendAs => "send_as",
            Self::SendOnBehalf => "send_on_behalf",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxAccountAccess {
    pub account_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub is_owned: bool,
    pub may_read: bool,
    pub may_write: bool,
    pub may_send_as: bool,
    pub may_send_on_behalf: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SenderIdentity {
    pub id: String,
    pub owner_account_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub authorization_kind: String,
    pub sender_address: Option<String>,
    pub sender_display: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MailboxDelegationGrantInput {
    pub owner_account_id: Uuid,
    pub grantee_email: String,
}

#[derive(Debug, Clone)]
pub struct SenderDelegationGrantInput {
    pub owner_account_id: Uuid,
    pub grantee_email: String,
    pub sender_right: SenderDelegationRight,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxDelegationGrant {
    pub id: Uuid,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub grantee_account_id: Uuid,
    pub grantee_email: String,
    pub grantee_display_name: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SenderDelegationGrant {
    pub id: Uuid,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub grantee_account_id: Uuid,
    pub grantee_email: String,
    pub grantee_display_name: String,
    pub sender_right: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxDelegationOverview {
    pub outgoing_mailboxes: Vec<MailboxDelegationGrant>,
    pub incoming_mailboxes: Vec<MailboxAccountAccess>,
    pub outgoing_sender_rights: Vec<SenderDelegationGrant>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MailFlowEntry {
    pub queue_id: Uuid,
    pub message_id: Uuid,
    pub account_email: String,
    pub subject: String,
    pub status: String,
    pub delivery_status: String,
    pub submitted_at: String,
    pub last_attempt_at: Option<String>,
    pub next_attempt_at: Option<String>,
    pub trace_id: Option<String>,
    pub remote_message_ref: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutboundQueueStatusUpdate {
    pub queue_id: Uuid,
    pub message_id: Uuid,
    pub status: String,
    pub remote_message_ref: Option<String>,
    pub retry_after_seconds: Option<i32>,
    pub retry_policy: Option<String>,
    pub technical_status: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapMailbox {
    pub id: Uuid,
    pub role: String,
    pub name: String,
    pub sort_order: i32,
    pub total_emails: u32,
    pub unread_emails: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmailAddress {
    pub address: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmail {
    pub id: Uuid,
    pub thread_id: Uuid,
    pub mailbox_id: Uuid,
    pub mailbox_role: String,
    pub mailbox_name: String,
    pub received_at: String,
    pub sent_at: Option<String>,
    pub from_address: String,
    pub from_display: Option<String>,
    pub sender_address: Option<String>,
    pub sender_display: Option<String>,
    pub sender_authorization_kind: String,
    pub submitted_by_account_id: Uuid,
    pub to: Vec<JmapEmailAddress>,
    pub cc: Vec<JmapEmailAddress>,
    pub bcc: Vec<JmapEmailAddress>,
    pub subject: String,
    pub preview: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub unread: bool,
    pub flagged: bool,
    pub has_attachments: bool,
    pub size_octets: i64,
    pub internet_message_id: Option<String>,
    pub mime_blob_ref: Option<String>,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImapEmail {
    pub id: Uuid,
    pub uid: u32,
    pub thread_id: Uuid,
    pub mailbox_id: Uuid,
    pub mailbox_role: String,
    pub mailbox_name: String,
    pub received_at: String,
    pub sent_at: Option<String>,
    pub from_address: String,
    pub from_display: Option<String>,
    pub to: Vec<JmapEmailAddress>,
    pub cc: Vec<JmapEmailAddress>,
    pub bcc: Vec<JmapEmailAddress>,
    pub subject: String,
    pub preview: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub unread: bool,
    pub flagged: bool,
    pub has_attachments: bool,
    pub size_octets: i64,
    pub internet_message_id: Option<String>,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmailQuery {
    pub ids: Vec<Uuid>,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapThreadQuery {
    pub ids: Vec<Uuid>,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmailSubmission {
    pub id: Uuid,
    pub email_id: Uuid,
    pub thread_id: Uuid,
    pub identity_id: String,
    pub identity_email: String,
    pub envelope_mail_from: String,
    pub envelope_rcpt_to: Vec<String>,
    pub send_at: String,
    pub undo_status: String,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapQuota {
    pub id: String,
    pub name: String,
    pub used: u64,
    pub hard_limit: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapUploadBlob {
    pub id: Uuid,
    pub account_id: Uuid,
    pub media_type: String,
    pub octet_size: u64,
    pub blob_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct JmapMailboxCreateInput {
    pub account_id: Uuid,
    pub name: String,
    pub sort_order: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct JmapMailboxUpdateInput {
    pub account_id: Uuid,
    pub mailbox_id: Uuid,
    pub name: Option<String>,
    pub sort_order: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct JmapImportedEmailInput {
    pub account_id: Uuid,
    pub submitted_by_account_id: Uuid,
    pub mailbox_id: Uuid,
    pub source: String,
    pub from_display: Option<String>,
    pub from_address: String,
    pub sender_display: Option<String>,
    pub sender_address: Option<String>,
    pub to: Vec<SubmittedRecipientInput>,
    pub cc: Vec<SubmittedRecipientInput>,
    pub bcc: Vec<SubmittedRecipientInput>,
    pub subject: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub internet_message_id: Option<String>,
    pub mime_blob_ref: String,
    pub size_octets: i64,
    pub received_at: Option<String>,
    pub attachments: Vec<AttachmentUploadInput>,
}

#[derive(Debug, FromRow)]
struct AccountRow {
    id: Uuid,
    primary_email: String,
    display_name: String,
    quota_mb: i32,
    used_mb: i32,
    status: String,
    gal_visibility: String,
    directory_kind: String,
}

#[derive(Debug, FromRow)]
struct MailboxRow {
    id: Uuid,
    account_id: Uuid,
    display_name: String,
    role: String,
    message_count: i64,
    retention_days: i32,
}

#[derive(Debug, FromRow)]
struct PstTransferJobRow {
    id: Uuid,
    mailbox_id: Uuid,
    direction: String,
    server_path: String,
    status: String,
    requested_by: String,
    created_at: String,
    completed_at: Option<String>,
    processed_messages: i32,
    error_message: Option<String>,
}

#[derive(Debug, FromRow)]
struct DomainRow {
    id: Uuid,
    name: String,
    status: String,
    inbound_enabled: bool,
    outbound_enabled: bool,
    default_quota_mb: i32,
    default_sieve_script: String,
}

#[derive(Debug, FromRow)]
struct AliasRow {
    id: Uuid,
    source: String,
    target: String,
    kind: String,
    status: String,
}

#[derive(Debug, FromRow)]
struct AuditRow {
    id: Uuid,
    timestamp: String,
    actor: String,
    action: String,
    subject: String,
}

#[derive(Debug, FromRow)]
struct JmapMailboxRow {
    id: Uuid,
    role: String,
    display_name: String,
    sort_order: i32,
    total_emails: i64,
    unread_emails: i64,
}

#[derive(Debug, FromRow)]
struct JmapEmailRow {
    id: Uuid,
    imap_uid: i64,
    thread_id: Uuid,
    mailbox_id: Uuid,
    mailbox_role: String,
    mailbox_name: String,
    received_at: String,
    sent_at: Option<String>,
    from_address: String,
    from_display: Option<String>,
    sender_address: Option<String>,
    sender_display: Option<String>,
    sender_authorization_kind: String,
    submitted_by_account_id: Uuid,
    subject: String,
    preview: String,
    body_text: String,
    body_html_sanitized: Option<String>,
    unread: bool,
    flagged: bool,
    has_attachments: bool,
    size_octets: i64,
    internet_message_id: Option<String>,
    mime_blob_ref: Option<String>,
    delivery_status: String,
}

#[derive(Debug, FromRow)]
struct JmapEmailRecipientRow {
    message_id: Uuid,
    kind: String,
    address: String,
    display_name: Option<String>,
    _ordinal: i32,
}

#[derive(Debug, FromRow)]
struct JmapEmailSubmissionRow {
    id: Uuid,
    email_id: Uuid,
    thread_id: Uuid,
    from_address: String,
    sender_address: Option<String>,
    sender_authorization_kind: String,
    send_at: String,
    queue_status: String,
    delivery_status: String,
}

#[derive(Debug, FromRow)]
struct MailboxAccountAccessRow {
    account_id: Uuid,
    email: String,
    display_name: String,
    may_send_as: bool,
    may_send_on_behalf: bool,
}

#[derive(Debug, FromRow)]
struct MailboxDelegationGrantRow {
    id: Uuid,
    owner_account_id: Uuid,
    owner_email: String,
    owner_display_name: String,
    grantee_account_id: Uuid,
    grantee_email: String,
    grantee_display_name: String,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, FromRow)]
struct SenderDelegationGrantRow {
    id: Uuid,
    owner_account_id: Uuid,
    owner_email: String,
    owner_display_name: String,
    grantee_account_id: Uuid,
    grantee_email: String,
    grantee_display_name: String,
    sender_right: String,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, FromRow)]
struct PendingOutboundQueueRow {
    queue_id: Uuid,
    message_id: Uuid,
    account_id: Uuid,
    attempts: i32,
    from_address: String,
    from_display: Option<String>,
    sender_address: Option<String>,
    sender_display: Option<String>,
    sender_authorization_kind: String,
    subject: String,
    body_text: String,
    body_html_sanitized: Option<String>,
    internet_message_id: Option<String>,
    last_error: Option<String>,
}

#[derive(Debug, FromRow)]
struct MessageBccRecipientRow {
    address: String,
    display_name: Option<String>,
}

#[derive(Debug, FromRow)]
struct MessageBccRecipientRecordRow {
    message_id: Uuid,
    address: String,
    display_name: Option<String>,
}

#[derive(Debug, FromRow)]
struct AccountQuotaRow {
    quota_mb: i32,
    used_mb: i32,
}

#[derive(Debug, FromRow)]
struct JmapUploadBlobRow {
    id: Uuid,
    account_id: Uuid,
    media_type: String,
    octet_size: i64,
    blob_bytes: Vec<u8>,
}

#[derive(Debug, FromRow)]
struct ServerAdministratorRow {
    id: Uuid,
    domain_id: Option<Uuid>,
    domain_name: Option<String>,
    email: String,
    display_name: String,
    role: String,
    rights_summary: String,
    permissions_json: String,
}

#[derive(Debug, FromRow)]
struct AdminLoginRow {
    tenant_id: String,
    email: String,
    password_hash: String,
    status: String,
    display_name: Option<String>,
    role: Option<String>,
    domain_id: Option<Uuid>,
    domain_name: Option<String>,
    rights_summary: Option<String>,
    permissions_json: Option<String>,
}

#[derive(Debug, FromRow)]
struct AccountLoginRow {
    tenant_id: String,
    account_id: Uuid,
    email: String,
    password_hash: String,
    status: String,
    display_name: String,
}

#[derive(Debug, FromRow)]
struct AuthenticatedAdminRow {
    tenant_id: String,
    email: String,
    display_name: Option<String>,
    role: Option<String>,
    domain_id: Option<Uuid>,
    domain_name: Option<String>,
    rights_summary: Option<String>,
    permissions_json: Option<String>,
    auth_method: String,
    expires_at: String,
}

#[derive(Debug, FromRow)]
struct AdminAuthFactorRow {
    id: Uuid,
    factor_type: String,
    status: String,
    created_at: String,
    verified_at: Option<String>,
    secret_ciphertext: Option<String>,
}

#[derive(Debug, FromRow)]
struct AccountAuthFactorRow {
    id: Uuid,
    factor_type: String,
    status: String,
    created_at: String,
    verified_at: Option<String>,
    secret_ciphertext: Option<String>,
}

#[derive(Debug, FromRow)]
struct AccountAppPasswordRow {
    id: Uuid,
    label: String,
    status: String,
    created_at: String,
    last_used_at: Option<String>,
    password_hash: Option<String>,
}

#[derive(Debug, FromRow)]
struct AuthenticatedAccountRow {
    tenant_id: String,
    account_id: Uuid,
    email: String,
    display_name: String,
    expires_at: String,
}

#[derive(Debug, FromRow)]
struct ActiveSyncSyncStateRow {
    sync_key: String,
    snapshot_json: String,
}

#[derive(Debug, FromRow)]
struct ClientMessageRow {
    id: Uuid,
    mailbox_role: String,
    from_name: String,
    from_address: String,
    to_recipients: String,
    cc_recipients: String,
    subject: String,
    preview: String,
    received_at: String,
    time_label: String,
    unread: bool,
    flagged: bool,
    delivery_status: String,
    body_text: String,
}

#[derive(Debug, FromRow)]
struct ClientAttachmentRow {
    id: Uuid,
    message_id: Uuid,
    name: String,
    media_type: String,
    size_octets: i64,
}

#[derive(Debug, FromRow)]
struct ActiveSyncAttachmentRow {
    id: Uuid,
    message_id: Uuid,
    file_name: String,
    media_type: String,
    size_octets: i64,
}

#[derive(Debug, FromRow)]
struct ClientEventRow {
    id: Uuid,
    date: String,
    time: String,
    time_zone: String,
    duration_minutes: i32,
    recurrence_rule: String,
    title: String,
    location: String,
    attendees: String,
    attendees_json: String,
    notes: String,
}

#[derive(Debug, FromRow)]
struct ClientContactRow {
    id: Uuid,
    name: String,
    role: String,
    email: String,
    phone: String,
    team: String,
    notes: String,
}

#[derive(Debug, FromRow)]
struct CollaborationCollectionRow {
    owner_account_id: Uuid,
    owner_email: String,
    owner_display_name: String,
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
}

#[derive(Debug, FromRow)]
struct CollaborationGrantRow {
    id: Uuid,
    kind: String,
    owner_account_id: Uuid,
    owner_email: String,
    owner_display_name: String,
    grantee_account_id: Uuid,
    grantee_email: String,
    grantee_display_name: String,
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, FromRow)]
struct AccessibleContactRow {
    id: Uuid,
    owner_account_id: Uuid,
    owner_email: String,
    owner_display_name: String,
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
    name: String,
    role: String,
    email: String,
    phone: String,
    team: String,
    notes: String,
}

#[derive(Debug, FromRow)]
struct AccessibleEventRow {
    id: Uuid,
    owner_account_id: Uuid,
    owner_email: String,
    owner_display_name: String,
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
    date: String,
    time: String,
    time_zone: String,
    duration_minutes: i32,
    recurrence_rule: String,
    title: String,
    location: String,
    attendees: String,
    attendees_json: String,
    notes: String,
}

#[derive(Debug, Clone)]
struct AccountIdentity {
    id: Uuid,
    email: String,
    display_name: String,
}

#[derive(Debug)]
struct ResolvedSubmissionAuthorization {
    submitted_by: AccountIdentity,
    from_address: String,
    from_display: Option<String>,
    sender_address: Option<String>,
    sender_display: Option<String>,
    authorization_kind: SenderAuthorizationKind,
}

#[derive(Debug, FromRow)]
struct ClientTaskListRow {
    id: Uuid,
    owner_account_id: Uuid,
    owner_email: String,
    owner_display_name: String,
    is_owned: bool,
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
    name: String,
    role: Option<String>,
    sort_order: i32,
    updated_at: String,
}

#[derive(Debug, FromRow)]
struct ClientTaskRow {
    id: Uuid,
    owner_account_id: Uuid,
    owner_email: String,
    owner_display_name: String,
    is_owned: bool,
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
    task_list_id: Uuid,
    task_list_sort_order: i32,
    title: String,
    description: String,
    status: String,
    due_at: Option<String>,
    completed_at: Option<String>,
    sort_order: i32,
    updated_at: String,
}

#[derive(Debug, FromRow)]
struct DavTaskRow {
    id: Uuid,
    owner_account_id: Uuid,
    owner_email: String,
    owner_display_name: String,
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
    task_list_id: Uuid,
    task_list_name: String,
    title: String,
    description: String,
    status: String,
    due_at: Option<String>,
    completed_at: Option<String>,
    sort_order: i32,
    updated_at: String,
}

#[derive(Debug, FromRow)]
struct TaskListGrantRow {
    id: Uuid,
    task_list_id: Uuid,
    task_list_name: String,
    owner_account_id: Uuid,
    owner_email: String,
    owner_display_name: String,
    grantee_account_id: Uuid,
    grantee_email: String,
    grantee_display_name: String,
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, FromRow)]
struct PendingPstJobRow {
    id: Uuid,
    tenant_id: String,
    mailbox_id: Uuid,
    account_id: Uuid,
    direction: String,
    server_path: String,
    requested_by: String,
}

#[derive(Debug)]
struct StoredAttachmentBlob {
    id: Uuid,
}

#[derive(Debug)]
struct PstImportedMessage {
    internet_message_id: String,
    from_address: String,
    subject: String,
    body_text: String,
    attachments: Vec<AttachmentUploadInput>,
}

#[derive(Debug, FromRow)]
struct FilterRuleRow {
    id: Uuid,
    name: String,
    scope: String,
    action: String,
    status: String,
}

#[derive(Debug, FromRow)]
struct QuarantineRow {
    id: Uuid,
    message_ref: String,
    sender: String,
    recipient: String,
    reason: String,
    status: String,
    created_at: String,
}

#[derive(Debug, FromRow)]
struct EmailTraceRow {
    message_id: Uuid,
    internet_message_id: Option<String>,
    subject: String,
    sender: String,
    account_email: String,
    mailbox: String,
    received_at: String,
}

#[derive(Debug, FromRow)]
struct MailFlowRow {
    queue_id: Uuid,
    message_id: Uuid,
    account_email: String,
    subject: String,
    status: String,
    delivery_status: String,
    submitted_at: String,
    last_attempt_at: Option<String>,
    next_attempt_at: Option<String>,
    trace_id: Option<String>,
    remote_message_ref: Option<String>,
    last_error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CanonicalChangeNotification {
    tenant_id: String,
    category: String,
    principal_account_ids: Vec<String>,
    account_ids: Vec<String>,
}

impl Storage {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = Pool::<Postgres>::connect(database_url).await?;
        let storage = Self::new(pool);
        storage.assert_schema_version().await?;
        Ok(storage)
    }

    pub async fn create_canonical_change_listener(
        &self,
        principal_account_id: Uuid,
    ) -> Result<CanonicalChangeListener> {
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(CANONICAL_CHANGE_CHANNEL).await?;
        Ok(CanonicalChangeListener {
            principal_account_id,
            tenant_id,
            listener,
        })
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }

    async fn assert_schema_version(&self) -> Result<()> {
        let schema_version = sqlx::query_scalar::<_, String>(
            r#"
            SELECT schema_version
            FROM schema_metadata
            WHERE singleton = TRUE
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context(
            "database schema is not initialized for LPE 0.1.4; recreate the database and apply crates/lpe-storage/sql/schema.sql",
        )?;

        if schema_version != EXPECTED_SCHEMA_VERSION {
            bail!(
                "unsupported database schema version {schema_version}; expected {EXPECTED_SCHEMA_VERSION}. Release 0.1.5 requires a fresh database initialized from crates/lpe-storage/sql/schema.sql"
            );
        }

        Ok(())
    }

    pub async fn fetch_admin_dashboard(&self) -> Result<AdminDashboard> {
        let tenant_id = PLATFORM_TENANT_ID;
        let account_rows = sqlx::query_as::<_, AccountRow>(
            r#"
            SELECT
                id,
                primary_email,
                display_name,
                quota_mb,
                used_mb,
                status,
                gal_visibility,
                directory_kind
            FROM accounts
            WHERE tenant_id = $1
            ORDER BY created_at ASC
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;

        let mailbox_rows = sqlx::query_as::<_, MailboxRow>(
            r#"
            SELECT
                mb.id,
                mb.account_id,
                mb.display_name,
                mb.role,
                COALESCE(COUNT(m.id), 0)::BIGINT AS message_count,
                mb.retention_days
            FROM mailboxes mb
            LEFT JOIN messages m ON m.mailbox_id = mb.id
            WHERE mb.tenant_id = $1
            GROUP BY mb.id, mb.account_id, mb.display_name, mb.role, mb.retention_days, mb.created_at
            ORDER BY mb.created_at ASC
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;

        let pst_job_rows = sqlx::query_as::<_, PstTransferJobRow>(
            r#"
            SELECT
                id,
                mailbox_id,
                direction,
                server_path,
                status,
                requested_by,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN completed_at IS NULL THEN NULL
                    ELSE to_char(completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS completed_at,
                processed_messages,
                error_message
            FROM mailbox_pst_jobs
            WHERE tenant_id = $1
            ORDER BY created_at DESC
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;

        let mut accounts = Vec::with_capacity(account_rows.len());
        for row in account_rows {
            let mailboxes = mailbox_rows
                .iter()
                .filter(|mailbox| mailbox.account_id == row.id)
                .map(|mailbox| MailboxRecord {
                    id: mailbox.id,
                    display_name: mailbox.display_name.clone(),
                    role: mailbox.role.clone(),
                    message_count: mailbox.message_count as u32,
                    retention_days: mailbox.retention_days as u16,
                    pst_jobs: pst_job_rows
                        .iter()
                        .filter(|job| job.mailbox_id == mailbox.id)
                        .map(|job| PstTransferJobRecord {
                            id: job.id,
                            direction: job.direction.clone(),
                            server_path: job.server_path.clone(),
                            status: job.status.clone(),
                            requested_by: job.requested_by.clone(),
                            created_at: job.created_at.clone(),
                            completed_at: job.completed_at.clone(),
                            processed_messages: job.processed_messages.max(0) as u32,
                            error_message: job.error_message.clone(),
                        })
                        .collect(),
                })
                .collect::<Vec<_>>();

            accounts.push(AccountRecord {
                id: row.id,
                email: row.primary_email,
                display_name: row.display_name,
                quota_mb: row.quota_mb as u32,
                used_mb: row.used_mb as u32,
                status: row.status,
                gal_visibility: row.gal_visibility,
                directory_kind: row.directory_kind,
                mailboxes,
            });
        }

        let domains = sqlx::query_as::<_, DomainRow>(
            r#"
            SELECT
                id,
                name,
                status,
                inbound_enabled,
                outbound_enabled,
                default_quota_mb,
                default_sieve_script
            FROM domains
            WHERE tenant_id = $1
            ORDER BY created_at ASC
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| DomainRecord {
            id: row.id,
            name: row.name,
            status: row.status,
            inbound_enabled: row.inbound_enabled,
            outbound_enabled: row.outbound_enabled,
            default_quota_mb: row.default_quota_mb as u32,
            default_sieve_script: row.default_sieve_script,
        })
        .collect::<Vec<_>>();

        let aliases = sqlx::query_as::<_, AliasRow>(
            r#"
            SELECT id, source, target, kind, status
            FROM aliases
            WHERE tenant_id = $1
            ORDER BY created_at ASC
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| AliasRecord {
            id: row.id,
            source: row.source,
            target: row.target,
            kind: row.kind,
            status: row.status,
        })
        .collect::<Vec<_>>();

        let server_settings = self.fetch_server_settings().await?;
        let security_settings = self.fetch_security_settings().await?;
        let local_ai_settings = self.fetch_local_ai_settings().await?;
        let antispam_settings = self.fetch_antispam_settings().await?;
        let server_admins = self.fetch_server_administrators().await?;
        let antispam_rules = self.fetch_antispam_rules().await?;
        let quarantine_items = self.fetch_quarantine_items().await?;

        let audit_log = sqlx::query_as::<_, AuditRow>(
            r#"
            SELECT
                id,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
                actor,
                action,
                subject
            FROM audit_events
            WHERE tenant_id = $1
            ORDER BY created_at DESC
            LIMIT 20
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| AuditEvent {
            id: row.id,
            timestamp: row.timestamp,
            actor: row.actor,
            action: row.action,
            subject: row.subject,
        })
        .collect::<Vec<_>>();

        let total_mailboxes = accounts.iter().map(|account| account.mailboxes.len()).sum();
        let pending_queue_items = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM outbound_message_queue
            WHERE tenant_id = $1
              AND status IN ('queued', 'deferred')
            "#,
        )
        .bind(tenant_id)
        .fetch_one(&self.pool)
        .await?
        .max(0) as u32;

        let protocols = vec![
            ProtocolStatus {
                name: "JMAP".to_string(),
                enabled: true,
                bind_address: server_settings.jmap_bind_address.clone(),
                state: "serving".to_string(),
            },
            ProtocolStatus {
                name: "IMAP".to_string(),
                enabled: true,
                bind_address: server_settings.imap_bind_address.clone(),
                state: "compatibility".to_string(),
            },
            ProtocolStatus {
                name: "SMTP ingress".to_string(),
                enabled: true,
                bind_address: server_settings.smtp_bind_address.clone(),
                state: "receiving".to_string(),
            },
            ProtocolStatus {
                name: "SMTP submission".to_string(),
                enabled: true,
                bind_address: "0.0.0.0:587".to_string(),
                state: "ready".to_string(),
            },
            ProtocolStatus {
                name: "Admin API".to_string(),
                enabled: true,
                bind_address: server_settings.admin_bind_address.clone(),
                state: "healthy".to_string(),
            },
        ];

        Ok(AdminDashboard {
            health: HealthResponse {
                service: "lpe-admin-api",
                status: "ok",
            },
            overview: OverviewStats {
                total_accounts: accounts.len(),
                total_mailboxes,
                total_domains: domains.len(),
                total_aliases: aliases.len(),
                pending_queue_items,
                attachment_formats: 3,
                local_ai_enabled: local_ai_settings.enabled,
            },
            protocols,
            accounts,
            domains,
            aliases,
            server_admins,
            server_settings,
            security_settings,
            local_ai_settings,
            antispam_settings,
            antispam_rules,
            quarantine_items,
            storage: StorageOverview {
                primary_store: "PostgreSQL".to_string(),
                search_engine: "PostgreSQL FTS + pg_trgm".to_string(),
                attachment_formats: vec!["PDF".to_string(), "DOCX".to_string(), "ODT".to_string()],
                replication_mode: "single node bootstrap".to_string(),
            },
            audit_log,
        })
    }

    pub async fn create_account(&self, input: NewAccount, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let account_id = Uuid::new_v4();
        let email = input.email.trim().to_lowercase();
        let display_name = input.display_name.trim();
        let tenant_id = self.tenant_id_for_account_email(&email).await?;

        let insert_result = sqlx::query(
            r#"
            INSERT INTO accounts (id, tenant_id, primary_email, display_name, quota_mb, used_mb, status)
            VALUES ($1, $2, $3, $4, $5, 0, 'active')
            ON CONFLICT (tenant_id, primary_email) DO NOTHING
            "#,
        )
        .bind(account_id)
        .bind(&tenant_id)
        .bind(&email)
        .bind(display_name)
        .bind(input.quota_mb as i32)
        .execute(&mut *tx)
        .await?;

        if insert_result.rows_affected() > 0 {
            sqlx::query(
                r#"
                UPDATE accounts
                SET gal_visibility = $1, directory_kind = $2
                WHERE tenant_id = $3 AND id = $4
                "#,
            )
            .bind(normalize_gal_visibility(&input.gal_visibility)?)
            .bind(normalize_directory_kind(&input.directory_kind)?)
            .bind(&tenant_id)
            .bind(account_id)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO mailboxes (id, tenant_id, account_id, role, display_name, sort_order, retention_days)
                VALUES ($1, $2, $3, 'inbox', 'Inbox', 0, 365)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(account_id)
            .execute(&mut *tx)
            .await?;

            self.insert_audit(&mut tx, &tenant_id, audit).await?;
            Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn update_account(&self, input: UpdateAccount, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let display_name = input.display_name.trim();
        let status = input.status.trim().to_lowercase();
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;

        if display_name.is_empty() {
            bail!("account display name is required");
        }

        if !matches!(status.as_str(), "active" | "disabled" | "suspended") {
            bail!("unsupported account status");
        }

        let account_email = sqlx::query_scalar::<_, String>(
            r#"
            UPDATE accounts
            SET display_name = $1,
                quota_mb = $2,
                status = $3,
                gal_visibility = $4,
                directory_kind = $5
            WHERE tenant_id = $6 AND id = $7
            RETURNING primary_email
            "#,
        )
        .bind(display_name)
        .bind(input.quota_mb.max(256) as i32)
        .bind(&status)
        .bind(normalize_gal_visibility(&input.gal_visibility)?)
        .bind(normalize_directory_kind(&input.directory_kind)?)
        .bind(&tenant_id)
        .bind(input.account_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(account_email) = account_email else {
            bail!("account not found");
        };

        if let Some(password_hash) = input.password_hash {
            if password_hash.trim().is_empty() {
                bail!("account password hash is required");
            }

            sqlx::query(
                r#"
                INSERT INTO account_credentials (account_email, tenant_id, password_hash, status)
                VALUES ($1, $2, $3, 'active')
                ON CONFLICT (tenant_id, account_email) DO UPDATE SET
                    password_hash = EXCLUDED.password_hash,
                    status = 'active',
                    updated_at = NOW()
                "#,
            )
            .bind(normalize_email(&account_email))
            .bind(&tenant_id)
            .bind(password_hash)
            .execute(&mut *tx)
            .await?;
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn create_mailbox(&self, input: NewMailbox, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let exists = sqlx::query(
            r#"
            SELECT 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND lower(display_name) = lower($3)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.display_name.trim())
        .fetch_optional(&mut *tx)
        .await?;

        if exists.is_none() {
            let sort_order = sqlx::query_scalar::<_, i32>(
                r#"
                SELECT COALESCE(MAX(sort_order), 0) + 1
                FROM mailboxes
                WHERE tenant_id = $1 AND account_id = $2
                "#,
            )
            .bind(&tenant_id)
            .bind(input.account_id)
            .fetch_one(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO mailboxes (id, tenant_id, account_id, role, display_name, sort_order, retention_days)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(input.role.trim())
            .bind(input.display_name.trim())
            .bind(sort_order)
            .bind(input.retention_days as i32)
            .execute(&mut *tx)
            .await?;

            self.insert_audit(&mut tx, &tenant_id, audit).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn create_pst_transfer_job(
        &self,
        input: NewPstTransferJob,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let direction = input.direction.trim().to_lowercase();
        let server_path = input.server_path.trim();
        let requested_by = input.requested_by.trim().to_lowercase();
        let tenant_id = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM mailboxes
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(input.mailbox_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("mailbox not found"))?;

        let mailbox_exists = sqlx::query(
            r#"
            SELECT 1
            FROM mailboxes
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.mailbox_id)
        .fetch_optional(&mut *tx)
        .await?;

        if mailbox_exists.is_some()
            && !server_path.is_empty()
            && !requested_by.is_empty()
            && (direction == "import" || direction == "export")
        {
            sqlx::query(
                r#"
                INSERT INTO mailbox_pst_jobs (
                    id, tenant_id, mailbox_id, direction, server_path, status, requested_by
                )
                VALUES ($1, $2, $3, $4, $5, 'requested', $6)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(input.mailbox_id)
            .bind(direction)
            .bind(server_path)
            .bind(requested_by)
            .execute(&mut *tx)
            .await?;

            self.insert_audit(&mut tx, &tenant_id, audit).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn create_domain(&self, input: NewDomain, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        // The admin console manages the platform tenant domain inventory, so
        // newly created domains must stay in the same tenant scope that the
        // dashboard, updates, and related admin records already use.
        let tenant_id = PLATFORM_TENANT_ID;
        let result = sqlx::query(
            r#"
            INSERT INTO domains (
                id, tenant_id, name, status, inbound_enabled, outbound_enabled, default_quota_mb,
                default_sieve_script
            )
            VALUES ($1, $2, $3, 'active', $4, $5, $6, $7)
            ON CONFLICT (tenant_id, name) DO NOTHING
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(input.name.trim().to_lowercase())
        .bind(input.inbound_enabled)
        .bind(input.outbound_enabled)
        .bind(input.default_quota_mb as i32)
        .bind(input.default_sieve_script.trim())
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() > 0 {
            self.insert_audit(&mut tx, &tenant_id, audit).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn update_domain(&self, input: UpdateDomain, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = PLATFORM_TENANT_ID;
        let updated = sqlx::query(
            r#"
            UPDATE domains
            SET default_quota_mb = $1,
                inbound_enabled = $2,
                outbound_enabled = $3,
                default_sieve_script = $4
            WHERE tenant_id = $5 AND id = $6
            "#,
        )
        .bind(input.default_quota_mb.max(256) as i32)
        .bind(input.inbound_enabled)
        .bind(input.outbound_enabled)
        .bind(input.default_sieve_script.trim())
        .bind(tenant_id)
        .bind(input.domain_id)
        .execute(&mut *tx)
        .await?;

        if updated.rows_affected() == 0 {
            bail!("domain not found");
        }

        self.insert_audit(&mut tx, tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn create_alias(&self, input: NewAlias, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self
            .tenant_id_for_account_email(input.source.trim())
            .await?;
        let result = sqlx::query(
            r#"
            INSERT INTO aliases (id, tenant_id, source, target, kind, status)
            VALUES ($1, $2, $3, $4, $5, 'active')
            ON CONFLICT (tenant_id, source) DO NOTHING
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(input.source.trim().to_lowercase())
        .bind(input.target.trim().to_lowercase())
        .bind(input.kind.trim())
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() > 0 {
            self.insert_audit(&mut tx, &tenant_id, audit).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn create_server_administrator(
        &self,
        input: NewServerAdministrator,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = match input.domain_id {
            Some(domain_id) => self.tenant_id_for_domain_id(domain_id).await?,
            None => PLATFORM_TENANT_ID.to_string(),
        };
        let normalized_permissions =
            normalize_admin_permissions(&input.role, &input.rights_summary, &input.permissions);
        sqlx::query(
            r#"
            INSERT INTO server_administrators (
                id, tenant_id, domain_id, email, display_name, role, rights_summary, permissions_json
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(input.domain_id)
        .bind(input.email.trim().to_lowercase())
        .bind(input.display_name.trim())
        .bind(input.role.trim())
        .bind(permission_summary(&normalized_permissions))
        .bind(serde_json::to_string(&normalized_permissions)?)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_admin_credential(
        &self,
        input: AdminCredentialInput,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let email = normalize_email(&input.email);
        let tenant_id = self.tenant_id_for_admin_email(&email).await?;
        if email.is_empty() || input.password_hash.trim().is_empty() {
            bail!("admin credential email and password hash are required");
        }

        sqlx::query(
            r#"
            INSERT INTO admin_credentials (email, tenant_id, password_hash, status)
            VALUES ($1, $2, $3, 'active')
            ON CONFLICT (tenant_id, email) DO UPDATE SET
                password_hash = EXCLUDED.password_hash,
                status = 'active',
                updated_at = NOW()
            "#,
        )
        .bind(email)
        .bind(&tenant_id)
        .bind(input.password_hash)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn ensure_admin_credential_stub(&self, email: &str) -> Result<()> {
        let email = normalize_email(email);
        let tenant_id = self.tenant_id_for_admin_email(&email).await?;
        if email.is_empty() {
            bail!("admin credential email is required");
        }

        sqlx::query(
            r#"
            INSERT INTO admin_credentials (email, tenant_id, password_hash, status)
            VALUES ($1, $2, 'federated-only', 'active')
            ON CONFLICT (tenant_id, email) DO UPDATE SET
                status = 'active',
                updated_at = NOW()
            "#,
        )
        .bind(email)
        .bind(&tenant_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn find_server_administrator_by_email(
        &self,
        email: &str,
    ) -> Result<Option<ServerAdministrator>> {
        let email = normalize_email(email);
        let tenant_id = self.tenant_id_for_admin_email(&email).await?;
        let row = sqlx::query_as::<_, ServerAdministratorRow>(
            r#"
            SELECT
                sa.id,
                sa.domain_id,
                d.name AS domain_name,
                sa.email,
                sa.display_name,
                sa.role,
                sa.rights_summary,
                sa.permissions_json
            FROM server_administrators sa
            LEFT JOIN domains d ON d.id = sa.domain_id
            WHERE sa.tenant_id = $1
              AND lower(sa.email) = lower($2)
            ORDER BY sa.created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| {
            let permissions = permissions_from_storage(
                &row.role,
                Some(&row.rights_summary),
                Some(&row.permissions_json),
            );
            ServerAdministrator {
                id: row.id,
                domain_id: row.domain_id,
                domain_name: row.domain_name.unwrap_or_else(|| "All domains".to_string()),
                email: row.email,
                display_name: row.display_name,
                role: row.role,
                rights_summary: permission_summary(&permissions),
                permissions,
            }
        }))
    }

    pub async fn find_admin_oidc_identity(
        &self,
        issuer_url: &str,
        subject: &str,
    ) -> Result<Option<String>> {
        let tenant_id = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM admin_oidc_identities
            WHERE issuer_url = $1 AND subject = $2
            LIMIT 1
            "#,
        )
        .bind(issuer_url.trim())
        .bind(subject.trim())
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or_else(|| PLATFORM_TENANT_ID.to_string());
        let email = sqlx::query_scalar::<_, String>(
            r#"
            SELECT admin_email
            FROM admin_oidc_identities
            WHERE tenant_id = $1 AND issuer_url = $2 AND subject = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(issuer_url.trim())
        .bind(subject.trim())
        .fetch_optional(&self.pool)
        .await?;

        Ok(email)
    }

    pub async fn upsert_admin_oidc_identity(&self, claims: &AdminOidcClaims) -> Result<()> {
        let tenant_id = self.tenant_id_for_admin_email(&claims.email).await?;
        sqlx::query(
            r#"
            INSERT INTO admin_oidc_identities (
                tenant_id, issuer_url, subject, admin_email, created_at, last_login_at
            )
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            ON CONFLICT (tenant_id, issuer_url, subject) DO UPDATE SET
                admin_email = EXCLUDED.admin_email,
                last_login_at = NOW()
            "#,
        )
        .bind(&tenant_id)
        .bind(claims.issuer_url.trim())
        .bind(claims.subject.trim())
        .bind(normalize_email(&claims.email))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn create_admin_auth_factor(&self, input: NewAdminAuthFactor) -> Result<Uuid> {
        let admin_email = normalize_email(&input.admin_email);
        let tenant_id = self.tenant_id_for_admin_email(&admin_email).await?;
        let factor_id = Uuid::new_v4();

        sqlx::query(
            r#"
            INSERT INTO admin_auth_factors (
                id, tenant_id, admin_email, factor_type, status, secret_ciphertext
            )
            VALUES ($1, $2, $3, $4, 'pending', $5)
            "#,
        )
        .bind(factor_id)
        .bind(&tenant_id)
        .bind(admin_email)
        .bind(input.factor_type.trim().to_lowercase())
        .bind(input.secret_ciphertext)
        .execute(&self.pool)
        .await?;

        Ok(factor_id)
    }

    pub async fn fetch_admin_auth_factors(
        &self,
        admin_email: &str,
    ) -> Result<Vec<AdminAuthFactor>> {
        let admin_email = normalize_email(admin_email);
        let tenant_id = self.tenant_id_for_admin_email(&admin_email).await?;
        let rows = sqlx::query_as::<_, AdminAuthFactorRow>(
            r#"
            SELECT
                id,
                factor_type,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN verified_at IS NULL THEN NULL
                    ELSE to_char(verified_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS verified_at,
                secret_ciphertext
            FROM admin_auth_factors
            WHERE tenant_id = $1 AND lower(admin_email) = lower($2)
            ORDER BY created_at DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(admin_email)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AdminAuthFactor {
                id: row.id,
                factor_type: row.factor_type,
                status: row.status,
                created_at: row.created_at,
                verified_at: row.verified_at,
            })
            .collect())
    }

    pub async fn fetch_admin_totp_secret(
        &self,
        admin_email: &str,
    ) -> Result<Option<(Uuid, String)>> {
        let admin_email = normalize_email(admin_email);
        let tenant_id = self.tenant_id_for_admin_email(&admin_email).await?;
        let row = sqlx::query_as::<_, AdminAuthFactorRow>(
            r#"
            SELECT
                id,
                factor_type,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN verified_at IS NULL THEN NULL
                    ELSE to_char(verified_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS verified_at,
                secret_ciphertext
            FROM admin_auth_factors
            WHERE tenant_id = $1
              AND lower(admin_email) = lower($2)
              AND factor_type = 'totp'
              AND status = 'active'
            ORDER BY verified_at DESC NULLS LAST, created_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(admin_email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.and_then(|row| row.secret_ciphertext.map(|secret| (row.id, secret))))
    }

    pub async fn fetch_pending_admin_factor_secret(
        &self,
        admin_email: &str,
        factor_id: Uuid,
    ) -> Result<Option<String>> {
        let admin_email = normalize_email(admin_email);
        let tenant_id = self.tenant_id_for_admin_email(&admin_email).await?;
        sqlx::query_scalar(
            r#"
            SELECT secret_ciphertext
            FROM admin_auth_factors
            WHERE tenant_id = $1
              AND lower(admin_email) = lower($2)
              AND id = $3
              AND status = 'pending'
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(admin_email)
        .bind(factor_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn activate_admin_auth_factor(
        &self,
        admin_email: &str,
        factor_id: Uuid,
    ) -> Result<bool> {
        let admin_email = normalize_email(admin_email);
        let tenant_id = self.tenant_id_for_admin_email(&admin_email).await?;
        let updated = sqlx::query(
            r#"
            UPDATE admin_auth_factors
            SET status = 'active', verified_at = NOW()
            WHERE tenant_id = $1
              AND lower(admin_email) = lower($2)
              AND id = $3
              AND status = 'pending'
            "#,
        )
        .bind(&tenant_id)
        .bind(admin_email)
        .bind(factor_id)
        .execute(&self.pool)
        .await?;

        Ok(updated.rows_affected() > 0)
    }

    pub async fn revoke_admin_auth_factor(
        &self,
        admin_email: &str,
        factor_id: Uuid,
    ) -> Result<bool> {
        let admin_email = normalize_email(admin_email);
        let tenant_id = self.tenant_id_for_admin_email(&admin_email).await?;
        let updated = sqlx::query(
            r#"
            UPDATE admin_auth_factors
            SET status = 'revoked'
            WHERE tenant_id = $1
              AND lower(admin_email) = lower($2)
              AND id = $3
              AND status IN ('pending', 'active')
            "#,
        )
        .bind(&tenant_id)
        .bind(admin_email)
        .bind(factor_id)
        .execute(&self.pool)
        .await?;

        Ok(updated.rows_affected() > 0)
    }

    pub async fn find_account_oidc_identity(
        &self,
        issuer_url: &str,
        subject: &str,
    ) -> Result<Option<String>> {
        let tenant_id = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM account_oidc_identities
            WHERE issuer_url = $1 AND subject = $2
            LIMIT 1
            "#,
        )
        .bind(issuer_url.trim())
        .bind(subject.trim())
        .fetch_optional(&self.pool)
        .await?;
        let Some(tenant_id) = tenant_id else {
            return Ok(None);
        };

        sqlx::query_scalar::<_, String>(
            r#"
            SELECT account_email
            FROM account_oidc_identities
            WHERE tenant_id = $1 AND issuer_url = $2 AND subject = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(issuer_url.trim())
        .bind(subject.trim())
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn upsert_account_oidc_identity(&self, claims: &AccountOidcClaims) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_email(&claims.email).await?;
        sqlx::query(
            r#"
            INSERT INTO account_oidc_identities (
                tenant_id, issuer_url, subject, account_email, created_at, last_login_at
            )
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            ON CONFLICT (tenant_id, issuer_url, subject) DO UPDATE SET
                account_email = EXCLUDED.account_email,
                last_login_at = NOW()
            "#,
        )
        .bind(&tenant_id)
        .bind(claims.issuer_url.trim())
        .bind(claims.subject.trim())
        .bind(normalize_email(&claims.email))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn create_account_auth_factor(&self, input: NewAccountAuthFactor) -> Result<Uuid> {
        let account_email = normalize_email(&input.account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let factor_id = Uuid::new_v4();

        sqlx::query(
            r#"
            INSERT INTO account_auth_factors (
                id, tenant_id, account_email, factor_type, status, secret_ciphertext
            )
            VALUES ($1, $2, $3, $4, 'pending', $5)
            "#,
        )
        .bind(factor_id)
        .bind(&tenant_id)
        .bind(account_email)
        .bind(input.factor_type.trim().to_lowercase())
        .bind(input.secret_ciphertext)
        .execute(&self.pool)
        .await?;

        Ok(factor_id)
    }

    pub async fn fetch_account_auth_factors(
        &self,
        account_email: &str,
    ) -> Result<Vec<AccountAuthFactor>> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let rows = sqlx::query_as::<_, AccountAuthFactorRow>(
            r#"
            SELECT
                id,
                factor_type,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN verified_at IS NULL THEN NULL
                    ELSE to_char(verified_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS verified_at,
                secret_ciphertext
            FROM account_auth_factors
            WHERE tenant_id = $1 AND lower(account_email) = lower($2)
            ORDER BY created_at DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AccountAuthFactor {
                id: row.id,
                factor_type: row.factor_type,
                status: row.status,
                created_at: row.created_at,
                verified_at: row.verified_at,
            })
            .collect())
    }

    pub async fn fetch_account_totp_secret(
        &self,
        account_email: &str,
    ) -> Result<Option<(Uuid, String)>> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let row = sqlx::query_as::<_, AccountAuthFactorRow>(
            r#"
            SELECT
                id,
                factor_type,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN verified_at IS NULL THEN NULL
                    ELSE to_char(verified_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS verified_at,
                secret_ciphertext
            FROM account_auth_factors
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND factor_type = 'totp'
              AND status = 'active'
            ORDER BY verified_at DESC NULLS LAST, created_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.and_then(|row| row.secret_ciphertext.map(|secret| (row.id, secret))))
    }

    pub async fn fetch_pending_account_factor_secret(
        &self,
        account_email: &str,
        factor_id: Uuid,
    ) -> Result<Option<String>> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        sqlx::query_scalar(
            r#"
            SELECT secret_ciphertext
            FROM account_auth_factors
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND id = $3
              AND status = 'pending'
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .bind(factor_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn activate_account_auth_factor(
        &self,
        account_email: &str,
        factor_id: Uuid,
    ) -> Result<bool> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let updated = sqlx::query(
            r#"
            UPDATE account_auth_factors
            SET status = 'active', verified_at = NOW()
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND id = $3
              AND status = 'pending'
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .bind(factor_id)
        .execute(&self.pool)
        .await?;

        Ok(updated.rows_affected() > 0)
    }

    pub async fn revoke_account_auth_factor(
        &self,
        account_email: &str,
        factor_id: Uuid,
    ) -> Result<bool> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let updated = sqlx::query(
            r#"
            UPDATE account_auth_factors
            SET status = 'revoked'
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND id = $3
              AND status IN ('pending', 'active')
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .bind(factor_id)
        .execute(&self.pool)
        .await?;

        Ok(updated.rows_affected() > 0)
    }

    pub async fn create_account_app_password(
        &self,
        account_email: &str,
        label: &str,
        password_hash: &str,
    ) -> Result<Uuid> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO account_app_passwords (
                id, tenant_id, account_email, label, password_hash, status
            )
            VALUES ($1, $2, $3, $4, $5, 'active')
            "#,
        )
        .bind(id)
        .bind(&tenant_id)
        .bind(account_email)
        .bind(label.trim())
        .bind(password_hash.trim())
        .execute(&self.pool)
        .await?;

        Ok(id)
    }

    pub async fn list_account_app_passwords(
        &self,
        account_email: &str,
    ) -> Result<Vec<AccountAppPassword>> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let rows = sqlx::query_as::<_, AccountAppPasswordRow>(
            r#"
            SELECT
                id,
                label,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN last_used_at IS NULL THEN NULL
                    ELSE to_char(last_used_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS last_used_at,
                NULL AS password_hash
            FROM account_app_passwords
            WHERE tenant_id = $1 AND lower(account_email) = lower($2)
            ORDER BY created_at DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AccountAppPassword {
                id: row.id,
                label: row.label,
                status: row.status,
                created_at: row.created_at,
                last_used_at: row.last_used_at,
            })
            .collect())
    }

    pub async fn fetch_active_account_app_passwords(
        &self,
        account_email: &str,
    ) -> Result<Vec<StoredAccountAppPassword>> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let rows = sqlx::query_as::<_, AccountAppPasswordRow>(
            r#"
            SELECT
                id,
                label,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                CASE
                    WHEN last_used_at IS NULL THEN NULL
                    ELSE to_char(last_used_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS last_used_at,
                password_hash
            FROM account_app_passwords
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND status = 'active'
            ORDER BY created_at DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .filter_map(|row| {
                row.password_hash
                    .map(|password_hash| StoredAccountAppPassword {
                        id: row.id,
                        password_hash,
                    })
            })
            .collect())
    }

    pub async fn touch_account_app_password(
        &self,
        account_email: &str,
        app_password_id: Uuid,
    ) -> Result<()> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        sqlx::query(
            r#"
            UPDATE account_app_passwords
            SET last_used_at = NOW()
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .bind(app_password_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn revoke_account_app_password(
        &self,
        account_email: &str,
        app_password_id: Uuid,
    ) -> Result<bool> {
        let account_email = normalize_email(account_email);
        let tenant_id = self.tenant_id_for_account_email(&account_email).await?;
        let updated = sqlx::query(
            r#"
            UPDATE account_app_passwords
            SET status = 'disabled'
            WHERE tenant_id = $1
              AND lower(account_email) = lower($2)
              AND id = $3
              AND status = 'active'
            "#,
        )
        .bind(&tenant_id)
        .bind(account_email)
        .bind(app_password_id)
        .execute(&self.pool)
        .await?;

        Ok(updated.rows_affected() > 0)
    }

    pub async fn append_audit_event(&self, tenant_id: &str, audit: AuditEntryInput) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        self.insert_audit(&mut tx, tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_account_credential(
        &self,
        input: AccountCredentialInput,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let email = normalize_email(&input.email);
        let tenant_id = self.tenant_id_for_account_email(&email).await?;
        if email.is_empty() || input.password_hash.trim().is_empty() {
            bail!("account credential email and password hash are required");
        }

        let account_exists = sqlx::query(
            r#"
            SELECT 1
            FROM accounts
            WHERE tenant_id = $1 AND lower(primary_email) = lower($2)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(&email)
        .fetch_optional(&mut *tx)
        .await?;

        if account_exists.is_none() {
            bail!("account not found");
        }

        sqlx::query(
            r#"
            INSERT INTO account_credentials (account_email, tenant_id, password_hash, status)
            VALUES ($1, $2, $3, 'active')
            ON CONFLICT (tenant_id, account_email) DO UPDATE SET
                password_hash = EXCLUDED.password_hash,
                status = 'active',
                updated_at = NOW()
            "#,
        )
        .bind(email)
        .bind(&tenant_id)
        .bind(input.password_hash)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn has_admin_bootstrap_state(&self) -> Result<bool> {
        let credentials_exist = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM admin_credentials
                WHERE tenant_id = $1
            )
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_one(&self.pool)
        .await?;
        if credentials_exist {
            return Ok(true);
        }

        let administrators_exist = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM server_administrators
                WHERE tenant_id = $1
            )
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_one(&self.pool)
        .await?;

        Ok(administrators_exist)
    }

    pub async fn fetch_admin_login(&self, email: &str) -> Result<Option<AdminLogin>> {
        let email = normalize_email(email);
        let tenant_id = self.tenant_id_for_admin_email(&email).await?;
        let row = sqlx::query_as::<_, AdminLoginRow>(
            r#"
            SELECT
                ac.tenant_id,
                ac.email,
                ac.password_hash,
                ac.status,
                sa.display_name,
                sa.role,
                sa.domain_id,
                d.name AS domain_name,
                sa.rights_summary,
                sa.permissions_json
            FROM admin_credentials ac
            LEFT JOIN server_administrators sa
                ON sa.tenant_id = ac.tenant_id AND lower(sa.email) = lower(ac.email)
            LEFT JOIN domains d ON d.id = sa.domain_id
            WHERE ac.tenant_id = $1 AND lower(ac.email) = lower($2)
            ORDER BY sa.created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| {
            let role = row.role.unwrap_or_else(|| "server-admin".to_string());
            let permissions = permissions_from_storage(
                &role,
                row.rights_summary.as_deref(),
                row.permissions_json.as_deref(),
            );
            AdminLogin {
                tenant_id: row.tenant_id,
                email: row.email,
                password_hash: row.password_hash,
                status: row.status,
                display_name: row
                    .display_name
                    .unwrap_or_else(|| "LPE Administrator".to_string()),
                role,
                domain_id: row.domain_id,
                domain_name: row.domain_name.unwrap_or_else(|| "All domains".to_string()),
                rights_summary: permission_summary(&permissions),
                permissions,
            }
        }))
    }

    pub async fn create_admin_session(
        &self,
        token: &str,
        tenant_id: &str,
        email: &str,
        session_timeout_minutes: u32,
        auth_method: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO admin_sessions (id, tenant_id, token, admin_email, auth_method, expires_at)
            VALUES ($1, $2, $3, $4, $5, NOW() + ($6::TEXT || ' minutes')::INTERVAL)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(token)
        .bind(normalize_email(email))
        .bind(normalize_admin_session_auth_method(auth_method))
        .bind(session_timeout_minutes.max(5) as i32)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_account_login(&self, email: &str) -> Result<Option<AccountLogin>> {
        let email = normalize_email(email);
        let tenant_id = self.tenant_id_for_account_email(&email).await?;
        let row = sqlx::query_as::<_, AccountLoginRow>(
            r#"
            SELECT
                a.tenant_id,
                a.id AS account_id,
                ac.account_email AS email,
                ac.password_hash,
                ac.status,
                a.display_name
            FROM account_credentials ac
            JOIN accounts a
              ON a.tenant_id = ac.tenant_id
             AND lower(a.primary_email) = lower(ac.account_email)
            WHERE ac.tenant_id = $1 AND lower(ac.account_email) = lower($2)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| AccountLogin {
            tenant_id: row.tenant_id,
            account_id: row.account_id,
            email: row.email,
            password_hash: row.password_hash,
            status: row.status,
            display_name: row.display_name,
        }))
    }

    pub async fn create_account_session(
        &self,
        token: &str,
        tenant_id: &str,
        account_email: &str,
        session_timeout_minutes: u32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO account_sessions (id, tenant_id, token, account_email, expires_at)
            VALUES ($1, $2, $3, $4, NOW() + ($5::TEXT || ' minutes')::INTERVAL)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(token)
        .bind(normalize_email(account_email))
        .bind(session_timeout_minutes.max(5) as i32)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_admin_session(&self, token: &str) -> Result<Option<AuthenticatedAdmin>> {
        let Some(tenant_id) = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM admin_sessions
            WHERE token = $1
            LIMIT 1
            "#,
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let row = sqlx::query_as::<_, AuthenticatedAdminRow>(
            r#"
            SELECT
                s.tenant_id,
                ac.email,
                sa.display_name,
                sa.role,
                sa.domain_id,
                d.name AS domain_name,
                sa.rights_summary,
                sa.permissions_json,
                s.auth_method,
                to_char(s.expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS expires_at
            FROM admin_sessions s
            JOIN admin_credentials ac
              ON ac.tenant_id = s.tenant_id
             AND ac.email = s.admin_email
            LEFT JOIN server_administrators sa
                ON sa.tenant_id = s.tenant_id AND lower(sa.email) = lower(s.admin_email)
            LEFT JOIN domains d ON d.id = sa.domain_id
            WHERE s.tenant_id = $1
              AND s.token = $2
              AND s.expires_at > NOW()
              AND ac.status = 'active'
            ORDER BY sa.created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| {
            let role = row.role.unwrap_or_else(|| "server-admin".to_string());
            let permissions = permissions_from_storage(
                &role,
                row.rights_summary.as_deref(),
                row.permissions_json.as_deref(),
            );
            AuthenticatedAdmin {
                tenant_id: row.tenant_id,
                email: row.email,
                display_name: row
                    .display_name
                    .unwrap_or_else(|| "LPE Administrator".to_string()),
                role,
                domain_id: row.domain_id,
                domain_name: row.domain_name.unwrap_or_else(|| "All domains".to_string()),
                rights_summary: permission_summary(&permissions),
                permissions,
                auth_method: row.auth_method,
                expires_at: row.expires_at,
            }
        }))
    }

    pub async fn delete_admin_session(&self, token: &str) -> Result<()> {
        let tenant_id = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM admin_sessions
            WHERE token = $1
            LIMIT 1
            "#,
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or_else(|| PLATFORM_TENANT_ID.to_string());
        sqlx::query(
            r#"
            DELETE FROM admin_sessions
            WHERE tenant_id = $1 AND token = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(token)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_account_session(&self, token: &str) -> Result<Option<AuthenticatedAccount>> {
        let Some(tenant_id) = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM account_sessions
            WHERE token = $1
            LIMIT 1
            "#,
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let row = sqlx::query_as::<_, AuthenticatedAccountRow>(
            r#"
            SELECT
                s.tenant_id,
                a.id AS account_id,
                ac.account_email AS email,
                a.display_name,
                to_char(s.expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS expires_at
            FROM account_sessions s
            JOIN account_credentials ac
              ON ac.tenant_id = s.tenant_id
             AND ac.account_email = s.account_email
            JOIN accounts a
              ON a.tenant_id = s.tenant_id
             AND lower(a.primary_email) = lower(s.account_email)
            WHERE s.tenant_id = $1
              AND s.token = $2
              AND s.expires_at > NOW()
              AND ac.status = 'active'
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| AuthenticatedAccount {
            tenant_id: row.tenant_id,
            account_id: row.account_id,
            email: row.email,
            display_name: row.display_name,
            expires_at: row.expires_at,
        }))
    }

    pub async fn delete_account_session(&self, token: &str) -> Result<()> {
        let tenant_id = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM account_sessions
            WHERE token = $1
            LIMIT 1
            "#,
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or_else(|| PLATFORM_TENANT_ID.to_string());
        sqlx::query(
            r#"
            DELETE FROM account_sessions
            WHERE tenant_id = $1 AND token = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(token)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn list_sieve_scripts(&self, account_id: Uuid) -> Result<Vec<SieveScriptSummary>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                name,
                is_active,
                octet_length(content)::BIGINT AS size_octets,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY lower(name) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(SieveScriptSummary {
                    name: row.try_get("name")?,
                    is_active: row.try_get("is_active")?,
                    size_octets: row.try_get::<i64, _>("size_octets")?.max(0) as u64,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect()
    }

    pub async fn get_sieve_script(
        &self,
        account_id: Uuid,
        name: &str,
    ) -> Result<Option<SieveScriptDocument>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let normalized_name = validate_sieve_script_name(name)?;
        let row = sqlx::query(
            r#"
            SELECT
                name,
                content,
                is_active,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&normalized_name)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(SieveScriptDocument {
                name: row.try_get("name")?,
                content: row.try_get("content")?,
                is_active: row.try_get("is_active")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .transpose()
    }

    pub async fn put_sieve_script(
        &self,
        account_id: Uuid,
        name: &str,
        content: &str,
        activate: bool,
        audit: AuditEntryInput,
    ) -> Result<SieveScriptDocument> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let name = validate_sieve_script_name(name)?;
        let content = validate_sieve_script_content(content)?;
        parse_script(&content)?;

        let mut tx = self.pool.begin().await?;
        self.ensure_account_exists(&mut tx, &tenant_id, account_id)
            .await?;

        let existing_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_one(&mut *tx)
        .await?;

        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM sieve_scripts
                WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            )
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&name)
        .fetch_one(&mut *tx)
        .await?;

        if !exists && existing_count >= MAX_SIEVE_SCRIPTS_PER_ACCOUNT {
            bail!("too many sieve scripts for account");
        }

        if activate {
            sqlx::query(
                r#"
                UPDATE sieve_scripts
                SET is_active = FALSE, updated_at = NOW()
                WHERE tenant_id = $1 AND account_id = $2 AND is_active = TRUE
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .execute(&mut *tx)
            .await?;
        }

        let row = sqlx::query(
            r#"
            INSERT INTO sieve_scripts (id, tenant_id, account_id, name, content, is_active)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, account_id, normalized_name) DO UPDATE SET
                name = EXCLUDED.name,
                content = EXCLUDED.content,
                is_active = EXCLUDED.is_active OR sieve_scripts.is_active,
                updated_at = NOW()
            RETURNING
                name,
                content,
                is_active,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&name)
        .bind(&content)
        .bind(activate)
        .fetch_one(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;

        Ok(SieveScriptDocument {
            name: row.try_get("name")?,
            content: row.try_get("content")?,
            is_active: row.try_get("is_active")?,
            updated_at: row.try_get("updated_at")?,
        })
    }

    pub async fn delete_sieve_script(
        &self,
        account_id: Uuid,
        name: &str,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let name = validate_sieve_script_name(name)?;
        let mut tx = self.pool.begin().await?;

        let active = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT is_active
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&name)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("sieve script not found"))?;

        if active {
            bail!("cannot delete the active sieve script");
        }

        sqlx::query(
            r#"
            DELETE FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&name)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn rename_sieve_script(
        &self,
        account_id: Uuid,
        old_name: &str,
        new_name: &str,
        audit: AuditEntryInput,
    ) -> Result<SieveScriptSummary> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let old_name = validate_sieve_script_name(old_name)?;
        let new_name = validate_sieve_script_name(new_name)?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            UPDATE sieve_scripts
            SET name = $4, updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
            RETURNING
                name,
                is_active,
                octet_length(content)::BIGINT AS size_octets,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&old_name)
        .bind(&new_name)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("sieve script not found"))?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;

        Ok(SieveScriptSummary {
            name: row.try_get("name")?,
            is_active: row.try_get("is_active")?,
            size_octets: row.try_get::<i64, _>("size_octets")?.max(0) as u64,
            updated_at: row.try_get("updated_at")?,
        })
    }

    pub async fn set_active_sieve_script(
        &self,
        account_id: Uuid,
        name: Option<&str>,
        audit: AuditEntryInput,
    ) -> Result<Option<String>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            UPDATE sieve_scripts
            SET is_active = FALSE, updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND is_active = TRUE
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .execute(&mut *tx)
        .await?;

        let active_name = if let Some(name) = name {
            let name = validate_sieve_script_name(name)?;
            let updated = sqlx::query(
                r#"
                UPDATE sieve_scripts
                SET is_active = TRUE, updated_at = NOW()
                WHERE tenant_id = $1 AND account_id = $2 AND lower(name) = lower($3)
                RETURNING name
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(&name)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| anyhow!("sieve script not found"))?;
            Some(updated.try_get("name")?)
        } else {
            None
        };

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(active_name)
    }

    pub async fn fetch_active_sieve_script(
        &self,
        account_id: Uuid,
    ) -> Result<Option<SieveScriptDocument>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query(
            r#"
            SELECT
                name,
                content,
                is_active,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sieve_scripts
            WHERE tenant_id = $1 AND account_id = $2 AND is_active = TRUE
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(SieveScriptDocument {
                name: row.try_get("name")?,
                content: row.try_get("content")?,
                is_active: row.try_get("is_active")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .transpose()
    }

    pub async fn create_filter_rule(
        &self,
        input: NewFilterRule,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO antispam_filter_rules (id, tenant_id, name, scope, action, status)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(PLATFORM_TENANT_ID)
        .bind(input.name.trim())
        .bind(input.scope.trim())
        .bind(input.action.trim())
        .bind(input.status.trim())
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, PLATFORM_TENANT_ID, audit)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn update_settings(
        &self,
        update: DashboardUpdate,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            INSERT INTO server_settings (
                tenant_id, primary_hostname, admin_bind_address, smtp_bind_address,
                imap_bind_address, jmap_bind_address, default_locale, max_message_size_mb, tls_mode
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id) DO UPDATE SET
                primary_hostname = EXCLUDED.primary_hostname,
                admin_bind_address = EXCLUDED.admin_bind_address,
                smtp_bind_address = EXCLUDED.smtp_bind_address,
                imap_bind_address = EXCLUDED.imap_bind_address,
                jmap_bind_address = EXCLUDED.jmap_bind_address,
                default_locale = EXCLUDED.default_locale,
                max_message_size_mb = EXCLUDED.max_message_size_mb,
                tls_mode = EXCLUDED.tls_mode,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.server_settings.primary_hostname)
        .bind(update.server_settings.admin_bind_address)
        .bind(update.server_settings.smtp_bind_address)
        .bind(update.server_settings.imap_bind_address)
        .bind(update.server_settings.jmap_bind_address)
        .bind(update.server_settings.default_locale)
        .bind(update.server_settings.max_message_size_mb as i32)
        .bind(update.server_settings.tls_mode)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO security_settings (
                tenant_id, password_login_enabled, mfa_required_for_admins,
                session_timeout_minutes, audit_retention_days, oidc_login_enabled,
                oidc_provider_label, oidc_auto_link_by_email,
                mailbox_password_login_enabled, mailbox_oidc_login_enabled,
                mailbox_oidc_provider_label, mailbox_oidc_auto_link_by_email,
                mailbox_app_passwords_enabled
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (tenant_id) DO UPDATE SET
                password_login_enabled = EXCLUDED.password_login_enabled,
                mfa_required_for_admins = EXCLUDED.mfa_required_for_admins,
                session_timeout_minutes = EXCLUDED.session_timeout_minutes,
                audit_retention_days = EXCLUDED.audit_retention_days,
                oidc_login_enabled = EXCLUDED.oidc_login_enabled,
                oidc_provider_label = EXCLUDED.oidc_provider_label,
                oidc_auto_link_by_email = EXCLUDED.oidc_auto_link_by_email,
                mailbox_password_login_enabled = EXCLUDED.mailbox_password_login_enabled,
                mailbox_oidc_login_enabled = EXCLUDED.mailbox_oidc_login_enabled,
                mailbox_oidc_provider_label = EXCLUDED.mailbox_oidc_provider_label,
                mailbox_oidc_auto_link_by_email = EXCLUDED.mailbox_oidc_auto_link_by_email,
                mailbox_app_passwords_enabled = EXCLUDED.mailbox_app_passwords_enabled,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.security_settings.password_login_enabled)
        .bind(update.security_settings.mfa_required_for_admins)
        .bind(update.security_settings.session_timeout_minutes as i32)
        .bind(update.security_settings.audit_retention_days as i32)
        .bind(update.security_settings.oidc_login_enabled)
        .bind(update.security_settings.oidc_provider_label)
        .bind(update.security_settings.oidc_auto_link_by_email)
        .bind(update.security_settings.mailbox_password_login_enabled)
        .bind(update.security_settings.mailbox_oidc_login_enabled)
        .bind(update.security_settings.mailbox_oidc_provider_label)
        .bind(update.security_settings.mailbox_oidc_auto_link_by_email)
        .bind(update.security_settings.mailbox_app_passwords_enabled)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO admin_oidc_config (
                tenant_id,
                issuer_url,
                authorization_endpoint,
                token_endpoint,
                userinfo_endpoint,
                client_id,
                client_secret,
                scopes,
                claim_email,
                claim_display_name,
                claim_subject
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (tenant_id) DO UPDATE SET
                issuer_url = EXCLUDED.issuer_url,
                authorization_endpoint = EXCLUDED.authorization_endpoint,
                token_endpoint = EXCLUDED.token_endpoint,
                userinfo_endpoint = EXCLUDED.userinfo_endpoint,
                client_id = EXCLUDED.client_id,
                client_secret = EXCLUDED.client_secret,
                scopes = EXCLUDED.scopes,
                claim_email = EXCLUDED.claim_email,
                claim_display_name = EXCLUDED.claim_display_name,
                claim_subject = EXCLUDED.claim_subject,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.security_settings.oidc_issuer_url)
        .bind(update.security_settings.oidc_authorization_endpoint)
        .bind(update.security_settings.oidc_token_endpoint)
        .bind(update.security_settings.oidc_userinfo_endpoint)
        .bind(update.security_settings.oidc_client_id)
        .bind(update.security_settings.oidc_client_secret)
        .bind(update.security_settings.oidc_scopes)
        .bind(update.security_settings.oidc_claim_email)
        .bind(update.security_settings.oidc_claim_display_name)
        .bind(update.security_settings.oidc_claim_subject)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO account_oidc_config (
                tenant_id,
                issuer_url,
                authorization_endpoint,
                token_endpoint,
                userinfo_endpoint,
                client_id,
                client_secret,
                scopes,
                claim_email,
                claim_display_name,
                claim_subject
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (tenant_id) DO UPDATE SET
                issuer_url = EXCLUDED.issuer_url,
                authorization_endpoint = EXCLUDED.authorization_endpoint,
                token_endpoint = EXCLUDED.token_endpoint,
                userinfo_endpoint = EXCLUDED.userinfo_endpoint,
                client_id = EXCLUDED.client_id,
                client_secret = EXCLUDED.client_secret,
                scopes = EXCLUDED.scopes,
                claim_email = EXCLUDED.claim_email,
                claim_display_name = EXCLUDED.claim_display_name,
                claim_subject = EXCLUDED.claim_subject,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.security_settings.mailbox_oidc_issuer_url)
        .bind(update.security_settings.mailbox_oidc_authorization_endpoint)
        .bind(update.security_settings.mailbox_oidc_token_endpoint)
        .bind(update.security_settings.mailbox_oidc_userinfo_endpoint)
        .bind(update.security_settings.mailbox_oidc_client_id)
        .bind(update.security_settings.mailbox_oidc_client_secret)
        .bind(update.security_settings.mailbox_oidc_scopes)
        .bind(update.security_settings.mailbox_oidc_claim_email)
        .bind(update.security_settings.mailbox_oidc_claim_display_name)
        .bind(update.security_settings.mailbox_oidc_claim_subject)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO local_ai_settings (
                tenant_id, enabled, provider, model, offline_only, indexing_enabled
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                provider = EXCLUDED.provider,
                model = EXCLUDED.model,
                offline_only = EXCLUDED.offline_only,
                indexing_enabled = EXCLUDED.indexing_enabled,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.local_ai_settings.enabled)
        .bind(update.local_ai_settings.provider)
        .bind(update.local_ai_settings.model)
        .bind(update.local_ai_settings.offline_only)
        .bind(update.local_ai_settings.indexing_enabled)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO antispam_settings (
                tenant_id, content_filtering_enabled, spam_engine,
                quarantine_enabled, quarantine_retention_days
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (tenant_id) DO UPDATE SET
                content_filtering_enabled = EXCLUDED.content_filtering_enabled,
                spam_engine = EXCLUDED.spam_engine,
                quarantine_enabled = EXCLUDED.quarantine_enabled,
                quarantine_retention_days = EXCLUDED.quarantine_retention_days,
                updated_at = NOW()
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(update.antispam_settings.content_filtering_enabled)
        .bind(update.antispam_settings.spam_engine)
        .bind(update.antispam_settings.quarantine_enabled)
        .bind(update.antispam_settings.quarantine_retention_days as i32)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, PLATFORM_TENANT_ID, audit)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_client_workspace(&self, account_id: Uuid) -> Result<ClientWorkspace> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let message_rows = sqlx::query_as::<_, ClientMessageRow>(
            r#"
            SELECT
                m.id,
                mb.role AS mailbox_role,
                COALESCE(NULLIF(m.from_display, ''), m.from_address) AS from_name,
                m.from_address,
                COALESCE((
                    SELECT string_agg(r.address, ', ' ORDER BY r.ordinal)
                    FROM message_recipients r
                    WHERE r.message_id = m.id AND r.kind = 'to'
                ), '') AS to_recipients,
                COALESCE((
                    SELECT string_agg(r.address, ', ' ORDER BY r.ordinal)
                    FROM message_recipients r
                    WHERE r.message_id = m.id AND r.kind = 'cc'
                ), '') AS cc_recipients,
                m.subject_normalized AS subject,
                m.preview_text AS preview,
                to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS received_at,
                to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'HH24:MI') AS time_label,
                m.unread,
                m.flagged,
                m.delivery_status,
                COALESCE(b.body_text, '') AS body_text
            FROM messages m
            JOIN mailboxes mb ON mb.id = m.mailbox_id
            LEFT JOIN message_bodies b ON b.message_id = m.id
            WHERE m.tenant_id = $1 AND m.account_id = $2
            ORDER BY COALESCE(m.sent_at, m.received_at) DESC
            LIMIT 250
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        let attachment_rows = sqlx::query_as::<_, ClientAttachmentRow>(
            r#"
            SELECT
                a.id,
                a.message_id,
                a.file_name AS name,
                a.media_type,
                a.size_octets
            FROM attachments a
            JOIN messages m ON m.id = a.message_id
            WHERE a.tenant_id = $1 AND m.account_id = $2
            ORDER BY a.file_name ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        let events = self.fetch_client_events(account_id).await?;
        let contacts = self.fetch_client_contacts(account_id).await?;
        let tasks = self.fetch_client_tasks(account_id).await?;

        let messages = message_rows
            .into_iter()
            .map(|row| {
                let attachments = attachment_rows
                    .iter()
                    .filter(|attachment| attachment.message_id == row.id)
                    .map(|attachment| ClientAttachment {
                        id: attachment.id,
                        name: attachment.name.clone(),
                        kind: attachment_kind(&attachment.media_type, &attachment.name),
                        size: format_size(attachment.size_octets),
                    })
                    .collect();

                ClientMessage {
                    id: row.id,
                    folder: client_folder(&row.mailbox_role),
                    from: row.from_name,
                    from_address: row.from_address,
                    to: row.to_recipients,
                    cc: row.cc_recipients,
                    subject: row.subject,
                    preview: row.preview,
                    received_at: row.received_at,
                    time_label: row.time_label,
                    unread: row.unread,
                    flagged: row.flagged,
                    tags: client_message_tags(&row.mailbox_role, &row.delivery_status),
                    attachments,
                    body: body_paragraphs(&row.body_text),
                }
            })
            .collect();

        Ok(ClientWorkspace {
            messages,
            events,
            contacts,
            tasks,
        })
    }

    pub async fn fetch_jmap_mailboxes(&self, account_id: Uuid) -> Result<Vec<JmapMailbox>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, JmapMailboxRow>(
            r#"
            SELECT
                mb.id,
                mb.role,
                mb.display_name,
                mb.sort_order,
                COUNT(m.id) AS total_emails,
                COUNT(*) FILTER (WHERE m.unread) AS unread_emails
            FROM mailboxes mb
            LEFT JOIN messages m
              ON m.mailbox_id = mb.id
             AND m.tenant_id = mb.tenant_id
             AND m.account_id = mb.account_id
            WHERE mb.tenant_id = $1
              AND mb.account_id = $2
            GROUP BY mb.id, mb.role, mb.display_name, mb.sort_order
            ORDER BY mb.sort_order ASC, lower(mb.display_name) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| JmapMailbox {
                id: row.id,
                role: row.role,
                name: row.display_name,
                sort_order: row.sort_order,
                total_emails: row.total_emails.max(0) as u32,
                unread_emails: row.unread_emails.max(0) as u32,
            })
            .collect())
    }

    pub async fn ensure_imap_mailboxes(&self, account_id: Uuid) -> Result<Vec<JmapMailbox>> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, account_id)
            .await?;
        self.ensure_mailbox(&mut tx, &tenant_id, account_id, "inbox", "Inbox", 0, 365)
            .await?;
        self.ensure_mailbox(&mut tx, &tenant_id, account_id, "drafts", "Drafts", 10, 365)
            .await?;
        self.ensure_mailbox(&mut tx, &tenant_id, account_id, "sent", "Sent", 20, 365)
            .await?;
        tx.commit().await?;

        self.fetch_jmap_mailboxes(account_id).await
    }

    pub async fn fetch_jmap_mailbox_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        Ok(self
            .fetch_jmap_mailboxes(account_id)
            .await?
            .into_iter()
            .map(|mailbox| mailbox.id)
            .collect())
    }

    pub async fn create_jmap_mailbox(
        &self,
        input: JmapMailboxCreateInput,
        audit: AuditEntryInput,
    ) -> Result<JmapMailbox> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;

        let name = input.name.trim();
        if name.is_empty() {
            bail!("mailbox name is required");
        }

        let duplicate = sqlx::query(
            r#"
            SELECT 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND lower(display_name) = lower($3)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?;
        if duplicate.is_some() {
            bail!("mailbox already exists");
        }

        let next_sort_order = sqlx::query_scalar::<_, i32>(
            r#"
            SELECT COALESCE(MAX(sort_order), 0) + 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .fetch_one(&mut *tx)
        .await?;

        let mailbox_id = Uuid::new_v4();
        let sort_order = input.sort_order.unwrap_or(next_sort_order);
        sqlx::query(
            r#"
            INSERT INTO mailboxes (id, tenant_id, account_id, role, display_name, sort_order, retention_days)
            VALUES ($1, $2, $3, '', $4, $5, 365)
            "#,
        )
        .bind(mailbox_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(name)
        .bind(sort_order)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;

        self.fetch_jmap_mailboxes(input.account_id)
            .await?
            .into_iter()
            .find(|mailbox| mailbox.id == mailbox_id)
            .ok_or_else(|| anyhow::anyhow!("mailbox creation failed"))
    }

    pub async fn update_jmap_mailbox(
        &self,
        input: JmapMailboxUpdateInput,
        audit: AuditEntryInput,
    ) -> Result<JmapMailbox> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let current = sqlx::query(
            r#"
            SELECT role, display_name, sort_order
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("mailbox not found"))?;

        let role = current.try_get::<String, _>("role")?;
        if !role.trim().is_empty() {
            bail!("system mailbox cannot be modified through JMAP");
        }

        let current_name = current.try_get::<String, _>("display_name")?;
        let current_sort_order = current.try_get::<i32, _>("sort_order")?;
        let name = input
            .name
            .as_deref()
            .unwrap_or(&current_name)
            .trim()
            .to_string();
        if name.is_empty() {
            bail!("mailbox name is required");
        }

        let duplicate = sqlx::query(
            r#"
            SELECT 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND lower(display_name) = lower($3) AND id <> $4
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(&name)
        .bind(input.mailbox_id)
        .fetch_optional(&mut *tx)
        .await?;
        if duplicate.is_some() {
            bail!("mailbox already exists");
        }

        sqlx::query(
            r#"
            UPDATE mailboxes
            SET display_name = $4, sort_order = $5
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.mailbox_id)
        .bind(name)
        .bind(input.sort_order.unwrap_or(current_sort_order))
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_mailboxes(input.account_id)
            .await?
            .into_iter()
            .find(|mailbox| mailbox.id == input.mailbox_id)
            .ok_or_else(|| anyhow::anyhow!("mailbox update failed"))
    }

    pub async fn destroy_jmap_mailbox(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let current = sqlx::query(
            r#"
            SELECT role
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("mailbox not found"))?;

        let role = current.try_get::<String, _>("role")?;
        if !role.trim().is_empty() {
            bail!("system mailbox cannot be deleted through JMAP");
        }

        let message_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM messages
            WHERE tenant_id = $1 AND account_id = $2 AND mailbox_id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_one(&mut *tx)
        .await?;
        if message_count > 0 {
            bail!("mailbox is not empty");
        }

        sqlx::query(
            r#"
            DELETE FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn query_jmap_email_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        position: u64,
        limit: u64,
    ) -> Result<JmapEmailQuery> {
        let normalized_search = search_text
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let ids = sqlx::query(
            r#"
            SELECT s.message_id AS id
            FROM searchable_mail_documents s
            WHERE s.account_id = $1
              AND ($2::uuid IS NULL OR s.mailbox_id = $2)
              AND (
                $3::text IS NULL
                OR (s.message_search_vector || s.attachment_search_vector)
                    @@ websearch_to_tsquery('simple', $3)
              )
            ORDER BY s.received_at DESC, s.message_id DESC
            OFFSET $4
            LIMIT $5
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .bind(position as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| row.try_get("id"))
        .collect::<std::result::Result<Vec<Uuid>, sqlx::Error>>()?;

        let total: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM searchable_mail_documents s
            WHERE s.account_id = $1
              AND ($2::uuid IS NULL OR s.mailbox_id = $2)
              AND (
                $3::text IS NULL
                OR (s.message_search_vector || s.attachment_search_vector)
                    @@ websearch_to_tsquery('simple', $3)
              )
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(JmapEmailQuery {
            ids,
            total: total.max(0) as u64,
        })
    }

    pub async fn fetch_all_jmap_email_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT id
            FROM messages
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY COALESCE(sent_at, received_at) DESC, id DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| row.try_get("id").map_err(Into::into))
            .collect()
    }

    pub async fn fetch_all_jmap_thread_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT thread_id
            FROM messages
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY thread_id
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| row.try_get("thread_id").map_err(Into::into))
            .collect()
    }

    pub async fn query_jmap_thread_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        position: u64,
        limit: u64,
    ) -> Result<JmapThreadQuery> {
        let normalized_search = search_text
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let ids = sqlx::query(
            r#"
            WITH matched_threads AS (
                SELECT
                    m.thread_id,
                    MAX(s.received_at) AS latest_received_at
                FROM searchable_mail_documents s
                JOIN messages m ON m.id = s.message_id
                WHERE s.account_id = $1
                  AND ($2::uuid IS NULL OR s.mailbox_id = $2)
                  AND (
                    $3::text IS NULL
                    OR (s.message_search_vector || s.attachment_search_vector)
                        @@ websearch_to_tsquery('simple', $3)
                  )
                GROUP BY m.thread_id
            )
            SELECT thread_id
            FROM matched_threads
            ORDER BY latest_received_at DESC, thread_id DESC
            OFFSET $4
            LIMIT $5
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .bind(position as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| row.try_get("thread_id"))
        .collect::<std::result::Result<Vec<Uuid>, sqlx::Error>>()?;

        let total: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM (
                SELECT m.thread_id
                FROM searchable_mail_documents s
                JOIN messages m ON m.id = s.message_id
                WHERE s.account_id = $1
                  AND ($2::uuid IS NULL OR s.mailbox_id = $2)
                  AND (
                    $3::text IS NULL
                    OR (s.message_search_vector || s.attachment_search_vector)
                        @@ websearch_to_tsquery('simple', $3)
                  )
                GROUP BY m.thread_id
            ) matched_threads
            "#,
        )
        .bind(account_id)
        .bind(mailbox_id)
        .bind(normalized_search.as_deref())
        .fetch_one(&self.pool)
        .await?;

        Ok(JmapThreadQuery {
            ids,
            total: total.max(0) as u64,
        })
    }

    pub async fn fetch_jmap_emails(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JmapEmail>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query_as::<_, JmapEmailRow>(
            r#"
            SELECT
                m.id,
                m.imap_uid,
                m.thread_id,
                m.mailbox_id,
                mb.role AS mailbox_role,
                mb.display_name AS mailbox_name,
                to_char(m.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at,
                CASE
                    WHEN m.sent_at IS NULL THEN NULL
                    ELSE to_char(m.sent_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS sent_at,
                m.from_address,
                NULLIF(m.from_display, '') AS from_display,
                NULLIF(m.sender_address, '') AS sender_address,
                NULLIF(m.sender_display, '') AS sender_display,
                m.sender_authorization_kind,
                m.submitted_by_account_id,
                m.subject_normalized AS subject,
                m.preview_text AS preview,
                COALESCE(b.body_text, '') AS body_text,
                b.body_html_sanitized,
                m.unread,
                m.flagged,
                m.has_attachments,
                m.size_octets,
                m.internet_message_id,
                m.mime_blob_ref,
                m.delivery_status
            FROM messages m
            JOIN mailboxes mb ON mb.id = m.mailbox_id
            LEFT JOIN message_bodies b ON b.message_id = m.id
            WHERE m.tenant_id = $1
              AND m.account_id = $2
              AND m.id = ANY($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        let recipient_rows = sqlx::query_as::<_, JmapEmailRecipientRow>(
            r#"
            SELECT
                r.message_id,
                r.kind,
                r.address,
                r.display_name,
                r.ordinal AS _ordinal
            FROM message_recipients r
            JOIN messages m ON m.id = r.message_id
            WHERE r.tenant_id = $1
              AND m.account_id = $2
              AND r.message_id = ANY($3)
            ORDER BY r.message_id ASC, r.kind ASC, r.ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        let mut emails = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(row) = rows.iter().find(|row| row.id == *id) {
                let to = recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == *id && recipient.kind == "to")
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();
                let cc = recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == *id && recipient.kind == "cc")
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();
                let bcc = sqlx::query(
                    r#"
                    SELECT address, display_name
                    FROM message_bcc_recipients
                    WHERE tenant_id = $1 AND message_id = $2
                    ORDER BY ordinal ASC
                    "#,
                )
                .bind(&tenant_id)
                .bind(*id)
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .map(|row| JmapEmailAddress {
                    address: row.try_get("address").unwrap_or_default(),
                    display_name: row.try_get("display_name").ok(),
                })
                .collect();

                emails.push(JmapEmail {
                    id: row.id,
                    thread_id: row.thread_id,
                    mailbox_id: row.mailbox_id,
                    mailbox_role: row.mailbox_role.clone(),
                    mailbox_name: row.mailbox_name.clone(),
                    received_at: row.received_at.clone(),
                    sent_at: row.sent_at.clone(),
                    from_address: row.from_address.clone(),
                    from_display: row.from_display.clone(),
                    sender_address: row.sender_address.clone(),
                    sender_display: row.sender_display.clone(),
                    sender_authorization_kind: row.sender_authorization_kind.clone(),
                    submitted_by_account_id: row.submitted_by_account_id,
                    to,
                    cc,
                    bcc,
                    subject: row.subject.clone(),
                    preview: row.preview.clone(),
                    body_text: row.body_text.clone(),
                    body_html_sanitized: row.body_html_sanitized.clone(),
                    unread: row.unread,
                    flagged: row.flagged,
                    has_attachments: row.has_attachments,
                    size_octets: row.size_octets,
                    internet_message_id: row.internet_message_id.clone(),
                    mime_blob_ref: row.mime_blob_ref.clone(),
                    delivery_status: row.delivery_status.clone(),
                });
            }
        }

        Ok(emails)
    }

    pub async fn fetch_imap_emails(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
    ) -> Result<Vec<ImapEmail>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, JmapEmailRow>(
            r#"
            SELECT
                m.id,
                m.imap_uid,
                m.thread_id,
                m.mailbox_id,
                mb.role AS mailbox_role,
                mb.display_name AS mailbox_name,
                to_char(m.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at,
                CASE
                    WHEN m.sent_at IS NULL THEN NULL
                    ELSE to_char(m.sent_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS sent_at,
                m.from_address,
                NULLIF(m.from_display, '') AS from_display,
                NULLIF(m.sender_address, '') AS sender_address,
                NULLIF(m.sender_display, '') AS sender_display,
                m.sender_authorization_kind,
                m.submitted_by_account_id,
                m.subject_normalized AS subject,
                m.preview_text AS preview,
                COALESCE(b.body_text, '') AS body_text,
                b.body_html_sanitized,
                m.unread,
                m.flagged,
                m.has_attachments,
                m.size_octets,
                m.internet_message_id,
                m.mime_blob_ref,
                m.delivery_status
            FROM messages m
            JOIN mailboxes mb ON mb.id = m.mailbox_id
            LEFT JOIN message_bodies b ON b.message_id = m.id
            WHERE m.tenant_id = $1
              AND m.account_id = $2
              AND m.mailbox_id = $3
            ORDER BY m.imap_uid ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .fetch_all(&self.pool)
        .await?;

        let message_ids = rows.iter().map(|row| row.id).collect::<Vec<_>>();
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        let recipient_rows = sqlx::query_as::<_, JmapEmailRecipientRow>(
            r#"
            SELECT
                r.message_id,
                r.kind,
                r.address,
                r.display_name,
                r.ordinal AS _ordinal
            FROM message_recipients r
            JOIN messages m ON m.id = r.message_id
            WHERE r.tenant_id = $1
              AND m.account_id = $2
              AND r.message_id = ANY($3)
            ORDER BY r.message_id ASC, r.kind ASC, r.ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&message_ids)
        .fetch_all(&self.pool)
        .await?;

        let bcc_rows = sqlx::query_as::<_, MessageBccRecipientRecordRow>(
            r#"
            SELECT message_id, address, display_name
            FROM message_bcc_recipients
            WHERE tenant_id = $1 AND message_id = ANY($2)
            ORDER BY message_id ASC, ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(&message_ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let uid = u32::try_from(row.imap_uid)
                    .map_err(|_| anyhow!("message IMAP UID is out of range"))?;
                let to = recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == row.id && recipient.kind == "to")
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();
                let cc = recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == row.id && recipient.kind == "cc")
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();
                let bcc = bcc_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == row.id)
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect();

                Ok(ImapEmail {
                    id: row.id,
                    uid,
                    thread_id: row.thread_id,
                    mailbox_id: row.mailbox_id,
                    mailbox_role: row.mailbox_role,
                    mailbox_name: row.mailbox_name,
                    received_at: row.received_at,
                    sent_at: row.sent_at,
                    from_address: row.from_address,
                    from_display: row.from_display,
                    to,
                    cc,
                    bcc,
                    subject: row.subject,
                    preview: row.preview,
                    body_text: row.body_text,
                    body_html_sanitized: row.body_html_sanitized,
                    unread: row.unread,
                    flagged: row.flagged,
                    has_attachments: row.has_attachments,
                    size_octets: row.size_octets,
                    internet_message_id: row.internet_message_id,
                    delivery_status: row.delivery_status,
                })
            })
            .collect()
    }

    pub async fn update_imap_flags(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        message_ids: &[Uuid],
        unread: Option<bool>,
        flagged: Option<bool>,
    ) -> Result<()> {
        if message_ids.is_empty() {
            return Ok(());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            UPDATE messages
            SET
                unread = COALESCE($4, unread),
                flagged = COALESCE($5, flagged)
            WHERE tenant_id = $1
              AND account_id = $2
              AND mailbox_id = $3
              AND id = ANY($6)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(unread)
        .bind(flagged)
        .bind(message_ids)
        .execute(&mut *tx)
        .await?;

        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn fetch_jmap_draft(&self, account_id: Uuid, id: Uuid) -> Result<Option<JmapEmail>> {
        let emails = self.fetch_jmap_emails(account_id, &[id]).await?;
        Ok(emails
            .into_iter()
            .find(|email| email.mailbox_role == "drafts"))
    }

    pub async fn fetch_jmap_email_submissions(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JmapEmailSubmission>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, JmapEmailSubmissionRow>(
            r#"
            SELECT
                q.id,
                q.message_id AS email_id,
                m.thread_id,
                m.from_address,
                NULLIF(m.sender_address, '') AS sender_address,
                m.sender_authorization_kind,
                to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS send_at,
                q.status AS queue_status,
                m.delivery_status
            FROM outbound_message_queue q
            JOIN messages m ON m.id = q.message_id
            WHERE q.tenant_id = $1
              AND q.account_id = $2
              AND ($3::uuid[] IS NULL OR q.id = ANY($3))
            ORDER BY q.created_at DESC, q.id DESC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(if ids.is_empty() {
            None::<Vec<Uuid>>
        } else {
            Some(ids.to_vec())
        })
        .fetch_all(&self.pool)
        .await?;

        let message_ids = rows.iter().map(|row| row.email_id).collect::<Vec<_>>();
        let recipient_rows = if message_ids.is_empty() {
            Vec::new()
        } else {
            sqlx::query_as::<_, JmapEmailRecipientRow>(
                r#"
                SELECT
                    r.message_id,
                    r.kind,
                    r.address,
                    r.display_name,
                    r.ordinal AS _ordinal
                FROM message_recipients r
                WHERE r.tenant_id = $1
                  AND r.message_id = ANY($2)
                ORDER BY r.message_id ASC, r.kind ASC, r.ordinal ASC
                "#,
            )
            .bind(&tenant_id)
            .bind(&message_ids)
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows
            .into_iter()
            .map(|row| JmapEmailSubmission {
                id: row.id,
                email_id: row.email_id,
                thread_id: row.thread_id,
                identity_id: sender_identity_id(
                    sender_authorization_kind_from_str(&row.sender_authorization_kind),
                    account_id,
                ),
                identity_email: row.from_address.clone(),
                envelope_mail_from: row
                    .sender_address
                    .clone()
                    .unwrap_or_else(|| row.from_address.clone()),
                envelope_rcpt_to: recipient_rows
                    .iter()
                    .filter(|recipient| recipient.message_id == row.email_id)
                    .map(|recipient| recipient.address.clone())
                    .collect(),
                send_at: row.send_at,
                undo_status: "final".to_string(),
                delivery_status: if row.delivery_status.trim().is_empty() {
                    row.queue_status
                } else {
                    row.delivery_status
                },
            })
            .collect())
    }

    pub async fn fetch_jmap_quota(&self, account_id: Uuid) -> Result<JmapQuota> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, AccountQuotaRow>(
            r#"
            SELECT quota_mb, used_mb
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("account not found"))?;

        Ok(JmapQuota {
            id: "mail".to_string(),
            name: "Mail".to_string(),
            used: (row.used_mb.max(0) as u64) * 1024 * 1024,
            hard_limit: (row.quota_mb.max(0) as u64) * 1024 * 1024,
        })
    }

    pub async fn fetch_outbound_handoff_batch(
        &self,
        limit: i64,
    ) -> Result<Vec<OutboundMessageHandoffRequest>> {
        let rows = sqlx::query_as::<_, PendingOutboundQueueRow>(
            r#"
            SELECT
                q.id AS queue_id,
                q.message_id,
                q.account_id,
                q.attempts,
                m.from_address,
                m.from_display,
                m.sender_address,
                m.sender_display,
                m.sender_authorization_kind,
                m.subject_normalized AS subject,
                b.body_text,
                b.body_html_sanitized,
                m.internet_message_id,
                q.last_error
            FROM outbound_message_queue q
            JOIN messages m ON m.id = q.message_id
            JOIN message_bodies b ON b.message_id = m.id
            WHERE q.status IN ('queued', 'deferred')
              AND q.next_attempt_at <= NOW()
            ORDER BY q.created_at ASC, q.id ASC
            LIMIT $1
            "#,
        )
        .bind(limit.max(1))
        .fetch_all(&self.pool)
        .await?;

        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let recipients = sqlx::query_as::<_, JmapEmailRecipientRow>(
                r#"
                SELECT
                    r.message_id,
                    r.kind,
                    r.address,
                    r.display_name,
                    r.ordinal AS _ordinal
                FROM message_recipients r
                WHERE r.tenant_id = $1
                  AND r.message_id = $2
                ORDER BY r.kind ASC, r.ordinal ASC
                "#,
            )
            .bind(self.tenant_id_for_account_id(row.account_id).await?)
            .bind(row.message_id)
            .fetch_all(&self.pool)
            .await?;

            let bcc = sqlx::query_as::<_, MessageBccRecipientRow>(
                r#"
                SELECT address, display_name
                FROM message_bcc_recipients
                WHERE tenant_id = $1 AND message_id = $2
                ORDER BY ordinal ASC
                "#,
            )
            .bind(self.tenant_id_for_account_id(row.account_id).await?)
            .bind(row.message_id)
            .fetch_all(&self.pool)
            .await?;

            let to = recipients
                .iter()
                .filter(|recipient| recipient.kind == "to")
                .map(|recipient| TransportRecipient {
                    address: recipient.address.clone(),
                    display_name: recipient.display_name.clone(),
                })
                .collect();
            let cc = recipients
                .iter()
                .filter(|recipient| recipient.kind == "cc")
                .map(|recipient| TransportRecipient {
                    address: recipient.address.clone(),
                    display_name: recipient.display_name.clone(),
                })
                .collect();
            let bcc = bcc
                .into_iter()
                .map(|recipient| TransportRecipient {
                    address: recipient.address,
                    display_name: recipient.display_name,
                })
                .collect();

            items.push(OutboundMessageHandoffRequest {
                queue_id: row.queue_id,
                message_id: row.message_id,
                account_id: row.account_id,
                from_address: row.from_address,
                from_display: row.from_display,
                sender_address: row.sender_address,
                sender_display: row.sender_display,
                sender_authorization_kind: row.sender_authorization_kind,
                to,
                cc,
                bcc,
                subject: row.subject,
                body_text: row.body_text,
                body_html_sanitized: row.body_html_sanitized,
                internet_message_id: row.internet_message_id,
                attempt_count: row.attempts.max(0) as u32,
                last_attempt_error: row.last_error,
            });
        }

        Ok(items)
    }

    pub async fn update_outbound_queue_status(
        &self,
        response: &OutboundMessageHandoffResponse,
    ) -> Result<OutboundQueueStatusUpdate> {
        let tenant_id = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM outbound_message_queue
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(response.queue_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("outbound queue item not found"))?;
        let status_value = response.status.as_str().to_string();
        let retry_after_seconds = response
            .retry
            .as_ref()
            .map(|retry| retry.retry_after_seconds.min(i32::MAX as u32) as i32);
        let retry_policy = response.retry.as_ref().map(|retry| retry.policy.clone());
        let technical_status = serde_json::to_value(response)?;
        let row = sqlx::query(
            r#"
            UPDATE outbound_message_queue
            SET status = $3,
                attempts = attempts + 1,
                next_attempt_at = CASE
                    WHEN $3 = 'deferred'
                        THEN NOW() + make_interval(secs => GREATEST(1, COALESCE($4, LEAST(3600, GREATEST(1, attempts + 1) * 300))))
                    ELSE NOW()
                END,
                last_error = CASE
                    WHEN $3 = 'relayed' THEN NULL
                    ELSE $5
                END,
                remote_message_ref = COALESCE($6, remote_message_ref),
                last_result_json = $7,
                last_attempt_at = NOW(),
                retry_after_seconds = $4,
                retry_policy = $8,
                last_dsn_action = $9,
                last_dsn_status = $10,
                last_smtp_code = $11,
                last_enhanced_status = $12,
                last_routing_rule = $13,
                last_throttle_scope = $14,
                last_throttle_delay_seconds = $15,
                updated_at = NOW()
            WHERE tenant_id = $1 AND id = $2
            RETURNING message_id, status, remote_message_ref, retry_after_seconds, retry_policy, last_result_json
            "#,
        )
        .bind(&tenant_id)
        .bind(response.queue_id)
        .bind(&status_value)
        .bind(retry_after_seconds)
        .bind(response.detail.as_deref())
        .bind(response.remote_message_ref.as_deref())
        .bind(&technical_status)
        .bind(retry_policy.as_deref())
        .bind(response.dsn.as_ref().map(|dsn| dsn.action.as_str()))
        .bind(response.dsn.as_ref().map(|dsn| dsn.status.as_str()))
        .bind(response.technical.as_ref().and_then(|status| status.smtp_code.map(i32::from)))
        .bind(
            response
                .technical
                .as_ref()
                .and_then(|status| status.enhanced_code.as_deref()),
        )
        .bind(response.route.as_ref().and_then(|route| route.rule_id.as_deref()))
        .bind(
            response
                .throttle
                .as_ref()
                .map(|throttle| throttle.scope.as_str()),
        )
        .bind(response.throttle.as_ref().map(|throttle| {
            throttle.retry_after_seconds.min(i32::MAX as u32) as i32
        }))
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("outbound queue item not found"))?;

        let message_id: Uuid = row.try_get("message_id")?;
        let stored_status: String = row.try_get("status")?;
        let stored_remote_message_ref: Option<String> = row.try_get("remote_message_ref")?;
        let stored_retry_after_seconds: Option<i32> = row.try_get("retry_after_seconds")?;
        let stored_retry_policy: Option<String> = row.try_get("retry_policy")?;
        let stored_technical_status: Value = row.try_get("last_result_json")?;

        sqlx::query(
            r#"
            UPDATE messages
            SET delivery_status = $3
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .bind(&stored_status)
        .execute(&self.pool)
        .await?;

        Ok(OutboundQueueStatusUpdate {
            queue_id: response.queue_id,
            message_id,
            status: stored_status,
            remote_message_ref: stored_remote_message_ref,
            retry_after_seconds: stored_retry_after_seconds,
            retry_policy: stored_retry_policy,
            technical_status: stored_technical_status,
        })
    }

    pub async fn mark_outbound_queue_attempt_failure(
        &self,
        queue_id: Uuid,
        detail: &str,
    ) -> Result<OutboundQueueStatusUpdate> {
        self.update_outbound_queue_status(&OutboundMessageHandoffResponse {
            queue_id,
            status: TransportDeliveryStatus::Deferred,
            trace_id: format!("lpe-dispatch-{queue_id}"),
            detail: Some(detail.to_string()),
            remote_message_ref: None,
            retry: None,
            dsn: None,
            technical: None,
            route: None,
            throttle: None,
        })
        .await
    }

    pub async fn deliver_inbound_message(
        &self,
        request: InboundDeliveryRequest,
    ) -> Result<InboundDeliveryResponse> {
        let mail_from = normalize_email(&request.mail_from);
        let subject = normalize_subject(&request.subject);
        let body_text = request.body_text.trim().to_string();
        let headers = parse_headers_map(&request.raw_message);
        let rcpt_to = request
            .rcpt_to
            .iter()
            .map(|recipient| normalize_email(recipient))
            .filter(|recipient| !recipient.is_empty())
            .collect::<Vec<_>>();

        if mail_from.is_empty() {
            bail!("mail_from is required");
        }
        if rcpt_to.is_empty() {
            bail!("at least one recipient is required");
        }

        let visible_to = parse_header_recipients(&request.raw_message, "to");
        let visible_cc = parse_header_recipients(&request.raw_message, "cc");
        let mut visible_recipients = Vec::with_capacity(visible_to.len() + visible_cc.len());
        push_recipients(&mut visible_recipients, "to", &visible_to);
        push_recipients(&mut visible_recipients, "cc", &visible_cc);
        let participants = participants_normalized(&mail_from, &visible_recipients);
        let preview = preview_text(&body_text);
        let size_octets = request.raw_message.len() as i64;

        let account_rows = sqlx::query(
            r#"
            SELECT id, tenant_id, primary_email, display_name
            FROM accounts
            WHERE lower(primary_email) = ANY($1)
            ORDER BY primary_email ASC
            "#,
        )
        .bind(&rcpt_to)
        .fetch_all(&self.pool)
        .await?;

        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        let mut stored_message_ids = Vec::new();
        let mut tx = self.pool.begin().await?;
        let thread_id = Uuid::new_v4();
        let attachments = parse_message_attachments(&request.raw_message)?;
        let mut followups = Vec::new();

        for recipient in &rcpt_to {
            let Some(row) = account_rows.iter().find(|row| {
                row.try_get::<String, _>("primary_email")
                    .map(|value| normalize_email(&value) == *recipient)
                    .unwrap_or(false)
            }) else {
                rejected.push(recipient.clone());
                continue;
            };

            let account_id: Uuid = row.try_get("id")?;
            let tenant_id: String = row.try_get("tenant_id")?;
            let account_email: String = row.try_get("primary_email")?;
            let account_display_name: String = row.try_get("display_name")?;
            let sieve_outcome = self
                .evaluate_inbound_sieve(account_id, &mail_from, recipient, &headers, &account_email)
                .await?;
            let had_sieve_actions = sieve_outcome.file_into.is_some()
                || sieve_outcome.discard
                || !sieve_outcome.redirects.is_empty()
                || sieve_outcome.vacation.is_some();

            if !sieve_outcome.discard {
                let mailbox_id = if let Some(folder_name) = sieve_outcome.file_into.as_deref() {
                    self.ensure_named_mailbox(
                        &mut tx,
                        &tenant_id,
                        account_id,
                        folder_name,
                        DEFAULT_SIEVE_MAILBOX_RETENTION_DAYS,
                    )
                    .await?
                } else {
                    self.ensure_mailbox(
                        &mut tx,
                        &tenant_id,
                        account_id,
                        "inbox",
                        "Inbox",
                        0,
                        DEFAULT_SIEVE_MAILBOX_RETENTION_DAYS,
                    )
                    .await?
                };
                let message_id = Uuid::new_v4();
                self.store_inbound_message_in_tx(
                    &mut tx,
                    &tenant_id,
                    account_id,
                    mailbox_id,
                    thread_id,
                    message_id,
                    &request,
                    &mail_from,
                    &subject,
                    &preview,
                    size_octets,
                    &body_text,
                    &participants,
                    &visible_recipients,
                    &attachments,
                )
                .await?;
                stored_message_ids.push(message_id);
            }

            if !sieve_outcome.redirects.is_empty() || sieve_outcome.vacation.is_some() {
                followups.push(SieveFollowUp {
                    account_id,
                    account_email: normalize_email(&account_email),
                    account_display_name,
                    redirects: sieve_outcome.redirects,
                    vacation: sieve_outcome.vacation,
                    subject: subject.clone(),
                    body_text: body_text.clone(),
                    attachments: attachments.clone(),
                    sender_address: mail_from.clone(),
                });
            }

            if had_sieve_actions {
                self.insert_audit(
                    &mut tx,
                    &tenant_id,
                    AuditEntryInput {
                        actor: account_email.clone(),
                        action: "mail.sieve.applied".to_string(),
                        subject: format!("{}:{}", request.trace_id, recipient),
                    },
                )
                .await?;
            }

            accepted.push(recipient.clone());
        }

        let audit_action = if accepted.is_empty() {
            "mail.inbound.delivery-rejected"
        } else {
            "mail.inbound.delivered"
        };
        self.insert_audit(
            &mut tx,
            PLATFORM_TENANT_ID,
            AuditEntryInput {
                actor: "lpe-ct".to_string(),
                action: audit_action.to_string(),
                subject: request.trace_id.clone(),
            },
        )
        .await?;
        tx.commit().await?;
        let mut followup_errors = Vec::new();

        for followup in followups {
            if let Err(error) = self.dispatch_sieve_followups(&followup).await {
                followup_errors.push(error.to_string());
            }
        }

        Ok(InboundDeliveryResponse {
            trace_id: request.trace_id,
            status: if accepted.is_empty() {
                TransportDeliveryStatus::Failed
            } else {
                TransportDeliveryStatus::Relayed
            },
            accepted_recipients: accepted,
            rejected_recipients: rejected,
            stored_message_ids,
            detail: if followup_errors.is_empty() {
                None
            } else {
                Some(format!(
                    "sieve follow-up errors: {}",
                    followup_errors.join(" | ")
                ))
            },
        })
    }

    async fn evaluate_inbound_sieve(
        &self,
        account_id: Uuid,
        envelope_from: &str,
        envelope_to: &str,
        headers: &std::collections::HashMap<String, String>,
        account_email: &str,
    ) -> Result<SieveExecutionOutcome> {
        let Some(script) = self.fetch_active_sieve_script(account_id).await? else {
            return Ok(SieveExecutionOutcome::default());
        };
        let Ok(script) = parse_script(&script.content) else {
            return Ok(SieveExecutionOutcome::default());
        };

        let mut normalized_headers = BTreeMap::new();
        for (name, value) in headers {
            normalized_headers.insert(name.to_lowercase(), vec![value.clone()]);
        }
        if !normalized_headers.contains_key("to") {
            normalized_headers.insert("to".to_string(), vec![account_email.to_string()]);
        }

        evaluate_script(
            &script,
            &SieveMessageContext {
                envelope_from: envelope_from.to_string(),
                envelope_to: envelope_to.to_string(),
                headers: normalized_headers,
            },
        )
    }

    async fn dispatch_sieve_followups(&self, followup: &SieveFollowUp) -> Result<()> {
        for redirect in followup
            .redirects
            .iter()
            .take(MAX_SIEVE_REDIRECTS_PER_MESSAGE)
        {
            if redirect.eq_ignore_ascii_case(&followup.account_email) {
                continue;
            }
            self.submit_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id: followup.account_id,
                    submitted_by_account_id: followup.account_id,
                    source: "sieve-redirect".to_string(),
                    from_display: Some(followup.account_display_name.clone()),
                    from_address: followup.account_email.clone(),
                    sender_display: None,
                    sender_address: None,
                    to: vec![SubmittedRecipientInput {
                        address: redirect.clone(),
                        display_name: None,
                    }],
                    cc: Vec::new(),
                    bcc: Vec::new(),
                    subject: followup.subject.clone(),
                    body_text: followup.body_text.clone(),
                    body_html_sanitized: None,
                    internet_message_id: None,
                    mime_blob_ref: None,
                    size_octets: estimate_generated_message_size(
                        &followup.subject,
                        &followup.body_text,
                        &followup.attachments,
                    ),
                    unread: Some(false),
                    flagged: Some(false),
                    attachments: followup.attachments.clone(),
                },
                AuditEntryInput {
                    actor: followup.account_email.clone(),
                    action: "mail.sieve.redirect".to_string(),
                    subject: redirect.clone(),
                },
            )
            .await?;
        }

        if let Some(vacation) = &followup.vacation {
            if !followup
                .sender_address
                .eq_ignore_ascii_case(&followup.account_email)
                && self
                    .should_send_sieve_vacation(
                        followup.account_id,
                        &followup.sender_address,
                        vacation,
                    )
                    .await?
            {
                self.submit_message(
                    SubmitMessageInput {
                        draft_message_id: None,
                        account_id: followup.account_id,
                        submitted_by_account_id: followup.account_id,
                        source: "sieve-vacation".to_string(),
                        from_display: Some(followup.account_display_name.clone()),
                        from_address: followup.account_email.clone(),
                        sender_display: None,
                        sender_address: None,
                        to: vec![SubmittedRecipientInput {
                            address: followup.sender_address.clone(),
                            display_name: None,
                        }],
                        cc: Vec::new(),
                        bcc: Vec::new(),
                        subject: vacation
                            .subject
                            .clone()
                            .unwrap_or_else(|| format!("Re: {}", followup.subject)),
                        body_text: vacation.reason.clone(),
                        body_html_sanitized: None,
                        internet_message_id: None,
                        mime_blob_ref: None,
                        size_octets: vacation.reason.len() as i64,
                        unread: Some(false),
                        flagged: Some(false),
                        attachments: Vec::new(),
                    },
                    AuditEntryInput {
                        actor: followup.account_email.clone(),
                        action: "mail.sieve.vacation".to_string(),
                        subject: followup.sender_address.clone(),
                    },
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn should_send_sieve_vacation(
        &self,
        account_id: Uuid,
        sender_address: &str,
        vacation: &VacationAction,
    ) -> Result<bool> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let sender_address = normalize_email(sender_address);
        let response_key = hash_sieve_vacation_key(vacation);
        let updated = sqlx::query(
            r#"
            INSERT INTO sieve_vacation_responses (
                tenant_id, account_id, sender_address, response_key, last_sent_at
            )
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (tenant_id, account_id, sender_address, response_key) DO UPDATE SET
                last_sent_at = EXCLUDED.last_sent_at
            WHERE sieve_vacation_responses.last_sent_at
                <= NOW() - make_interval(days => GREATEST(1, $5))
            RETURNING last_sent_at
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&sender_address)
        .bind(&response_key)
        .bind(vacation.days as i32)
        .fetch_optional(&self.pool)
        .await?;

        Ok(updated.is_some())
    }

    async fn store_inbound_message_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        mailbox_id: Uuid,
        thread_id: Uuid,
        message_id: Uuid,
        request: &InboundDeliveryRequest,
        mail_from: &str,
        subject: &str,
        preview: &str,
        size_octets: i64,
        body_text: &str,
        participants: &str,
        visible_recipients: &[(&'static str, SubmittedRecipientInput)],
        attachments: &[AttachmentUploadInput],
    ) -> Result<()> {
        let mime_blob_ref = format!("lpe-ct-inbound:{}:{message_id}", request.trace_id);

        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                received_at, sent_at, from_display, from_address, sender_display,
                sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                submission_source, delivery_status
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                NOW(), NULL, NULL, $7, NULL,
                NULL, 'self', $3, $8, $9, TRUE, FALSE, FALSE, $10, $11,
                'lpe-ct', 'stored'
            )
            "#,
        )
        .bind(message_id)
        .bind(tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(thread_id)
        .bind(request.internet_message_id.as_deref())
        .bind(mail_from)
        .bind(subject)
        .bind(preview)
        .bind(size_octets.max(0))
        .bind(&mime_blob_ref)
        .execute(&mut **tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_bodies (
                message_id, body_text, body_html_sanitized, participants_normalized,
                language_code, content_hash, search_vector
            )
            VALUES ($1, $2, NULL, $3, NULL, $4, to_tsvector('simple', $5))
            "#,
        )
        .bind(message_id)
        .bind(body_text)
        .bind(participants)
        .bind(format!("inbound:{}:{message_id}", request.trace_id))
        .bind(format!("{subject} {body_text} {participants}"))
        .execute(&mut **tx)
        .await?;

        for (ordinal, (kind, recipient_value)) in visible_recipients.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (
                    id, tenant_id, message_id, kind, address, display_name, ordinal
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id)
            .bind(message_id)
            .bind(kind)
            .bind(&recipient_value.address)
            .bind(recipient_value.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut **tx)
            .await?;
        }

        self.ingest_message_attachments_in_tx(tx, tenant_id, account_id, message_id, attachments)
            .await
    }

    async fn ensure_named_mailbox(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        display_name: &str,
        retention_days: i32,
    ) -> Result<Uuid> {
        let display_name = display_name.trim();
        if display_name.is_empty() {
            bail!("fileinto target mailbox is required");
        }
        if let Some(mailbox_id) = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND lower(display_name) = lower($3)
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(display_name)
        .fetch_optional(&mut **tx)
        .await?
        {
            return Ok(mailbox_id);
        }

        let sort_order = sqlx::query_scalar::<_, i32>(
            r#"
            SELECT COALESCE(MAX(sort_order), 0) + 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_one(&mut **tx)
        .await?;
        let mailbox_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO mailboxes (
                id, tenant_id, account_id, role, display_name, sort_order, retention_days
            )
            VALUES ($1, $2, $3, '', $4, $5, $6)
            "#,
        )
        .bind(mailbox_id)
        .bind(tenant_id)
        .bind(account_id)
        .bind(display_name)
        .bind(sort_order)
        .bind(retention_days)
        .execute(&mut **tx)
        .await?;
        Ok(mailbox_id)
    }

    pub async fn save_jmap_upload_blob(
        &self,
        account_id: Uuid,
        media_type: &str,
        blob_bytes: &[u8],
    ) -> Result<JmapUploadBlob> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, account_id)
            .await?;

        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO jmap_upload_blobs (id, tenant_id, account_id, media_type, octet_size, blob_bytes)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(id)
        .bind(&tenant_id)
        .bind(account_id)
        .bind(media_type.trim())
        .bind(blob_bytes.len() as i64)
        .bind(blob_bytes)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(JmapUploadBlob {
            id,
            account_id,
            media_type: media_type.trim().to_string(),
            octet_size: blob_bytes.len() as u64,
            blob_bytes: blob_bytes.to_vec(),
        })
    }

    pub async fn fetch_jmap_upload_blob(
        &self,
        account_id: Uuid,
        blob_id: Uuid,
    ) -> Result<Option<JmapUploadBlob>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, JmapUploadBlobRow>(
            r#"
            SELECT id, account_id, media_type, octet_size, blob_bytes
            FROM jmap_upload_blobs
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(blob_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| JmapUploadBlob {
            id: row.id,
            account_id: row.account_id,
            media_type: row.media_type,
            octet_size: row.octet_size.max(0) as u64,
            blob_bytes: row.blob_bytes,
        }))
    }

    pub async fn save_draft_message(
        &self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> Result<SavedDraftMessage> {
        let from_address = normalize_email(&input.from_address);
        let sender_address = input
            .sender_address
            .as_deref()
            .map(normalize_email)
            .filter(|value| !value.is_empty());
        let subject = normalize_subject(&input.subject);
        let body_text = input.body_text.trim().to_string();
        let visible_recipients = normalize_visible_recipients(&input);
        let bcc_recipients = normalize_bcc_recipients(&input);

        if from_address.is_empty() {
            bail!("from_address is required");
        }

        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;
        self.ensure_same_tenant_account_in_tx(&mut tx, &tenant_id, input.submitted_by_account_id)
            .await?;
        let draft_mailbox_id = self
            .ensure_mailbox(
                &mut tx,
                &tenant_id,
                input.account_id,
                "drafts",
                "Drafts",
                10,
                365,
            )
            .await?;

        let message_id = input.draft_message_id.unwrap_or_else(Uuid::new_v4);
        let thread_id = Uuid::new_v4();
        let preview_text = preview_text(&body_text);
        let participants_normalized = participants_normalized(&from_address, &visible_recipients);
        let mime_blob_ref = input
            .mime_blob_ref
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("draft-message:{message_id}"));
        let content_hash = format!("draft:{message_id}");
        let unread = input.unread.unwrap_or(false);
        let flagged = input.flagged.unwrap_or(false);

        if input.draft_message_id.is_some() {
            let updated = sqlx::query(
                r#"
                UPDATE messages m
                SET
                    internet_message_id = $5,
                    received_at = NOW(),
                    sent_at = NULL,
                    from_display = $6,
                    from_address = $7,
                    sender_display = $8,
                    sender_address = $9,
                    sender_authorization_kind = $10,
                    submitted_by_account_id = $11,
                    subject_normalized = $12,
                    preview_text = $13,
                    size_octets = $14,
                    mime_blob_ref = $15,
                    unread = $16,
                    flagged = $17,
                    has_attachments = FALSE,
                    submission_source = $18,
                    delivery_status = 'draft'
                FROM mailboxes mb
                WHERE m.mailbox_id = mb.id
                  AND m.tenant_id = $1
                  AND m.account_id = $2
                  AND m.id = $3
                  AND mb.role = 'drafts'
                  AND mb.id = $4
                "#,
            )
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(message_id)
            .bind(draft_mailbox_id)
            .bind(input.internet_message_id)
            .bind(input.from_display.map(|value| value.trim().to_string()))
            .bind(&from_address)
            .bind(input.sender_display.map(|value| value.trim().to_string()))
            .bind(sender_address.as_deref())
            .bind(SenderAuthorizationKind::SelfSend.as_str())
            .bind(input.submitted_by_account_id)
            .bind(&subject)
            .bind(&preview_text)
            .bind(input.size_octets.max(0))
            .bind(&mime_blob_ref)
            .bind(unread)
            .bind(flagged)
            .bind(input.source.trim().to_lowercase())
            .execute(&mut *tx)
            .await?;

            if updated.rows_affected() == 0 {
                bail!("draft not found");
            }

            sqlx::query("DELETE FROM message_recipients WHERE tenant_id = $1 AND message_id = $2")
                .bind(&tenant_id)
                .bind(message_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "DELETE FROM message_bcc_recipients WHERE tenant_id = $1 AND message_id = $2",
            )
            .bind(&tenant_id)
            .bind(message_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query("DELETE FROM attachments WHERE tenant_id = $1 AND message_id = $2")
                .bind(&tenant_id)
                .bind(message_id)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query(
                r#"
                INSERT INTO messages (
                    id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                    received_at, sent_at, from_display, from_address, sender_display,
                    sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                    preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                    submission_source, delivery_status
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6,
                    NOW(), NULL, $7, $8, $9,
                    $10, $11, $12, $13, $14, $15, FALSE, $16, $17,
                    $18, 'draft'
                )
                "#,
            )
            .bind(message_id)
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(draft_mailbox_id)
            .bind(thread_id)
            .bind(input.internet_message_id)
            .bind(input.from_display.map(|value| value.trim().to_string()))
            .bind(&from_address)
            .bind(input.sender_display.map(|value| value.trim().to_string()))
            .bind(sender_address.as_deref())
            .bind(SenderAuthorizationKind::SelfSend.as_str())
            .bind(input.submitted_by_account_id)
            .bind(&subject)
            .bind(&preview_text)
            .bind(unread)
            .bind(flagged)
            .bind(input.size_octets.max(0))
            .bind(&mime_blob_ref)
            .bind(input.source.trim().to_lowercase())
            .execute(&mut *tx)
            .await?;
        }

        sqlx::query(
            r#"
            INSERT INTO message_bodies (
                message_id, body_text, body_html_sanitized, participants_normalized,
                language_code, content_hash, search_vector
            )
            VALUES ($1, $2, $3, $4, NULL, $5, to_tsvector('simple', $6))
            ON CONFLICT (message_id) DO UPDATE SET
                body_text = EXCLUDED.body_text,
                body_html_sanitized = EXCLUDED.body_html_sanitized,
                participants_normalized = EXCLUDED.participants_normalized,
                content_hash = EXCLUDED.content_hash,
                search_vector = EXCLUDED.search_vector
            "#,
        )
        .bind(message_id)
        .bind(&body_text)
        .bind(input.body_html_sanitized)
        .bind(&participants_normalized)
        .bind(content_hash)
        .bind(format!("{subject} {body_text} {participants_normalized}"))
        .execute(&mut *tx)
        .await?;

        for (ordinal, (kind, recipient)) in visible_recipients.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (
                    id, tenant_id, message_id, kind, address, display_name, ordinal
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(kind)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }

        for (ordinal, recipient) in bcc_recipients.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_bcc_recipients (
                    id, tenant_id, message_id, address, display_name, ordinal, metadata_scope
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'audit-compliance')
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }

        self.ingest_message_attachments_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            message_id,
            &input.attachments,
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        Ok(SavedDraftMessage {
            message_id,
            account_id: input.account_id,
            submitted_by_account_id: input.submitted_by_account_id,
            draft_mailbox_id,
            delivery_status: "draft".to_string(),
        })
    }

    pub async fn upsert_client_contact(
        &self,
        input: UpsertClientContactInput,
    ) -> Result<ClientContact> {
        let name = input.name.trim();
        let email = normalize_email(&input.email);
        if name.is_empty() || email.is_empty() {
            bail!("contact name and email are required");
        }

        let contact_id = input.id.unwrap_or_else(Uuid::new_v4);
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query_as::<_, ClientContactRow>(
            r#"
            INSERT INTO contacts (
                id, tenant_id, account_id, name, role, email, phone, team, notes
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                role = EXCLUDED.role,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                team = EXCLUDED.team,
                notes = EXCLUDED.notes,
                updated_at = NOW()
            WHERE contacts.tenant_id = EXCLUDED.tenant_id
              AND contacts.account_id = EXCLUDED.account_id
            RETURNING id, name, role, email, phone, team, notes
            "#,
        )
        .bind(contact_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(name)
        .bind(input.role.trim())
        .bind(email)
        .bind(input.phone.trim())
        .bind(input.team.trim())
        .bind(input.notes.trim())
        .fetch_one(&mut *tx)
        .await?;

        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Contacts,
            input.account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(map_contact(row))
    }

    pub async fn upsert_client_event(&self, input: UpsertClientEventInput) -> Result<ClientEvent> {
        if input.date.trim().is_empty()
            || input.time.trim().is_empty()
            || input.title.trim().is_empty()
        {
            bail!("event date, time, and title are required");
        }

        let event_id = input.id.unwrap_or_else(Uuid::new_v4);
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query_as::<_, ClientEventRow>(
            r#"
            INSERT INTO calendar_events (
                id, tenant_id, account_id, event_date, event_time,
                time_zone, duration_minutes, recurrence_rule,
                title, location, attendees, attendees_json, notes
            )
            VALUES ($1, $2, $3, $4::date, $5::time, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (id) DO UPDATE SET
                event_date = EXCLUDED.event_date,
                event_time = EXCLUDED.event_time,
                time_zone = EXCLUDED.time_zone,
                duration_minutes = EXCLUDED.duration_minutes,
                recurrence_rule = EXCLUDED.recurrence_rule,
                title = EXCLUDED.title,
                location = EXCLUDED.location,
                attendees = EXCLUDED.attendees,
                attendees_json = EXCLUDED.attendees_json,
                notes = EXCLUDED.notes,
                updated_at = NOW()
            WHERE calendar_events.tenant_id = EXCLUDED.tenant_id
              AND calendar_events.account_id = EXCLUDED.account_id
            RETURNING
                id,
                to_char(event_date, 'YYYY-MM-DD') AS date,
                to_char(event_time, 'HH24:MI') AS time,
                time_zone,
                duration_minutes,
                recurrence_rule,
                title,
                location,
                attendees,
                attendees_json,
                notes
            "#,
        )
        .bind(event_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.date.trim())
        .bind(input.time.trim())
        .bind(input.time_zone.trim())
        .bind(input.duration_minutes.max(0))
        .bind(input.recurrence_rule.trim())
        .bind(input.title.trim())
        .bind(input.location.trim())
        .bind(input.attendees.trim())
        .bind(input.attendees_json.trim())
        .bind(input.notes.trim())
        .fetch_one(&mut *tx)
        .await?;

        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            input.account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(map_event(row))
    }

    pub async fn upsert_client_task(&self, input: UpsertClientTaskInput) -> Result<ClientTask> {
        let title = input.title.trim();
        if title.is_empty() {
            bail!("task title is required");
        }

        let status = normalize_task_status(&input.status)?;
        let principal_account_id = input.principal_account_id;
        let task_id = input.id.unwrap_or_else(Uuid::new_v4);
        let existing_task = match input.id {
            Some(task_id) => self
                .fetch_client_tasks_by_ids(principal_account_id, &[task_id])
                .await?
                .into_iter()
                .next(),
            None => None,
        };
        let target_task_list = match input.task_list_id {
            Some(task_list_id) => self
                .fetch_task_lists_by_ids(principal_account_id, &[task_list_id])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("task list not found"))?,
            None => {
                if let Some(existing_task) = existing_task.as_ref() {
                    self.fetch_task_lists_by_ids(principal_account_id, &[existing_task.task_list_id])
                        .await?
                        .into_iter()
                        .next()
                        .ok_or_else(|| anyhow!("task list not found"))?
                } else {
                    let task_lists = self.fetch_task_lists(input.account_id).await?;
                    task_lists
                        .into_iter()
                        .find(|task_list| {
                            task_list.owner_account_id == input.account_id
                                && task_list.role.as_deref() == Some(DEFAULT_TASK_LIST_ROLE)
                        })
                        .ok_or_else(|| anyhow!("default task list not found"))?
                }
            }
        };
        if !target_task_list.rights.may_write {
            bail!("write access is not granted on this task list");
        }
        if let Some(existing_task) = existing_task.as_ref() {
            if !existing_task.rights.may_write {
                bail!("write access is not granted on this task");
            }
        }

        let owner_account_id = target_task_list.owner_account_id;
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        if owner_account_id == input.account_id {
            Self::ensure_default_task_list(&mut tx, &tenant_id, owner_account_id).await?;
        }
        let task_list_id = target_task_list.id;
        let row = sqlx::query_as::<_, ClientTaskRow>(
            r#"
            INSERT INTO tasks (
                id,
                tenant_id,
                account_id,
                task_list_id,
                title,
                description,
                status,
                due_at,
                completed_at,
                sort_order
            )
            VALUES (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                NULLIF($8, '')::timestamptz,
                CASE
                    WHEN $7 = 'completed' THEN COALESCE(NULLIF($9, '')::timestamptz, NOW())
                    ELSE NULL
                END,
                $10
            )
            ON CONFLICT (id) DO UPDATE SET
                task_list_id = EXCLUDED.task_list_id,
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                status = EXCLUDED.status,
                due_at = EXCLUDED.due_at,
                completed_at = EXCLUDED.completed_at,
                sort_order = EXCLUDED.sort_order,
                updated_at = NOW()
            WHERE tasks.tenant_id = EXCLUDED.tenant_id
              AND tasks.account_id = EXCLUDED.account_id
            RETURNING
                tasks.id,
                tasks.task_list_id,
                (
                    SELECT sort_order
                    FROM task_lists
                    WHERE task_lists.tenant_id = tasks.tenant_id
                      AND task_lists.account_id = tasks.account_id
                      AND task_lists.id = tasks.task_list_id
                ) AS task_list_sort_order,
                tasks.title,
                tasks.description,
                tasks.status,
                CASE
                    WHEN tasks.due_at IS NULL THEN NULL
                    ELSE to_char(tasks.due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS due_at,
                CASE
                    WHEN tasks.completed_at IS NULL THEN NULL
                    ELSE to_char(tasks.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS completed_at,
                tasks.sort_order,
                to_char(tasks.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(task_id)
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(task_list_id)
        .bind(title)
        .bind(input.description.trim())
        .bind(status)
        .bind(input.due_at.as_deref().unwrap_or_default().trim())
        .bind(input.completed_at.as_deref().unwrap_or_default().trim())
        .bind(input.sort_order)
        .fetch_one(&mut *tx)
        .await?;

        Self::emit_task_access_change(
            &mut tx,
            &tenant_id,
            owner_account_id,
            principal_account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(map_task(row))
    }

    pub async fn delete_client_contact(&self, account_id: Uuid, contact_id: Uuid) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM contacts
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(contact_id)
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("contact not found");
        }

        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Contacts,
            account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn delete_client_event(&self, account_id: Uuid, event_id: Uuid) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM calendar_events
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(event_id)
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("event not found");
        }

        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn upsert_collaboration_grant(
        &self,
        input: CollaborationGrantInput,
        audit: AuditEntryInput,
    ) -> Result<CollaborationGrant> {
        let tenant_id = self
            .tenant_id_for_account_id(input.owner_account_id)
            .await?;
        let grantee_email = normalize_email(&input.grantee_email);
        validate_collaboration_rights(
            input.may_read,
            input.may_write,
            input.may_delete,
            input.may_share,
        )?;
        if grantee_email.is_empty() {
            bail!("grantee email is required");
        }

        let mut tx = self.pool.begin().await?;
        let owner = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, input.owner_account_id)
            .await?;
        let grantee = self
            .load_account_identity_by_email_in_tx(&mut tx, &tenant_id, &grantee_email)
            .await?;

        if owner.id == grantee.id {
            bail!("self-delegation is not supported");
        }

        sqlx::query(
            r#"
            INSERT INTO collaboration_collection_grants (
                id, tenant_id, collection_kind, owner_account_id, grantee_account_id,
                may_read, may_write, may_delete, may_share
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id, collection_kind, owner_account_id, grantee_account_id)
            DO UPDATE SET
                may_read = EXCLUDED.may_read,
                may_write = EXCLUDED.may_write,
                may_delete = EXCLUDED.may_delete,
                may_share = EXCLUDED.may_share,
                updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(input.kind.as_str())
        .bind(input.owner_account_id)
        .bind(grantee.id)
        .bind(input.may_read)
        .bind(input.may_write)
        .bind(input.may_delete)
        .bind(input.may_share)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;

        self.fetch_collaboration_grant(input.kind, owner.id, grantee.id)
            .await?
            .ok_or_else(|| anyhow!("collaboration grant not found after upsert"))
    }

    pub async fn delete_collaboration_grant(
        &self,
        owner_account_id: Uuid,
        kind: CollaborationResourceKind,
        grantee_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM collaboration_collection_grants
            WHERE tenant_id = $1
              AND collection_kind = $2
              AND owner_account_id = $3
              AND grantee_account_id = $4
            "#,
        )
        .bind(&tenant_id)
        .bind(kind.as_str())
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("collaboration grant not found");
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_collaboration_grant_change(
            &mut tx,
            &tenant_id,
            kind,
            owner_account_id,
            grantee_account_id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_collaboration_grant(
        &self,
        kind: CollaborationResourceKind,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
    ) -> Result<Option<CollaborationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let row = sqlx::query_as::<_, CollaborationGrantRow>(
            r#"
            SELECT
                g.id,
                g.collection_kind AS kind,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.may_read,
                g.may_write,
                g.may_delete,
                g.may_share,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM collaboration_collection_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.collection_kind = $2
              AND g.owner_account_id = $3
              AND g.grantee_account_id = $4
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(kind.as_str())
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_collaboration_grant))
    }

    pub async fn fetch_outgoing_collaboration_grants(
        &self,
        owner_account_id: Uuid,
        kind: CollaborationResourceKind,
    ) -> Result<Vec<CollaborationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let rows = sqlx::query_as::<_, CollaborationGrantRow>(
            r#"
            SELECT
                g.id,
                g.collection_kind AS kind,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.may_read,
                g.may_write,
                g.may_delete,
                g.may_share,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM collaboration_collection_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.collection_kind = $2
              AND g.owner_account_id = $3
            ORDER BY lower(grantee.primary_email) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(kind.as_str())
        .bind(owner_account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_collaboration_grant).collect())
    }

    pub async fn upsert_task_list_grant(
        &self,
        input: TaskListGrantInput,
        audit: AuditEntryInput,
    ) -> Result<TaskListGrant> {
        let tenant_id = self.tenant_id_for_account_id(input.owner_account_id).await?;
        let grantee_email = normalize_email(&input.grantee_email);
        validate_collaboration_rights(
            input.may_read,
            input.may_write,
            input.may_delete,
            input.may_share,
        )?;
        if grantee_email.is_empty() {
            bail!("grantee email is required");
        }

        let mut tx = self.pool.begin().await?;
        let owner = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, input.owner_account_id)
            .await?;
        let task_list =
            Self::load_task_list_in_tx(&mut tx, &tenant_id, owner.id, input.task_list_id).await?;
        let grantee = self
            .load_account_identity_by_email_in_tx(&mut tx, &tenant_id, &grantee_email)
            .await?;

        if owner.id == grantee.id {
            bail!("self-delegation is not supported");
        }

        sqlx::query(
            r#"
            INSERT INTO task_list_grants (
                id, tenant_id, task_list_id, owner_account_id, grantee_account_id,
                may_read, may_write, may_delete, may_share
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id, task_list_id, grantee_account_id)
            DO UPDATE SET
                may_read = EXCLUDED.may_read,
                may_write = EXCLUDED.may_write,
                may_delete = EXCLUDED.may_delete,
                may_share = EXCLUDED.may_share,
                updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(task_list.id)
        .bind(owner.id)
        .bind(grantee.id)
        .bind(input.may_read)
        .bind(input.may_write)
        .bind(input.may_delete)
        .bind(input.may_share)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_task_access_change(&mut tx, &tenant_id, owner.id, grantee.id).await?;
        tx.commit().await?;

        self.fetch_task_list_grant(owner.id, task_list.id, grantee.id)
            .await?
            .ok_or_else(|| anyhow!("task-list grant not found after upsert"))
    }

    pub async fn delete_task_list_grant(
        &self,
        owner_account_id: Uuid,
        task_list_id: Uuid,
        grantee_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM task_list_grants
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND task_list_id = $3
              AND grantee_account_id = $4
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(task_list_id)
        .bind(grantee_account_id)
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("task-list grant not found");
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_task_access_change(&mut tx, &tenant_id, owner_account_id, grantee_account_id)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_task_list_grant(
        &self,
        owner_account_id: Uuid,
        task_list_id: Uuid,
        grantee_account_id: Uuid,
    ) -> Result<Option<TaskListGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let row = sqlx::query_as::<_, TaskListGrantRow>(
            r#"
            SELECT
                g.id,
                g.task_list_id,
                task_lists.name AS task_list_name,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.may_read,
                g.may_write,
                g.may_delete,
                g.may_share,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM task_list_grants g
            JOIN task_lists
              ON task_lists.tenant_id = g.tenant_id
             AND task_lists.account_id = g.owner_account_id
             AND task_lists.id = g.task_list_id
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
              AND g.task_list_id = $3
              AND g.grantee_account_id = $4
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(task_list_id)
        .bind(grantee_account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_task_list_grant))
    }

    pub async fn fetch_outgoing_task_list_grants(
        &self,
        owner_account_id: Uuid,
    ) -> Result<Vec<TaskListGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let rows = sqlx::query_as::<_, TaskListGrantRow>(
            r#"
            SELECT
                g.id,
                g.task_list_id,
                task_lists.name AS task_list_name,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.may_read,
                g.may_write,
                g.may_delete,
                g.may_share,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM task_list_grants g
            JOIN task_lists
              ON task_lists.tenant_id = g.tenant_id
             AND task_lists.account_id = g.owner_account_id
             AND task_lists.id = g.task_list_id
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
            ORDER BY lower(task_lists.name) ASC, lower(grantee.primary_email) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_task_list_grant).collect())
    }

    pub async fn fetch_account_identity(&self, account_id: Uuid) -> Result<MailboxAccountAccess> {
        let account = self.account_identity_for_id(account_id).await?;
        Ok(MailboxAccountAccess {
            account_id: account.id,
            email: account.email,
            display_name: account.display_name,
            is_owned: true,
            may_read: true,
            may_write: true,
            may_send_as: true,
            may_send_on_behalf: false,
        })
    }

    pub async fn upsert_mailbox_delegation_grant(
        &self,
        input: MailboxDelegationGrantInput,
        audit: AuditEntryInput,
    ) -> Result<MailboxDelegationGrant> {
        let tenant_id = self
            .tenant_id_for_account_id(input.owner_account_id)
            .await?;
        let grantee_email = normalize_email(&input.grantee_email);
        if grantee_email.is_empty() {
            bail!("grantee email is required");
        }

        let mut tx = self.pool.begin().await?;
        let owner = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, input.owner_account_id)
            .await?;
        let grantee = self
            .load_account_identity_by_email_in_tx(&mut tx, &tenant_id, &grantee_email)
            .await?;

        if owner.id == grantee.id {
            bail!("self-delegation is not supported");
        }

        sqlx::query(
            r#"
            INSERT INTO mailbox_delegation_grants (
                id, tenant_id, owner_account_id, grantee_account_id
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (tenant_id, owner_account_id, grantee_account_id)
            DO UPDATE SET updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(owner.id)
        .bind(grantee.id)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_delegation_change(&mut tx, &tenant_id, owner.id, grantee.id).await?;
        tx.commit().await?;

        self.fetch_mailbox_delegation_grant(owner.id, grantee.id)
            .await?
            .ok_or_else(|| anyhow!("mailbox delegation grant not found after upsert"))
    }

    pub async fn delete_mailbox_delegation_grant(
        &self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM mailbox_delegation_grants
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND grantee_account_id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("mailbox delegation grant not found");
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_delegation_change(
            &mut tx,
            &tenant_id,
            owner_account_id,
            grantee_account_id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_mailbox_delegation_grant(
        &self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
    ) -> Result<Option<MailboxDelegationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let row = sqlx::query_as::<_, MailboxDelegationGrantRow>(
            r#"
            SELECT
                g.id,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM mailbox_delegation_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
              AND g.grantee_account_id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_mailbox_delegation_grant))
    }

    pub async fn upsert_sender_delegation_grant(
        &self,
        input: SenderDelegationGrantInput,
        audit: AuditEntryInput,
    ) -> Result<SenderDelegationGrant> {
        let tenant_id = self
            .tenant_id_for_account_id(input.owner_account_id)
            .await?;
        let grantee_email = normalize_email(&input.grantee_email);
        if grantee_email.is_empty() {
            bail!("grantee email is required");
        }

        let mut tx = self.pool.begin().await?;
        let owner = self
            .load_account_identity_in_tx(&mut tx, &tenant_id, input.owner_account_id)
            .await?;
        let grantee = self
            .load_account_identity_by_email_in_tx(&mut tx, &tenant_id, &grantee_email)
            .await?;

        if owner.id == grantee.id {
            bail!("self-delegation is not supported");
        }

        sqlx::query(
            r#"
            INSERT INTO sender_delegation_grants (
                id, tenant_id, owner_account_id, grantee_account_id, sender_right
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (tenant_id, owner_account_id, grantee_account_id, sender_right)
            DO UPDATE SET updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(owner.id)
        .bind(grantee.id)
        .bind(input.sender_right.as_str())
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_delegation_change(&mut tx, &tenant_id, owner.id, grantee.id).await?;
        tx.commit().await?;

        self.fetch_sender_delegation_grant(owner.id, grantee.id, input.sender_right)
            .await?
            .ok_or_else(|| anyhow!("sender delegation grant not found after upsert"))
    }

    pub async fn delete_sender_delegation_grant(
        &self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        sender_right: SenderDelegationRight,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM sender_delegation_grants
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND grantee_account_id = $3
              AND sender_right = $4
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .bind(sender_right.as_str())
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("sender delegation grant not found");
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_delegation_change(
            &mut tx,
            &tenant_id,
            owner_account_id,
            grantee_account_id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_sender_delegation_grant(
        &self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        sender_right: SenderDelegationRight,
    ) -> Result<Option<SenderDelegationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let row = sqlx::query_as::<_, SenderDelegationGrantRow>(
            r#"
            SELECT
                g.id,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.sender_right,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sender_delegation_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
              AND g.grantee_account_id = $3
              AND g.sender_right = $4
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .bind(sender_right.as_str())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(map_sender_delegation_grant))
    }

    pub async fn fetch_outgoing_mailbox_delegation_grants(
        &self,
        owner_account_id: Uuid,
    ) -> Result<Vec<MailboxDelegationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let rows = sqlx::query_as::<_, MailboxDelegationGrantRow>(
            r#"
            SELECT
                g.id,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM mailbox_delegation_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
            ORDER BY lower(grantee.primary_email) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_mailbox_delegation_grant).collect())
    }

    pub async fn fetch_outgoing_sender_delegation_grants(
        &self,
        owner_account_id: Uuid,
    ) -> Result<Vec<SenderDelegationGrant>> {
        let tenant_id = self.tenant_id_for_account_id(owner_account_id).await?;
        let rows = sqlx::query_as::<_, SenderDelegationGrantRow>(
            r#"
            SELECT
                g.id,
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.grantee_account_id,
                grantee.primary_email AS grantee_email,
                grantee.display_name AS grantee_display_name,
                g.sender_right,
                to_char(g.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
                to_char(g.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM sender_delegation_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            JOIN accounts grantee ON grantee.id = g.grantee_account_id
            WHERE g.tenant_id = $1
              AND g.owner_account_id = $2
            ORDER BY lower(grantee.primary_email) ASC, g.sender_right ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(owner_account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_sender_delegation_grant).collect())
    }

    pub async fn fetch_accessible_mailbox_accounts(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<MailboxAccountAccess>> {
        let principal = self.account_identity_for_id(principal_account_id).await?;
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let mut accounts = vec![MailboxAccountAccess {
            account_id: principal.id,
            email: principal.email,
            display_name: principal.display_name,
            is_owned: true,
            may_read: true,
            may_write: true,
            may_send_as: true,
            may_send_on_behalf: false,
        }];

        let rows = sqlx::query_as::<_, MailboxAccountAccessRow>(
            r#"
            SELECT
                owner.id AS account_id,
                owner.primary_email AS email,
                owner.display_name,
                EXISTS(
                    SELECT 1
                    FROM sender_delegation_grants sg
                    WHERE sg.tenant_id = g.tenant_id
                      AND sg.owner_account_id = g.owner_account_id
                      AND sg.grantee_account_id = g.grantee_account_id
                      AND sg.sender_right = 'send_as'
                ) AS may_send_as,
                EXISTS(
                    SELECT 1
                    FROM sender_delegation_grants sg
                    WHERE sg.tenant_id = g.tenant_id
                      AND sg.owner_account_id = g.owner_account_id
                      AND sg.grantee_account_id = g.grantee_account_id
                      AND sg.sender_right = 'send_on_behalf'
                ) AS may_send_on_behalf
            FROM mailbox_delegation_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            WHERE g.tenant_id = $1
              AND g.grantee_account_id = $2
            ORDER BY lower(owner.primary_email) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(principal_account_id)
        .fetch_all(&self.pool)
        .await?;

        accounts.extend(rows.into_iter().map(|row| MailboxAccountAccess {
            account_id: row.account_id,
            email: row.email,
            display_name: row.display_name,
            is_owned: false,
            may_read: true,
            may_write: true,
            may_send_as: row.may_send_as,
            may_send_on_behalf: row.may_send_on_behalf,
        }));
        Ok(accounts)
    }

    pub async fn fetch_mail_flow_entries(&self) -> Result<Vec<MailFlowEntry>> {
        let rows = sqlx::query_as::<_, MailFlowRow>(
            r#"
            SELECT
                q.id AS queue_id,
                q.message_id,
                a.primary_email AS account_email,
                m.subject_normalized AS subject,
                q.status,
                m.delivery_status,
                to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS submitted_at,
                CASE
                    WHEN q.last_attempt_at IS NULL THEN NULL
                    ELSE to_char(q.last_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS last_attempt_at,
                CASE
                    WHEN q.next_attempt_at IS NULL THEN NULL
                    ELSE to_char(q.next_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS next_attempt_at,
                NULL::TEXT AS trace_id,
                q.remote_message_ref,
                q.last_error
            FROM outbound_message_queue q
            JOIN messages m ON m.id = q.message_id
            JOIN accounts a ON a.id = m.account_id
            WHERE q.tenant_id = $1
            ORDER BY q.created_at DESC
            LIMIT 100
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| MailFlowEntry {
                queue_id: row.queue_id,
                message_id: row.message_id,
                account_email: row.account_email,
                subject: row.subject,
                status: row.status,
                delivery_status: row.delivery_status,
                submitted_at: row.submitted_at,
                last_attempt_at: row.last_attempt_at,
                next_attempt_at: row.next_attempt_at,
                trace_id: row.trace_id,
                remote_message_ref: row.remote_message_ref,
                last_error: row.last_error,
            })
            .collect())
    }

    pub async fn require_mailbox_account_access(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<MailboxAccountAccess> {
        self.fetch_accessible_mailbox_accounts(principal_account_id)
            .await?
            .into_iter()
            .find(|account| account.account_id == target_account_id)
            .ok_or_else(|| anyhow!("mailbox account is not accessible"))
    }

    pub async fn fetch_sender_identities(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<Vec<SenderIdentity>> {
        let access = self
            .require_mailbox_account_access(principal_account_id, target_account_id)
            .await?;
        let principal = self.account_identity_for_id(principal_account_id).await?;

        let mut identities = Vec::new();
        if access.is_owned {
            identities.push(SenderIdentity {
                id: sender_identity_id(SenderAuthorizationKind::SelfSend, target_account_id),
                owner_account_id: target_account_id,
                email: access.email.clone(),
                display_name: access.display_name.clone(),
                authorization_kind: SenderAuthorizationKind::SelfSend.as_str().to_string(),
                sender_address: None,
                sender_display: None,
            });
        } else {
            if access.may_send_as {
                identities.push(SenderIdentity {
                    id: sender_identity_id(SenderAuthorizationKind::SendAs, target_account_id),
                    owner_account_id: target_account_id,
                    email: access.email.clone(),
                    display_name: access.display_name.clone(),
                    authorization_kind: SenderAuthorizationKind::SendAs.as_str().to_string(),
                    sender_address: None,
                    sender_display: None,
                });
            }
            if access.may_send_on_behalf {
                identities.push(SenderIdentity {
                    id: sender_identity_id(
                        SenderAuthorizationKind::SendOnBehalf,
                        target_account_id,
                    ),
                    owner_account_id: target_account_id,
                    email: access.email,
                    display_name: access.display_name,
                    authorization_kind: SenderAuthorizationKind::SendOnBehalf.as_str().to_string(),
                    sender_address: Some(principal.email),
                    sender_display: Some(principal.display_name),
                });
            }
        }

        Ok(identities)
    }

    pub async fn fetch_accessible_contact_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>> {
        self.fetch_accessible_collections(principal_account_id, CollaborationResourceKind::Contacts)
            .await
    }

    pub async fn fetch_accessible_calendar_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>> {
        self.fetch_accessible_collections(principal_account_id, CollaborationResourceKind::Calendar)
            .await
    }

    pub async fn fetch_accessible_task_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>> {
        let task_lists = self.fetch_task_lists(principal_account_id).await?;
        Ok(task_lists
            .into_iter()
            .map(|task_list| CollaborationCollection {
                id: task_list.id.to_string(),
                kind: CollaborationResourceKind::Tasks.as_str().to_string(),
                owner_account_id: task_list.owner_account_id,
                owner_email: task_list.owner_email.clone(),
                owner_display_name: task_list.owner_display_name.clone(),
                display_name: task_list.name.clone(),
                is_owned: task_list.is_owned,
                rights: task_list.rights.clone(),
            })
            .collect())
    }

    pub async fn fetch_accessible_task_list_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>> {
        Ok(self
            .fetch_accessible_task_collections(principal_account_id)
            .await?
            .into_iter()
            .filter(|collection| !collection.is_owned)
            .collect())
    }

    pub async fn fetch_accessible_contacts(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<AccessibleContact>> {
        self.fetch_accessible_contacts_internal(principal_account_id, None, None)
            .await
    }

    pub async fn fetch_accessible_contacts_by_ids(
        &self,
        principal_account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<AccessibleContact>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.fetch_accessible_contacts_internal(principal_account_id, None, Some(ids))
            .await
    }

    pub async fn fetch_accessible_contacts_in_collection(
        &self,
        principal_account_id: Uuid,
        collection_id: &str,
    ) -> Result<Vec<AccessibleContact>> {
        self.fetch_accessible_contacts_internal(principal_account_id, Some(collection_id), None)
            .await
    }

    pub async fn create_accessible_contact(
        &self,
        principal_account_id: Uuid,
        collection_id: Option<&str>,
        input: UpsertClientContactInput,
    ) -> Result<AccessibleContact> {
        let access = self
            .resolve_collection_access(
                principal_account_id,
                CollaborationResourceKind::Contacts,
                collection_id.unwrap_or(DEFAULT_COLLECTION_ID),
            )
            .await?;
        if !access.rights.may_write {
            bail!("write access is not granted on this address book");
        }

        let contact = self
            .upsert_client_contact(UpsertClientContactInput {
                id: input.id,
                account_id: access.owner_account_id,
                name: input.name,
                role: input.role,
                email: input.email,
                phone: input.phone,
                team: input.team,
                notes: input.notes,
            })
            .await?;

        self.fetch_accessible_contacts_by_ids(principal_account_id, &[contact.id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("contact not visible after create"))
    }

    pub async fn update_accessible_contact(
        &self,
        principal_account_id: Uuid,
        contact_id: Uuid,
        input: UpsertClientContactInput,
    ) -> Result<AccessibleContact> {
        let existing = self
            .fetch_accessible_contacts_by_ids(principal_account_id, &[contact_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("contact not found"))?;
        if !existing.rights.may_write {
            bail!("write access is not granted on this address book");
        }

        self.upsert_client_contact(UpsertClientContactInput {
            id: Some(contact_id),
            account_id: existing.owner_account_id,
            name: input.name,
            role: input.role,
            email: input.email,
            phone: input.phone,
            team: input.team,
            notes: input.notes,
        })
        .await?;

        self.fetch_accessible_contacts_by_ids(principal_account_id, &[contact_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("contact not visible after update"))
    }

    pub async fn delete_accessible_contact(
        &self,
        principal_account_id: Uuid,
        contact_id: Uuid,
    ) -> Result<()> {
        let existing = self
            .fetch_accessible_contacts_by_ids(principal_account_id, &[contact_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("contact not found"))?;
        if !existing.rights.may_delete {
            bail!("delete access is not granted on this address book");
        }

        self.delete_client_contact(existing.owner_account_id, contact_id)
            .await
    }

    pub async fn fetch_accessible_events(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<AccessibleEvent>> {
        self.fetch_accessible_events_internal(principal_account_id, None, None)
            .await
    }

    pub async fn fetch_accessible_events_by_ids(
        &self,
        principal_account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<AccessibleEvent>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.fetch_accessible_events_internal(principal_account_id, None, Some(ids))
            .await
    }

    pub async fn fetch_accessible_events_in_collection(
        &self,
        principal_account_id: Uuid,
        collection_id: &str,
    ) -> Result<Vec<AccessibleEvent>> {
        self.fetch_accessible_events_internal(principal_account_id, Some(collection_id), None)
            .await
    }

    pub async fn create_accessible_event(
        &self,
        principal_account_id: Uuid,
        collection_id: Option<&str>,
        input: UpsertClientEventInput,
    ) -> Result<AccessibleEvent> {
        let access = self
            .resolve_collection_access(
                principal_account_id,
                CollaborationResourceKind::Calendar,
                collection_id.unwrap_or(DEFAULT_COLLECTION_ID),
            )
            .await?;
        if !access.rights.may_write {
            bail!("write access is not granted on this calendar");
        }

        let event = self
            .upsert_client_event(UpsertClientEventInput {
                id: input.id,
                account_id: access.owner_account_id,
                date: input.date,
                time: input.time,
                time_zone: input.time_zone,
                duration_minutes: input.duration_minutes,
                recurrence_rule: input.recurrence_rule,
                title: input.title,
                location: input.location,
                attendees: input.attendees,
                attendees_json: input.attendees_json,
                notes: input.notes,
            })
            .await?;

        self.fetch_accessible_events_by_ids(principal_account_id, &[event.id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("event not visible after create"))
    }

    pub async fn update_accessible_event(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
        input: UpsertClientEventInput,
    ) -> Result<AccessibleEvent> {
        let existing = self
            .fetch_accessible_events_by_ids(principal_account_id, &[event_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("event not found"))?;
        if !existing.rights.may_write {
            bail!("write access is not granted on this calendar");
        }

        self.upsert_client_event(UpsertClientEventInput {
            id: Some(event_id),
            account_id: existing.owner_account_id,
            date: input.date,
            time: input.time,
            time_zone: input.time_zone,
            duration_minutes: input.duration_minutes,
            recurrence_rule: input.recurrence_rule,
            title: input.title,
            location: input.location,
            attendees: input.attendees,
            attendees_json: input.attendees_json,
            notes: input.notes,
        })
        .await?;

        self.fetch_accessible_events_by_ids(principal_account_id, &[event_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("event not visible after update"))
    }

    pub async fn delete_accessible_event(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
    ) -> Result<()> {
        let existing = self
            .fetch_accessible_events_by_ids(principal_account_id, &[event_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("event not found"))?;
        if !existing.rights.may_delete {
            bail!("delete access is not granted on this calendar");
        }

        self.delete_client_event(existing.owner_account_id, event_id)
            .await
    }

    pub async fn fetch_task_lists(&self, account_id: Uuid) -> Result<Vec<ClientTaskList>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, account_id).await?;
        let rows = sqlx::query_as::<_, ClientTaskListRow>(
            r#"
            SELECT
                task_lists.id,
                task_lists.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                (task_lists.account_id = $2) AS is_owned,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_read, FALSE)
                END AS may_read,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_write, FALSE)
                END AS may_write,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_delete, FALSE)
                END AS may_delete,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_share, FALSE)
                END AS may_share,
                task_lists.name,
                task_lists.role,
                task_lists.sort_order,
                to_char(task_lists.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM task_lists
            JOIN accounts owner ON owner.id = task_lists.account_id
            LEFT JOIN task_list_grants g
              ON g.tenant_id = task_lists.tenant_id
             AND g.task_list_id = task_lists.id
             AND g.owner_account_id = task_lists.account_id
             AND g.grantee_account_id = $2
            WHERE task_lists.tenant_id = $1
              AND (task_lists.account_id = $2 OR COALESCE(g.may_read, FALSE))
            ORDER BY
                CASE WHEN task_lists.account_id = $2 THEN 0 ELSE 1 END ASC,
                lower(owner.primary_email) ASC,
                task_lists.sort_order ASC,
                task_lists.created_at ASC,
                task_lists.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows.into_iter().map(map_task_list).collect())
    }

    pub async fn fetch_task_lists_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientTaskList>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, account_id).await?;
        let rows = sqlx::query_as::<_, ClientTaskListRow>(
            r#"
            SELECT
                task_lists.id,
                task_lists.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                (task_lists.account_id = $2) AS is_owned,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_read, FALSE)
                END AS may_read,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_write, FALSE)
                END AS may_write,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_delete, FALSE)
                END AS may_delete,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_share, FALSE)
                END AS may_share,
                task_lists.name,
                task_lists.role,
                task_lists.sort_order,
                to_char(task_lists.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM task_lists
            JOIN accounts owner ON owner.id = task_lists.account_id
            LEFT JOIN task_list_grants g
              ON g.tenant_id = task_lists.tenant_id
             AND g.task_list_id = task_lists.id
             AND g.owner_account_id = task_lists.account_id
             AND g.grantee_account_id = $2
            WHERE task_lists.tenant_id = $1
              AND task_lists.id = ANY($3)
              AND (task_lists.account_id = $2 OR COALESCE(g.may_read, FALSE))
            ORDER BY
                CASE WHEN task_lists.account_id = $2 THEN 0 ELSE 1 END ASC,
                lower(owner.primary_email) ASC,
                task_lists.sort_order ASC,
                task_lists.created_at ASC,
                task_lists.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows.into_iter().map(map_task_list).collect())
    }

    pub async fn create_task_list(&self, input: CreateTaskListInput) -> Result<ClientTaskList> {
        let name = normalize_task_list_name(&input.name)?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, input.account_id).await?;
        let row = sqlx::query_as::<_, ClientTaskListRow>(
            r#"
            INSERT INTO task_lists (id, tenant_id, account_id, name, role, sort_order)
            VALUES ($1, $2, $3, $4, NULL, $5)
            RETURNING
                id,
                account_id AS owner_account_id,
                ''::text AS owner_email,
                ''::text AS owner_display_name,
                TRUE AS is_owned,
                TRUE AS may_read,
                TRUE AS may_write,
                TRUE AS may_delete,
                TRUE AS may_share,
                name,
                role,
                sort_order,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(name)
        .bind(input.sort_order)
        .fetch_one(&mut *tx)
        .await?;
        Self::emit_task_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        Ok(map_task_list(row))
    }

    pub async fn update_task_list(&self, input: UpdateTaskListInput) -> Result<ClientTaskList> {
        let normalized_name = input
            .name
            .as_deref()
            .map(normalize_task_list_name)
            .transpose()?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, input.account_id).await?;
        let row = sqlx::query_as::<_, ClientTaskListRow>(
            r#"
            UPDATE task_lists
            SET
                name = COALESCE($4, name),
                sort_order = COALESCE($5, sort_order),
                updated_at = CASE
                    WHEN $4 IS NULL AND $5 IS NULL THEN updated_at
                    ELSE NOW()
                END
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = $3
            RETURNING
                id,
                account_id AS owner_account_id,
                ''::text AS owner_email,
                ''::text AS owner_display_name,
                TRUE AS is_owned,
                TRUE AS may_read,
                TRUE AS may_write,
                TRUE AS may_delete,
                TRUE AS may_share,
                name,
                role,
                sort_order,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.task_list_id)
        .bind(normalized_name)
        .bind(input.sort_order)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("task list not found"))?;
        Self::emit_task_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        Ok(map_task_list(row))
    }

    pub async fn delete_task_list(&self, account_id: Uuid, task_list_id: Uuid) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let target =
            Self::load_task_list_in_tx(&mut tx, &tenant_id, account_id, task_list_id).await?;
        if target.role.as_deref() == Some(DEFAULT_TASK_LIST_ROLE) {
            bail!("default task list cannot be destroyed");
        }
        let task_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM tasks
            WHERE tenant_id = $1 AND account_id = $2 AND task_list_id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(task_list_id)
        .fetch_one(&mut *tx)
        .await?;
        if task_count > 0 {
            bail!("task list must be empty before it can be destroyed");
        }
        sqlx::query(
            r#"
            DELETE FROM task_lists
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(task_list_id)
        .execute(&mut *tx)
        .await?;
        Self::emit_task_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn delete_client_task(&self, account_id: Uuid, task_id: Uuid) -> Result<()> {
        let existing = self
            .fetch_client_tasks_by_ids(account_id, &[task_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("task not found"))?;
        if !existing.rights.may_delete {
            bail!("delete access is not granted on this task");
        }

        let tenant_id = self.tenant_id_for_account_id(existing.owner_account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM tasks
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(existing.owner_account_id)
        .bind(task_id)
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("task not found");
        }

        Self::emit_task_access_change(
            &mut tx,
            &tenant_id,
            existing.owner_account_id,
            account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn fetch_dav_tasks(&self, account_id: Uuid) -> Result<Vec<DavTask>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, account_id).await?;
        let rows = sqlx::query_as::<_, DavTaskRow>(
            r#"
            SELECT
                tasks.id,
                task_lists.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                (task_lists.account_id = $2) AS is_owned,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_read, FALSE)
                END AS may_read,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_write, FALSE)
                END AS may_write,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_delete, FALSE)
                END AS may_delete,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_share, FALSE)
                END AS may_share,
                tasks.task_list_id,
                task_lists.name AS task_list_name,
                tasks.title,
                tasks.description,
                tasks.status,
                CASE
                    WHEN tasks.due_at IS NULL THEN NULL
                    ELSE to_char(tasks.due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS due_at,
                CASE
                    WHEN tasks.completed_at IS NULL THEN NULL
                    ELSE to_char(tasks.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS completed_at,
                tasks.sort_order,
                to_char(tasks.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM tasks
            JOIN task_lists
              ON task_lists.tenant_id = tasks.tenant_id
             AND task_lists.account_id = tasks.account_id
             AND task_lists.id = tasks.task_list_id
            JOIN accounts owner ON owner.id = task_lists.account_id
            LEFT JOIN task_list_grants g
              ON g.tenant_id = task_lists.tenant_id
             AND g.task_list_id = task_lists.id
             AND g.owner_account_id = task_lists.account_id
             AND g.grantee_account_id = $2
            WHERE tasks.tenant_id = $1
              AND (task_lists.account_id = $2 OR COALESCE(g.may_read, FALSE))
            ORDER BY
                CASE WHEN task_lists.account_id = $2 THEN 0 ELSE 1 END ASC,
                lower(owner.primary_email) ASC,
                task_lists.sort_order ASC,
                tasks.sort_order ASC,
                tasks.updated_at ASC,
                tasks.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows.into_iter().map(map_dav_task).collect())
    }

    pub async fn fetch_dav_tasks_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<DavTask>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, account_id).await?;
        let rows = sqlx::query_as::<_, DavTaskRow>(
            r#"
            SELECT
                tasks.id,
                task_lists.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                (task_lists.account_id = $2) AS is_owned,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_read, FALSE)
                END AS may_read,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_write, FALSE)
                END AS may_write,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_delete, FALSE)
                END AS may_delete,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_share, FALSE)
                END AS may_share,
                tasks.task_list_id,
                task_lists.name AS task_list_name,
                tasks.title,
                tasks.description,
                tasks.status,
                CASE
                    WHEN tasks.due_at IS NULL THEN NULL
                    ELSE to_char(tasks.due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS due_at,
                CASE
                    WHEN tasks.completed_at IS NULL THEN NULL
                    ELSE to_char(tasks.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS completed_at,
                tasks.sort_order,
                to_char(tasks.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM tasks
            JOIN task_lists
              ON task_lists.tenant_id = tasks.tenant_id
             AND task_lists.account_id = tasks.account_id
             AND task_lists.id = tasks.task_list_id
            JOIN accounts owner ON owner.id = task_lists.account_id
            LEFT JOIN task_list_grants g
              ON g.tenant_id = task_lists.tenant_id
             AND g.task_list_id = task_lists.id
             AND g.owner_account_id = task_lists.account_id
             AND g.grantee_account_id = $2
            WHERE tasks.tenant_id = $1
              AND tasks.id = ANY($3)
              AND (task_lists.account_id = $2 OR COALESCE(g.may_read, FALSE))
            ORDER BY
                CASE WHEN task_lists.account_id = $2 THEN 0 ELSE 1 END ASC,
                lower(owner.primary_email) ASC,
                task_lists.sort_order ASC,
                tasks.sort_order ASC,
                tasks.updated_at ASC,
                tasks.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows.into_iter().map(map_dav_task).collect())
    }

    pub async fn upsert_dav_task(&self, input: UpsertClientTaskInput) -> Result<DavTask> {
        let task = self
            .upsert_client_task(UpsertClientTaskInput {
                task_list_id: None,
                ..input.clone()
            })
            .await?;
        let task_list_name = self
            .fetch_task_lists_by_ids(input.principal_account_id, &[task.task_list_id])
            .await?
            .into_iter()
            .next()
            .map(|task_list| task_list.name)
            .unwrap_or_default();
        Ok(DavTask {
            id: task.id,
            collection_id: task.task_list_id.to_string(),
            owner_account_id: task.owner_account_id,
            owner_email: task.owner_email,
            owner_display_name: task.owner_display_name,
            rights: task.rights,
            task_list_id: task.task_list_id,
            task_list_name,
            title: task.title,
            description: task.description,
            status: task.status,
            due_at: task.due_at,
            completed_at: task.completed_at,
            sort_order: task.sort_order,
            updated_at: task.updated_at,
        })
    }

    pub async fn delete_dav_task(&self, account_id: Uuid, task_id: Uuid) -> Result<()> {
        self.delete_client_task(account_id, task_id).await
    }

    pub async fn submit_message(
        &self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> Result<SubmittedMessage> {
        let subject = normalize_subject(&input.subject);
        let body_text = input.body_text.trim().to_string();
        let visible_recipients = normalize_visible_recipients(&input);
        let bcc_recipients = normalize_bcc_recipients(&input);

        if visible_recipients.is_empty() && bcc_recipients.is_empty() {
            bail!("at least one recipient is required");
        }
        if subject.is_empty() && body_text.is_empty() {
            bail!("subject or body_text is required");
        }

        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;

        let account_exists = sqlx::query(
            r#"
            SELECT 1
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .fetch_optional(&mut *tx)
        .await?;

        if account_exists.is_none() {
            bail!("account not found");
        }

        let authorization = self
            .resolve_submission_authorization_in_tx(&mut tx, &tenant_id, &input)
            .await?;

        let sent_mailbox_id = self
            .ensure_mailbox(
                &mut tx,
                &tenant_id,
                input.account_id,
                "sent",
                "Sent",
                20,
                365,
            )
            .await?;

        let message_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        let outbound_queue_id = Uuid::new_v4();
        let preview_text = preview_text(&body_text);
        let participants_normalized =
            participants_normalized(&authorization.from_address, &visible_recipients);
        let mime_blob_ref = input
            .mime_blob_ref
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("canonical-message:{message_id}"));
        let content_hash = format!("message:{message_id}");

        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                received_at, sent_at, from_display, from_address, sender_display,
                sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                submission_source, delivery_status
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                NOW(), NOW(), $7, $8, $9,
                $10, $11, $12, $13, FALSE, FALSE, FALSE, $14, $15,
                $16, 'queued'
            )
            "#,
        )
        .bind(message_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(sent_mailbox_id)
        .bind(thread_id)
        .bind(input.internet_message_id)
        .bind(authorization.from_display.as_deref())
        .bind(&authorization.from_address)
        .bind(authorization.sender_display.as_deref())
        .bind(authorization.sender_address.as_deref())
        .bind(authorization.authorization_kind.as_str())
        .bind(authorization.submitted_by.id)
        .bind(&subject)
        .bind(&preview_text)
        .bind(input.size_octets.max(0))
        .bind(&mime_blob_ref)
        .bind(input.source.trim().to_lowercase())
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_bodies (
                message_id, body_text, body_html_sanitized, participants_normalized,
                language_code, content_hash, search_vector
            )
            VALUES ($1, $2, $3, $4, NULL, $5, to_tsvector('simple', $6))
            "#,
        )
        .bind(message_id)
        .bind(&body_text)
        .bind(input.body_html_sanitized)
        .bind(&participants_normalized)
        .bind(content_hash)
        .bind(format!("{subject} {body_text} {participants_normalized}"))
        .execute(&mut *tx)
        .await?;

        for (ordinal, (kind, recipient)) in visible_recipients.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (
                    id, tenant_id, message_id, kind, address, display_name, ordinal
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(kind)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }

        for (ordinal, recipient) in bcc_recipients.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_bcc_recipients (
                    id, tenant_id, message_id, address, display_name, ordinal, metadata_scope
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'audit-compliance')
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }

        self.ingest_message_attachments_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            message_id,
            &input.attachments,
        )
        .await?;

        sqlx::query(
            r#"
            INSERT INTO outbound_message_queue (
                id, tenant_id, message_id, account_id, transport, status
            )
            VALUES ($1, $2, $3, $4, 'lpe-ct-smtp', 'queued')
            "#,
        )
        .bind(outbound_queue_id)
        .bind(&tenant_id)
        .bind(message_id)
        .bind(input.account_id)
        .execute(&mut *tx)
        .await?;

        if let Some(draft_message_id) = input.draft_message_id {
            self.delete_draft_message_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                draft_message_id,
            )
            .await?;
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        Ok(SubmittedMessage {
            message_id,
            thread_id,
            account_id: input.account_id,
            submitted_by_account_id: authorization.submitted_by.id,
            sent_mailbox_id,
            outbound_queue_id,
            delivery_status: "queued".to_string(),
        })
    }

    pub async fn submit_draft_message(
        &self,
        account_id: Uuid,
        draft_message_id: Uuid,
        source: &str,
        audit: AuditEntryInput,
    ) -> Result<SubmittedMessage> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let draft = self
            .fetch_jmap_draft(account_id, draft_message_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("draft not found"))?;

        let recipient_rows = sqlx::query_as::<_, JmapEmailRecipientRow>(
            r#"
            SELECT
                r.message_id,
                r.kind,
                r.address,
                r.display_name,
                r.ordinal AS _ordinal
            FROM message_recipients r
            WHERE r.tenant_id = $1
              AND r.message_id = $2
            ORDER BY r.kind ASC, r.ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(draft_message_id)
        .fetch_all(&self.pool)
        .await?;

        let bcc_rows = sqlx::query(
            r#"
            SELECT address, display_name
            FROM message_bcc_recipients
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(draft_message_id)
        .fetch_all(&self.pool)
        .await?;

        let to = recipient_rows
            .iter()
            .filter(|recipient| recipient.kind == "to")
            .map(|recipient| SubmittedRecipientInput {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect();
        let cc = recipient_rows
            .iter()
            .filter(|recipient| recipient.kind == "cc")
            .map(|recipient| SubmittedRecipientInput {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect();
        let bcc = bcc_rows
            .into_iter()
            .map(|row| SubmittedRecipientInput {
                address: row.try_get("address").unwrap_or_default(),
                display_name: row.try_get("display_name").ok(),
            })
            .collect();

        self.submit_message(
            SubmitMessageInput {
                draft_message_id: Some(draft_message_id),
                account_id,
                submitted_by_account_id: draft.submitted_by_account_id,
                source: source.trim().to_lowercase(),
                from_display: draft.from_display,
                from_address: draft.from_address,
                sender_display: draft.sender_display,
                sender_address: draft.sender_address,
                to,
                cc,
                bcc,
                subject: draft.subject,
                body_text: draft.body_text,
                body_html_sanitized: draft.body_html_sanitized,
                internet_message_id: draft.internet_message_id,
                mime_blob_ref: None,
                size_octets: draft.size_octets,
                unread: Some(draft.unread),
                flagged: Some(draft.flagged),
                attachments: Vec::new(),
            },
            audit,
        )
        .await
    }

    pub async fn copy_jmap_email(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("message not found"))?;

        let target_mailbox = sqlx::query(
            r#"
            SELECT role, display_name
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("target mailbox not found"))?;

        let target_role = target_mailbox.try_get::<String, _>("role")?;
        let copied_message_id = Uuid::new_v4();
        let delivery_status = if target_role == "drafts" {
            "draft"
        } else {
            "stored"
        };

        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                received_at, sent_at, from_display, from_address, sender_display,
                sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                submission_source, delivery_status
            )
            SELECT
                $4, tenant_id, account_id, $5, thread_id, internet_message_id,
                NOW(),
                CASE WHEN $6 = 'draft' THEN NULL ELSE sent_at END,
                from_display, from_address, sender_display,
                sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                submission_source, $6
            FROM messages
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(copied_message_id)
        .bind(target_mailbox_id)
        .bind(delivery_status)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_bodies (
                message_id, body_text, body_html_sanitized, participants_normalized,
                language_code, content_hash, search_vector
            )
            SELECT
                $2, body_text, body_html_sanitized, participants_normalized,
                language_code, $3, search_vector
            FROM message_bodies
            WHERE message_id = $1
            "#,
        )
        .bind(message_id)
        .bind(copied_message_id)
        .bind(format!("copy:{copied_message_id}"))
        .execute(&mut *tx)
        .await?;

        let recipient_rows = sqlx::query(
            r#"
            SELECT kind, address, display_name, ordinal
            FROM message_recipients
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY kind ASC, ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .fetch_all(&mut *tx)
        .await?;
        for row in recipient_rows {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (id, tenant_id, message_id, kind, address, display_name, ordinal)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(copied_message_id)
            .bind(row.try_get::<String, _>("kind")?)
            .bind(row.try_get::<String, _>("address")?)
            .bind(row.try_get::<Option<String>, _>("display_name")?)
            .bind(row.try_get::<i32, _>("ordinal")?)
            .execute(&mut *tx)
            .await?;
        }

        let bcc_rows = sqlx::query(
            r#"
            SELECT address, display_name, ordinal, metadata_scope
            FROM message_bcc_recipients
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .fetch_all(&mut *tx)
        .await?;
        for row in bcc_rows {
            sqlx::query(
                r#"
                INSERT INTO message_bcc_recipients (id, tenant_id, message_id, address, display_name, ordinal, metadata_scope)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(copied_message_id)
            .bind(row.try_get::<String, _>("address")?)
            .bind(row.try_get::<Option<String>, _>("display_name")?)
            .bind(row.try_get::<i32, _>("ordinal")?)
            .bind(row.try_get::<String, _>("metadata_scope")?)
            .execute(&mut *tx)
            .await?;
        }

        let attachment_rows = sqlx::query(
            r#"
            SELECT file_name, media_type, size_octets, blob_ref, extracted_text, attachment_blob_id
            FROM attachments
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY file_name ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .fetch_all(&mut *tx)
        .await?;
        for row in attachment_rows {
            sqlx::query(
                r#"
                INSERT INTO attachments (
                    id, tenant_id, message_id, file_name, media_type, size_octets,
                    blob_ref, extracted_text, extracted_text_tsv, attachment_blob_id
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, to_tsvector('simple', COALESCE($8, '')), $9
                )
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(copied_message_id)
            .bind(row.try_get::<String, _>("file_name")?)
            .bind(row.try_get::<String, _>("media_type")?)
            .bind(row.try_get::<i64, _>("size_octets")?)
            .bind(row.try_get::<String, _>("blob_ref")?)
            .bind(row.try_get::<Option<String>, _>("extracted_text")?)
            .bind(row.try_get::<Option<Uuid>, _>("attachment_blob_id")?)
            .execute(&mut *tx)
            .await?;
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(account_id, &[copied_message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("copied message not found"))
    }

    pub async fn move_jmap_email(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("message not found"))?;

        let target_mailbox_exists = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .fetch_optional(&self.pool)
        .await?
        .is_some();
        if !target_mailbox_exists {
            bail!("target mailbox not found");
        }

        let mut tx = self.pool.begin().await?;
        let moved = sqlx::query(
            r#"
            UPDATE messages
            SET mailbox_id = $4, imap_uid = nextval('message_imap_uid_seq')
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(target_mailbox_id)
        .execute(&mut *tx)
        .await?;
        if moved.rows_affected() == 0 {
            bail!("message not found");
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("moved message not found"))
    }

    pub async fn import_jmap_email(
        &self,
        input: JmapImportedEmailInput,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let target_mailbox = sqlx::query(
            r#"
            SELECT role
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.mailbox_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("target mailbox not found"))?;
        let target_role = target_mailbox.try_get::<String, _>("role")?;

        let message_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        let preview = preview_text(&input.body_text);
        let recipients = input
            .to
            .iter()
            .cloned()
            .map(|recipient| ("to", recipient))
            .chain(input.cc.iter().cloned().map(|recipient| ("cc", recipient)))
            .collect::<Vec<_>>();
        let participants =
            participants_normalized(&normalize_email(&input.from_address), &recipients);
        let delivery_status = if target_role == "drafts" {
            "draft"
        } else {
            "stored"
        };

        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;
        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                received_at, sent_at, from_display, from_address, sender_display,
                sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                submission_source, delivery_status
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                NOW(), NULL, $7, $8, $9,
                $10, $11, $12, $13, $14, FALSE, FALSE, FALSE, $15, $16,
                $17, $18
            )
            "#,
        )
        .bind(message_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.mailbox_id)
        .bind(thread_id)
        .bind(input.internet_message_id)
        .bind(input.from_display)
        .bind(normalize_email(&input.from_address))
        .bind(input.sender_display)
        .bind(input.sender_address.map(|value| normalize_email(&value)))
        .bind(SenderAuthorizationKind::SelfSend.as_str())
        .bind(input.submitted_by_account_id)
        .bind(normalize_subject(&input.subject))
        .bind(preview)
        .bind(input.size_octets.max(0))
        .bind(input.mime_blob_ref)
        .bind(input.source)
        .bind(delivery_status)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_bodies (
                message_id, body_text, body_html_sanitized, participants_normalized,
                language_code, content_hash, search_vector
            )
            VALUES ($1, $2, $3, $4, NULL, $5, to_tsvector('simple', $6))
            "#,
        )
        .bind(message_id)
        .bind(&input.body_text)
        .bind(input.body_html_sanitized)
        .bind(&participants)
        .bind(format!("import:{message_id}"))
        .bind(format!(
            "{} {} {}",
            normalize_subject(&input.subject),
            input.body_text,
            participants
        ))
        .execute(&mut *tx)
        .await?;

        for (ordinal, recipient) in input.to.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (id, tenant_id, message_id, kind, address, display_name, ordinal)
                VALUES ($1, $2, $3, 'to', $4, $5, $6)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }
        for (ordinal, recipient) in input.cc.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (id, tenant_id, message_id, kind, address, display_name, ordinal)
                VALUES ($1, $2, $3, 'cc', $4, $5, $6)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }
        for (ordinal, recipient) in input.bcc.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_bcc_recipients (id, tenant_id, message_id, address, display_name, ordinal, metadata_scope)
                VALUES ($1, $2, $3, $4, $5, $6, 'audit-compliance')
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }

        self.ingest_message_attachments_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            message_id,
            &input.attachments,
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(input.account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("imported message not found"))
    }

    pub async fn delete_draft_message(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.delete_draft_message_in_tx(&mut tx, &tenant_id, account_id, message_id)
            .await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn store_activesync_sync_state(
        &self,
        account_id: Uuid,
        device_id: &str,
        collection_id: &str,
        sync_key: &str,
        snapshot_json: &str,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        sqlx::query(
            r#"
            INSERT INTO activesync_sync_states (
                id, tenant_id, account_id, device_id, collection_id, sync_key, snapshot_json
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (tenant_id, account_id, device_id, collection_id, sync_key)
            DO UPDATE SET snapshot_json = EXCLUDED.snapshot_json
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(collection_id.trim())
        .bind(sync_key.trim())
        .bind(snapshot_json.trim())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn fetch_activesync_sync_state(
        &self,
        account_id: Uuid,
        device_id: &str,
        collection_id: &str,
        sync_key: &str,
    ) -> Result<Option<ActiveSyncSyncState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, ActiveSyncSyncStateRow>(
            r#"
            SELECT sync_key, snapshot_json
            FROM activesync_sync_states
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
              AND collection_id = $4
              AND sync_key = $5
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(collection_id.trim())
        .bind(sync_key.trim())
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(ActiveSyncSyncState {
                sync_key: row.sync_key,
                snapshot_json: row.snapshot_json,
            })
        })
        .transpose()
    }

    pub async fn fetch_activesync_email_states(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        position: u64,
        limit: u64,
    ) -> Result<Vec<ActiveSyncItemState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                m.id,
                concat_ws(
                    '|',
                    m.subject_normalized,
                    m.preview_text,
                    COALESCE(b.content_hash, ''),
                    to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                    CASE WHEN m.unread THEN '1' ELSE '0' END,
                    CASE WHEN m.flagged THEN '1' ELSE '0' END,
                    COALESCE(m.from_display, ''),
                    m.from_address,
                    COALESCE(recipients.to_recipients, ''),
                    COALESCE(recipients.cc_recipients, ''),
                    m.delivery_status
                ) AS fingerprint
            FROM messages m
            LEFT JOIN message_bodies b ON b.message_id = m.id
            LEFT JOIN LATERAL (
                SELECT
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.kind = 'to') AS to_recipients,
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.kind = 'cc') AS cc_recipients
                FROM message_recipients r
                WHERE r.tenant_id = $1
                  AND r.message_id = m.id
            ) recipients ON TRUE
            WHERE m.tenant_id = $1
              AND m.account_id = $2
              AND m.mailbox_id = $3
            ORDER BY COALESCE(m.sent_at, m.received_at) DESC, m.id DESC
            OFFSET $4
            LIMIT $5
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(position as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_email_states_by_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ActiveSyncItemState>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
                m.id,
                concat_ws(
                    '|',
                    m.subject_normalized,
                    m.preview_text,
                    COALESCE(b.content_hash, ''),
                    to_char(COALESCE(m.sent_at, m.received_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                    CASE WHEN m.unread THEN '1' ELSE '0' END,
                    CASE WHEN m.flagged THEN '1' ELSE '0' END,
                    COALESCE(m.from_display, ''),
                    m.from_address,
                    COALESCE(recipients.to_recipients, ''),
                    COALESCE(recipients.cc_recipients, ''),
                    m.delivery_status
                ) AS fingerprint
            FROM messages m
            LEFT JOIN message_bodies b ON b.message_id = m.id
            LEFT JOIN LATERAL (
                SELECT
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.kind = 'to') AS to_recipients,
                    string_agg(
                        lower(r.address) || ':' || COALESCE(r.display_name, ''),
                        ',' ORDER BY r.ordinal
                    ) FILTER (WHERE r.kind = 'cc') AS cc_recipients
                FROM message_recipients r
                WHERE r.tenant_id = $1
                  AND r.message_id = m.id
            ) recipients ON TRUE
            WHERE m.tenant_id = $1
              AND m.account_id = $2
              AND m.mailbox_id = $3
              AND m.id = ANY($4)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_contact_states(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<ActiveSyncItemState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM contacts
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY name ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_contact_states_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ActiveSyncItemState>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM contacts
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = ANY($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_event_states(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<ActiveSyncItemState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM calendar_events
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY event_date ASC, event_time ASC, id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn fetch_activesync_event_states_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ActiveSyncItemState>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
                id,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS fingerprint
            FROM calendar_events
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = ANY($3)
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveSyncItemState {
                    id: row.try_get("id")?,
                    fingerprint: row.try_get("fingerprint")?,
                })
            })
            .collect()
    }

    pub async fn search_email_trace(
        &self,
        input: EmailTraceSearchInput,
    ) -> Result<Vec<EmailTraceResult>> {
        let like_query = format!("%{}%", input.query.trim().to_lowercase());
        let rows = sqlx::query_as::<_, EmailTraceRow>(
            r#"
            SELECT
                m.id AS message_id,
                m.internet_message_id,
                m.subject_normalized AS subject,
                m.from_address AS sender,
                a.primary_email AS account_email,
                mb.display_name AS mailbox,
                to_char(m.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS received_at
            FROM messages m
            JOIN accounts a ON a.id = m.account_id
            JOIN mailboxes mb ON mb.id = m.mailbox_id
            WHERE m.tenant_id = $1
              AND (
                lower(m.from_address) LIKE $2
                OR lower(m.subject_normalized) LIKE $2
                OR lower(COALESCE(m.internet_message_id, '')) LIKE $2
                OR lower(a.primary_email) LIKE $2
                OR EXISTS (
                    SELECT 1
                    FROM attachments at
                    WHERE at.tenant_id = m.tenant_id
                      AND at.message_id = m.id
                      AND lower(COALESCE(at.extracted_text, '')) LIKE $2
                )
              )
            ORDER BY m.received_at DESC
            LIMIT 50
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .bind(like_query)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| EmailTraceResult {
                message_id: row.message_id,
                internet_message_id: row.internet_message_id,
                subject: row.subject,
                sender: row.sender,
                account_email: row.account_email,
                mailbox: row.mailbox,
                received_at: row.received_at,
            })
            .collect())
    }

    pub async fn process_pending_pst_jobs(&self) -> Result<PstJobExecutionSummary> {
        let jobs = sqlx::query_as::<_, PendingPstJobRow>(
            r#"
            SELECT
                j.id,
                j.tenant_id,
                j.mailbox_id,
                mb.account_id,
                j.direction,
                j.server_path,
                j.requested_by
            FROM mailbox_pst_jobs j
            JOIN mailboxes mb ON mb.id = j.mailbox_id
            WHERE j.status IN ('requested', 'failed')
            ORDER BY j.created_at ASC
            LIMIT 10
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut summary = PstJobExecutionSummary {
            processed_jobs: 0,
            completed_jobs: 0,
            failed_jobs: 0,
        };

        for job in jobs {
            summary.processed_jobs += 1;
            if let Err(error) = self.mark_pst_job_running(&job.tenant_id, job.id).await {
                summary.failed_jobs += 1;
                let _ = self
                    .mark_pst_job_failed(
                        &job.tenant_id,
                        job.id,
                        &format!("cannot start job: {error}"),
                    )
                    .await;
                continue;
            }

            let result = if job.direction == "export" {
                self.export_mailbox_to_pst(&job).await
            } else {
                self.import_mailbox_from_pst(&job).await
            };

            match result {
                Ok(processed_messages) => {
                    self.mark_pst_job_completed(&job.tenant_id, job.id, processed_messages)
                        .await?;
                    summary.completed_jobs += 1;
                }
                Err(error) => {
                    self.mark_pst_job_failed(&job.tenant_id, job.id, &error.to_string())
                        .await?;
                    summary.failed_jobs += 1;
                }
            }
        }

        Ok(summary)
    }

    async fn mark_pst_job_running(&self, tenant_id: &str, job_id: Uuid) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE mailbox_pst_jobs
            SET status = 'running', error_message = NULL
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(job_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn mark_pst_job_completed(
        &self,
        tenant_id: &str,
        job_id: Uuid,
        processed_messages: u32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE mailbox_pst_jobs
            SET status = 'completed',
                processed_messages = $3,
                error_message = NULL,
                completed_at = NOW()
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(job_id)
        .bind(processed_messages as i32)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn mark_pst_job_failed(
        &self,
        tenant_id: &str,
        job_id: Uuid,
        error_message: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE mailbox_pst_jobs
            SET status = 'failed',
                error_message = $3,
                completed_at = NOW()
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(job_id)
        .bind(error_message.chars().take(1000).collect::<String>())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn export_mailbox_to_pst(&self, job: &PendingPstJobRow) -> Result<u32> {
        ensure_parent_directory(&job.server_path)?;
        let rows = sqlx::query(
            r#"
            SELECT
                m.id,
                m.internet_message_id,
                m.from_address,
                m.subject_normalized,
                COALESCE(mb.body_text, '') AS body_text
            FROM messages m
            LEFT JOIN message_bodies mb ON mb.message_id = m.id
            WHERE m.tenant_id = $1 AND m.mailbox_id = $2
            ORDER BY m.received_at ASC
            "#,
        )
        .bind(&job.tenant_id)
        .bind(job.mailbox_id)
        .fetch_all(&self.pool)
        .await?;

        let mut file = File::create(&job.server_path)?;
        writeln!(file, "LPE-PST-V1")?;
        writeln!(file, "mailbox_id={}", job.mailbox_id)?;
        writeln!(file, "requested_by={}", job.requested_by)?;

        for row in &rows {
            let internet_message_id = row
                .try_get::<Option<String>, _>("internet_message_id")?
                .unwrap_or_default();
            let from_address: String = row.try_get("from_address")?;
            let subject: String = row.try_get("subject_normalized")?;
            let body_text: String = row.try_get("body_text")?;
            writeln!(
                file,
                "MESSAGE\t{}\t{}\t{}\t{}",
                encode_pst_field(&internet_message_id),
                encode_pst_field(&from_address),
                encode_pst_field(&subject),
                encode_pst_field(&body_text)
            )?;

            let attachments = sqlx::query(
                r#"
                SELECT
                    a.file_name,
                    a.media_type,
                    b.blob_bytes
                FROM attachments a
                JOIN attachment_blobs b ON b.id = a.attachment_blob_id
                WHERE a.tenant_id = $1 AND a.message_id = $2
                ORDER BY a.file_name ASC
                "#,
            )
            .bind(&job.tenant_id)
            .bind(row.try_get::<Uuid, _>("id")?)
            .fetch_all(&self.pool)
            .await?;

            for attachment in attachments {
                let file_name: String = attachment.try_get("file_name")?;
                let media_type: String = attachment.try_get("media_type")?;
                let blob_bytes: Vec<u8> = attachment.try_get("blob_bytes")?;
                writeln!(
                    file,
                    "ATTACHMENT\t{}\t{}\t{}",
                    encode_pst_field(&file_name),
                    encode_pst_field(&media_type),
                    BASE64.encode(blob_bytes)
                )?;
            }
        }

        Ok(rows.len() as u32)
    }

    async fn import_mailbox_from_pst(&self, job: &PendingPstJobRow) -> Result<u32> {
        validate_pst_import_path(Path::new(&job.server_path))?;

        let file = File::open(&job.server_path)?;
        let mut reader = BufReader::new(file);
        let mut header = String::new();
        reader.read_line(&mut header)?;
        if header.trim() != "LPE-PST-V1" {
            bail!("unsupported PST file for this bootstrap engine");
        }

        let mut processed_messages = 0;
        let mut pending_message: Option<PstImportedMessage> = None;
        let mut tx = self.pool.begin().await?;
        for line in reader.lines() {
            let line = line?;
            if line.starts_with("ATTACHMENT\t") {
                let parts = line.split('\t').collect::<Vec<_>>();
                if parts.len() != 4 {
                    continue;
                }
                if let Some(message) = pending_message.as_mut() {
                    message.attachments.push(AttachmentUploadInput {
                        file_name: decode_pst_field(parts[1]),
                        media_type: decode_pst_field(parts[2]),
                        blob_bytes: BASE64
                            .decode(parts[3])
                            .context("decode PST attachment payload")?,
                    });
                }
                continue;
            }

            if !line.starts_with("MESSAGE\t") {
                continue;
            }

            if let Some(message) = pending_message.take() {
                self.persist_pst_imported_message_in_tx(&mut tx, job, message)
                    .await?;
                processed_messages += 1;
            }

            let parts = line.split('\t').collect::<Vec<_>>();
            if parts.len() != 5 {
                continue;
            }
            pending_message = Some(PstImportedMessage {
                internet_message_id: decode_pst_field(parts[1]),
                from_address: decode_pst_field(parts[2]),
                subject: decode_pst_field(parts[3]),
                body_text: decode_pst_field(parts[4]),
                attachments: Vec::new(),
            });
        }

        if let Some(message) = pending_message.take() {
            self.persist_pst_imported_message_in_tx(&mut tx, job, message)
                .await?;
            processed_messages += 1;
        }

        tx.commit().await?;
        Ok(processed_messages)
    }

    async fn fetch_server_settings(&self) -> Result<ServerSettings> {
        let row = sqlx::query(
            r#"
            SELECT primary_hostname, admin_bind_address, smtp_bind_address, imap_bind_address,
                   jmap_bind_address, default_locale, max_message_size_mb, tls_mode
            FROM server_settings
            WHERE tenant_id = $1
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(row) => ServerSettings {
                primary_hostname: row.try_get("primary_hostname")?,
                admin_bind_address: row.try_get("admin_bind_address")?,
                smtp_bind_address: row.try_get("smtp_bind_address")?,
                imap_bind_address: row.try_get("imap_bind_address")?,
                jmap_bind_address: row.try_get("jmap_bind_address")?,
                default_locale: row.try_get("default_locale")?,
                max_message_size_mb: row.try_get::<i32, _>("max_message_size_mb")? as u32,
                tls_mode: row.try_get("tls_mode")?,
            },
            None => ServerSettings {
                primary_hostname: env_hostname("LPE_PUBLIC_HOSTNAME")
                    .or_else(|| env_hostname("LPE_SERVER_NAME"))
                    .unwrap_or_else(|| "localhost".to_string()),
                admin_bind_address: env_bind_address("LPE_BIND_ADDRESS", "127.0.0.1:8080"),
                smtp_bind_address: env_bind_address("LPE_SMTP_BIND_ADDRESS", "0.0.0.0:25"),
                imap_bind_address: env_bind_address("LPE_IMAP_BIND_ADDRESS", "0.0.0.0:143"),
                jmap_bind_address: env_bind_address("LPE_JMAP_BIND_ADDRESS", "0.0.0.0:8081"),
                default_locale: "en".to_string(),
                max_message_size_mb: 64,
                tls_mode: "required".to_string(),
            },
        })
    }

    async fn fetch_security_settings(&self) -> Result<SecuritySettings> {
        let row = sqlx::query(
            r#"
            SELECT
                s.password_login_enabled,
                s.mfa_required_for_admins,
                s.session_timeout_minutes,
                s.audit_retention_days,
                s.oidc_login_enabled,
                s.oidc_provider_label,
                s.oidc_auto_link_by_email,
                s.mailbox_password_login_enabled,
                s.mailbox_oidc_login_enabled,
                s.mailbox_oidc_provider_label,
                s.mailbox_oidc_auto_link_by_email,
                s.mailbox_app_passwords_enabled,
                c.issuer_url,
                c.authorization_endpoint,
                c.token_endpoint,
                c.userinfo_endpoint,
                c.client_id,
                c.client_secret,
                c.scopes,
                c.claim_email,
                c.claim_display_name,
                c.claim_subject,
                mc.issuer_url AS mailbox_issuer_url,
                mc.authorization_endpoint AS mailbox_authorization_endpoint,
                mc.token_endpoint AS mailbox_token_endpoint,
                mc.userinfo_endpoint AS mailbox_userinfo_endpoint,
                mc.client_id AS mailbox_client_id,
                mc.client_secret AS mailbox_client_secret,
                mc.scopes AS mailbox_scopes,
                mc.claim_email AS mailbox_claim_email,
                mc.claim_display_name AS mailbox_claim_display_name,
                mc.claim_subject AS mailbox_claim_subject
            FROM security_settings s
            LEFT JOIN admin_oidc_config c ON c.tenant_id = s.tenant_id
            LEFT JOIN account_oidc_config mc ON mc.tenant_id = s.tenant_id
            WHERE s.tenant_id = $1
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(row) => SecuritySettings {
                password_login_enabled: row.try_get("password_login_enabled")?,
                mfa_required_for_admins: row.try_get("mfa_required_for_admins")?,
                session_timeout_minutes: row.try_get::<i32, _>("session_timeout_minutes")? as u32,
                audit_retention_days: row.try_get::<i32, _>("audit_retention_days")? as u32,
                oidc_login_enabled: row.try_get("oidc_login_enabled")?,
                oidc_provider_label: row.try_get("oidc_provider_label")?,
                oidc_auto_link_by_email: row.try_get("oidc_auto_link_by_email")?,
                oidc_issuer_url: row
                    .try_get::<Option<String>, _>("issuer_url")?
                    .unwrap_or_default(),
                oidc_authorization_endpoint: row
                    .try_get::<Option<String>, _>("authorization_endpoint")?
                    .unwrap_or_default(),
                oidc_token_endpoint: row
                    .try_get::<Option<String>, _>("token_endpoint")?
                    .unwrap_or_default(),
                oidc_userinfo_endpoint: row
                    .try_get::<Option<String>, _>("userinfo_endpoint")?
                    .unwrap_or_default(),
                oidc_client_id: row
                    .try_get::<Option<String>, _>("client_id")?
                    .unwrap_or_default(),
                oidc_client_secret: row
                    .try_get::<Option<String>, _>("client_secret")?
                    .unwrap_or_default(),
                oidc_scopes: row
                    .try_get::<Option<String>, _>("scopes")?
                    .unwrap_or_else(|| "openid profile email".to_string()),
                oidc_claim_email: row
                    .try_get::<Option<String>, _>("claim_email")?
                    .unwrap_or_else(|| "email".to_string()),
                oidc_claim_display_name: row
                    .try_get::<Option<String>, _>("claim_display_name")?
                    .unwrap_or_else(|| "name".to_string()),
                oidc_claim_subject: row
                    .try_get::<Option<String>, _>("claim_subject")?
                    .unwrap_or_else(|| "sub".to_string()),
                mailbox_password_login_enabled: row.try_get("mailbox_password_login_enabled")?,
                mailbox_oidc_login_enabled: row.try_get("mailbox_oidc_login_enabled")?,
                mailbox_oidc_provider_label: row.try_get("mailbox_oidc_provider_label")?,
                mailbox_oidc_auto_link_by_email: row.try_get("mailbox_oidc_auto_link_by_email")?,
                mailbox_oidc_issuer_url: row
                    .try_get::<Option<String>, _>("mailbox_issuer_url")?
                    .unwrap_or_default(),
                mailbox_oidc_authorization_endpoint: row
                    .try_get::<Option<String>, _>("mailbox_authorization_endpoint")?
                    .unwrap_or_default(),
                mailbox_oidc_token_endpoint: row
                    .try_get::<Option<String>, _>("mailbox_token_endpoint")?
                    .unwrap_or_default(),
                mailbox_oidc_userinfo_endpoint: row
                    .try_get::<Option<String>, _>("mailbox_userinfo_endpoint")?
                    .unwrap_or_default(),
                mailbox_oidc_client_id: row
                    .try_get::<Option<String>, _>("mailbox_client_id")?
                    .unwrap_or_default(),
                mailbox_oidc_client_secret: row
                    .try_get::<Option<String>, _>("mailbox_client_secret")?
                    .unwrap_or_default(),
                mailbox_oidc_scopes: row
                    .try_get::<Option<String>, _>("mailbox_scopes")?
                    .unwrap_or_else(|| "openid profile email".to_string()),
                mailbox_oidc_claim_email: row
                    .try_get::<Option<String>, _>("mailbox_claim_email")?
                    .unwrap_or_else(|| "email".to_string()),
                mailbox_oidc_claim_display_name: row
                    .try_get::<Option<String>, _>("mailbox_claim_display_name")?
                    .unwrap_or_else(|| "name".to_string()),
                mailbox_oidc_claim_subject: row
                    .try_get::<Option<String>, _>("mailbox_claim_subject")?
                    .unwrap_or_else(|| "sub".to_string()),
                mailbox_app_passwords_enabled: row.try_get("mailbox_app_passwords_enabled")?,
            },
            None => SecuritySettings {
                password_login_enabled: true,
                mfa_required_for_admins: true,
                session_timeout_minutes: 45,
                audit_retention_days: 365,
                oidc_login_enabled: false,
                oidc_provider_label: "Corporate SSO".to_string(),
                oidc_auto_link_by_email: true,
                oidc_issuer_url: String::new(),
                oidc_authorization_endpoint: String::new(),
                oidc_token_endpoint: String::new(),
                oidc_userinfo_endpoint: String::new(),
                oidc_client_id: String::new(),
                oidc_client_secret: String::new(),
                oidc_scopes: "openid profile email".to_string(),
                oidc_claim_email: "email".to_string(),
                oidc_claim_display_name: "name".to_string(),
                oidc_claim_subject: "sub".to_string(),
                mailbox_password_login_enabled: true,
                mailbox_oidc_login_enabled: false,
                mailbox_oidc_provider_label: "Mailbox SSO".to_string(),
                mailbox_oidc_auto_link_by_email: true,
                mailbox_oidc_issuer_url: String::new(),
                mailbox_oidc_authorization_endpoint: String::new(),
                mailbox_oidc_token_endpoint: String::new(),
                mailbox_oidc_userinfo_endpoint: String::new(),
                mailbox_oidc_client_id: String::new(),
                mailbox_oidc_client_secret: String::new(),
                mailbox_oidc_scopes: "openid profile email".to_string(),
                mailbox_oidc_claim_email: "email".to_string(),
                mailbox_oidc_claim_display_name: "name".to_string(),
                mailbox_oidc_claim_subject: "sub".to_string(),
                mailbox_app_passwords_enabled: true,
            },
        })
    }

    async fn fetch_local_ai_settings(&self) -> Result<LocalAiSettings> {
        let row = sqlx::query(
            r#"
            SELECT enabled, provider, model, offline_only, indexing_enabled
            FROM local_ai_settings
            WHERE tenant_id = $1
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(row) => LocalAiSettings {
                enabled: row.try_get("enabled")?,
                provider: row.try_get("provider")?,
                model: row.try_get("model")?,
                offline_only: row.try_get("offline_only")?,
                indexing_enabled: row.try_get("indexing_enabled")?,
            },
            None => LocalAiSettings {
                enabled: true,
                provider: "stub-local".to_string(),
                model: "gemma3-local".to_string(),
                offline_only: true,
                indexing_enabled: true,
            },
        })
    }

    async fn fetch_antispam_settings(&self) -> Result<AntispamSettings> {
        let row = sqlx::query(
            r#"
            SELECT content_filtering_enabled, spam_engine, quarantine_enabled, quarantine_retention_days
            FROM antispam_settings
            WHERE tenant_id = $1
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(row) => AntispamSettings {
                content_filtering_enabled: row.try_get("content_filtering_enabled")?,
                spam_engine: row.try_get("spam_engine")?,
                quarantine_enabled: row.try_get("quarantine_enabled")?,
                quarantine_retention_days: row.try_get::<i32, _>("quarantine_retention_days")?
                    as u32,
            },
            None => AntispamSettings {
                content_filtering_enabled: true,
                spam_engine: "rspamd-ready".to_string(),
                quarantine_enabled: true,
                quarantine_retention_days: 30,
            },
        })
    }

    async fn fetch_server_administrators(&self) -> Result<Vec<ServerAdministrator>> {
        let rows = sqlx::query_as::<_, ServerAdministratorRow>(
            r#"
            SELECT
                sa.id,
                sa.domain_id,
                d.name AS domain_name,
                sa.email,
                sa.display_name,
                sa.role,
                sa.rights_summary,
                sa.permissions_json
            FROM server_administrators sa
            LEFT JOIN domains d ON d.id = sa.domain_id
            WHERE sa.tenant_id = $1
            ORDER BY sa.created_at ASC
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let permissions = permissions_from_storage(
                    &row.role,
                    Some(&row.rights_summary),
                    Some(&row.permissions_json),
                );
                ServerAdministrator {
                    id: row.id,
                    domain_id: row.domain_id,
                    domain_name: row.domain_name.unwrap_or_else(|| "All domains".to_string()),
                    email: row.email,
                    display_name: row.display_name,
                    role: row.role,
                    rights_summary: permission_summary(&permissions),
                    permissions,
                }
            })
            .collect())
    }

    async fn fetch_antispam_rules(&self) -> Result<Vec<FilterRule>> {
        let rows = sqlx::query_as::<_, FilterRuleRow>(
            r#"
            SELECT id, name, scope, action, status
            FROM antispam_filter_rules
            WHERE tenant_id = $1
            ORDER BY created_at ASC
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| FilterRule {
                id: row.id,
                name: row.name,
                scope: row.scope,
                action: row.action,
                status: row.status,
            })
            .collect())
    }

    async fn fetch_quarantine_items(&self) -> Result<Vec<QuarantineItem>> {
        let rows = sqlx::query_as::<_, QuarantineRow>(
            r#"
            SELECT
                id,
                message_ref,
                sender,
                recipient,
                reason,
                status,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
            FROM antispam_quarantine
            WHERE tenant_id = $1
            ORDER BY created_at DESC
            LIMIT 50
            "#,
        )
        .bind(PLATFORM_TENANT_ID)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| QuarantineItem {
                id: row.id,
                message_ref: row.message_ref,
                sender: row.sender,
                recipient: row.recipient,
                reason: row.reason,
                status: row.status,
                created_at: row.created_at,
            })
            .collect())
    }

    pub async fn fetch_client_events(&self, account_id: Uuid) -> Result<Vec<ClientEvent>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ClientEventRow>(
            r#"
            SELECT
                id,
                to_char(event_date, 'YYYY-MM-DD') AS date,
                to_char(event_time, 'HH24:MI') AS time,
                time_zone,
                duration_minutes,
                recurrence_rule,
                title,
                location,
                attendees,
                attendees_json,
                notes
            FROM calendar_events
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY event_date ASC, event_time ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_event).collect())
    }

    pub async fn fetch_client_events_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientEvent>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query_as::<_, ClientEventRow>(
            r#"
            SELECT
                id,
                to_char(event_date, 'YYYY-MM-DD') AS date,
                to_char(event_time, 'HH24:MI') AS time,
                time_zone,
                duration_minutes,
                recurrence_rule,
                title,
                location,
                attendees,
                attendees_json,
                notes
            FROM calendar_events
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = ANY($3)
            ORDER BY event_date ASC, event_time ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_event).collect())
    }

    pub async fn fetch_client_contacts(&self, account_id: Uuid) -> Result<Vec<ClientContact>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ClientContactRow>(
            r#"
            SELECT id, name, role, email, phone, team, notes
            FROM contacts
            WHERE tenant_id = $1 AND account_id = $2
            ORDER BY name ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_contact).collect())
    }

    pub async fn fetch_client_contacts_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientContact>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let rows = sqlx::query_as::<_, ClientContactRow>(
            r#"
            SELECT id, name, role, email, phone, team, notes
            FROM contacts
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = ANY($3)
            ORDER BY name ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(map_contact).collect())
    }

    pub async fn fetch_client_tasks(&self, account_id: Uuid) -> Result<Vec<ClientTask>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, account_id).await?;
        let rows = sqlx::query_as::<_, ClientTaskRow>(
            r#"
            SELECT
                tasks.id,
                task_lists.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                (task_lists.account_id = $2) AS is_owned,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_read, FALSE)
                END AS may_read,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_write, FALSE)
                END AS may_write,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_delete, FALSE)
                END AS may_delete,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_share, FALSE)
                END AS may_share,
                tasks.task_list_id,
                task_lists.sort_order AS task_list_sort_order,
                tasks.title,
                tasks.description,
                tasks.status,
                CASE
                    WHEN tasks.due_at IS NULL THEN NULL
                    ELSE to_char(tasks.due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS due_at,
                CASE
                    WHEN tasks.completed_at IS NULL THEN NULL
                    ELSE to_char(tasks.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS completed_at,
                tasks.sort_order,
                to_char(tasks.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM tasks
            JOIN task_lists
              ON task_lists.tenant_id = tasks.tenant_id
             AND task_lists.account_id = tasks.account_id
             AND task_lists.id = tasks.task_list_id
            JOIN accounts owner ON owner.id = task_lists.account_id
            LEFT JOIN task_list_grants g
              ON g.tenant_id = task_lists.tenant_id
             AND g.task_list_id = task_lists.id
             AND g.owner_account_id = task_lists.account_id
             AND g.grantee_account_id = $2
            WHERE tasks.tenant_id = $1
              AND (task_lists.account_id = $2 OR COALESCE(g.may_read, FALSE))
            ORDER BY
                CASE WHEN task_lists.account_id = $2 THEN 0 ELSE 1 END ASC,
                lower(owner.primary_email) ASC,
                task_lists.sort_order ASC,
                tasks.sort_order ASC,
                tasks.updated_at ASC,
                tasks.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows.into_iter().map(map_task).collect())
    }

    pub async fn fetch_client_tasks_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientTask>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_default_task_list(&mut tx, &tenant_id, account_id).await?;

        let rows = sqlx::query_as::<_, ClientTaskRow>(
            r#"
            SELECT
                tasks.id,
                task_lists.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                (task_lists.account_id = $2) AS is_owned,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_read, FALSE)
                END AS may_read,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_write, FALSE)
                END AS may_write,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_delete, FALSE)
                END AS may_delete,
                CASE
                    WHEN task_lists.account_id = $2 THEN TRUE
                    ELSE COALESCE(g.may_share, FALSE)
                END AS may_share,
                tasks.task_list_id,
                task_lists.sort_order AS task_list_sort_order,
                tasks.title,
                tasks.description,
                tasks.status,
                CASE
                    WHEN tasks.due_at IS NULL THEN NULL
                    ELSE to_char(tasks.due_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS due_at,
                CASE
                    WHEN tasks.completed_at IS NULL THEN NULL
                    ELSE to_char(tasks.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                END AS completed_at,
                tasks.sort_order,
                to_char(tasks.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM tasks
            JOIN task_lists
              ON task_lists.tenant_id = tasks.tenant_id
             AND task_lists.account_id = tasks.account_id
             AND task_lists.id = tasks.task_list_id
            JOIN accounts owner ON owner.id = task_lists.account_id
            LEFT JOIN task_list_grants g
              ON g.tenant_id = task_lists.tenant_id
             AND g.task_list_id = task_lists.id
             AND g.owner_account_id = task_lists.account_id
             AND g.grantee_account_id = $2
            WHERE tasks.tenant_id = $1
              AND tasks.id = ANY($3)
              AND (task_lists.account_id = $2 OR COALESCE(g.may_read, FALSE))
            ORDER BY
                CASE WHEN task_lists.account_id = $2 THEN 0 ELSE 1 END ASC,
                lower(owner.primary_email) ASC,
                task_lists.sort_order ASC,
                tasks.sort_order ASC,
                tasks.updated_at ASC,
                tasks.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(ids)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(rows.into_iter().map(map_task).collect())
    }

    pub async fn fetch_latest_activesync_sync_state(
        &self,
        account_id: Uuid,
        device_id: &str,
        collection_id: &str,
    ) -> Result<Option<ActiveSyncSyncState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let row = sqlx::query_as::<_, ActiveSyncSyncStateRow>(
            r#"
            SELECT sync_key, snapshot_json
            FROM activesync_sync_states
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
              AND collection_id = $4
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(collection_id.trim())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| ActiveSyncSyncState {
            sync_key: row.sync_key,
            snapshot_json: row.snapshot_json,
        }))
    }

    pub async fn fetch_activesync_message_attachments(
        &self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<Vec<ActiveSyncAttachment>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let rows = sqlx::query_as::<_, ActiveSyncAttachmentRow>(
            r#"
            SELECT a.id, a.message_id, a.file_name, a.media_type, a.size_octets
            FROM attachments a
            JOIN messages m ON m.id = a.message_id
            WHERE a.tenant_id = $1
              AND m.account_id = $2
              AND a.message_id = $3
            ORDER BY a.file_name ASC, a.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| ActiveSyncAttachment {
                id: row.id,
                message_id: row.message_id,
                file_name: row.file_name,
                media_type: row.media_type,
                size_octets: row.size_octets.max(0) as u64,
                file_reference: format!("attachment:{}:{}", row.message_id, row.id),
            })
            .collect())
    }

    pub async fn fetch_activesync_attachment_content(
        &self,
        account_id: Uuid,
        file_reference: &str,
    ) -> Result<Option<ActiveSyncAttachmentContent>> {
        let Some((message_id, attachment_id)) = parse_activesync_file_reference(file_reference)
        else {
            return Ok(None);
        };
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;

        let row = sqlx::query(
            r#"
            SELECT a.file_name, a.media_type, b.blob_bytes
            FROM attachments a
            JOIN messages m ON m.id = a.message_id
            JOIN attachment_blobs b ON b.id = a.attachment_blob_id
            WHERE a.tenant_id = $1
              AND a.id = $2
              AND a.message_id = $3
              AND m.account_id = $4
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(attachment_id)
        .bind(message_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| ActiveSyncAttachmentContent {
            file_reference: file_reference.trim().to_string(),
            file_name: row.try_get("file_name").unwrap_or_default(),
            media_type: row.try_get("media_type").unwrap_or_default(),
            blob_bytes: row.try_get("blob_bytes").unwrap_or_default(),
        }))
    }

    async fn ingest_message_attachments_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        message_id: Uuid,
        attachments: &[AttachmentUploadInput],
    ) -> Result<()> {
        if attachments.is_empty() {
            return Ok(());
        }

        let domain_name = self.load_account_domain_in_tx(tx, account_id).await?;
        let mut search_fragments = Vec::new();

        for attachment in attachments {
            let blob = self
                .store_attachment_blob_in_tx(
                    tx,
                    &domain_name,
                    attachment.media_type.trim(),
                    &attachment.blob_bytes,
                )
                .await?;
            let extracted_text = extract_supported_attachment_text(
                attachment.media_type.trim(),
                attachment.file_name.as_str(),
                &attachment.blob_bytes,
            )?;
            if let Some(text) = extracted_text
                .as_ref()
                .filter(|text| !text.trim().is_empty())
            {
                search_fragments.push(text.clone());
            }

            sqlx::query(
                r#"
                INSERT INTO attachments (
                    id, tenant_id, message_id, file_name, media_type, size_octets,
                    blob_ref, extracted_text, extracted_text_tsv, attachment_blob_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, to_tsvector('simple', COALESCE($8, '')), $9)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id)
            .bind(message_id)
            .bind(attachment.file_name.trim())
            .bind(attachment.media_type.trim())
            .bind(attachment.blob_bytes.len() as i64)
            .bind(format!("attachment-blob:{}", blob.id))
            .bind(extracted_text)
            .bind(blob.id)
            .execute(&mut **tx)
            .await?;
        }

        sqlx::query(
            r#"
            UPDATE messages
            SET has_attachments = TRUE
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .execute(&mut **tx)
        .await?;

        if !search_fragments.is_empty() {
            let attachment_text = search_fragments.join("\n");
            sqlx::query(
                r#"
                UPDATE message_bodies
                SET search_vector = to_tsvector(
                    'simple',
                    concat_ws(' ', body_text, participants_normalized, $2)
                )
                WHERE message_id = $1
                "#,
            )
            .bind(message_id)
            .bind(attachment_text)
            .execute(&mut **tx)
            .await?;
        }

        Ok(())
    }

    async fn store_attachment_blob_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        domain_name: &str,
        media_type: &str,
        blob_bytes: &[u8],
    ) -> Result<StoredAttachmentBlob> {
        let content_sha256 = sha256_hex(blob_bytes);

        if let Some(row) = sqlx::query(
            r#"
            SELECT id
            FROM attachment_blobs
            WHERE tenant_id = $1 AND domain_name = $2 AND content_sha256 = $3
            LIMIT 1
            "#,
        )
        .bind(self.tenant_id_for_domain_name(domain_name).await?)
        .bind(domain_name)
        .bind(&content_sha256)
        .fetch_optional(&mut **tx)
        .await?
        {
            return Ok(StoredAttachmentBlob {
                id: row.try_get("id")?,
            });
        }

        let blob_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO attachment_blobs (
                id, tenant_id, domain_name, content_sha256, media_type, size_octets, blob_bytes
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(blob_id)
        .bind(self.tenant_id_for_domain_name(domain_name).await?)
        .bind(domain_name)
        .bind(content_sha256)
        .bind(media_type)
        .bind(blob_bytes.len() as i64)
        .bind(blob_bytes)
        .execute(&mut **tx)
        .await?;

        Ok(StoredAttachmentBlob { id: blob_id })
    }

    async fn load_account_domain_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        account_id: Uuid,
    ) -> Result<String> {
        let row = sqlx::query(
            r#"
            SELECT primary_email
            FROM accounts
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(account_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("account not found"))?;
        let primary_email: String = row.try_get("primary_email")?;
        domain_from_email(&primary_email)
    }

    async fn persist_pst_imported_message_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        job: &PendingPstJobRow,
        message: PstImportedMessage,
    ) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(job.account_id).await?;
        let message_id = Uuid::new_v4();
        let preview_text = preview_text(&message.body_text);
        let size_octets = message.body_text.len().saturating_add(
            message
                .attachments
                .iter()
                .map(|attachment| attachment.blob_bytes.len())
                .sum::<usize>(),
        ) as i64;

        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                received_at, sent_at, from_display, from_address, sender_display,
                sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                submission_source, delivery_status
            )
            VALUES (
                $1, $2, $3, $4, $5, NULLIF($6, ''),
                NOW(), NULL, NULL, $7, NULL,
                NULL, 'self', $3, $8, $9, TRUE, FALSE, FALSE, $10, $11,
                'pst-import', 'stored'
            )
            "#,
        )
        .bind(message_id)
        .bind(&tenant_id)
        .bind(job.account_id)
        .bind(job.mailbox_id)
        .bind(Uuid::new_v4())
        .bind(message.internet_message_id)
        .bind(message.from_address)
        .bind(message.subject.clone())
        .bind(preview_text)
        .bind(size_octets.max(0))
        .bind(format!("pst-import:{message_id}"))
        .execute(&mut **tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_bodies (
                message_id, body_text, body_html_sanitized, participants_normalized,
                language_code, content_hash, search_vector
            )
            VALUES ($1, $2, NULL, '', NULL, $3, to_tsvector('simple', $4))
            "#,
        )
        .bind(message_id)
        .bind(message.body_text.clone())
        .bind(format!("pst-import:{message_id}"))
        .bind(format!("{} {}", message.subject, message.body_text))
        .execute(&mut **tx)
        .await?;

        self.ingest_message_attachments_in_tx(
            tx,
            &self.tenant_id_for_account_id(job.account_id).await?,
            job.account_id,
            message_id,
            &message.attachments,
        )
        .await?;

        Ok(())
    }

    async fn ensure_account_exists(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<()> {
        let account_exists = sqlx::query(
            r#"
            SELECT 1
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_optional(&mut **tx)
        .await?;

        if account_exists.is_none() {
            bail!("account not found");
        }

        Ok(())
    }

    async fn delete_draft_message_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<()> {
        let deleted = sqlx::query(
            r#"
            DELETE FROM messages m
            USING mailboxes mb
            WHERE m.mailbox_id = mb.id
              AND m.tenant_id = $1
              AND m.account_id = $2
              AND m.id = $3
              AND mb.role = 'drafts'
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(message_id)
        .execute(&mut **tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("draft not found");
        }

        Ok(())
    }

    async fn ensure_mailbox(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        role: &str,
        display_name: &str,
        sort_order: i32,
        retention_days: i32,
    ) -> Result<Uuid> {
        if let Some(row) = sqlx::query(
            r#"
            SELECT id
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND role = $3
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(role)
        .fetch_optional(&mut **tx)
        .await?
        {
            return row.try_get("id").map_err(Into::into);
        }

        let mailbox_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO mailboxes (
                id, tenant_id, account_id, role, display_name, sort_order, retention_days
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(mailbox_id)
        .bind(tenant_id)
        .bind(account_id)
        .bind(role)
        .bind(display_name)
        .bind(sort_order)
        .bind(retention_days)
        .execute(&mut **tx)
        .await?;

        Ok(mailbox_id)
    }

    async fn insert_audit(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        audit: AuditEntryInput,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO audit_events (id, tenant_id, actor, action, subject)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(audit.actor)
        .bind(audit.action)
        .bind(audit.subject)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn emit_canonical_change<'e, E>(
        executor: E,
        tenant_id: &str,
        category: CanonicalChangeCategory,
        principal_account_ids: &[Uuid],
        account_ids: &[Uuid],
    ) -> Result<()>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let payload = serde_json::to_string(&CanonicalChangeNotification {
            tenant_id: tenant_id.to_string(),
            category: category.as_str().to_string(),
            principal_account_ids: principal_account_ids.iter().map(Uuid::to_string).collect(),
            account_ids: account_ids.iter().map(Uuid::to_string).collect(),
        })?;
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(CANONICAL_CHANGE_CHANNEL)
            .bind(payload)
            .execute(executor)
            .await?;
        Ok(())
    }

    async fn emit_mail_change(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<()> {
        let mut principal_account_ids = HashSet::from([account_id]);
        let delegated_account_ids = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT grantee_account_id
            FROM mailbox_delegation_grants
            WHERE tenant_id = $1
              AND owner_account_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_all(&mut **tx)
        .await?;
        principal_account_ids.extend(delegated_account_ids);

        let mut principal_account_ids = principal_account_ids.into_iter().collect::<Vec<_>>();
        principal_account_ids.sort();

        Self::emit_canonical_change(
            &mut **tx,
            tenant_id,
            CanonicalChangeCategory::Mail,
            &principal_account_ids,
            &[account_id],
        )
        .await
    }

    async fn emit_mail_delegation_change(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
    ) -> Result<()> {
        let mut principal_account_ids = vec![owner_account_id, grantee_account_id];
        principal_account_ids.sort();
        principal_account_ids.dedup();
        Self::emit_canonical_change(
            &mut **tx,
            tenant_id,
            CanonicalChangeCategory::Mail,
            &principal_account_ids,
            &[owner_account_id],
        )
        .await
    }

    async fn emit_collaboration_change(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        category: CanonicalChangeCategory,
        owner_account_id: Uuid,
    ) -> Result<()> {
        let collection_kind = match category {
            CanonicalChangeCategory::Contacts => CollaborationResourceKind::Contacts,
            CanonicalChangeCategory::Calendar => CollaborationResourceKind::Calendar,
            CanonicalChangeCategory::Tasks => CollaborationResourceKind::Tasks,
            _ => bail!("unsupported collaboration change category"),
        };

        let mut principal_account_ids = HashSet::from([owner_account_id]);
        let shared_with = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT grantee_account_id
            FROM collaboration_collection_grants
            WHERE tenant_id = $1
              AND collection_kind = $2
              AND owner_account_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(collection_kind.as_str())
        .bind(owner_account_id)
        .fetch_all(&mut **tx)
        .await?;
        principal_account_ids.extend(shared_with);

        let mut principal_account_ids = principal_account_ids.into_iter().collect::<Vec<_>>();
        principal_account_ids.sort();

        Self::emit_canonical_change(
            &mut **tx,
            tenant_id,
            category,
            &principal_account_ids,
            &principal_account_ids,
        )
        .await
    }

    async fn emit_collaboration_grant_change(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        kind: CollaborationResourceKind,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
    ) -> Result<()> {
        let category = match kind {
            CollaborationResourceKind::Contacts => CanonicalChangeCategory::Contacts,
            CollaborationResourceKind::Calendar => CanonicalChangeCategory::Calendar,
            CollaborationResourceKind::Tasks => CanonicalChangeCategory::Tasks,
        };
        let mut principal_account_ids = vec![owner_account_id, grantee_account_id];
        principal_account_ids.sort();
        principal_account_ids.dedup();

        Self::emit_canonical_change(
            &mut **tx,
            tenant_id,
            category,
            &principal_account_ids,
            &principal_account_ids,
        )
        .await
    }

    async fn emit_task_change(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<()> {
        Self::emit_task_access_change(tx, tenant_id, account_id, account_id).await
    }

    async fn emit_task_access_change(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        owner_account_id: Uuid,
        principal_account_id: Uuid,
    ) -> Result<()> {
        let mut principal_account_ids = vec![owner_account_id, principal_account_id];
        principal_account_ids.sort();
        principal_account_ids.dedup();
        Self::emit_canonical_change(
            &mut **tx,
            tenant_id,
            CanonicalChangeCategory::Tasks,
            &principal_account_ids,
            &principal_account_ids,
        )
        .await
    }

    async fn tenant_id_for_domain_name(&self, domain_name: &str) -> Result<String> {
        let domain_name = domain_name.trim().to_lowercase();
        if domain_name.is_empty() {
            bail!("domain name is required");
        }

        let existing = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM domains
            WHERE lower(name) = lower($1)
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&domain_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(existing.unwrap_or(domain_name))
    }

    async fn tenant_id_for_domain_id(&self, domain_id: Uuid) -> Result<String> {
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM domains
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(domain_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("domain not found"))
    }

    async fn fetch_accessible_collections(
        &self,
        principal_account_id: Uuid,
        kind: CollaborationResourceKind,
    ) -> Result<Vec<CollaborationCollection>> {
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let principal = self.account_identity_for_id(principal_account_id).await?;
        let mut collections = vec![CollaborationCollection {
            id: DEFAULT_COLLECTION_ID.to_string(),
            kind: kind.as_str().to_string(),
            owner_account_id: principal.id,
            owner_email: principal.email.clone(),
            owner_display_name: principal.display_name.clone(),
            display_name: kind.collection_label().to_string(),
            is_owned: true,
            rights: CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            },
        }];

        let rows = sqlx::query_as::<_, CollaborationCollectionRow>(
            r#"
            SELECT
                g.owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                g.may_read,
                g.may_write,
                g.may_delete,
                g.may_share
            FROM collaboration_collection_grants g
            JOIN accounts owner ON owner.id = g.owner_account_id
            WHERE g.tenant_id = $1
              AND g.collection_kind = $2
              AND g.grantee_account_id = $3
              AND g.may_read = TRUE
            ORDER BY lower(owner.primary_email) ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(kind.as_str())
        .bind(principal_account_id)
        .fetch_all(&self.pool)
        .await?;

        collections.extend(rows.into_iter().map(|row| CollaborationCollection {
            id: shared_collection_id(kind, row.owner_account_id),
            kind: kind.as_str().to_string(),
            owner_account_id: row.owner_account_id,
            owner_email: row.owner_email.clone(),
            owner_display_name: row.owner_display_name.clone(),
            display_name: shared_collection_display_name(
                kind,
                &row.owner_display_name,
                &row.owner_email,
            ),
            is_owned: false,
            rights: CollaborationRights {
                may_read: row.may_read,
                may_write: row.may_write,
                may_delete: row.may_delete,
                may_share: row.may_share,
            },
        }));

        Ok(collections)
    }

    async fn resolve_collection_access(
        &self,
        principal_account_id: Uuid,
        kind: CollaborationResourceKind,
        collection_id: &str,
    ) -> Result<CollaborationCollection> {
        let collection_id = collection_id.trim();
        let collections = self
            .fetch_accessible_collections(principal_account_id, kind)
            .await?;
        collections
            .into_iter()
            .find(|collection| collection.id == collection_id)
            .ok_or_else(|| anyhow!("collection not found"))
    }

    async fn ensure_default_task_list(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<ClientTaskListRow> {
        sqlx::query_as::<_, ClientTaskListRow>(
            r#"
            INSERT INTO task_lists (id, tenant_id, account_id, name, role, sort_order)
            VALUES ($1, $2, $3, $4, $5, 0)
            ON CONFLICT (tenant_id, account_id, role) DO UPDATE SET
                name = task_lists.name
            RETURNING
                id,
                account_id AS owner_account_id,
                ''::text AS owner_email,
                ''::text AS owner_display_name,
                TRUE AS is_owned,
                TRUE AS may_read,
                TRUE AS may_write,
                TRUE AS may_delete,
                TRUE AS may_share,
                name,
                role,
                sort_order,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(account_id)
        .bind(DEFAULT_TASK_LIST_NAME)
        .bind(DEFAULT_TASK_LIST_ROLE)
        .fetch_one(&mut **tx)
        .await
        .map_err(Into::into)
    }

    async fn load_task_list_in_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        task_list_id: Uuid,
    ) -> Result<ClientTaskListRow> {
        sqlx::query_as::<_, ClientTaskListRow>(
            r#"
            SELECT
                id,
                account_id AS owner_account_id,
                ''::text AS owner_email,
                ''::text AS owner_display_name,
                TRUE AS is_owned,
                TRUE AS may_read,
                TRUE AS may_write,
                TRUE AS may_delete,
                TRUE AS may_share,
                name,
                role,
                sort_order,
                to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at
            FROM task_lists
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = $3
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(task_list_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("task list not found"))
    }

    async fn fetch_accessible_contacts_internal(
        &self,
        principal_account_id: Uuid,
        collection_id: Option<&str>,
        ids: Option<&[Uuid]>,
    ) -> Result<Vec<AccessibleContact>> {
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let owner_account_id =
            if let Some(collection_id) = collection_id.filter(|value| !value.trim().is_empty()) {
                Some(
                    self.resolve_collection_access(
                        principal_account_id,
                        CollaborationResourceKind::Contacts,
                        collection_id,
                    )
                    .await?
                    .owner_account_id,
                )
            } else {
                None
            };

        let rows = sqlx::query_as::<_, AccessibleContactRow>(
            r#"
            SELECT
                c.id,
                c.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                CASE WHEN c.account_id = $2 THEN TRUE ELSE COALESCE(g.may_read, FALSE) END AS may_read,
                CASE WHEN c.account_id = $2 THEN TRUE ELSE COALESCE(g.may_write, FALSE) END AS may_write,
                CASE WHEN c.account_id = $2 THEN TRUE ELSE COALESCE(g.may_delete, FALSE) END AS may_delete,
                CASE WHEN c.account_id = $2 THEN TRUE ELSE COALESCE(g.may_share, FALSE) END AS may_share,
                c.name,
                c.role,
                c.email,
                c.phone,
                c.team,
                c.notes
            FROM contacts c
            JOIN accounts owner ON owner.id = c.account_id
            LEFT JOIN collaboration_collection_grants g
              ON g.tenant_id = c.tenant_id
             AND g.collection_kind = 'contacts'
             AND g.owner_account_id = c.account_id
             AND g.grantee_account_id = $2
            WHERE c.tenant_id = $1
              AND (c.account_id = $2 OR COALESCE(g.may_read, FALSE))
              AND ($3::uuid IS NULL OR c.account_id = $3)
              AND ($4::uuid[] IS NULL OR c.id = ANY($4))
            ORDER BY lower(c.name) ASC, c.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(principal_account_id)
        .bind(owner_account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AccessibleContact {
                id: row.id,
                collection_id: collection_id_for_owner(
                    CollaborationResourceKind::Contacts,
                    principal_account_id,
                    row.owner_account_id,
                ),
                owner_account_id: row.owner_account_id,
                owner_email: row.owner_email,
                owner_display_name: row.owner_display_name,
                rights: CollaborationRights {
                    may_read: row.may_read,
                    may_write: row.may_write,
                    may_delete: row.may_delete,
                    may_share: row.may_share,
                },
                name: row.name,
                role: row.role,
                email: row.email,
                phone: row.phone,
                team: row.team,
                notes: row.notes,
            })
            .collect())
    }

    async fn fetch_accessible_events_internal(
        &self,
        principal_account_id: Uuid,
        collection_id: Option<&str>,
        ids: Option<&[Uuid]>,
    ) -> Result<Vec<AccessibleEvent>> {
        let tenant_id = self.tenant_id_for_account_id(principal_account_id).await?;
        let owner_account_id =
            if let Some(collection_id) = collection_id.filter(|value| !value.trim().is_empty()) {
                Some(
                    self.resolve_collection_access(
                        principal_account_id,
                        CollaborationResourceKind::Calendar,
                        collection_id,
                    )
                    .await?
                    .owner_account_id,
                )
            } else {
                None
            };

        let rows = sqlx::query_as::<_, AccessibleEventRow>(
            r#"
            SELECT
                e.id,
                e.account_id AS owner_account_id,
                owner.primary_email AS owner_email,
                owner.display_name AS owner_display_name,
                CASE WHEN e.account_id = $2 THEN TRUE ELSE COALESCE(g.may_read, FALSE) END AS may_read,
                CASE WHEN e.account_id = $2 THEN TRUE ELSE COALESCE(g.may_write, FALSE) END AS may_write,
                CASE WHEN e.account_id = $2 THEN TRUE ELSE COALESCE(g.may_delete, FALSE) END AS may_delete,
                CASE WHEN e.account_id = $2 THEN TRUE ELSE COALESCE(g.may_share, FALSE) END AS may_share,
                to_char(e.event_date, 'YYYY-MM-DD') AS date,
                to_char(e.event_time, 'HH24:MI') AS time,
                e.time_zone,
                e.duration_minutes,
                e.recurrence_rule,
                e.title,
                e.location,
                e.attendees,
                e.attendees_json,
                e.notes
            FROM calendar_events e
            JOIN accounts owner ON owner.id = e.account_id
            LEFT JOIN collaboration_collection_grants g
              ON g.tenant_id = e.tenant_id
             AND g.collection_kind = 'calendar'
             AND g.owner_account_id = e.account_id
             AND g.grantee_account_id = $2
            WHERE e.tenant_id = $1
              AND (e.account_id = $2 OR COALESCE(g.may_read, FALSE))
              AND ($3::uuid IS NULL OR e.account_id = $3)
              AND ($4::uuid[] IS NULL OR e.id = ANY($4))
            ORDER BY e.event_date ASC, e.event_time ASC, e.id ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(principal_account_id)
        .bind(owner_account_id)
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AccessibleEvent {
                id: row.id,
                collection_id: collection_id_for_owner(
                    CollaborationResourceKind::Calendar,
                    principal_account_id,
                    row.owner_account_id,
                ),
                owner_account_id: row.owner_account_id,
                owner_email: row.owner_email,
                owner_display_name: row.owner_display_name,
                rights: CollaborationRights {
                    may_read: row.may_read,
                    may_write: row.may_write,
                    may_delete: row.may_delete,
                    may_share: row.may_share,
                },
                date: row.date,
                time: row.time,
                time_zone: row.time_zone,
                duration_minutes: row.duration_minutes,
                recurrence_rule: row.recurrence_rule,
                title: row.title,
                location: row.location,
                attendees: row.attendees,
                attendees_json: row.attendees_json,
                notes: row.notes,
            })
            .collect())
    }

    async fn account_identity_for_id(&self, account_id: Uuid) -> Result<AccountIdentity> {
        let row = sqlx::query(
            r#"
            SELECT id, primary_email, display_name
            FROM accounts
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("account not found"))?;

        Ok(AccountIdentity {
            id: row.try_get("id")?,
            email: row.try_get("primary_email")?,
            display_name: row.try_get("display_name")?,
        })
    }

    async fn load_account_identity_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<AccountIdentity> {
        let row = sqlx::query(
            r#"
            SELECT id, primary_email, display_name
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("account not found"))?;

        Ok(AccountIdentity {
            id: row.try_get("id")?,
            email: row.try_get("primary_email")?,
            display_name: row.try_get("display_name")?,
        })
    }

    async fn load_account_identity_by_email_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        email: &str,
    ) -> Result<AccountIdentity> {
        let row = sqlx::query(
            r#"
            SELECT id, primary_email, display_name
            FROM accounts
            WHERE tenant_id = $1 AND lower(primary_email) = lower($2)
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(email)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("grantee account not found in the same tenant"))?;

        Ok(AccountIdentity {
            id: row.try_get("id")?,
            email: row.try_get("primary_email")?,
            display_name: row.try_get("display_name")?,
        })
    }

    async fn ensure_same_tenant_account_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<()> {
        self.load_account_identity_in_tx(tx, tenant_id, account_id)
            .await
            .map(|_| ())
    }

    async fn has_sender_right_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        sender_right: SenderDelegationRight,
    ) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM sender_delegation_grants
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND grantee_account_id = $3
                  AND sender_right = $4
            )
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .bind(sender_right.as_str())
        .fetch_one(&mut **tx)
        .await
        .map_err(Into::into)
    }

    async fn resolve_submission_authorization_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        input: &SubmitMessageInput,
    ) -> Result<ResolvedSubmissionAuthorization> {
        let owner = self
            .load_account_identity_in_tx(tx, tenant_id, input.account_id)
            .await?;
        let submitted_by = self
            .load_account_identity_in_tx(tx, tenant_id, input.submitted_by_account_id)
            .await?;
        let requested_from = normalize_email(&input.from_address);
        let requested_sender = input
            .sender_address
            .as_deref()
            .map(normalize_email)
            .filter(|value| !value.is_empty());
        let owner_display_name = owner.display_name.clone();
        let submitted_by_display_name = submitted_by.display_name.clone();

        if requested_from.is_empty() {
            bail!("from_address is required");
        }

        if owner.id == submitted_by.id {
            if requested_from != owner.email {
                bail!("from email must match authenticated account");
            }
            if let Some(sender_address) = requested_sender {
                if sender_address != submitted_by.email {
                    bail!("sender email must match authenticated account");
                }
            }
            return Ok(ResolvedSubmissionAuthorization {
                submitted_by,
                from_address: requested_from,
                from_display: trim_optional_text(input.from_display.as_deref())
                    .or_else(|| Some(owner_display_name.clone())),
                sender_address: None,
                sender_display: None,
                authorization_kind: SenderAuthorizationKind::SelfSend,
            });
        }

        if requested_from != owner.email {
            bail!("from email must match delegated mailbox");
        }

        if let Some(sender_address) = requested_sender {
            if sender_address != submitted_by.email {
                bail!("sender email must match authenticated account");
            }
            if !self
                .has_sender_right_in_tx(
                    tx,
                    tenant_id,
                    owner.id,
                    submitted_by.id,
                    SenderDelegationRight::SendOnBehalf,
                )
                .await?
            {
                bail!("send on behalf is not granted for this mailbox");
            }
            return Ok(ResolvedSubmissionAuthorization {
                submitted_by,
                from_address: requested_from,
                from_display: trim_optional_text(input.from_display.as_deref())
                    .or_else(|| Some(owner_display_name.clone())),
                sender_address: Some(sender_address),
                sender_display: trim_optional_text(input.sender_display.as_deref())
                    .or_else(|| Some(submitted_by_display_name)),
                authorization_kind: SenderAuthorizationKind::SendOnBehalf,
            });
        }

        if !self
            .has_sender_right_in_tx(
                tx,
                tenant_id,
                owner.id,
                submitted_by.id,
                SenderDelegationRight::SendAs,
            )
            .await?
        {
            bail!("send as is not granted for this mailbox");
        }

        Ok(ResolvedSubmissionAuthorization {
            submitted_by,
            from_address: requested_from,
            from_display: trim_optional_text(input.from_display.as_deref())
                .or_else(|| Some(owner_display_name)),
            sender_address: None,
            sender_display: None,
            authorization_kind: SenderAuthorizationKind::SendAs,
        })
    }

    async fn tenant_id_for_account_id(&self, account_id: Uuid) -> Result<String> {
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM accounts
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("account not found"))
    }

    pub async fn find_submission_account_by_email_in_same_tenant(
        &self,
        reference_account_id: Uuid,
        email: &str,
    ) -> Result<Option<SubmissionAccountIdentity>> {
        let tenant_id = self.tenant_id_for_account_id(reference_account_id).await?;
        let normalized_email = normalize_email(email);
        if normalized_email.is_empty() {
            return Ok(None);
        }

        let row = sqlx::query(
            r#"
            SELECT id, primary_email, display_name
            FROM accounts
            WHERE tenant_id = $1 AND lower(primary_email) = lower($2)
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(&normalized_email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| SubmissionAccountIdentity {
            account_id: row.get("id"),
            email: row.get("primary_email"),
            display_name: row.get("display_name"),
        }))
    }

    async fn tenant_id_for_account_email(&self, email: &str) -> Result<String> {
        let email = normalize_email(email);
        if email.is_empty() {
            bail!("account email is required");
        }

        if let Some(tenant_id) = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM accounts
            WHERE lower(primary_email) = lower($1)
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&email)
        .fetch_optional(&self.pool)
        .await?
        {
            return Ok(tenant_id);
        }

        let domain = domain_from_email(&email)?;
        self.tenant_id_for_domain_name(&domain).await
    }

    async fn tenant_id_for_admin_email(&self, email: &str) -> Result<String> {
        let email = normalize_email(email);
        if email.is_empty() {
            bail!("admin email is required");
        }

        if let Some(tenant_id) = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM server_administrators
            WHERE lower(email) = lower($1)
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&email)
        .fetch_optional(&self.pool)
        .await?
        {
            return Ok(tenant_id);
        }

        if let Some(tenant_id) = sqlx::query_scalar::<_, String>(
            r#"
            SELECT tenant_id
            FROM admin_credentials
            WHERE lower(email) = lower($1)
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&email)
        .fetch_optional(&self.pool)
        .await?
        {
            return Ok(tenant_id);
        }

        Ok(PLATFORM_TENANT_ID.to_string())
    }
}

fn normalize_email(value: &str) -> String {
    value.trim().to_lowercase()
}

fn normalize_admin_session_auth_method(value: &str) -> &'static str {
    match value.trim().to_ascii_lowercase().as_str() {
        "oidc" => "oidc",
        // The persisted admin session tracks the broad login family so
        // password+totp continues to work against the 0.1.3 schema.
        _ => "password",
    }
}

fn normalize_subject(value: &str) -> String {
    value.trim().to_string()
}

fn normalize_task_status(value: &str) -> Result<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "needs-action" => Ok("needs-action"),
        "in-progress" => Ok("in-progress"),
        "completed" => Ok("completed"),
        "cancelled" => Ok("cancelled"),
        other => bail!("unsupported task status: {other}"),
    }
}

fn normalize_task_list_name(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("task list name is required");
    }
    Ok(trimmed.to_string())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn domain_from_email(email: &str) -> Result<String> {
    email
        .split_once('@')
        .map(|(_, domain)| domain.trim().to_lowercase())
        .filter(|domain| !domain.is_empty())
        .ok_or_else(|| anyhow!("account email does not contain a domain"))
}

fn extract_supported_attachment_text(
    media_type: &str,
    file_name: &str,
    blob_bytes: &[u8],
) -> Result<Option<String>> {
    match extract_text_from_bytes(blob_bytes, Some(media_type), Some(file_name)) {
        Ok(text) => Ok(Some(text)),
        Err(error) => {
            let message = error.to_string();
            if message.contains("unsupported validated attachment format")
                || message.contains("blocked extraction")
            {
                Ok(None)
            } else {
                Err(error)
            }
        }
    }
}

fn preview_text(body_text: &str) -> String {
    let preview = body_text
        .split_whitespace()
        .take(28)
        .collect::<Vec<_>>()
        .join(" ");

    if preview.is_empty() {
        "(no preview)".to_string()
    } else {
        preview
    }
}

fn body_paragraphs(body_text: &str) -> Vec<String> {
    let paragraphs = body_text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    if paragraphs.is_empty() {
        vec!["".to_string()]
    } else {
        paragraphs
    }
}

fn client_folder(role: &str) -> String {
    match role {
        "drafts" => "drafts",
        "sent" => "sent",
        "archive" => "archive",
        _ => "inbox",
    }
    .to_string()
}

fn permissions_from_storage(
    role: &str,
    rights_summary: Option<&str>,
    permissions_json: Option<&str>,
) -> Vec<String> {
    let explicit = permissions_json
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .unwrap_or_default();
    normalize_admin_permissions(role, rights_summary.unwrap_or_default(), &explicit)
}

fn normalize_admin_permissions(
    role: &str,
    rights_summary: &str,
    explicit: &[String],
) -> Vec<String> {
    let mut permissions = default_permissions_for_role(role);
    permissions.extend(split_permissions(rights_summary));
    permissions.extend(
        explicit
            .iter()
            .map(|permission| permission.trim().to_lowercase())
            .filter(|permission| !permission.is_empty()),
    );
    if !permissions.is_empty() {
        permissions.push("dashboard".to_string());
    }
    permissions.sort();
    permissions.dedup();
    permissions
}

fn permission_summary(permissions: &[String]) -> String {
    permissions.join(", ")
}

fn split_permissions(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|entry| entry.trim().to_lowercase())
        .filter(|entry| !entry.is_empty())
        .collect()
}

fn default_permissions_for_role(role: &str) -> Vec<String> {
    match role.trim().to_lowercase().as_str() {
        "server-admin" | "super-admin" => vec!["*".to_string()],
        "tenant-admin" => vec![
            "dashboard",
            "domains",
            "accounts",
            "aliases",
            "admins",
            "policies",
            "security",
            "ai",
            "antispam",
            "pst",
            "audit",
            "mail",
            "operations",
            "protocols",
        ]
        .into_iter()
        .map(ToString::to_string)
        .collect(),
        "domain-admin" => vec![
            "dashboard",
            "domains",
            "accounts",
            "aliases",
            "admins",
            "mail",
            "pst",
        ]
        .into_iter()
        .map(ToString::to_string)
        .collect(),
        "compliance-admin" => vec!["dashboard", "audit", "policies"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        "helpdesk" | "support" => vec!["dashboard", "accounts", "mail"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        "transport-operator" => vec!["dashboard", "antispam", "operations", "protocols"]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

fn client_message_tags(role: &str, delivery_status: &str) -> Vec<String> {
    if role == "drafts" || delivery_status == "draft" {
        return vec!["Draft".to_string()];
    }
    if role == "sent" {
        return vec!["Outgoing".to_string()];
    }
    Vec::new()
}

fn attachment_kind(media_type: &str, name: &str) -> String {
    let lower_media = media_type.to_lowercase();
    let lower_name = name.to_lowercase();
    if lower_media.contains("pdf") || lower_name.ends_with(".pdf") {
        "PDF".to_string()
    } else if lower_media.contains("word")
        || lower_name.ends_with(".docx")
        || lower_name.ends_with(".doc")
    {
        "DOCX".to_string()
    } else if lower_media.contains("opendocument") || lower_name.ends_with(".odt") {
        "ODT".to_string()
    } else {
        attachment_extension_label(name)
            .or_else(|| media_type_label(&lower_media))
            .unwrap_or_else(|| "FILE".to_string())
    }
}

fn attachment_extension_label(name: &str) -> Option<String> {
    let extension = name
        .rsplit_once('.')
        .map(|(_, extension)| extension.trim())
        .filter(|extension| !extension.is_empty())?;
    let normalized = extension
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>()
        .to_uppercase();
    if normalized.is_empty() || normalized.len() > 8 {
        return None;
    }
    Some(normalized)
}

fn media_type_label(media_type: &str) -> Option<String> {
    let subtype = media_type
        .split('/')
        .nth(1)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let normalized = subtype
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>()
        .to_uppercase();
    if normalized.is_empty() || normalized.len() > 8 {
        return None;
    }
    Some(normalized)
}

fn format_size(size_octets: i64) -> String {
    let size = size_octets.max(0) as f64;
    if size >= 1_048_576.0 {
        format!("{:.1} MB", size / 1_048_576.0)
    } else if size >= 1024.0 {
        format!("{:.0} KB", size / 1024.0)
    } else {
        format!("{} B", size as i64)
    }
}

fn parse_activesync_file_reference(value: &str) -> Option<(Uuid, Uuid)> {
    let mut parts = value.trim().split(':');
    if parts.next()? != "attachment" {
        return None;
    }
    let message_id = Uuid::parse_str(parts.next()?).ok()?;
    let attachment_id = Uuid::parse_str(parts.next()?).ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((message_id, attachment_id))
}

fn map_event(row: ClientEventRow) -> ClientEvent {
    ClientEvent {
        id: row.id,
        date: row.date,
        time: row.time,
        time_zone: row.time_zone,
        duration_minutes: row.duration_minutes,
        recurrence_rule: row.recurrence_rule,
        title: row.title,
        location: row.location,
        attendees: row.attendees,
        attendees_json: row.attendees_json,
        notes: row.notes,
    }
}

fn map_contact(row: ClientContactRow) -> ClientContact {
    ClientContact {
        id: row.id,
        name: row.name,
        role: row.role,
        email: row.email,
        phone: row.phone,
        team: row.team,
        notes: row.notes,
    }
}

fn map_task_list(row: ClientTaskListRow) -> ClientTaskList {
    ClientTaskList {
        id: row.id,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        is_owned: row.is_owned,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        name: row.name,
        role: row.role,
        sort_order: row.sort_order,
        updated_at: row.updated_at,
    }
}

fn map_task_list_grant(row: TaskListGrantRow) -> TaskListGrant {
    TaskListGrant {
        id: row.id,
        task_list_id: row.task_list_id,
        task_list_name: row.task_list_name,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        grantee_account_id: row.grantee_account_id,
        grantee_email: row.grantee_email,
        grantee_display_name: row.grantee_display_name,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn map_collaboration_grant(row: CollaborationGrantRow) -> CollaborationGrant {
    CollaborationGrant {
        id: row.id,
        kind: row.kind,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        grantee_account_id: row.grantee_account_id,
        grantee_email: row.grantee_email,
        grantee_display_name: row.grantee_display_name,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn map_mailbox_delegation_grant(row: MailboxDelegationGrantRow) -> MailboxDelegationGrant {
    MailboxDelegationGrant {
        id: row.id,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        grantee_account_id: row.grantee_account_id,
        grantee_email: row.grantee_email,
        grantee_display_name: row.grantee_display_name,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn map_sender_delegation_grant(row: SenderDelegationGrantRow) -> SenderDelegationGrant {
    SenderDelegationGrant {
        id: row.id,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        grantee_account_id: row.grantee_account_id,
        grantee_email: row.grantee_email,
        grantee_display_name: row.grantee_display_name,
        sender_right: row.sender_right,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn map_task(row: ClientTaskRow) -> ClientTask {
    ClientTask {
        id: row.id,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        is_owned: row.is_owned,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        task_list_id: row.task_list_id,
        task_list_sort_order: row.task_list_sort_order,
        title: row.title,
        description: row.description,
        status: row.status,
        due_at: row.due_at,
        completed_at: row.completed_at,
        sort_order: row.sort_order,
        updated_at: row.updated_at,
    }
}

fn map_dav_task(row: DavTaskRow) -> DavTask {
    DavTask {
        id: row.id,
        collection_id: row.task_list_id.to_string(),
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        task_list_id: row.task_list_id,
        task_list_name: row.task_list_name,
        title: row.title,
        description: row.description,
        status: row.status,
        due_at: row.due_at,
        completed_at: row.completed_at,
        sort_order: row.sort_order,
        updated_at: row.updated_at,
    }
}

fn normalize_visible_recipients(
    input: &SubmitMessageInput,
) -> Vec<(&'static str, SubmittedRecipientInput)> {
    let mut recipients = Vec::new();
    push_recipients(&mut recipients, "to", &input.to);
    push_recipients(&mut recipients, "cc", &input.cc);
    recipients
}

fn normalize_bcc_recipients(input: &SubmitMessageInput) -> Vec<SubmittedRecipientInput> {
    let mut recipients = Vec::new();
    push_bcc_recipients(&mut recipients, &input.bcc);
    recipients
}

fn push_recipients(
    output: &mut Vec<(&'static str, SubmittedRecipientInput)>,
    kind: &'static str,
    input: &[SubmittedRecipientInput],
) {
    for recipient in input {
        let address = normalize_email(&recipient.address);
        if address.is_empty() {
            continue;
        }

        output.push((
            kind,
            SubmittedRecipientInput {
                address,
                display_name: recipient
                    .display_name
                    .as_ref()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty()),
            },
        ));
    }
}

fn push_bcc_recipients(
    output: &mut Vec<SubmittedRecipientInput>,
    input: &[SubmittedRecipientInput],
) {
    for recipient in input {
        let address = normalize_email(&recipient.address);
        if address.is_empty() {
            continue;
        }

        output.push(SubmittedRecipientInput {
            address,
            display_name: recipient
                .display_name
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
        });
    }
}

fn participants_normalized(
    from_address: &str,
    recipients: &[(&'static str, SubmittedRecipientInput)],
) -> String {
    let mut participants = Vec::with_capacity(recipients.len() + 1);
    participants.push(from_address.to_string());
    participants.extend(
        recipients
            .iter()
            .map(|(_, recipient)| recipient.address.clone()),
    );
    participants.join(" ")
}

fn trim_optional_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn normalize_gal_visibility(value: &str) -> Result<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "tenant" | "" => Ok("tenant".to_string()),
        "hidden" => Ok("hidden".to_string()),
        other => bail!("unsupported GAL visibility: {other}"),
    }
}

fn normalize_directory_kind(value: &str) -> Result<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "person" | "" => Ok("person".to_string()),
        "room" => Ok("room".to_string()),
        "equipment" => Ok("equipment".to_string()),
        other => bail!("unsupported directory kind: {other}"),
    }
}

fn sender_authorization_kind_from_str(value: &str) -> SenderAuthorizationKind {
    match value.trim() {
        "send-as" => SenderAuthorizationKind::SendAs,
        "send-on-behalf" => SenderAuthorizationKind::SendOnBehalf,
        _ => SenderAuthorizationKind::SelfSend,
    }
}

fn sender_identity_id(kind: SenderAuthorizationKind, owner_account_id: Uuid) -> String {
    format!("{}:{}", kind.as_str(), owner_account_id)
}

fn validate_collaboration_rights(
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
) -> Result<()> {
    if !may_read && (may_write || may_delete || may_share) {
        bail!("read access is required when granting write, delete, or share");
    }
    if may_delete && !may_write {
        bail!("delete access requires write access");
    }
    if may_share && !may_write {
        bail!("share access requires write access");
    }
    Ok(())
}

fn collection_id_for_owner(
    kind: CollaborationResourceKind,
    principal_account_id: Uuid,
    owner_account_id: Uuid,
) -> String {
    if principal_account_id == owner_account_id {
        DEFAULT_COLLECTION_ID.to_string()
    } else {
        shared_collection_id(kind, owner_account_id)
    }
}

fn shared_collection_id(kind: CollaborationResourceKind, owner_account_id: Uuid) -> String {
    format!("shared-{}-{}", kind.as_str(), owner_account_id)
}

fn shared_collection_display_name(
    kind: CollaborationResourceKind,
    owner_display_name: &str,
    owner_email: &str,
) -> String {
    let owner_label = if owner_display_name.trim().is_empty() {
        owner_email.trim()
    } else {
        owner_display_name.trim()
    };
    format!("{owner_label} {}", kind.collection_label())
}

#[derive(Debug, Clone)]
struct SieveFollowUp {
    account_id: Uuid,
    account_email: String,
    account_display_name: String,
    redirects: Vec<String>,
    vacation: Option<VacationAction>,
    subject: String,
    body_text: String,
    attachments: Vec<AttachmentUploadInput>,
    sender_address: String,
}

fn validate_sieve_script_name(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("sieve script name is required");
    }
    if trimmed.len() > 128 {
        bail!("sieve script name is too long");
    }
    if trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains('\0') {
        bail!("sieve script name contains unsupported characters");
    }
    Ok(trimmed.to_string())
}

fn validate_sieve_script_content(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("sieve script content is required");
    }
    if trimmed.len() > MAX_SIEVE_SCRIPT_BYTES {
        bail!("sieve script exceeds the MVP size limit");
    }
    Ok(trimmed.to_string())
}

fn estimate_generated_message_size(
    subject: &str,
    body_text: &str,
    attachments: &[AttachmentUploadInput],
) -> i64 {
    let attachments_size = attachments
        .iter()
        .map(|attachment| attachment.blob_bytes.len() as i64)
        .sum::<i64>();
    (subject.len() as i64) + (body_text.len() as i64) + attachments_size
}

fn hash_sieve_vacation_key(vacation: &VacationAction) -> String {
    let mut hasher = Sha256::new();
    hasher.update(vacation.subject.as_deref().unwrap_or_default().as_bytes());
    hasher.update(b"\n");
    hasher.update(vacation.reason.as_bytes());
    hasher.update(b"\n");
    hasher.update(vacation.days.to_string().as_bytes());
    format!("{:x}", hasher.finalize())
}

fn env_hostname(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().trim_matches('_').to_string())
        .filter(|value| !value.is_empty())
}

fn env_bind_address(name: &str, fallback: &str) -> String {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        attachment_kind, default_permissions_for_role, domain_from_email,
        normalize_admin_permissions, normalize_admin_session_auth_method, normalize_bcc_recipients,
        normalize_task_status, normalize_visible_recipients, participants_normalized,
        validate_pst_import_path, SubmitMessageInput, SubmittedRecipientInput,
    };
    use lpe_magika::{
        write_validation_record, ExpectedKind, IngressContext, PolicyDecision, ValidationOutcome,
        ValidationRequest,
    };
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };
    use uuid::Uuid;

    fn submit_input() -> SubmitMessageInput {
        SubmitMessageInput {
            draft_message_id: None,
            account_id: Uuid::nil(),
            submitted_by_account_id: Uuid::nil(),
            source: "test".to_string(),
            from_display: None,
            from_address: "sender@example.test".to_string(),
            sender_display: None,
            sender_address: None,
            to: vec![SubmittedRecipientInput {
                address: "to@example.test".to_string(),
                display_name: None,
            }],
            cc: vec![SubmittedRecipientInput {
                address: "cc@example.test".to_string(),
                display_name: Some("  CC Person  ".to_string()),
            }],
            bcc: vec![SubmittedRecipientInput {
                address: "bcc@example.test".to_string(),
                display_name: Some("  Hidden Person  ".to_string()),
            }],
            subject: "subject".to_string(),
            body_text: "body".to_string(),
            body_html_sanitized: None,
            internet_message_id: None,
            mime_blob_ref: None,
            size_octets: 0,
            unread: None,
            flagged: None,
            attachments: Vec::new(),
        }
    }

    #[test]
    fn visible_recipients_exclude_bcc() {
        let recipients = normalize_visible_recipients(&submit_input());

        assert_eq!(recipients.len(), 2);
        assert_eq!(recipients[0].0, "to");
        assert_eq!(recipients[0].1.address, "to@example.test");
        assert_eq!(recipients[1].0, "cc");
        assert_eq!(recipients[1].1.address, "cc@example.test");
        assert_eq!(recipients[1].1.display_name.as_deref(), Some("CC Person"));
    }

    #[test]
    fn bcc_recipients_are_kept_separately() {
        let recipients = normalize_bcc_recipients(&submit_input());

        assert_eq!(recipients.len(), 1);
        assert_eq!(recipients[0].address, "bcc@example.test");
        assert_eq!(recipients[0].display_name.as_deref(), Some("Hidden Person"));
    }

    #[test]
    fn participants_normalized_ignores_bcc_addresses() {
        let visible = normalize_visible_recipients(&submit_input());
        let participants = participants_normalized("sender@example.test", &visible);

        assert!(participants.contains("sender@example.test"));
        assert!(participants.contains("to@example.test"));
        assert!(participants.contains("cc@example.test"));
        assert!(!participants.contains("bcc@example.test"));
    }

    #[test]
    fn pst_processing_requires_prior_validation_record() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("lpe-pst-validation-{suffix}"));
        fs::create_dir_all(&dir).unwrap();
        let pst_path = dir.join("mailbox.pst");
        fs::write(&pst_path, b"LPE-PST-V1\n").unwrap();

        assert!(validate_pst_import_path(&pst_path).is_err());

        let outcome = ValidationOutcome {
            detected_label: "pst".to_string(),
            detected_mime: "application/vnd.ms-outlook".to_string(),
            description: "pst".to_string(),
            group: "archive".to_string(),
            extensions: vec!["pst".to_string()],
            score: Some(0.99),
            declared_mime: Some("application/vnd.ms-outlook".to_string()),
            filename: Some("mailbox.pst".to_string()),
            mismatch: false,
            policy_decision: PolicyDecision::Accept,
            reason: "file validated".to_string(),
        };
        write_validation_record(
            &pst_path,
            &ValidationRequest {
                ingress_context: IngressContext::PstUpload,
                declared_mime: Some("application/vnd.ms-outlook".to_string()),
                filename: Some("mailbox.pst".to_string()),
                expected_kind: ExpectedKind::Pst,
            },
            &outcome,
            fs::metadata(&pst_path).unwrap().len(),
        )
        .unwrap();

        std::env::set_var("LPE_MAGIKA_BIN", "missing-magika-binary-for-test");
        let result = validate_pst_import_path(&pst_path);
        std::env::remove_var("LPE_MAGIKA_BIN");
        assert!(result.is_err());
    }

    #[test]
    fn domain_dedup_scope_comes_from_account_email_domain() {
        assert_eq!(
            domain_from_email("Alice@Example.Test").unwrap(),
            "example.test"
        );
    }

    #[test]
    fn task_status_defaults_to_needs_action() {
        assert_eq!(normalize_task_status("").unwrap(), "needs-action");
    }

    #[test]
    fn task_status_accepts_vtodo_aligned_values() {
        assert_eq!(
            normalize_task_status("needs-action").unwrap(),
            "needs-action"
        );
        assert_eq!(normalize_task_status("in-progress").unwrap(), "in-progress");
        assert_eq!(normalize_task_status("completed").unwrap(), "completed");
        assert_eq!(normalize_task_status("cancelled").unwrap(), "cancelled");
    }

    #[test]
    fn task_status_rejects_unknown_values() {
        assert!(normalize_task_status("done").is_err());
    }

    #[test]
    fn attachment_kind_falls_back_to_real_extension_label() {
        assert_eq!(
            attachment_kind("application/octet-stream", "archive.zip"),
            "ZIP"
        );
        assert_eq!(attachment_kind("application/octet-stream", "blob"), "FILE");
    }

    #[test]
    fn built_in_role_permissions_include_dashboard() {
        let permissions = default_permissions_for_role("tenant-admin");

        assert!(permissions
            .iter()
            .any(|permission| permission == "dashboard"));
        assert!(permissions.iter().any(|permission| permission == "domains"));
        assert!(!permissions.iter().any(|permission| permission == "*"));
    }

    #[test]
    fn explicit_permissions_are_normalized_and_deduplicated() {
        let permissions = normalize_admin_permissions(
            "custom",
            "mail, dashboard, mail",
            &[
                " dashboard ".to_string(),
                "audit".to_string(),
                String::new(),
                "mail".to_string(),
            ],
        );

        assert_eq!(permissions, vec!["audit", "dashboard", "mail"]);
    }

    #[test]
    fn admin_session_auth_method_collapses_totp_to_password_family() {
        assert_eq!(normalize_admin_session_auth_method("password"), "password");
        assert_eq!(
            normalize_admin_session_auth_method("password+totp"),
            "password"
        );
        assert_eq!(normalize_admin_session_auth_method("oidc"), "oidc");
    }
}

fn ensure_parent_directory(path: &str) -> Result<()> {
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

fn validate_pst_import_path(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path)?;
    let record = read_validation_record(path).with_context(|| {
        format!(
            "missing or unreadable PST validation record for {}",
            path.display()
        )
    })?;
    if record.ingress_context != IngressContext::PstUpload {
        bail!("PST validation record has an unexpected ingress context");
    }
    if record.expected_kind != ExpectedKind::Pst {
        bail!("PST validation record does not describe a PST upload");
    }
    if record.policy_decision != PolicyDecision::Accept {
        bail!(
            "PST validation record is not accepted: {}",
            record.outcome.reason
        );
    }
    if record.file_size != metadata.len() {
        bail!("PST validation record does not match the current file size");
    }

    let outcome = Validator::from_env().validate_path(
        ValidationRequest {
            ingress_context: IngressContext::PstProcessing,
            declared_mime: record.outcome.declared_mime.clone(),
            filename: path
                .file_name()
                .and_then(|value| value.to_str())
                .map(ToString::to_string),
            expected_kind: ExpectedKind::Pst,
        },
        path,
    )?;
    if outcome.policy_decision != PolicyDecision::Accept {
        bail!(
            "PST processing blocked by Magika validation: {}",
            outcome.reason
        );
    }

    Ok(())
}

fn encode_pst_field(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
        .replace('\n', "\\n")
}

fn decode_pst_field(value: &str) -> String {
    let mut output = String::new();
    let mut chars = value.chars();
    while let Some(char) = chars.next() {
        if char != '\\' {
            output.push(char);
            continue;
        }

        match chars.next() {
            Some('t') => output.push('\t'),
            Some('r') => output.push('\r'),
            Some('n') => output.push('\n'),
            Some('\\') => output.push('\\'),
            Some(other) => {
                output.push('\\');
                output.push(other);
            }
            None => output.push('\\'),
        }
    }
    output
}
