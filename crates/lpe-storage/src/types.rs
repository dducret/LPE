use crate::pst::PstTransferJobRecord;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

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
#[serde(rename_all = "camelCase")]
pub struct MailFlowEntry {
    pub queue_id: Uuid,
    pub message_id: Uuid,
    pub account_email: String,
    pub subject: String,
    pub status: String,
    pub delivery_status: String,
    pub attempts: u32,
    pub submitted_at: String,
    pub last_attempt_at: Option<String>,
    pub next_attempt_at: Option<String>,
    pub trace_id: Option<String>,
    pub remote_message_ref: Option<String>,
    pub last_error: Option<String>,
    pub retry_after_seconds: Option<i32>,
    pub retry_policy: Option<String>,
    pub last_dsn_status: Option<String>,
    pub last_smtp_code: Option<i32>,
    pub last_enhanced_status: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutboundQueueStatusUpdate {
    pub queue_id: Uuid,
    pub message_id: Uuid,
    pub status: String,
    pub trace_id: Option<String>,
    pub remote_message_ref: Option<String>,
    pub retry_after_seconds: Option<i32>,
    pub retry_policy: Option<String>,
    pub technical_status: Value,
}
