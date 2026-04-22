use axum::{http::StatusCode, Json};
use lpe_storage::{
    AccountAppPassword, AccountAuthFactor, AdminAuthFactor, AuthenticatedAccount,
    AuthenticatedAdmin, CollaborationCollection, CollaborationGrant, MailFlowEntry,
    MailboxDelegationOverview, SieveScriptDocument, SieveScriptSummary, TaskListGrant,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type ApiResult<T> = std::result::Result<Json<T>, (StatusCode, String)>;

#[derive(Debug, Clone)]
pub struct BootstrapAdminRequest {
    pub email: String,
    pub display_name: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BootstrapAdminResponse {
    pub email: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LocalAiHealthResponse {
    pub provider: String,
    pub models: Vec<String>,
    pub bootstrap_summary_payload: String,
    pub enabled: bool,
    pub offline_only: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttachmentSupportResponse {
    pub formats: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReadinessCheck {
    pub name: String,
    pub status: String,
    pub critical: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReadinessResponse {
    pub service: String,
    pub status: String,
    pub warnings: u32,
    pub checks: Vec<ReadinessCheck>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub admin: AuthenticatedAdmin,
}

#[derive(Debug, Clone, Serialize)]
pub struct OidcMetadataResponse {
    pub enabled: bool,
    pub provider_label: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OidcStartResponse {
    pub authorization_url: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientLoginResponse {
    pub token: String,
    pub account: AuthenticatedAccount,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientOidcMetadataResponse {
    pub enabled: bool,
    pub provider_label: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientOidcStartResponse {
    pub authorization_url: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientOauthAccessTokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u32,
    pub scope: String,
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
    pub totp_code: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminAuthFactorsResponse {
    pub factors: Vec<AdminAuthFactor>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollTotpRequest {
    pub label: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollTotpResponse {
    pub factor_id: Uuid,
    pub secret: String,
    pub otpauth_url: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifyTotpRequest {
    pub factor_id: Uuid,
    pub code: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountAuthFactorsResponse {
    pub factors: Vec<AccountAuthFactor>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountAppPasswordsResponse {
    pub app_passwords: Vec<AccountAppPassword>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateAccountAppPasswordRequest {
    pub label: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateClientOauthAccessTokenRequest {
    pub scope: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateAccountAppPasswordResponse {
    pub id: Uuid,
    pub label: String,
    pub secret: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateAccountRequest {
    pub email: String,
    pub display_name: String,
    pub quota_mb: u32,
    pub password: String,
    #[serde(default)]
    pub gal_visibility: Option<String>,
    #[serde(default)]
    pub directory_kind: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateAccountRequest {
    pub display_name: String,
    pub quota_mb: u32,
    pub status: String,
    pub password: Option<String>,
    #[serde(default)]
    pub gal_visibility: Option<String>,
    #[serde(default)]
    pub directory_kind: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateMailboxRequest {
    pub account_id: Uuid,
    pub display_name: String,
    pub role: String,
    pub retention_days: u16,
}

#[derive(Debug, Deserialize)]
pub struct CreatePstTransferJobRequest {
    pub mailbox_id: Uuid,
    pub direction: String,
    pub server_path: String,
    pub requested_by: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateDomainRequest {
    pub name: String,
    pub default_quota_mb: u32,
    pub inbound_enabled: bool,
    pub outbound_enabled: bool,
    #[serde(default)]
    pub default_sieve_script: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateDomainRequest {
    pub default_quota_mb: u32,
    pub inbound_enabled: bool,
    pub outbound_enabled: bool,
    #[serde(default)]
    pub default_sieve_script: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateAliasRequest {
    pub source: String,
    pub target: String,
    pub kind: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateServerSettingsRequest {
    pub primary_hostname: String,
    pub admin_bind_address: String,
    pub smtp_bind_address: String,
    pub imap_bind_address: String,
    pub jmap_bind_address: String,
    pub default_locale: String,
    pub max_message_size_mb: u32,
    pub tls_mode: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateSecuritySettingsRequest {
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
    #[serde(default)]
    pub mailbox_password_login_enabled: Option<bool>,
    #[serde(default)]
    pub mailbox_oidc_login_enabled: Option<bool>,
    #[serde(default)]
    pub mailbox_oidc_provider_label: Option<String>,
    #[serde(default)]
    pub mailbox_oidc_auto_link_by_email: Option<bool>,
    #[serde(default)]
    pub mailbox_oidc_issuer_url: Option<String>,
    #[serde(default)]
    pub mailbox_oidc_authorization_endpoint: Option<String>,
    #[serde(default)]
    pub mailbox_oidc_token_endpoint: Option<String>,
    #[serde(default)]
    pub mailbox_oidc_userinfo_endpoint: Option<String>,
    #[serde(default)]
    pub mailbox_oidc_client_id: Option<String>,
    #[serde(default)]
    pub mailbox_oidc_client_secret: Option<String>,
    #[serde(default)]
    pub mailbox_oidc_scopes: Option<String>,
    #[serde(default)]
    pub mailbox_oidc_claim_email: Option<String>,
    #[serde(default)]
    pub mailbox_oidc_claim_display_name: Option<String>,
    #[serde(default)]
    pub mailbox_oidc_claim_subject: Option<String>,
    #[serde(default)]
    pub mailbox_app_passwords_enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateLocalAiSettingsRequest {
    pub enabled: bool,
    pub provider: String,
    pub model: String,
    pub offline_only: bool,
    pub indexing_enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct UpdateAntispamSettingsRequest {
    pub content_filtering_enabled: bool,
    pub spam_engine: String,
    pub quarantine_enabled: bool,
    pub quarantine_retention_days: u32,
}

#[derive(Debug, Deserialize)]
pub struct CreateServerAdministratorRequest {
    pub domain_id: Option<Uuid>,
    pub email: String,
    pub display_name: String,
    pub role: String,
    pub rights_summary: String,
    #[serde(default)]
    pub permissions: Vec<String>,
    pub password: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateFilterRuleRequest {
    pub name: String,
    pub scope: String,
    pub action: String,
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct EmailTraceSearchRequest {
    pub query: String,
}

#[derive(Debug, Deserialize)]
pub struct SubmitMessageRequest {
    pub draft_message_id: Option<Uuid>,
    pub account_id: Uuid,
    pub source: Option<String>,
    pub from_display: Option<String>,
    pub from_address: String,
    pub sender_display: Option<String>,
    pub sender_address: Option<String>,
    pub to: Vec<SubmitRecipientRequest>,
    pub cc: Option<Vec<SubmitRecipientRequest>>,
    pub bcc: Option<Vec<SubmitRecipientRequest>>,
    pub subject: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub internet_message_id: Option<String>,
    pub mime_blob_ref: Option<String>,
    pub size_octets: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SubmitRecipientRequest {
    pub address: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpsertClientContactRequest {
    pub id: Option<Uuid>,
    #[serde(rename = "collectionId")]
    pub collection_id: Option<String>,
    pub name: String,
    pub role: String,
    pub email: String,
    pub phone: String,
    pub team: String,
    pub notes: String,
}

#[derive(Debug, Deserialize)]
pub struct UpsertClientEventRequest {
    pub id: Option<Uuid>,
    #[serde(rename = "collectionId")]
    pub collection_id: Option<String>,
    pub date: String,
    pub time: String,
    #[serde(default)]
    pub time_zone: String,
    #[serde(default)]
    pub duration_minutes: i32,
    #[serde(default)]
    pub recurrence_rule: String,
    pub title: String,
    pub location: String,
    pub attendees: String,
    #[serde(default)]
    pub attendees_json: String,
    pub notes: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertCollaborationGrantRequest {
    pub kind: String,
    pub grantee_email: String,
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaborationOverviewResponse {
    pub outgoing_contacts: Vec<CollaborationGrant>,
    pub outgoing_calendars: Vec<CollaborationGrant>,
    pub outgoing_task_lists: Vec<TaskListGrant>,
    pub incoming_contact_collections: Vec<CollaborationCollection>,
    pub incoming_calendar_collections: Vec<CollaborationCollection>,
    pub incoming_task_list_collections: Vec<CollaborationCollection>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertMailboxDelegationGrantRequest {
    pub grantee_email: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertSenderDelegationGrantRequest {
    pub grantee_email: String,
    pub sender_right: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MailFlowResponse {
    pub items: Vec<MailFlowEntry>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SieveOverviewResponse {
    pub scripts: Vec<SieveScriptSummary>,
    pub active_script: Option<SieveScriptDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertSieveScriptRequest {
    pub name: String,
    pub content: String,
    #[serde(default)]
    pub activate: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenameSieveScriptRequest {
    pub old_name: String,
    pub new_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetActiveSieveScriptRequest {
    pub name: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxDelegationResponse {
    pub overview: MailboxDelegationOverview,
}

#[derive(Debug, Deserialize)]
pub struct UpsertClientTaskRequest {
    pub id: Option<Uuid>,
    pub task_list_id: Option<Uuid>,
    pub title: String,
    pub description: String,
    pub status: String,
    pub due_at: Option<String>,
    pub completed_at: Option<String>,
    pub sort_order: Option<i32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertTaskListGrantRequest {
    pub grantee_email: String,
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}
