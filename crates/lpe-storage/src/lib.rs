use anyhow::{anyhow, bail, Context, Result};
use lpe_domain::{
    OutboundMessageHandoffRequest,
    OutboundMessageHandoffResponse, TransportDeliveryStatus, TransportRecipient,
};
use serde::Serialize;
use serde_json::Value;
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;

pub mod mail;
pub mod admin;
pub mod attachments;
pub mod calendar;
pub mod change;
pub mod collaboration;
pub mod auth;
pub mod inbound;
pub mod models;
pub mod protocols;
pub mod pst;
pub mod submission;
pub mod tasks;
pub mod util;
pub mod workspace;

pub use crate::calendar::{
    calendar_attendee_labels, calendar_participant_label, normalize_calendar_email,
    normalize_calendar_participation_status, parse_calendar_participants_metadata,
    serialize_calendar_participants_metadata, CalendarOrganizerMetadata,
    CalendarParticipantMetadata, CalendarParticipantsMetadata,
};
pub use crate::auth::{
    AccountAppPassword, AccountAuthFactor, AccountCredentialInput, AccountLogin,
    AccountOidcClaims, AdminAuthFactor, AdminCredentialInput, AdminLogin,
    AdminOidcClaims, AuthenticatedAccount, AuthenticatedAdmin, NewAccountAuthFactor,
    NewAdminAuthFactor, StoredAccountAppPassword,
};
pub use crate::attachments::ClientAttachment;
pub use crate::change::{
    CanonicalChangeCategory, CanonicalChangeListener, CanonicalPushChangeSet,
};
pub use crate::collaboration::{
    AccessibleContact, AccessibleEvent, CollaborationCollection, CollaborationGrant,
    CollaborationGrantInput, CollaborationResourceKind, CollaborationRights,
};
pub use crate::protocols::{
    ActiveSyncAttachment, ActiveSyncAttachmentContent, ActiveSyncItemState,
    ActiveSyncSyncState, ImapEmail, JmapEmail, JmapEmailAddress, JmapEmailQuery,
    JmapEmailSubmission, JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, JmapQuota, JmapThreadQuery, JmapUploadBlob,
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
    ClientTask, ClientTaskList, CreateTaskListInput, DavTask, TaskListGrant,
    TaskListGrantInput, UpdateTaskListInput, UpsertClientTaskInput,
};
pub use crate::workspace::{
    ClientContact, ClientEvent, ClientMessage, ClientWorkspace, UpsertClientContactInput,
    UpsertClientEventInput,
};

pub(crate) use crate::models::*;
pub(crate) use crate::pst::PstTransferJobRow;
pub(crate) use crate::util::*;

const PLATFORM_TENANT_ID: &str = "__platform__";
const MAX_SIEVE_SCRIPT_BYTES: usize = 64 * 1024;
const MAX_SIEVE_SCRIPTS_PER_ACCOUNT: i64 = 16;
const DEFAULT_COLLECTION_ID: &str = "default";
const DEFAULT_TASK_LIST_NAME: &str = "Tasks";
const DEFAULT_TASK_LIST_ROLE: &str = "inbox";
const CANONICAL_CHANGE_CHANNEL: &str = "lpe_canonical_changes";
const EXPECTED_SCHEMA_VERSION: &str = "0.1.6";

#[derive(Clone)]
pub struct Storage {
    pool: Pool<Postgres>,
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
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                imap_modseq, received_at, sent_at, from_display, from_address, sender_display,
                sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                submission_source, delivery_status
            )
            SELECT
                $4, tenant_id, account_id, $5, thread_id, internet_message_id,
                $6, NOW(),
                CASE WHEN $7 = 'draft' THEN NULL ELSE sent_at END,
                from_display, from_address, sender_display,
                sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                submission_source, $7
            FROM messages
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(copied_message_id)
        .bind(target_mailbox_id)
        .bind(modseq)
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
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let moved = sqlx::query(
            r#"
            UPDATE messages
            SET
                mailbox_id = $4,
                imap_uid = nextval('message_imap_uid_seq'),
                imap_modseq = $5
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(target_mailbox_id)
        .bind(modseq)
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
        let participants = submission::participants_normalized(
            &normalize_email(&input.from_address),
            &recipients,
        );
        let delivery_status = if target_role == "drafts" {
            "draft"
        } else {
            "stored"
        };

        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, input.account_id)
            .await?;
        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                imap_modseq, received_at, sent_at, from_display, from_address, sender_display,
                sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                submission_source, delivery_status
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, NOW(), NULL, $8, $9, $10,
                $11, $12, $13, $14, $15, FALSE, FALSE, FALSE, $16, $17,
                $18, $19
            )
            "#,
        )
        .bind(message_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.mailbox_id)
        .bind(thread_id)
        .bind(input.internet_message_id)
        .bind(modseq)
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

    pub(crate) async fn allocate_mail_modseq_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
    ) -> Result<i64> {
        let modseq = sqlx::query_scalar::<_, i64>("SELECT nextval('message_modseq_seq')")
            .fetch_one(&mut **tx)
            .await?;

        let updated = sqlx::query(
            r#"
            UPDATE accounts
            SET mail_sync_modseq = GREATEST(mail_sync_modseq, $3)
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(modseq)
        .execute(&mut **tx)
        .await?;
        if updated.rows_affected() == 0 {
            bail!("account not found");
        }

        Ok(modseq)
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

    pub(crate) async fn tenant_id_for_domain_name(&self, domain_name: &str) -> Result<String> {
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

    pub(crate) async fn tenant_id_for_account_id(&self, account_id: Uuid) -> Result<String> {
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

    pub(crate) async fn tenant_id_for_account_email(&self, email: &str) -> Result<String> {
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

    pub(crate) async fn tenant_id_for_admin_email(&self, email: &str) -> Result<String> {
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

#[cfg(test)]
mod tests {
    use super::{
        default_permissions_for_role, domain_from_email, normalize_admin_permissions,
        normalize_admin_session_auth_method, normalize_task_status, SubmitMessageInput,
        SubmittedRecipientInput,
    };
    use super::attachments::attachment_kind;
    use super::pst::validate_pst_import_path;
    use super::submission::{
        normalize_bcc_recipients, normalize_visible_recipients, participants_normalized,
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

