use anyhow::{anyhow, bail, Result};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::util::system_mailbox_aliases;

pub mod admin;
pub mod attachments;
pub mod auth;
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
pub mod submission;
pub mod tasks;
pub mod types;
pub mod util;
pub mod workspace;

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
    ImapEmail, JmapEmail, JmapEmailAddress, JmapEmailQuery, JmapEmailSubmission,
    JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput, JmapMailboxUpdateInput, JmapQuota,
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
    NewFilterRule, NewMailbox, NewServerAdministrator, OutboundQueueStatusUpdate, OverviewStats,
    ProtocolStatus, QuarantineItem, SecuritySettings, ServerAdministrator, ServerSettings,
    SieveScriptDocument, SieveScriptSummary, StorageOverview, UpdateAccount, UpdateDomain,
};
pub use crate::workspace::{
    ClientContact, ClientEvent, ClientMessage, ClientWorkspace, UpsertClientContactInput,
    UpsertClientEventInput,
};

pub(crate) use crate::models::*;
pub(crate) use crate::pst::PstTransferJobRow;
pub(crate) use crate::tasks::{map_dav_task, map_task, map_task_list, map_task_list_grant};
pub(crate) use crate::util::*;

const PLATFORM_TENANT_ID: &str = "__platform__";
const MAX_SIEVE_SCRIPT_BYTES: usize = 64 * 1024;
const MAX_SIEVE_SCRIPTS_PER_ACCOUNT: i64 = 16;
const DEFAULT_COLLECTION_ID: &str = "default";
const DEFAULT_TASK_LIST_NAME: &str = "Tasks";
const DEFAULT_TASK_LIST_ROLE: &str = "inbox";
const CANONICAL_CHANGE_CHANNEL: &str = "lpe_canonical_changes";
const EXPECTED_SCHEMA_VERSION: &str = "0.1.9";

impl Storage {
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
        let aliases = system_mailbox_aliases(role, display_name);
        if !aliases.is_empty() {
            let rows = sqlx::query(
                r#"
                SELECT id, role
                FROM mailboxes
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND (
                    lower(btrim(role)) = $3
                    OR lower(btrim(display_name)) = ANY($4)
                  )
                ORDER BY
                    CASE
                        WHEN lower(btrim(role)) = $3
                         AND lower(btrim(display_name)) = lower(btrim($5)) THEN 0
                        WHEN lower(btrim(display_name)) = lower(btrim($5)) THEN 1
                        WHEN lower(btrim(role)) = $3 THEN 2
                        ELSE 3
                    END,
                    created_at ASC
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(role)
            .bind(&aliases)
            .bind(display_name)
            .fetch_all(&mut **tx)
            .await?;

            if let Some(canonical_row) = rows.first() {
                let canonical_id = canonical_row.try_get::<Uuid, _>("id")?;
                if canonical_row.try_get::<String, _>("role")?.trim() != role {
                    sqlx::query(
                        r#"
                        UPDATE mailboxes
                        SET role = $4,
                            sort_order = $5,
                            retention_days = $6
                        WHERE tenant_id = $1 AND account_id = $2 AND id = $3
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(account_id)
                    .bind(canonical_id)
                    .bind(role)
                    .bind(sort_order)
                    .bind(retention_days)
                    .execute(&mut **tx)
                    .await?;
                }

                let alias_ids = rows
                    .iter()
                    .skip(1)
                    .map(|row| row.try_get::<Uuid, _>("id"))
                    .collect::<std::result::Result<Vec<_>, sqlx::Error>>()?;
                if !alias_ids.is_empty() {
                    sqlx::query(
                        r#"
                        UPDATE mailbox_pst_jobs
                        SET mailbox_id = $4
                        WHERE tenant_id = $1
                          AND mailbox_id = ANY($2)
                          AND EXISTS (
                              SELECT 1
                              FROM mailboxes mb
                              WHERE mb.tenant_id = $1
                                AND mb.account_id = $3
                                AND mb.id = mailbox_pst_jobs.mailbox_id
                          )
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(&alias_ids)
                    .bind(account_id)
                    .bind(canonical_id)
                    .execute(&mut **tx)
                    .await?;

                    sqlx::query(
                        r#"
                        UPDATE messages
                        SET mailbox_id = $4,
                            imap_uid = nextval('message_imap_uid_seq'),
                            imap_modseq = nextval('message_modseq_seq')
                        WHERE tenant_id = $1
                          AND mailbox_id = ANY($2)
                          AND account_id = $3
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(&alias_ids)
                    .bind(account_id)
                    .bind(canonical_id)
                    .execute(&mut **tx)
                    .await?;

                    sqlx::query(
                        r#"
                        UPDATE accounts
                        SET mail_sync_modseq = GREATEST(
                            mail_sync_modseq,
                            COALESCE((
                                SELECT MAX(imap_modseq)
                                FROM messages
                                WHERE tenant_id = $1 AND account_id = $2
                            ), mail_sync_modseq)
                        )
                        WHERE tenant_id = $1 AND id = $2
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(account_id)
                    .execute(&mut **tx)
                    .await?;

                    sqlx::query(
                        r#"
                        DELETE FROM mailboxes mb
                        WHERE mb.tenant_id = $1
                          AND mb.account_id = $2
                          AND mb.id = ANY($3)
                          AND NOT EXISTS (
                              SELECT 1
                              FROM messages m
                              WHERE m.tenant_id = mb.tenant_id
                                AND m.mailbox_id = mb.id
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM mailbox_pst_jobs job
                              WHERE job.tenant_id = mb.tenant_id
                                AND job.mailbox_id = mb.id
                          )
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(account_id)
                    .bind(&alias_ids)
                    .execute(&mut **tx)
                    .await?;
                }

                return Ok(canonical_id);
            }
        } else if let Some(row) = sqlx::query(
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

#[cfg(test)]
mod tests {
    use super::attachments::attachment_kind;
    use super::pst::validate_pst_import_path;
    use super::submission::{
        normalize_bcc_recipients, normalize_visible_recipients, participants_normalized,
    };
    use super::{
        default_permissions_for_role, domain_from_email, normalize_admin_permissions,
        normalize_admin_session_auth_method, normalize_task_status, SubmitMessageInput,
        SubmittedRecipientInput,
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
    fn participants_normalized_remains_visible_only_even_with_bcc_display_name() {
        let input = submit_input();
        let visible = normalize_visible_recipients(&input);
        let participants = participants_normalized("sender@example.test", &visible);

        assert!(!participants.contains("Hidden Person"));
        assert!(!participants.contains("bcc@example.test"));
    }

    #[test]
    fn participants_normalized_allows_null_reverse_path() {
        let visible = normalize_visible_recipients(&submit_input());
        let participants = participants_normalized("", &visible);

        assert_eq!(participants, "to@example.test cc@example.test");
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
