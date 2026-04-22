use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow)]
pub(crate) struct AccountRow {
    pub(crate) id: Uuid,
    pub(crate) primary_email: String,
    pub(crate) display_name: String,
    pub(crate) quota_mb: i32,
    pub(crate) used_mb: i32,
    pub(crate) status: String,
    pub(crate) gal_visibility: String,
    pub(crate) directory_kind: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct MailboxRow {
    pub(crate) id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) display_name: String,
    pub(crate) role: String,
    pub(crate) message_count: i64,
    pub(crate) retention_days: i32,
}

#[derive(Debug, FromRow)]
pub(crate) struct DomainRow {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) status: String,
    pub(crate) inbound_enabled: bool,
    pub(crate) outbound_enabled: bool,
    pub(crate) default_quota_mb: i32,
    pub(crate) default_sieve_script: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct AliasRow {
    pub(crate) id: Uuid,
    pub(crate) source: String,
    pub(crate) target: String,
    pub(crate) kind: String,
    pub(crate) status: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct AuditRow {
    pub(crate) id: Uuid,
    pub(crate) timestamp: String,
    pub(crate) actor: String,
    pub(crate) action: String,
    pub(crate) subject: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct JmapMailboxRow {
    pub(crate) id: Uuid,
    pub(crate) role: String,
    pub(crate) display_name: String,
    pub(crate) sort_order: i32,
    pub(crate) total_emails: i64,
    pub(crate) unread_emails: i64,
}

#[derive(Debug, FromRow)]
pub(crate) struct JmapEmailRow {
    pub(crate) id: Uuid,
    pub(crate) thread_id: Uuid,
    pub(crate) mailbox_id: Uuid,
    pub(crate) mailbox_role: String,
    pub(crate) mailbox_name: String,
    pub(crate) received_at: String,
    pub(crate) sent_at: Option<String>,
    pub(crate) from_address: String,
    pub(crate) from_display: Option<String>,
    pub(crate) sender_address: Option<String>,
    pub(crate) sender_display: Option<String>,
    pub(crate) sender_authorization_kind: String,
    pub(crate) submitted_by_account_id: Uuid,
    pub(crate) subject: String,
    pub(crate) preview: String,
    pub(crate) body_text: String,
    pub(crate) body_html_sanitized: Option<String>,
    pub(crate) unread: bool,
    pub(crate) flagged: bool,
    pub(crate) has_attachments: bool,
    pub(crate) size_octets: i64,
    pub(crate) internet_message_id: Option<String>,
    pub(crate) mime_blob_ref: Option<String>,
    pub(crate) delivery_status: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct ImapEmailRow {
    pub(crate) id: Uuid,
    pub(crate) imap_uid: i64,
    pub(crate) imap_modseq: i64,
    pub(crate) thread_id: Uuid,
    pub(crate) mailbox_id: Uuid,
    pub(crate) mailbox_role: String,
    pub(crate) mailbox_name: String,
    pub(crate) received_at: String,
    pub(crate) sent_at: Option<String>,
    pub(crate) from_address: String,
    pub(crate) from_display: Option<String>,
    pub(crate) subject: String,
    pub(crate) preview: String,
    pub(crate) body_text: String,
    pub(crate) body_html_sanitized: Option<String>,
    pub(crate) unread: bool,
    pub(crate) flagged: bool,
    pub(crate) has_attachments: bool,
    pub(crate) size_octets: i64,
    pub(crate) internet_message_id: Option<String>,
    pub(crate) delivery_status: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct JmapEmailRecipientRow {
    pub(crate) message_id: Uuid,
    pub(crate) kind: String,
    pub(crate) address: String,
    pub(crate) display_name: Option<String>,
    pub(crate) _ordinal: i32,
}

#[derive(Debug, FromRow)]
pub(crate) struct JmapEmailSubmissionRow {
    pub(crate) id: Uuid,
    pub(crate) email_id: Uuid,
    pub(crate) thread_id: Uuid,
    pub(crate) from_address: String,
    pub(crate) sender_address: Option<String>,
    pub(crate) sender_authorization_kind: String,
    pub(crate) send_at: String,
    pub(crate) queue_status: String,
    pub(crate) delivery_status: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct MailboxAccountAccessRow {
    pub(crate) account_id: Uuid,
    pub(crate) email: String,
    pub(crate) display_name: String,
    pub(crate) may_send_as: bool,
    pub(crate) may_send_on_behalf: bool,
}

#[derive(Debug, FromRow)]
pub(crate) struct MailboxDelegationGrantRow {
    pub(crate) id: Uuid,
    pub(crate) owner_account_id: Uuid,
    pub(crate) owner_email: String,
    pub(crate) owner_display_name: String,
    pub(crate) grantee_account_id: Uuid,
    pub(crate) grantee_email: String,
    pub(crate) grantee_display_name: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct SenderDelegationGrantRow {
    pub(crate) id: Uuid,
    pub(crate) owner_account_id: Uuid,
    pub(crate) owner_email: String,
    pub(crate) owner_display_name: String,
    pub(crate) grantee_account_id: Uuid,
    pub(crate) grantee_email: String,
    pub(crate) grantee_display_name: String,
    pub(crate) sender_right: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct PendingOutboundQueueRow {
    pub(crate) queue_id: Uuid,
    pub(crate) message_id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) attempts: i32,
    pub(crate) from_address: String,
    pub(crate) from_display: Option<String>,
    pub(crate) sender_address: Option<String>,
    pub(crate) sender_display: Option<String>,
    pub(crate) sender_authorization_kind: String,
    pub(crate) subject: String,
    pub(crate) body_text: String,
    pub(crate) body_html_sanitized: Option<String>,
    pub(crate) internet_message_id: Option<String>,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, FromRow)]
pub(crate) struct OutboundQueueStateRow {
    pub(crate) tenant_id: String,
    pub(crate) message_id: Uuid,
    pub(crate) status: String,
    pub(crate) last_trace_id: Option<String>,
    pub(crate) remote_message_ref: Option<String>,
}

#[derive(Debug, FromRow)]
pub(crate) struct MessageBccRecipientRow {
    pub(crate) address: String,
    pub(crate) display_name: Option<String>,
}

#[derive(Debug, FromRow)]
pub(crate) struct MessageBccRecipientRecordRow {
    pub(crate) message_id: Uuid,
    pub(crate) address: String,
    pub(crate) display_name: Option<String>,
}

#[derive(Debug, FromRow)]
pub(crate) struct AccountQuotaRow {
    pub(crate) quota_mb: i32,
    pub(crate) used_mb: i32,
}

#[derive(Debug, FromRow)]
pub(crate) struct JmapUploadBlobRow {
    pub(crate) id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) media_type: String,
    pub(crate) octet_size: i64,
    pub(crate) blob_bytes: Vec<u8>,
}

#[derive(Debug, FromRow)]
pub(crate) struct ServerAdministratorRow {
    pub(crate) id: Uuid,
    pub(crate) domain_id: Option<Uuid>,
    pub(crate) domain_name: Option<String>,
    pub(crate) email: String,
    pub(crate) display_name: String,
    pub(crate) role: String,
    pub(crate) rights_summary: String,
    pub(crate) permissions_json: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct AdminLoginRow {
    pub(crate) tenant_id: String,
    pub(crate) email: String,
    pub(crate) password_hash: String,
    pub(crate) status: String,
    pub(crate) display_name: Option<String>,
    pub(crate) role: Option<String>,
    pub(crate) domain_id: Option<Uuid>,
    pub(crate) domain_name: Option<String>,
    pub(crate) rights_summary: Option<String>,
    pub(crate) permissions_json: Option<String>,
}

#[derive(Debug, FromRow)]
pub(crate) struct AccountLoginRow {
    pub(crate) tenant_id: String,
    pub(crate) account_id: Uuid,
    pub(crate) email: String,
    pub(crate) password_hash: String,
    pub(crate) status: String,
    pub(crate) display_name: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct AuthenticatedAdminRow {
    pub(crate) tenant_id: String,
    pub(crate) email: String,
    pub(crate) display_name: Option<String>,
    pub(crate) role: Option<String>,
    pub(crate) domain_id: Option<Uuid>,
    pub(crate) domain_name: Option<String>,
    pub(crate) rights_summary: Option<String>,
    pub(crate) permissions_json: Option<String>,
    pub(crate) auth_method: String,
    pub(crate) expires_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct AdminAuthFactorRow {
    pub(crate) id: Uuid,
    pub(crate) factor_type: String,
    pub(crate) status: String,
    pub(crate) created_at: String,
    pub(crate) verified_at: Option<String>,
    pub(crate) secret_ciphertext: Option<String>,
}

#[derive(Debug, FromRow)]
pub(crate) struct AccountAuthFactorRow {
    pub(crate) id: Uuid,
    pub(crate) factor_type: String,
    pub(crate) status: String,
    pub(crate) created_at: String,
    pub(crate) verified_at: Option<String>,
    pub(crate) secret_ciphertext: Option<String>,
}

#[derive(Debug, FromRow)]
pub(crate) struct AccountAppPasswordRow {
    pub(crate) id: Uuid,
    pub(crate) label: String,
    pub(crate) status: String,
    pub(crate) created_at: String,
    pub(crate) last_used_at: Option<String>,
    pub(crate) password_hash: Option<String>,
}

#[derive(Debug, FromRow)]
pub(crate) struct AuthenticatedAccountRow {
    pub(crate) tenant_id: String,
    pub(crate) account_id: Uuid,
    pub(crate) email: String,
    pub(crate) display_name: String,
    pub(crate) expires_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct ActiveSyncSyncStateRow {
    pub(crate) sync_key: String,
    pub(crate) snapshot_json: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct ClientMessageRow {
    pub(crate) id: Uuid,
    pub(crate) mailbox_role: String,
    pub(crate) from_name: String,
    pub(crate) from_address: String,
    pub(crate) to_recipients: String,
    pub(crate) cc_recipients: String,
    pub(crate) subject: String,
    pub(crate) preview: String,
    pub(crate) received_at: String,
    pub(crate) time_label: String,
    pub(crate) unread: bool,
    pub(crate) flagged: bool,
    pub(crate) delivery_status: String,
    pub(crate) body_text: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct ClientAttachmentRow {
    pub(crate) id: Uuid,
    pub(crate) message_id: Uuid,
    pub(crate) name: String,
    pub(crate) media_type: String,
    pub(crate) size_octets: i64,
}

#[derive(Debug, FromRow)]
pub(crate) struct ActiveSyncAttachmentRow {
    pub(crate) id: Uuid,
    pub(crate) message_id: Uuid,
    pub(crate) file_name: String,
    pub(crate) media_type: String,
    pub(crate) size_octets: i64,
}

#[derive(Debug, FromRow)]
pub(crate) struct ClientEventRow {
    pub(crate) id: Uuid,
    pub(crate) date: String,
    pub(crate) time: String,
    pub(crate) time_zone: String,
    pub(crate) duration_minutes: i32,
    pub(crate) recurrence_rule: String,
    pub(crate) title: String,
    pub(crate) location: String,
    pub(crate) attendees: String,
    pub(crate) attendees_json: String,
    pub(crate) notes: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct ClientContactRow {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) role: String,
    pub(crate) email: String,
    pub(crate) phone: String,
    pub(crate) team: String,
    pub(crate) notes: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct CollaborationCollectionRow {
    pub(crate) owner_account_id: Uuid,
    pub(crate) owner_email: String,
    pub(crate) owner_display_name: String,
    pub(crate) may_read: bool,
    pub(crate) may_write: bool,
    pub(crate) may_delete: bool,
    pub(crate) may_share: bool,
}

#[derive(Debug, FromRow)]
pub(crate) struct CollaborationGrantRow {
    pub(crate) id: Uuid,
    pub(crate) kind: String,
    pub(crate) owner_account_id: Uuid,
    pub(crate) owner_email: String,
    pub(crate) owner_display_name: String,
    pub(crate) grantee_account_id: Uuid,
    pub(crate) grantee_email: String,
    pub(crate) grantee_display_name: String,
    pub(crate) may_read: bool,
    pub(crate) may_write: bool,
    pub(crate) may_delete: bool,
    pub(crate) may_share: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct AccessibleContactRow {
    pub(crate) id: Uuid,
    pub(crate) owner_account_id: Uuid,
    pub(crate) owner_email: String,
    pub(crate) owner_display_name: String,
    pub(crate) may_read: bool,
    pub(crate) may_write: bool,
    pub(crate) may_delete: bool,
    pub(crate) may_share: bool,
    pub(crate) name: String,
    pub(crate) role: String,
    pub(crate) email: String,
    pub(crate) phone: String,
    pub(crate) team: String,
    pub(crate) notes: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct AccessibleEventRow {
    pub(crate) id: Uuid,
    pub(crate) owner_account_id: Uuid,
    pub(crate) owner_email: String,
    pub(crate) owner_display_name: String,
    pub(crate) may_read: bool,
    pub(crate) may_write: bool,
    pub(crate) may_delete: bool,
    pub(crate) may_share: bool,
    pub(crate) date: String,
    pub(crate) time: String,
    pub(crate) time_zone: String,
    pub(crate) duration_minutes: i32,
    pub(crate) recurrence_rule: String,
    pub(crate) title: String,
    pub(crate) location: String,
    pub(crate) attendees: String,
    pub(crate) attendees_json: String,
    pub(crate) notes: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct ClientTaskListRow {
    pub(crate) id: Uuid,
    pub(crate) owner_account_id: Uuid,
    pub(crate) owner_email: String,
    pub(crate) owner_display_name: String,
    pub(crate) is_owned: bool,
    pub(crate) may_read: bool,
    pub(crate) may_write: bool,
    pub(crate) may_delete: bool,
    pub(crate) may_share: bool,
    pub(crate) name: String,
    pub(crate) role: Option<String>,
    pub(crate) sort_order: i32,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct ClientTaskRow {
    pub(crate) id: Uuid,
    pub(crate) owner_account_id: Uuid,
    pub(crate) owner_email: String,
    pub(crate) owner_display_name: String,
    pub(crate) is_owned: bool,
    pub(crate) may_read: bool,
    pub(crate) may_write: bool,
    pub(crate) may_delete: bool,
    pub(crate) may_share: bool,
    pub(crate) task_list_id: Uuid,
    pub(crate) task_list_sort_order: i32,
    pub(crate) title: String,
    pub(crate) description: String,
    pub(crate) status: String,
    pub(crate) due_at: Option<String>,
    pub(crate) completed_at: Option<String>,
    pub(crate) sort_order: i32,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct DavTaskRow {
    pub(crate) id: Uuid,
    pub(crate) owner_account_id: Uuid,
    pub(crate) owner_email: String,
    pub(crate) owner_display_name: String,
    pub(crate) may_read: bool,
    pub(crate) may_write: bool,
    pub(crate) may_delete: bool,
    pub(crate) may_share: bool,
    pub(crate) task_list_id: Uuid,
    pub(crate) task_list_name: String,
    pub(crate) title: String,
    pub(crate) description: String,
    pub(crate) status: String,
    pub(crate) due_at: Option<String>,
    pub(crate) completed_at: Option<String>,
    pub(crate) sort_order: i32,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct TaskListGrantRow {
    pub(crate) id: Uuid,
    pub(crate) task_list_id: Uuid,
    pub(crate) task_list_name: String,
    pub(crate) owner_account_id: Uuid,
    pub(crate) owner_email: String,
    pub(crate) owner_display_name: String,
    pub(crate) grantee_account_id: Uuid,
    pub(crate) grantee_email: String,
    pub(crate) grantee_display_name: String,
    pub(crate) may_read: bool,
    pub(crate) may_write: bool,
    pub(crate) may_delete: bool,
    pub(crate) may_share: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct FilterRuleRow {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) scope: String,
    pub(crate) action: String,
    pub(crate) status: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct QuarantineRow {
    pub(crate) id: Uuid,
    pub(crate) message_ref: String,
    pub(crate) sender: String,
    pub(crate) recipient: String,
    pub(crate) reason: String,
    pub(crate) status: String,
    pub(crate) created_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct EmailTraceRow {
    pub(crate) message_id: Uuid,
    pub(crate) internet_message_id: Option<String>,
    pub(crate) subject: String,
    pub(crate) sender: String,
    pub(crate) account_email: String,
    pub(crate) mailbox: String,
    pub(crate) received_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct MailFlowRow {
    pub(crate) queue_id: Uuid,
    pub(crate) message_id: Uuid,
    pub(crate) account_email: String,
    pub(crate) subject: String,
    pub(crate) status: String,
    pub(crate) delivery_status: String,
    pub(crate) attempts: i32,
    pub(crate) submitted_at: String,
    pub(crate) last_attempt_at: Option<String>,
    pub(crate) next_attempt_at: Option<String>,
    pub(crate) trace_id: Option<String>,
    pub(crate) remote_message_ref: Option<String>,
    pub(crate) last_error: Option<String>,
    pub(crate) retry_after_seconds: Option<i32>,
    pub(crate) retry_policy: Option<String>,
    pub(crate) last_dsn_status: Option<String>,
    pub(crate) last_smtp_code: Option<i32>,
    pub(crate) last_enhanced_status: Option<String>,
}
