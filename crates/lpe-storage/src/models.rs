use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow)]
pub(crate) struct AccountRow {
    pub(crate) id: Uuid,
    pub(crate) primary_email: String,
    pub(crate) display_name: String,
    pub(crate) quota_mb: i32,
    pub(crate) quota_used_octets: i64,
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
    pub(crate) jmap_push_journal_retention_days: i32,
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
    pub(crate) parent_mailbox_id: Option<Uuid>,
    pub(crate) role: String,
    pub(crate) display_name: String,
    pub(crate) sort_order: i32,
    pub(crate) modseq: i64,
    pub(crate) total_emails: i64,
    pub(crate) unread_emails: i64,
    pub(crate) is_subscribed: bool,
}

#[derive(Debug, FromRow)]
pub(crate) struct JmapEmailRow {
    pub(crate) id: Uuid,
    pub(crate) thread_id: Uuid,
    pub(crate) mailbox_ids: Vec<Uuid>,
    pub(crate) mailbox_roles: Vec<String>,
    pub(crate) mailbox_names: Vec<String>,
    pub(crate) mailbox_modseqs: Vec<i64>,
    pub(crate) mailbox_unreads: Vec<bool>,
    pub(crate) mailbox_flaggeds: Vec<bool>,
    pub(crate) mailbox_followup_flag_statuses: Vec<String>,
    pub(crate) mailbox_followup_icons: Vec<i32>,
    pub(crate) mailbox_todo_item_flags: Vec<i32>,
    pub(crate) mailbox_followup_requests: Vec<String>,
    pub(crate) mailbox_followup_start_ats: Vec<Option<String>>,
    pub(crate) mailbox_followup_due_ats: Vec<Option<String>>,
    pub(crate) mailbox_followup_completed_ats: Vec<Option<String>>,
    pub(crate) mailbox_reminder_sets: Vec<bool>,
    pub(crate) mailbox_reminder_ats: Vec<Option<String>>,
    pub(crate) mailbox_reminder_dismissed_ats: Vec<Option<String>>,
    pub(crate) mailbox_swapped_todo_store_ids: Vec<Option<Uuid>>,
    pub(crate) mailbox_swapped_todo_datas: Vec<Option<Vec<u8>>>,
    pub(crate) mailbox_categories_json: Vec<String>,
    pub(crate) mailbox_drafts: Vec<bool>,
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
    pub(crate) followup_flag_status: String,
    pub(crate) followup_icon: i32,
    pub(crate) todo_item_flags: i32,
    pub(crate) followup_request: String,
    pub(crate) followup_start_at: Option<String>,
    pub(crate) followup_due_at: Option<String>,
    pub(crate) followup_completed_at: Option<String>,
    pub(crate) reminder_set: bool,
    pub(crate) reminder_at: Option<String>,
    pub(crate) reminder_dismissed_at: Option<String>,
    pub(crate) swapped_todo_store_id: Option<Uuid>,
    pub(crate) swapped_todo_data: Option<Vec<u8>>,
    pub(crate) categories: Vec<String>,
    pub(crate) has_attachments: bool,
    pub(crate) size_octets: i64,
    pub(crate) internet_message_id: Option<String>,
    pub(crate) mime_blob_ref: Option<String>,
    pub(crate) delivery_status: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct SearchFolderRow {
    pub(crate) id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) role: String,
    pub(crate) display_name: String,
    pub(crate) definition_kind: String,
    pub(crate) result_object_kind: String,
    pub(crate) scope_json: Value,
    pub(crate) restriction_json: Value,
    pub(crate) excluded_folder_roles: Vec<String>,
    pub(crate) is_builtin: bool,
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
    pub(crate) imap_deleted: bool,
    pub(crate) keywords: Vec<String>,
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
    pub(crate) may_write: bool,
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
    pub(crate) may_write: bool,
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
    pub(crate) tenant_id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) message_id: Uuid,
    pub(crate) status: String,
    pub(crate) attempts: i32,
    pub(crate) last_trace_id: Option<String>,
    pub(crate) remote_message_ref: Option<String>,
    pub(crate) retry_after_seconds: Option<i32>,
    pub(crate) retry_policy: Option<String>,
    pub(crate) last_result_json: Value,
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
    pub(crate) quota_used_octets: i64,
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
    pub(crate) tenant_id: Uuid,
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
    pub(crate) tenant_id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) email: String,
    pub(crate) password_hash: String,
    pub(crate) status: String,
    pub(crate) display_name: String,
    pub(crate) quota_mb: i32,
    pub(crate) quota_used_octets: i64,
}

#[derive(Debug, FromRow)]
pub(crate) struct AuthenticatedAdminRow {
    pub(crate) tenant_id: Uuid,
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
    pub(crate) tenant_id: Uuid,
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
pub(crate) struct ActiveSyncDeviceRow {
    pub(crate) account_id: Uuid,
    pub(crate) device_id: String,
    pub(crate) device_type: String,
    pub(crate) policy_key: Option<String>,
    pub(crate) pending_policy_key: Option<String>,
    pub(crate) provision_status: String,
    pub(crate) wipe_status: String,
    pub(crate) account_wipe_status: String,
    pub(crate) last_seen_at: String,
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
    pub(crate) followup_flag_status: String,
    pub(crate) followup_start_at: Option<String>,
    pub(crate) followup_due_at: Option<String>,
    pub(crate) followup_completed_at: Option<String>,
    pub(crate) reminder_set: bool,
    pub(crate) reminder_at: Option<String>,
    pub(crate) reminder_dismissed_at: Option<String>,
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
pub(crate) struct ClientEventRow {
    pub(crate) id: Uuid,
    pub(crate) uid: String,
    pub(crate) date: String,
    pub(crate) time: String,
    pub(crate) time_zone: String,
    pub(crate) duration_minutes: i32,
    pub(crate) all_day: bool,
    pub(crate) status: String,
    pub(crate) sequence: i32,
    pub(crate) recurrence_rule: String,
    pub(crate) recurrence_json: String,
    pub(crate) recurrence_exceptions_json: String,
    pub(crate) title: String,
    pub(crate) location: String,
    pub(crate) organizer_json: String,
    pub(crate) attendees: String,
    pub(crate) attendees_json: String,
    pub(crate) notes: String,
    pub(crate) body_html: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct ClientContactRow {
    pub(crate) id: Uuid,
    pub(crate) address_book_id: String,
    pub(crate) name: String,
    pub(crate) role: String,
    pub(crate) email: String,
    pub(crate) phone: String,
    pub(crate) team: String,
    pub(crate) notes: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct CollaborationCollectionRow {
    pub(crate) id: Uuid,
    pub(crate) owner_account_id: Uuid,
    pub(crate) owner_email: String,
    pub(crate) owner_display_name: String,
    pub(crate) display_name: String,
    pub(crate) role: String,
    pub(crate) may_read: bool,
    pub(crate) may_write: bool,
    pub(crate) may_delete: bool,
    pub(crate) may_share: bool,
}

#[derive(Debug, FromRow)]
pub(crate) struct CollaborationGrantRow {
    pub(crate) id: Uuid,
    pub(crate) kind: String,
    pub(crate) calendar_id: Option<Uuid>,
    pub(crate) calendar_name: Option<String>,
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
    pub(crate) contact_book_role: String,
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
    pub(crate) uid: String,
    pub(crate) calendar_id: Uuid,
    pub(crate) calendar_role: String,
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
    pub(crate) all_day: bool,
    pub(crate) status: String,
    pub(crate) sequence: i32,
    pub(crate) recurrence_rule: String,
    pub(crate) recurrence_json: String,
    pub(crate) recurrence_exceptions_json: String,
    pub(crate) title: String,
    pub(crate) location: String,
    pub(crate) organizer_json: String,
    pub(crate) attendees: String,
    pub(crate) attendees_json: String,
    pub(crate) notes: String,
    pub(crate) body_html: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct FreeBusyEventRow {
    pub(crate) starts_at: String,
    pub(crate) ends_at: String,
    pub(crate) status: String,
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
    pub(crate) recurrence_rule: String,
    pub(crate) sort_order: i32,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct ClientNoteRow {
    pub(crate) id: Uuid,
    pub(crate) title: String,
    pub(crate) body_text: String,
    pub(crate) color: String,
    pub(crate) categories_json: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct JournalEntryRow {
    pub(crate) id: Uuid,
    pub(crate) subject: String,
    pub(crate) body_text: String,
    pub(crate) entry_type: String,
    pub(crate) message_class: String,
    pub(crate) starts_at: Option<String>,
    pub(crate) ends_at: Option<String>,
    pub(crate) occurred_at: Option<String>,
    pub(crate) companies_json: String,
    pub(crate) contacts_json: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct ClientReminderRow {
    pub(crate) source_type: String,
    pub(crate) source_id: Uuid,
    pub(crate) occurrence_start_at: Option<String>,
    pub(crate) title: String,
    pub(crate) due_at: Option<String>,
    pub(crate) reminder_at: String,
    pub(crate) dismissed_at: Option<String>,
    pub(crate) completed_at: Option<String>,
    pub(crate) status: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct PublicFolderTreeRow {
    pub(crate) id: Uuid,
    pub(crate) canonical_id: Uuid,
    pub(crate) display_name: String,
    pub(crate) lifecycle_state: String,
    pub(crate) admin_owner_account_id: Uuid,
    pub(crate) root_folder_id: Option<Uuid>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct PublicFolderRow {
    pub(crate) id: Uuid,
    pub(crate) tree_id: Uuid,
    pub(crate) parent_folder_id: Option<Uuid>,
    pub(crate) canonical_id: Uuid,
    pub(crate) display_name: String,
    pub(crate) folder_class: String,
    pub(crate) path: String,
    pub(crate) sort_order: i32,
    pub(crate) lifecycle_state: String,
    pub(crate) change_counter: i64,
    pub(crate) may_read: bool,
    pub(crate) may_write: bool,
    pub(crate) may_delete: bool,
    pub(crate) may_share: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct PublicFolderItemRow {
    pub(crate) id: Uuid,
    pub(crate) public_folder_id: Uuid,
    pub(crate) message_id: Option<Uuid>,
    pub(crate) item_kind: String,
    pub(crate) message_class: String,
    pub(crate) subject: String,
    pub(crate) body_text: String,
    pub(crate) body_html_sanitized: Option<String>,
    pub(crate) source_payload_json: String,
    pub(crate) lifecycle_state: String,
    pub(crate) change_counter: i64,
    pub(crate) created_by_account_id: Uuid,
    pub(crate) updated_by_account_id: Uuid,
    pub(crate) is_read: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct PublicFolderPermissionRow {
    pub(crate) id: Uuid,
    pub(crate) public_folder_id: Uuid,
    pub(crate) principal_account_id: Uuid,
    pub(crate) principal_email: String,
    pub(crate) principal_display_name: String,
    pub(crate) may_read: bool,
    pub(crate) may_write: bool,
    pub(crate) may_delete: bool,
    pub(crate) may_share: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct PublicFolderReplicaRow {
    pub(crate) id: Uuid,
    pub(crate) public_folder_id: Uuid,
    pub(crate) server_name: String,
    pub(crate) lifecycle_state: String,
    pub(crate) sort_order: i32,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct PublicFolderPerUserStateRow {
    pub(crate) public_folder_id: Uuid,
    pub(crate) item_id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) is_read: bool,
    pub(crate) last_seen_change: i64,
    pub(crate) private_json: String,
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
    pub(crate) recurrence_rule: String,
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
pub(crate) struct EmailTraceRow {
    pub(crate) message_id: Uuid,
    pub(crate) internet_message_id: Option<String>,
    pub(crate) subject: String,
    pub(crate) sender: String,
    pub(crate) account_email: String,
    pub(crate) mailbox: String,
    pub(crate) delivery_status: String,
    pub(crate) was_submitted: bool,
    pub(crate) in_sent_mailbox: bool,
    pub(crate) sent_at: Option<String>,
    pub(crate) queue_status: Option<String>,
    pub(crate) latest_trace_id: Option<String>,
    pub(crate) remote_message_ref: Option<String>,
    pub(crate) last_attempt_at: Option<String>,
    pub(crate) next_attempt_at: Option<String>,
    pub(crate) last_error: Option<String>,
    pub(crate) last_dsn_status: Option<String>,
    pub(crate) last_smtp_code: Option<i32>,
    pub(crate) last_enhanced_status: Option<String>,
    pub(crate) received_at: String,
}

#[derive(Debug, FromRow)]
pub(crate) struct MailFlowRow {
    pub(crate) queue_id: Uuid,
    pub(crate) message_id: Uuid,
    pub(crate) account_email: String,
    pub(crate) subject: String,
    pub(crate) internet_message_id: Option<String>,
    pub(crate) status: String,
    pub(crate) delivery_status: String,
    pub(crate) was_submitted: bool,
    pub(crate) in_sent_mailbox: bool,
    pub(crate) attempts: i32,
    pub(crate) submitted_at: String,
    pub(crate) sent_at: Option<String>,
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
