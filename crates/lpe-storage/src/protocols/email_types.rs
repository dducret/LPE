use serde::Serialize;
use uuid::Uuid;

use crate::{CalendarMeetingRequest, CalendarMeetingResponse};

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmailAddress {
    pub address: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmail {
    pub id: Uuid,
    pub thread_id: Uuid,
    pub mailbox_ids: Vec<Uuid>,
    pub mailbox_states: Vec<JmapEmailMailboxState>,
    pub mailbox_id: Uuid,
    pub mailbox_role: String,
    pub mailbox_name: String,
    pub modseq: u64,
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
    pub followup_flag_status: String,
    pub followup_icon: i32,
    pub todo_item_flags: i32,
    pub followup_request: String,
    pub followup_start_at: Option<String>,
    pub followup_due_at: Option<String>,
    pub followup_completed_at: Option<String>,
    pub reminder_set: bool,
    pub reminder_at: Option<String>,
    pub reminder_dismissed_at: Option<String>,
    pub swapped_todo_store_id: Option<Uuid>,
    pub swapped_todo_data: Option<Vec<u8>>,
    pub categories: Vec<String>,
    pub has_attachments: bool,
    pub calendar_invitation: bool,
    #[serde(skip)]
    pub calendar_meeting_request: Option<CalendarMeetingRequest>,
    #[serde(skip)]
    pub calendar_meeting_response: Option<CalendarMeetingResponse>,
    pub size_octets: i64,
    pub internet_message_id: Option<String>,
    pub mime_blob_ref: Option<String>,
    pub delivery_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JmapEmailMailboxState {
    pub mailbox_id: Uuid,
    pub role: String,
    pub name: String,
    pub modseq: u64,
    pub unread: bool,
    pub flagged: bool,
    pub followup_flag_status: String,
    pub followup_icon: i32,
    pub todo_item_flags: i32,
    pub followup_request: String,
    pub followup_start_at: Option<String>,
    pub followup_due_at: Option<String>,
    pub followup_completed_at: Option<String>,
    pub reminder_set: bool,
    pub reminder_at: Option<String>,
    pub reminder_dismissed_at: Option<String>,
    pub swapped_todo_store_id: Option<Uuid>,
    pub swapped_todo_data: Option<Vec<u8>>,
    pub categories: Vec<String>,
    pub draft: bool,
}
