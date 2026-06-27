use anyhow::{anyhow, Result};
use axum::{
    http::{
        header::{CONTENT_LENGTH, CONTENT_TYPE, SET_COOKIE, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
};
use lpe_magika::{
    Detector, ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator,
};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, AttachmentUploadInput, AuditEntryInput,
    CalendarParticipantsMetadata, ClientNote, ClientTask, CollaborationRights, JmapEmail,
    JmapEmailAddress, JmapImportedEmailInput, JmapMailbox, JournalEntry, SubmitMessageInput,
    SubmittedMessage, SubmittedRecipientInput, UpsertClientContactInput, UpsertClientEventInput,
    UpsertClientNoteInput, UpsertClientTaskInput, UpsertJournalEntryInput,
};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    env,
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        Mutex, OnceLock,
    },
    time::{Duration, SystemTime},
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    mapi_mailstore,
    mapi_store::{
        MapiAttachment, MapiCollaborationFolder, MapiCollaborationFolderKind,
        MapiMailStoreSnapshot, MapiStore,
    },
    store::{
        ExchangeAddressBookDirectoryKind, ExchangeAddressBookEntry, ExchangeAddressBookEntryKind,
        ExchangeStore, MapiCheckpointKind, MapiIdentityObjectKind, MapiIdentityRequest,
    },
};

mod dispatch;
pub(crate) mod identity;
pub(crate) mod notifications;
mod nspi;
mod outlook_startup;
pub(crate) mod permissions;
pub(crate) mod properties;
mod rop;
mod session;
mod store_adapter;
mod sync;
mod tables;
mod transport;
pub(crate) mod wire;

pub(crate) use crate::mapi::{
    session::{create_rpc_emsmdb_context, execute_rpc_emsmdb_rops},
    transport::{
        client_flow_key, debug_payload_preview_hex, guid_counter_debug, handle_mapi,
        mapi_error_response, mapi_response_payload_bytes, request_cookie_transport_debug,
        safe_header, MapiEndpoint,
    },
};

static MAPI_FOLDER_PURGE_ATTEMPTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_FOLDER_PURGE_SUCCEEDED_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_FOLDER_PURGE_FAILED_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_FOLDER_PURGE_PARTIAL_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_INBOX_FAI_HANDOFF_WITHOUT_CONTENTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_POST_FAI_HIERARCHY_WITHOUT_CONTENTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_INBOX_NORMAL_CONTENTS_OPENED_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_IPM_SUBTREE_HIERARCHY_QUERY_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_IPM_SUBTREE_HIERARCHY_MISSING_CONVERSATION_ACTION_TOTAL: AtomicU64 =
    AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_IPM_SUBTREE_HIERARCHY_QUICK_STEP_PRESENT_TOTAL: AtomicU64 =
    AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_IPM_SUBTREE_HIERARCHY_ROW_COUNT_MISMATCH_TOTAL: AtomicU64 =
    AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_LAST_IPM_SUBTREE_HIERARCHY_RESPONSE_ROW_COUNT: AtomicU64 =
    AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_LAST_IPM_SUBTREE_HIERARCHY_TABLE_TOTAL_ROW_COUNT: AtomicU64 =
    AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_LAST_IPM_SUBTREE_HIERARCHY_HAS_CONVERSATION_ACTION: AtomicU64 =
    AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_LAST_IPM_SUBTREE_HIERARCHY_HAS_QUICK_STEP: AtomicU64 = AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_LAST_BOOTSTRAP_PHASE: AtomicU64 = AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_MAX_BOOTSTRAP_PHASE: AtomicU64 = AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_LAST_STALL_CODE: AtomicU64 = AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_STALL_AFTER_INBOX_FAI_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_STALL_AFTER_IPM_HIERARCHY_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_REPEATED_INBOX_FOLDER_TYPE_PROBE_TOTAL: AtomicU64 = AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_LAST_INBOX_OPEN_PROBE_COUNT: AtomicU64 = AtomicU64::new(0);
static MAPI_OUTLOOK_VIEW_LAST_INBOX_FOLDER_TYPE_GETPROPS_PROBE_COUNT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default)]
pub struct MapiFolderPurgeMetrics {
    pub attempted_total: u64,
    pub succeeded_total: u64,
    pub failed_total: u64,
    pub partial_total: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MapiOutlookViewMetrics {
    pub inbox_fai_handoff_without_contents_total: u64,
    pub post_fai_hierarchy_without_contents_total: u64,
    pub inbox_normal_contents_opened_total: u64,
    pub ipm_subtree_hierarchy_query_total: u64,
    pub ipm_subtree_hierarchy_missing_conversation_action_total: u64,
    pub ipm_subtree_hierarchy_quick_step_present_total: u64,
    pub ipm_subtree_hierarchy_row_count_mismatch_total: u64,
    pub last_ipm_subtree_hierarchy_response_row_count: u64,
    pub last_ipm_subtree_hierarchy_table_total_row_count: u64,
    pub last_ipm_subtree_hierarchy_has_conversation_action: u64,
    pub last_ipm_subtree_hierarchy_has_quick_step: u64,
    pub last_bootstrap_phase: u64,
    pub max_bootstrap_phase: u64,
    pub last_stall_code: u64,
    pub stall_after_inbox_fai_total: u64,
    pub stall_after_ipm_hierarchy_total: u64,
    pub repeated_inbox_folder_type_probe_total: u64,
    pub last_inbox_open_probe_count: u64,
    pub last_inbox_folder_type_getprops_probe_count: u64,
}

pub(crate) fn record_mapi_folder_purge_metrics(
    attempted: usize,
    succeeded: usize,
    failed: usize,
    partial_completion: bool,
) {
    MAPI_FOLDER_PURGE_ATTEMPTED_TOTAL.fetch_add(attempted as u64, AtomicOrdering::Relaxed);
    MAPI_FOLDER_PURGE_SUCCEEDED_TOTAL.fetch_add(succeeded as u64, AtomicOrdering::Relaxed);
    MAPI_FOLDER_PURGE_FAILED_TOTAL.fetch_add(failed as u64, AtomicOrdering::Relaxed);
    if partial_completion {
        MAPI_FOLDER_PURGE_PARTIAL_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
    }
}

pub fn mapi_folder_purge_metrics() -> MapiFolderPurgeMetrics {
    MapiFolderPurgeMetrics {
        attempted_total: MAPI_FOLDER_PURGE_ATTEMPTED_TOTAL.load(AtomicOrdering::Relaxed),
        succeeded_total: MAPI_FOLDER_PURGE_SUCCEEDED_TOTAL.load(AtomicOrdering::Relaxed),
        failed_total: MAPI_FOLDER_PURGE_FAILED_TOTAL.load(AtomicOrdering::Relaxed),
        partial_total: MAPI_FOLDER_PURGE_PARTIAL_TOTAL.load(AtomicOrdering::Relaxed),
    }
}

pub(crate) fn record_mapi_outlook_view_inbox_fai_handoff_without_contents() {
    MAPI_OUTLOOK_VIEW_INBOX_FAI_HANDOFF_WITHOUT_CONTENTS_TOTAL
        .fetch_add(1, AtomicOrdering::Relaxed);
}

pub(crate) fn record_mapi_outlook_view_post_fai_hierarchy_without_contents() {
    MAPI_OUTLOOK_VIEW_POST_FAI_HIERARCHY_WITHOUT_CONTENTS_TOTAL
        .fetch_add(1, AtomicOrdering::Relaxed);
}

pub(crate) fn record_mapi_outlook_view_inbox_normal_contents_opened() {
    MAPI_OUTLOOK_VIEW_INBOX_NORMAL_CONTENTS_OPENED_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
}

pub(crate) fn record_mapi_outlook_view_ipm_subtree_hierarchy_query(
    response_row_count: u64,
    table_total_row_count: u64,
    has_conversation_action: bool,
    has_quick_step: bool,
) {
    MAPI_OUTLOOK_VIEW_IPM_SUBTREE_HIERARCHY_QUERY_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
    MAPI_OUTLOOK_VIEW_LAST_IPM_SUBTREE_HIERARCHY_RESPONSE_ROW_COUNT
        .store(response_row_count, AtomicOrdering::Relaxed);
    MAPI_OUTLOOK_VIEW_LAST_IPM_SUBTREE_HIERARCHY_TABLE_TOTAL_ROW_COUNT
        .store(table_total_row_count, AtomicOrdering::Relaxed);
    MAPI_OUTLOOK_VIEW_LAST_IPM_SUBTREE_HIERARCHY_HAS_CONVERSATION_ACTION
        .store(has_conversation_action as u64, AtomicOrdering::Relaxed);
    MAPI_OUTLOOK_VIEW_LAST_IPM_SUBTREE_HIERARCHY_HAS_QUICK_STEP
        .store(has_quick_step as u64, AtomicOrdering::Relaxed);
    if !has_conversation_action {
        MAPI_OUTLOOK_VIEW_IPM_SUBTREE_HIERARCHY_MISSING_CONVERSATION_ACTION_TOTAL
            .fetch_add(1, AtomicOrdering::Relaxed);
    }
    if has_quick_step {
        MAPI_OUTLOOK_VIEW_IPM_SUBTREE_HIERARCHY_QUICK_STEP_PRESENT_TOTAL
            .fetch_add(1, AtomicOrdering::Relaxed);
    }
    if response_row_count != table_total_row_count {
        MAPI_OUTLOOK_VIEW_IPM_SUBTREE_HIERARCHY_ROW_COUNT_MISMATCH_TOTAL
            .fetch_add(1, AtomicOrdering::Relaxed);
    }
}

pub(crate) fn record_mapi_outlook_view_bootstrap_progress(
    phase: u64,
    stall_code: u64,
    inbox_open_probe_count: usize,
    inbox_folder_type_getprops_probe_count: usize,
) {
    MAPI_OUTLOOK_VIEW_LAST_BOOTSTRAP_PHASE.store(phase, AtomicOrdering::Relaxed);
    MAPI_OUTLOOK_VIEW_MAX_BOOTSTRAP_PHASE.fetch_max(phase, AtomicOrdering::Relaxed);
    MAPI_OUTLOOK_VIEW_LAST_STALL_CODE.store(stall_code, AtomicOrdering::Relaxed);
    MAPI_OUTLOOK_VIEW_LAST_INBOX_OPEN_PROBE_COUNT
        .store(inbox_open_probe_count as u64, AtomicOrdering::Relaxed);
    MAPI_OUTLOOK_VIEW_LAST_INBOX_FOLDER_TYPE_GETPROPS_PROBE_COUNT.store(
        inbox_folder_type_getprops_probe_count as u64,
        AtomicOrdering::Relaxed,
    );
}

pub(crate) fn record_mapi_outlook_view_bootstrap_stall(stall_code: u64) {
    match stall_code {
        1 => {
            MAPI_OUTLOOK_VIEW_STALL_AFTER_INBOX_FAI_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
        }
        2 => {
            MAPI_OUTLOOK_VIEW_STALL_AFTER_IPM_HIERARCHY_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
        }
        3 => {
            MAPI_OUTLOOK_VIEW_REPEATED_INBOX_FOLDER_TYPE_PROBE_TOTAL
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        _ => {}
    }
}

pub fn mapi_outlook_view_metrics() -> MapiOutlookViewMetrics {
    MapiOutlookViewMetrics {
        inbox_fai_handoff_without_contents_total:
            MAPI_OUTLOOK_VIEW_INBOX_FAI_HANDOFF_WITHOUT_CONTENTS_TOTAL.load(AtomicOrdering::Relaxed),
        post_fai_hierarchy_without_contents_total:
            MAPI_OUTLOOK_VIEW_POST_FAI_HIERARCHY_WITHOUT_CONTENTS_TOTAL
                .load(AtomicOrdering::Relaxed),
        inbox_normal_contents_opened_total: MAPI_OUTLOOK_VIEW_INBOX_NORMAL_CONTENTS_OPENED_TOTAL
            .load(AtomicOrdering::Relaxed),
        ipm_subtree_hierarchy_query_total: MAPI_OUTLOOK_VIEW_IPM_SUBTREE_HIERARCHY_QUERY_TOTAL
            .load(AtomicOrdering::Relaxed),
        ipm_subtree_hierarchy_missing_conversation_action_total:
            MAPI_OUTLOOK_VIEW_IPM_SUBTREE_HIERARCHY_MISSING_CONVERSATION_ACTION_TOTAL
                .load(AtomicOrdering::Relaxed),
        ipm_subtree_hierarchy_quick_step_present_total:
            MAPI_OUTLOOK_VIEW_IPM_SUBTREE_HIERARCHY_QUICK_STEP_PRESENT_TOTAL
                .load(AtomicOrdering::Relaxed),
        ipm_subtree_hierarchy_row_count_mismatch_total:
            MAPI_OUTLOOK_VIEW_IPM_SUBTREE_HIERARCHY_ROW_COUNT_MISMATCH_TOTAL
                .load(AtomicOrdering::Relaxed),
        last_ipm_subtree_hierarchy_response_row_count:
            MAPI_OUTLOOK_VIEW_LAST_IPM_SUBTREE_HIERARCHY_RESPONSE_ROW_COUNT
                .load(AtomicOrdering::Relaxed),
        last_ipm_subtree_hierarchy_table_total_row_count:
            MAPI_OUTLOOK_VIEW_LAST_IPM_SUBTREE_HIERARCHY_TABLE_TOTAL_ROW_COUNT
                .load(AtomicOrdering::Relaxed),
        last_ipm_subtree_hierarchy_has_conversation_action:
            MAPI_OUTLOOK_VIEW_LAST_IPM_SUBTREE_HIERARCHY_HAS_CONVERSATION_ACTION
                .load(AtomicOrdering::Relaxed),
        last_ipm_subtree_hierarchy_has_quick_step:
            MAPI_OUTLOOK_VIEW_LAST_IPM_SUBTREE_HIERARCHY_HAS_QUICK_STEP
                .load(AtomicOrdering::Relaxed),
        last_bootstrap_phase: MAPI_OUTLOOK_VIEW_LAST_BOOTSTRAP_PHASE.load(AtomicOrdering::Relaxed),
        max_bootstrap_phase: MAPI_OUTLOOK_VIEW_MAX_BOOTSTRAP_PHASE.load(AtomicOrdering::Relaxed),
        last_stall_code: MAPI_OUTLOOK_VIEW_LAST_STALL_CODE.load(AtomicOrdering::Relaxed),
        stall_after_inbox_fai_total: MAPI_OUTLOOK_VIEW_STALL_AFTER_INBOX_FAI_TOTAL
            .load(AtomicOrdering::Relaxed),
        stall_after_ipm_hierarchy_total: MAPI_OUTLOOK_VIEW_STALL_AFTER_IPM_HIERARCHY_TOTAL
            .load(AtomicOrdering::Relaxed),
        repeated_inbox_folder_type_probe_total:
            MAPI_OUTLOOK_VIEW_REPEATED_INBOX_FOLDER_TYPE_PROBE_TOTAL.load(AtomicOrdering::Relaxed),
        last_inbox_open_probe_count: MAPI_OUTLOOK_VIEW_LAST_INBOX_OPEN_PROBE_COUNT
            .load(AtomicOrdering::Relaxed),
        last_inbox_folder_type_getprops_probe_count:
            MAPI_OUTLOOK_VIEW_LAST_INBOX_FOLDER_TYPE_GETPROPS_PROBE_COUNT
                .load(AtomicOrdering::Relaxed),
    }
}
