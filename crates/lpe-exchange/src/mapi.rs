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

#[derive(Debug, Clone, Copy, Default)]
pub struct MapiFolderPurgeMetrics {
    pub attempted_total: u64,
    pub succeeded_total: u64,
    pub failed_total: u64,
    pub partial_total: u64,
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
