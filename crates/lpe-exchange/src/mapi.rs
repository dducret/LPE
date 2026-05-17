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
    serialize_calendar_participants_metadata, AccessibleContact, AccessibleEvent,
    AttachmentUploadInput, AuditEntryInput, CalendarParticipantsMetadata, ClientTask,
    CollaborationRights, JmapEmail, JmapEmailAddress, JmapImportedEmailInput, JmapMailbox,
    JmapMailboxCreateInput, SubmitMessageInput, SubmittedMessage, SubmittedRecipientInput,
    UpsertClientContactInput, UpsertClientEventInput, UpsertClientTaskInput,
};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    env,
    sync::{Mutex, OnceLock},
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
mod notifications;
mod nspi;
pub(crate) mod permissions;
mod properties;
mod rop;
mod session;
mod store_adapter;
mod sync;
mod tables;
mod transport;

pub(crate) use crate::mapi::{
    session::{create_rpc_emsmdb_context, execute_rpc_emsmdb_rops},
    transport::{
        debug_payload_preview_hex, handle_mapi, mapi_error_response, mapi_response_payload_bytes,
        mapi_response_payload_preview_hex, safe_header, MapiEndpoint,
    },
};
