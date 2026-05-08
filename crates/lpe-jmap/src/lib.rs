mod backbone;
mod blob;
mod calendar;
mod contacts;
mod convert;
mod drafts;
mod error;
mod eventsource;
mod mail;
mod mailboxes;
mod parse;
mod protocol;
mod service;
mod session;
mod state;
mod store;
mod tasks;
mod upload;
mod vacation;
mod validation;
mod websocket;

pub use crate::backbone::{
    JmapAddressObject, JmapEmailObject, JmapMailboxObject, JmapMailboxRights, JmapThreadObject,
};
pub use crate::service::{router, JmapService};

pub(crate) use crate::convert::resolve_creation_reference;
pub(crate) use crate::parse::parse_submission_email_id;
pub(crate) use crate::service::{
    collection_state_fingerprint, trim_snippet, DEFAULT_GET_LIMIT, JMAP_BLOB_CAPABILITY,
    JMAP_CALENDARS_CAPABILITY, JMAP_CONTACTS_CAPABILITY, JMAP_CORE_CAPABILITY,
    JMAP_MAIL_CAPABILITY, JMAP_SUBMISSION_CAPABILITY, JMAP_TASKS_CAPABILITY,
    JMAP_VACATION_RESPONSE_CAPABILITY, JMAP_WEBSOCKET_CAPABILITY, MAX_BLOB_DATA_SOURCES,
    MAX_CALLS_IN_REQUEST, MAX_CONCURRENT_REQUESTS, MAX_CONCURRENT_UPLOAD, MAX_OBJECTS_IN_GET,
    MAX_OBJECTS_IN_SET, MAX_QUERY_LIMIT, MAX_SIZE_REQUEST, MAX_SIZE_UPLOAD, PUSH_STATE_VERSION,
    QUERY_STATE_VERSION, SESSION_STATE, STATE_TOKEN_VERSION,
};
pub(crate) use crate::session::requested_account_id;
pub(crate) use crate::state::encode_query_state;
pub(crate) use crate::upload::blob_id_for_message;

#[cfg(test)]
mod tests;
