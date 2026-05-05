use anyhow::{anyhow, bail, Result};
use axum::{
    body::Bytes,
    extract::{ws::WebSocketUpgrade, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use lpe_magika::{ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, AuthenticatedAccount, ClientTask, ClientTaskList,
    CollaborationCollection, JmapEmail, JmapEmailSubmission, JmapMailbox, JmapUploadBlob,
    MailboxAccountAccess, SenderIdentity, Storage,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    convert::format_addresses,
    error::{http_error, method_error},
    parse::parse_uuid,
    protocol::{
        JmapApiRequest, JmapApiResponse, JmapMethodCall, JmapMethodResponse, SessionDocument,
    },
    session,
    state::{encode_state, StateEntry},
    store::JmapStore,
    upload::{message_rfc822_bytes, JmapBlobId},
};

pub(crate) const JMAP_CORE_CAPABILITY: &str = "urn:ietf:params:jmap:core";
pub(crate) const JMAP_MAIL_CAPABILITY: &str = "urn:ietf:params:jmap:mail";
pub(crate) const JMAP_SUBMISSION_CAPABILITY: &str = "urn:ietf:params:jmap:submission";
pub(crate) const JMAP_BLOB_CAPABILITY: &str = "urn:ietf:params:jmap:blob";
pub(crate) const JMAP_CONTACTS_CAPABILITY: &str = "urn:ietf:params:jmap:contacts";
pub(crate) const JMAP_CALENDARS_CAPABILITY: &str = "urn:ietf:params:jmap:calendars";
pub(crate) const JMAP_TASKS_CAPABILITY: &str = "urn:ietf:params:jmap:tasks";
pub(crate) const JMAP_VACATION_RESPONSE_CAPABILITY: &str = "urn:ietf:params:jmap:vacationresponse";
pub(crate) const JMAP_WEBSOCKET_CAPABILITY: &str = "urn:ietf:params:jmap:websocket";
pub(crate) const SESSION_STATE: &str = "mvp-3";
pub(crate) const QUERY_STATE_VERSION: &str = "mvp-3";
pub(crate) const STATE_TOKEN_VERSION: &str = "mvp-2";
pub(crate) const PUSH_STATE_VERSION: &str = "mvp-push-1";
pub(crate) const MAX_QUERY_LIMIT: u64 = 250;
pub(crate) const DEFAULT_GET_LIMIT: u64 = 100;
pub(crate) const MAX_SIZE_UPLOAD: u64 = 25 * 1024 * 1024;
pub(crate) const MAX_CONCURRENT_UPLOAD: u64 = 4;
pub(crate) const MAX_BLOB_DATA_SOURCES: u64 = 64;

type HttpResult<T> = std::result::Result<Json<T>, (StatusCode, String)>;

pub fn router() -> Router<Storage> {
    Router::new()
        .route("/session", get(session_handler))
        .route("/api", post(api_handler))
        .route("/ws", get(websocket_handler))
        .route("/upload/{account_id}", post(upload_handler))
        .route(
            "/download/{account_id}/{blob_id}/{name}",
            get(download_handler),
        )
}

#[derive(Clone)]
pub struct JmapService<S, V = lpe_magika::SystemDetector> {
    pub(crate) store: S,
    pub(crate) validator: Validator<V>,
}

impl<S> JmapService<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            validator: Validator::from_env(),
        }
    }
}

impl<S, V> JmapService<S, V> {
    pub fn new_with_validator(store: S, validator: Validator<V>) -> Self {
        Self { store, validator }
    }
}

async fn session_handler(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> HttpResult<SessionDocument> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    let websocket_url = session::websocket_url(&headers);
    let public_base_path = session::public_base_path(&headers);
    Ok(Json(
        service
            .session_document(
                authorization.as_deref(),
                websocket_url.as_deref(),
                Some(&public_base_path),
            )
            .await
            .map_err(http_error)?,
    ))
}

async fn api_handler(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<JmapApiRequest>,
) -> HttpResult<JmapApiResponse> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    Ok(Json(
        service
            .handle_api_request(authorization.as_deref(), request)
            .await
            .map_err(http_error)?,
    ))
}

async fn upload_handler(
    State(storage): State<Storage>,
    axum::extract::Path(account_id): axum::extract::Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    let content_type = headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let response = service
        .handle_upload(
            authorization.as_deref(),
            &account_id,
            &content_type,
            body.as_ref(),
        )
        .await
        .map_err(http_error)?;
    Ok((StatusCode::CREATED, Json(response)))
}

async fn download_handler(
    State(storage): State<Storage>,
    axum::extract::Path((account_id, blob_id, _name)): axum::extract::Path<(
        String,
        String,
        String,
    )>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    let blob = service
        .handle_download(authorization.as_deref(), &account_id, &blob_id)
        .await
        .map_err(http_error)?;
    Ok(([("content-type", blob.media_type.clone())], blob.blob_bytes))
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)> {
    let service = JmapService::new(storage);
    let authorization = authorization_header(&headers);
    let account = service
        .authenticate(authorization.as_deref())
        .await
        .map_err(http_error)?;
    Ok(ws.protocols(["jmap"]).on_upgrade(move |socket| async move {
        service.handle_websocket(socket, account).await;
    }))
}

impl<S: JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn requested_account_access(
        &self,
        account: &AuthenticatedAccount,
        requested_account_id: Option<&str>,
    ) -> Result<MailboxAccountAccess> {
        let requested_id = match requested_account_id {
            Some(value) => parse_uuid(value)?,
            None => account.account_id,
        };
        self.store
            .fetch_accessible_mailbox_accounts(account.account_id)
            .await?
            .into_iter()
            .find(|entry| entry.account_id == requested_id)
            .ok_or_else(|| anyhow!("accountId is not accessible"))
    }

    pub async fn handle_api_request(
        &self,
        authorization: Option<&str>,
        request: JmapApiRequest,
    ) -> Result<JmapApiResponse> {
        let account = self.authenticate(authorization).await?;
        self.handle_api_request_for_account(&account, request).await
    }

    pub(crate) async fn handle_api_request_for_account(
        &self,
        account: &AuthenticatedAccount,
        request: JmapApiRequest,
    ) -> Result<JmapApiResponse> {
        let declared_capabilities = request.using_capabilities;
        let mut method_responses = Vec::with_capacity(request.method_calls.len());
        let mut created_ids = HashMap::new();

        for JmapMethodCall(method_name, arguments, call_id) in request.method_calls {
            let response = if method_capability(method_name.as_str())
                .map(|capability| {
                    declared_capabilities
                        .iter()
                        .any(|declared| declared == capability)
                })
                .unwrap_or(true)
            {
                match method_name.as_str() {
                    "Mailbox/get" => self.handle_mailbox_get(account, arguments).await,
                    "Mailbox/query" => self.handle_mailbox_query(account, arguments).await,
                    "Mailbox/queryChanges" => {
                        self.handle_mailbox_query_changes(account, arguments).await
                    }
                    "Mailbox/changes" => self.handle_mailbox_changes(account, arguments).await,
                    "Mailbox/set" => {
                        self.handle_mailbox_set(account, arguments, &mut created_ids)
                            .await
                    }
                    "Email/query" => self.handle_email_query(account, arguments).await,
                    "Email/queryChanges" => {
                        self.handle_email_query_changes(account, arguments).await
                    }
                    "Email/get" => self.handle_email_get(account, arguments).await,
                    "Email/changes" => self.handle_email_changes(account, arguments).await,
                    "Email/set" => {
                        self.handle_email_set(account, arguments, &mut created_ids)
                            .await
                    }
                    "Email/copy" => {
                        self.handle_email_copy(account, arguments, &mut created_ids)
                            .await
                    }
                    "Email/import" => {
                        self.handle_email_import(account, arguments, &mut created_ids)
                            .await
                    }
                    "EmailSubmission/get" => {
                        self.handle_email_submission_get(account, arguments).await
                    }
                    "EmailSubmission/changes" => {
                        self.handle_email_submission_changes(account, arguments)
                            .await
                    }
                    "EmailSubmission/query" => {
                        self.handle_email_submission_query(account, arguments).await
                    }
                    "EmailSubmission/queryChanges" => {
                        self.handle_email_submission_query_changes(account, arguments)
                            .await
                    }
                    "EmailSubmission/set" => {
                        self.handle_email_submission_set(account, arguments, &mut created_ids)
                            .await
                    }
                    "AddressBook/get" => self.handle_address_book_get(account, arguments).await,
                    "AddressBook/query" => self.handle_address_book_query(account, arguments).await,
                    "AddressBook/queryChanges" => {
                        self.handle_address_book_query_changes(account, arguments)
                            .await
                    }
                    "AddressBook/changes" => {
                        self.handle_address_book_changes(account, arguments).await
                    }
                    "ContactCard/get" => self.handle_contact_get(account, arguments).await,
                    "ContactCard/query" => self.handle_contact_query(account, arguments).await,
                    "ContactCard/queryChanges" => {
                        self.handle_contact_query_changes(account, arguments).await
                    }
                    "ContactCard/changes" => self.handle_contact_changes(account, arguments).await,
                    "ContactCard/set" => {
                        self.handle_contact_set(account, arguments, &mut created_ids)
                            .await
                    }
                    "Calendar/get" => self.handle_calendar_get(account, arguments).await,
                    "Calendar/query" => self.handle_calendar_query(account, arguments).await,
                    "Calendar/queryChanges" => {
                        self.handle_calendar_query_changes(account, arguments).await
                    }
                    "Calendar/changes" => self.handle_calendar_changes(account, arguments).await,
                    "CalendarEvent/get" => self.handle_calendar_event_get(account, arguments).await,
                    "CalendarEvent/query" => {
                        self.handle_calendar_event_query(account, arguments).await
                    }
                    "CalendarEvent/queryChanges" => {
                        self.handle_calendar_event_query_changes(account, arguments)
                            .await
                    }
                    "CalendarEvent/changes" => {
                        self.handle_calendar_event_changes(account, arguments).await
                    }
                    "CalendarEvent/set" => {
                        self.handle_calendar_event_set(account, arguments, &mut created_ids)
                            .await
                    }
                    "TaskList/get" => self.handle_task_list_get(account, arguments).await,
                    "TaskList/changes" => self.handle_task_list_changes(account, arguments).await,
                    "TaskList/set" => self.handle_task_list_set(account, arguments).await,
                    "Task/get" => self.handle_task_get(account, arguments).await,
                    "Task/query" => self.handle_task_query(account, arguments).await,
                    "Task/queryChanges" => self.handle_task_query_changes(account, arguments).await,
                    "Task/changes" => self.handle_task_changes(account, arguments).await,
                    "Task/set" => {
                        self.handle_task_set(account, arguments, &mut created_ids)
                            .await
                    }
                    "Identity/get" => self.handle_identity_get(account, arguments).await,
                    "Identity/changes" => self.handle_identity_changes(account, arguments).await,
                    "Thread/query" => self.handle_thread_query(account, arguments).await,
                    "Thread/queryChanges" => {
                        self.handle_thread_query_changes(account, arguments).await
                    }
                    "Thread/get" => self.handle_thread_get(account, arguments).await,
                    "Thread/changes" => self.handle_thread_changes(account, arguments).await,
                    "Quota/get" => self.handle_quota_get(account, arguments).await,
                    "SearchSnippet/get" => self.handle_search_snippet_get(account, arguments).await,
                    "Blob/upload" => {
                        self.handle_blob_upload(account, arguments, &mut created_ids)
                            .await
                    }
                    "Blob/get" => self.handle_blob_get(account, arguments, &created_ids).await,
                    "Blob/lookup" => {
                        self.handle_blob_lookup(
                            account,
                            arguments,
                            &created_ids,
                            &declared_capabilities,
                        )
                        .await
                    }
                    "Blob/copy" => {
                        self.handle_blob_copy(account, arguments, &created_ids)
                            .await
                    }
                    "VacationResponse/get" => {
                        self.handle_vacation_response_get(account, arguments).await
                    }
                    "VacationResponse/set" => {
                        self.handle_vacation_response_set(account, arguments, &mut created_ids)
                            .await
                    }
                    _ => Ok(method_error("unknownMethod", "method is not supported")),
                }
            } else {
                Ok(method_error(
                    "unknownMethod",
                    "method capability is not requested",
                ))
            };

            let payload = match response {
                Ok(payload) => payload,
                Err(error) => method_error("invalidArguments", &error.to_string()),
            };
            method_responses.push(JmapMethodResponse(method_name, payload, call_id));
        }

        let accessible_accounts = self
            .store
            .fetch_accessible_mailbox_accounts(account.account_id)
            .await?;

        Ok(JmapApiResponse {
            method_responses,
            created_ids,
            session_state: session::session_state(&accessible_accounts),
        })
    }

    pub(crate) async fn object_state(&self, account_id: Uuid, data_type: &str) -> Result<String> {
        let entries = self.object_state_entries(account_id, data_type).await?;
        encode_state(account_id, data_type, entries)
    }

    pub(crate) async fn mailbox_object_state(
        &self,
        access: &MailboxAccountAccess,
    ) -> Result<String> {
        let entries = self.mailbox_object_state_entries(access).await?;
        encode_state(access.account_id, "Mailbox", entries)
    }

    pub(crate) async fn mailbox_object_state_entries(
        &self,
        access: &MailboxAccountAccess,
    ) -> Result<Vec<StateEntry>> {
        let mailboxes = self.store.fetch_jmap_mailboxes(access.account_id).await?;
        Ok(mailboxes
            .into_iter()
            .map(|mailbox| StateEntry {
                id: mailbox.id.to_string(),
                fingerprint: mailbox_state_fingerprint(&mailbox, Some(access)),
            })
            .collect())
    }

    pub(crate) async fn mail_object_state(
        &self,
        access: &MailboxAccountAccess,
        data_type: &str,
    ) -> Result<String> {
        let entries = self.mail_object_state_entries(access, data_type).await?;
        encode_state(access.account_id, data_type, entries)
    }

    pub(crate) async fn email_delivery_object_state(&self, account_id: Uuid) -> Result<String> {
        let ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
        let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
        let entries = emails
            .into_iter()
            .map(|email| StateEntry {
                id: email.id.to_string(),
                fingerprint: opaque_state_fingerprint(&email.received_at),
            })
            .collect();
        encode_state(account_id, "EmailDelivery", entries)
    }

    pub(crate) async fn email_submission_object_state(&self, account_id: Uuid) -> Result<String> {
        let entries = self
            .email_submission_object_state_entries(account_id)
            .await?;
        encode_state(account_id, "EmailSubmission", entries)
    }

    pub(crate) async fn email_submission_object_state_entries(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<StateEntry>> {
        let submissions = self
            .store
            .fetch_jmap_email_submissions(account_id, &[])
            .await?;
        Ok(submissions
            .into_iter()
            .map(|submission| StateEntry {
                id: submission.id.to_string(),
                fingerprint: email_submission_state_fingerprint(&submission),
            })
            .collect())
    }

    pub(crate) async fn identity_object_state(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<String> {
        let entries = self
            .identity_object_state_entries(principal_account_id, target_account_id)
            .await?;
        encode_state(target_account_id, "Identity", entries)
    }

    pub(crate) async fn identity_object_state_entries(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<Vec<StateEntry>> {
        let identities = self
            .store
            .fetch_sender_identities(principal_account_id, target_account_id)
            .await?;
        Ok(identities
            .into_iter()
            .map(|identity| StateEntry {
                id: identity.id.clone(),
                fingerprint: identity_state_fingerprint(&identity),
            })
            .collect())
    }

    pub(crate) async fn mail_object_state_entries(
        &self,
        access: &MailboxAccountAccess,
        data_type: &str,
    ) -> Result<Vec<StateEntry>> {
        self.mail_object_state_entries_with_bcc(access.account_id, data_type, access.is_owned)
            .await
    }

    async fn mail_object_state_entries_with_bcc(
        &self,
        account_id: Uuid,
        data_type: &str,
        include_bcc: bool,
    ) -> Result<Vec<StateEntry>> {
        match data_type {
            "Email" => {
                let ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
                let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
                Ok(emails
                    .into_iter()
                    .map(|email| StateEntry {
                        id: email.id.to_string(),
                        fingerprint: email_state_fingerprint(&email, include_bcc),
                    })
                    .collect())
            }
            "Thread" => {
                let ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
                let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
                let mut threads: HashMap<Uuid, Vec<String>> = HashMap::new();
                for email in emails {
                    threads.entry(email.thread_id).or_default().push(format!(
                        "{}:{}",
                        email.id,
                        email_state_fingerprint(&email, include_bcc)
                    ));
                }
                let mut entries = threads
                    .into_iter()
                    .map(|(thread_id, mut fingerprints)| {
                        fingerprints.sort();
                        StateEntry {
                            id: thread_id.to_string(),
                            fingerprint: opaque_state_fingerprint(&fingerprints.join("|")),
                        }
                    })
                    .collect::<Vec<_>>();
                entries.sort_by(|left, right| left.id.cmp(&right.id));
                Ok(entries)
            }
            _ => Ok(Vec::new()),
        }
    }

    pub(crate) async fn object_state_entries(
        &self,
        account_id: Uuid,
        data_type: &str,
    ) -> Result<Vec<StateEntry>> {
        match data_type {
            "Mailbox" => {
                let mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;
                Ok(mailboxes
                    .into_iter()
                    .map(|mailbox| StateEntry {
                        id: mailbox.id.to_string(),
                        fingerprint: mailbox_state_fingerprint(&mailbox, None),
                    })
                    .collect())
            }
            "Email" | "Thread" => {
                self.mail_object_state_entries_with_bcc(account_id, data_type, true)
                    .await
            }
            "AddressBook" => {
                let collections = self
                    .store
                    .fetch_accessible_contact_collections(account_id)
                    .await?;
                Ok(collections
                    .into_iter()
                    .map(|collection| StateEntry {
                        id: collection.id.clone(),
                        fingerprint: collection_state_fingerprint(&collection),
                    })
                    .collect())
            }
            "ContactCard" => {
                let contacts = self.store.fetch_accessible_contacts(account_id).await?;
                Ok(contacts
                    .into_iter()
                    .map(|contact| StateEntry {
                        id: contact.id.to_string(),
                        fingerprint: contact_state_fingerprint(&contact),
                    })
                    .collect())
            }
            "Calendar" => {
                let collections = self
                    .store
                    .fetch_accessible_calendar_collections(account_id)
                    .await?;
                Ok(collections
                    .into_iter()
                    .map(|collection| StateEntry {
                        id: collection.id.clone(),
                        fingerprint: collection_state_fingerprint(&collection),
                    })
                    .collect())
            }
            "CalendarEvent" => {
                let events = self.store.fetch_accessible_events(account_id).await?;
                Ok(events
                    .into_iter()
                    .map(|event| StateEntry {
                        id: event.id.to_string(),
                        fingerprint: event_state_fingerprint(&event),
                    })
                    .collect())
            }
            "TaskList" => {
                let task_lists = self.store.fetch_jmap_task_lists(account_id).await?;
                Ok(task_lists
                    .into_iter()
                    .map(|task_list| StateEntry {
                        id: task_list.id.to_string(),
                        fingerprint: task_list_state_fingerprint(&task_list),
                    })
                    .collect())
            }
            "Task" => {
                let tasks = self.store.fetch_jmap_tasks(account_id).await?;
                Ok(tasks
                    .into_iter()
                    .map(|task| StateEntry {
                        id: task.id.to_string(),
                        fingerprint: task_state_fingerprint(&task),
                    })
                    .collect())
            }
            _ => Ok(Vec::new()),
        }
    }

    pub(crate) async fn handle_upload(
        &self,
        authorization: Option<&str>,
        account_id: &str,
        media_type: &str,
        body: &[u8],
    ) -> Result<Value> {
        let account = self.authenticate(authorization).await?;
        let requested_account = self
            .requested_account_access(&account, Some(account_id))
            .await?;
        let requested_account_id = requested_account.account_id;
        if !requested_account.is_owned && !requested_account.may_write {
            bail!("accountId is read-only");
        }
        if body.len() as u64 > MAX_SIZE_UPLOAD {
            bail!("JMAP upload exceeds maxSizeUpload");
        }
        let outcome = self.validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::JmapUpload,
                declared_mime: Some(media_type.to_string()),
                filename: None,
                expected_kind: ExpectedKind::Any,
            },
            body,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "JMAP upload blocked by Magika validation: {}",
                outcome.reason
            );
        }
        let blob = self
            .store
            .save_jmap_upload_blob(requested_account_id, media_type, body)
            .await?;

        Ok(json!({
            "accountId": requested_account_id.to_string(),
            "blobId": blob.id.to_string(),
            "type": blob.media_type,
            "size": blob.octet_size,
        }))
    }

    pub(crate) async fn handle_download(
        &self,
        authorization: Option<&str>,
        account_id: &str,
        blob_id: &str,
    ) -> Result<JmapUploadBlob> {
        let account = self.authenticate(authorization).await?;
        let requested_account = self
            .requested_account_access(&account, Some(account_id))
            .await?;
        self.resolve_download_blob(&requested_account, blob_id)
            .await
    }

    pub(crate) async fn resolve_download_blob(
        &self,
        requested_account: &MailboxAccountAccess,
        blob_id: &str,
    ) -> Result<JmapUploadBlob> {
        self.resolve_download_blob_with_bcc(requested_account, blob_id, requested_account.is_owned)
            .await
    }

    pub(crate) async fn resolve_download_blob_with_bcc(
        &self,
        requested_account: &MailboxAccountAccess,
        blob_id: &str,
        include_bcc: bool,
    ) -> Result<JmapUploadBlob> {
        let requested_account_id = requested_account.account_id;
        match JmapBlobId::parse(blob_id)? {
            JmapBlobId::Upload(blob_id) => self
                .store
                .fetch_jmap_upload_blob(requested_account_id, blob_id)
                .await?
                .ok_or_else(|| anyhow!("blob not found")),
            JmapBlobId::Message(message_id) => {
                let email = self
                    .store
                    .fetch_jmap_emails(requested_account_id, &[message_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("blob not found"))?;
                let blob_bytes = message_rfc822_bytes(&email, include_bcc);
                Ok(JmapUploadBlob {
                    id: message_id,
                    account_id: requested_account_id,
                    media_type: "message/rfc822".to_string(),
                    octet_size: blob_bytes.len() as u64,
                    blob_bytes,
                })
            }
            JmapBlobId::Opaque(_) => Err(anyhow!("blob not found")),
        }
    }

    pub(crate) async fn authenticate(
        &self,
        authorization: Option<&str>,
    ) -> Result<AuthenticatedAccount> {
        let token = bearer_token(authorization).ok_or_else(|| anyhow!("missing bearer token"))?;
        self.store
            .fetch_account_session(token)
            .await?
            .ok_or_else(|| anyhow!("invalid or expired account session"))
    }
}

fn method_capability(method_name: &str) -> Option<&'static str> {
    match method_name {
        "Mailbox/get"
        | "Mailbox/query"
        | "Mailbox/queryChanges"
        | "Mailbox/changes"
        | "Mailbox/set"
        | "Email/query"
        | "Email/queryChanges"
        | "Email/get"
        | "Email/changes"
        | "Email/set"
        | "Email/copy"
        | "Email/import"
        | "Thread/query"
        | "Thread/queryChanges"
        | "Thread/get"
        | "Thread/changes"
        | "Quota/get"
        | "SearchSnippet/get" => Some(JMAP_MAIL_CAPABILITY),
        "EmailSubmission/get"
        | "EmailSubmission/changes"
        | "EmailSubmission/query"
        | "EmailSubmission/queryChanges"
        | "EmailSubmission/set"
        | "Identity/get"
        | "Identity/changes" => Some(JMAP_SUBMISSION_CAPABILITY),
        "AddressBook/get"
        | "AddressBook/query"
        | "AddressBook/queryChanges"
        | "AddressBook/changes"
        | "ContactCard/get"
        | "ContactCard/query"
        | "ContactCard/queryChanges"
        | "ContactCard/changes"
        | "ContactCard/set" => Some(JMAP_CONTACTS_CAPABILITY),
        "Calendar/get"
        | "Calendar/query"
        | "Calendar/queryChanges"
        | "Calendar/changes"
        | "CalendarEvent/get"
        | "CalendarEvent/query"
        | "CalendarEvent/queryChanges"
        | "CalendarEvent/changes"
        | "CalendarEvent/set" => Some(JMAP_CALENDARS_CAPABILITY),
        "TaskList/get" | "TaskList/changes" | "TaskList/set" | "Task/get" | "Task/query"
        | "Task/queryChanges" | "Task/changes" | "Task/set" => Some(JMAP_TASKS_CAPABILITY),
        "Blob/upload" | "Blob/get" | "Blob/lookup" => Some(JMAP_BLOB_CAPABILITY),
        "Blob/copy" => Some(JMAP_CORE_CAPABILITY),
        "VacationResponse/get" | "VacationResponse/set" => Some(JMAP_VACATION_RESPONSE_CAPABILITY),
        _ => None,
    }
}

fn authorization_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn bearer_token(authorization: Option<&str>) -> Option<&str> {
    authorization
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

pub(crate) fn collection_state_fingerprint(collection: &CollaborationCollection) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        collection.kind,
        collection.owner_account_id,
        collection.owner_email,
        collection.owner_display_name,
        collection.display_name,
        collection.is_owned,
        collection.rights.may_read,
        collection.rights.may_write,
        collection.rights.may_delete,
        collection.rights.may_share
    ))
}

fn email_submission_state_fingerprint(submission: &JmapEmailSubmission) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}",
        submission.email_id,
        submission.thread_id,
        submission.identity_id,
        submission.identity_email,
        submission.envelope_mail_from,
        submission.envelope_rcpt_to.join(","),
        submission.send_at,
        submission.undo_status,
        submission.delivery_status
    ))
}

fn identity_state_fingerprint(identity: &SenderIdentity) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}",
        identity.owner_account_id,
        identity.email,
        identity.display_name,
        identity.authorization_kind,
        identity.sender_address.as_deref().unwrap_or_default(),
        identity.sender_display.as_deref().unwrap_or_default()
    ))
}

fn mailbox_state_fingerprint(
    mailbox: &JmapMailbox,
    access: Option<&MailboxAccountAccess>,
) -> String {
    let is_drafts = mailbox.role == "drafts";
    let (may_read, may_write, may_draft, may_submit) = access
        .map(|access| {
            let may_write = crate::mailboxes::mailbox_account_may_write(access);
            let may_submit = crate::mailboxes::mailbox_account_may_submit(access);
            (
                access.may_read,
                may_write,
                is_drafts && may_write && may_submit,
                is_drafts && may_submit,
            )
        })
        .unwrap_or((true, true, is_drafts, false));
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        mailbox.role,
        mailbox.name,
        mailbox.sort_order,
        mailbox.total_emails,
        mailbox.unread_emails,
        may_read,
        may_draft,
        may_draft,
        may_write,
        may_write,
        may_submit,
    ))
}

fn contact_state_fingerprint(contact: &AccessibleContact) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        contact.collection_id,
        contact.owner_account_id,
        contact.owner_email,
        contact.owner_display_name,
        contact.name,
        contact.role,
        contact.email,
        contact.phone,
        contact.team,
        contact.notes,
        contact.rights.may_write,
        contact.rights.may_delete
    ))
}

fn event_state_fingerprint(event: &AccessibleEvent) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        event.collection_id,
        event.owner_account_id,
        event.owner_email,
        event.owner_display_name,
        event.date,
        event.time,
        event.time_zone,
        event.duration_minutes,
        event.recurrence_rule,
        event.title,
        event.location,
        event.attendees,
        event.attendees_json,
        event.notes,
        event.rights.may_write
    ))
}

fn task_state_fingerprint(task: &ClientTask) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        task.task_list_id,
        task.title,
        task.description,
        task.status,
        task.due_at.as_deref().unwrap_or_default(),
        task.completed_at.as_deref().unwrap_or_default(),
        task.sort_order,
        task.updated_at
    ))
}

fn task_list_state_fingerprint(task_list: &ClientTaskList) -> String {
    opaque_state_fingerprint(&format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        task_list.owner_account_id,
        task_list.owner_email,
        task_list.owner_display_name,
        task_list.is_owned,
        task_list.rights.may_read,
        task_list.rights.may_write,
        task_list.rights.may_delete,
        task_list.rights.may_share,
        task_list.name,
        task_list.role.clone().unwrap_or_default(),
        task_list.sort_order,
        task_list.updated_at
    ))
}

fn email_state_fingerprint(email: &JmapEmail, include_bcc: bool) -> String {
    opaque_state_fingerprint(
        &(format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            email.thread_id,
            email.mailbox_id,
            email.mailbox_role,
            email.mailbox_name,
            email.received_at,
            email.sent_at.as_deref().unwrap_or_default(),
            email.from_display.as_deref().unwrap_or_default(),
            email.from_address,
            format_addresses(&email.to),
            format_addresses(&email.cc),
            include_bcc
                .then(|| format_addresses(&email.bcc))
                .unwrap_or_default(),
            email.subject,
            email.preview,
            email.unread,
            email.flagged,
            email.delivery_status,
        ) + &format!(
            "|{}|{}|{}|{}|{}",
            email.body_text,
            email.body_html_sanitized.as_deref().unwrap_or_default(),
            email.has_attachments,
            email.size_octets,
            email.internet_message_id.as_deref().unwrap_or_default(),
        )),
    )
}

pub(crate) fn opaque_state_fingerprint(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

pub(crate) fn trim_snippet(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        normalized
    } else {
        normalized.chars().take(max_chars).collect::<String>()
    }
}
