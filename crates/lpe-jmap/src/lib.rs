use anyhow::{anyhow, bail, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use lpe_magika::{ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator};
use lpe_storage::{
    mail::parse_rfc822_message, AuditEntryInput, AuthenticatedAccount, ClientContact, ClientEvent,
    JmapEmail, JmapEmailAddress, JmapEmailSubmission, JmapImportedEmailInput, JmapMailbox,
    JmapMailboxCreateInput, JmapMailboxUpdateInput, JmapQuota, JmapUploadBlob, SavedDraftMessage,
    Storage, SubmitMessageInput, SubmittedRecipientInput, UpsertClientContactInput,
    UpsertClientEventInput,
};
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[cfg(test)]
use lpe_storage::{JmapEmailQuery, SubmittedMessage};

mod protocol;
mod store;

use crate::protocol::{
    AddressBookGetArguments, AddressBookQueryArguments, CalendarEventGetArguments,
    CalendarEventQueryArguments, CalendarEventQueryFilter, CalendarEventSetArguments,
    CalendarGetArguments, CalendarQueryArguments, ChangesArguments, ContactCardGetArguments,
    ContactCardQueryArguments, ContactCardQueryFilter, ContactCardSetArguments, DraftMutation,
    EmailAddressInput, EmailCopyArguments, EmailGetArguments, EmailImportArguments,
    EmailQueryArguments, EmailQuerySort, EmailSetArguments, EmailSubmissionGetArguments,
    EmailSubmissionSetArguments, EntityQuerySort, IdentityGetArguments, JmapApiRequest,
    JmapApiResponse, JmapMethodCall, JmapMethodResponse, MailboxCreateInput, MailboxGetArguments,
    MailboxQueryArguments, MailboxSetArguments, MailboxUpdateInput, QuotaGetArguments,
    SearchSnippetGetArguments, SessionAccount, SessionDocument, ThreadGetArguments,
};
use crate::store::JmapStore;

const JMAP_CORE_CAPABILITY: &str = "urn:ietf:params:jmap:core";
const JMAP_MAIL_CAPABILITY: &str = "urn:ietf:params:jmap:mail";
const JMAP_SUBMISSION_CAPABILITY: &str = "urn:ietf:params:jmap:submission";
const JMAP_CONTACTS_CAPABILITY: &str = "urn:ietf:params:jmap:contacts";
const JMAP_CALENDARS_CAPABILITY: &str = "urn:ietf:params:jmap:calendars";
const SESSION_STATE: &str = "mvp-1";
const MAX_QUERY_LIMIT: u64 = 250;
const DEFAULT_GET_LIMIT: u64 = 100;
const DEFAULT_ADDRESS_BOOK_ID: &str = "default";
const DEFAULT_CALENDAR_ID: &str = "default";

type HttpResult<T> = std::result::Result<Json<T>, (StatusCode, String)>;

pub fn router() -> Router<Storage> {
    Router::new()
        .route("/session", get(session_handler))
        .route("/api", post(api_handler))
        .route("/upload/{account_id}", post(upload_handler))
        .route(
            "/download/{account_id}/{blob_id}/{name}",
            get(download_handler),
        )
}

#[derive(Clone)]
pub struct JmapService<S, V = lpe_magika::SystemDetector> {
    store: S,
    validator: Validator<V>,
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
    Ok(Json(
        service
            .session_document(authorization.as_deref())
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

impl<S: JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub async fn session_document(&self, authorization: Option<&str>) -> Result<SessionDocument> {
        let account = self.authenticate(authorization).await?;
        let account_id = account.account_id.to_string();
        let capabilities = session_capabilities();
        let mut accounts = HashMap::new();
        accounts.insert(
            account_id.clone(),
            SessionAccount {
                name: account.email.clone(),
                is_personal: true,
                is_read_only: false,
                account_capabilities: capabilities.clone(),
            },
        );

        let mut primary_accounts = HashMap::new();
        primary_accounts.insert(JMAP_CORE_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_MAIL_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_SUBMISSION_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_CONTACTS_CAPABILITY.to_string(), account_id.clone());
        primary_accounts.insert(JMAP_CALENDARS_CAPABILITY.to_string(), account_id.clone());

        Ok(SessionDocument {
            capabilities,
            accounts,
            primary_accounts,
            username: account.email,
            api_url: "/jmap/api".to_string(),
            download_url: "/jmap/download/{accountId}/{blobId}/{name}".to_string(),
            upload_url: "/jmap/upload/{accountId}".to_string(),
            event_source_url: None,
            state: SESSION_STATE.to_string(),
        })
    }

    pub async fn handle_api_request(
        &self,
        authorization: Option<&str>,
        request: JmapApiRequest,
    ) -> Result<JmapApiResponse> {
        let _declared_capabilities = request.using_capabilities;
        let account = self.authenticate(authorization).await?;
        let mut method_responses = Vec::with_capacity(request.method_calls.len());
        let mut created_ids = HashMap::new();

        for JmapMethodCall(method_name, arguments, call_id) in request.method_calls {
            let response = match method_name.as_str() {
                "Mailbox/get" => self.handle_mailbox_get(&account, arguments).await,
                "Mailbox/query" => self.handle_mailbox_query(&account, arguments).await,
                "Mailbox/changes" => self.handle_mailbox_changes(&account, arguments).await,
                "Mailbox/set" => {
                    self.handle_mailbox_set(&account, arguments, &mut created_ids)
                        .await
                }
                "Email/query" => self.handle_email_query(&account, arguments).await,
                "Email/get" => self.handle_email_get(&account, arguments).await,
                "Email/changes" => self.handle_email_changes(&account, arguments).await,
                "Email/set" => {
                    self.handle_email_set(&account, arguments, &mut created_ids)
                        .await
                }
                "Email/copy" => {
                    self.handle_email_copy(&account, arguments, &mut created_ids)
                        .await
                }
                "Email/import" => {
                    self.handle_email_import(&account, arguments, &mut created_ids)
                        .await
                }
                "EmailSubmission/get" => {
                    self.handle_email_submission_get(&account, arguments).await
                }
                "EmailSubmission/set" => {
                    self.handle_email_submission_set(&account, arguments, &mut created_ids)
                        .await
                }
                "AddressBook/get" => self.handle_address_book_get(&account, arguments).await,
                "AddressBook/query" => self.handle_address_book_query(&account, arguments).await,
                "AddressBook/changes" => {
                    self.handle_address_book_changes(&account, arguments).await
                }
                "ContactCard/get" => self.handle_contact_get(&account, arguments).await,
                "ContactCard/query" => self.handle_contact_query(&account, arguments).await,
                "ContactCard/changes" => self.handle_contact_changes(&account, arguments).await,
                "ContactCard/set" => {
                    self.handle_contact_set(&account, arguments, &mut created_ids)
                        .await
                }
                "Calendar/get" => self.handle_calendar_get(&account, arguments).await,
                "Calendar/query" => self.handle_calendar_query(&account, arguments).await,
                "Calendar/changes" => self.handle_calendar_changes(&account, arguments).await,
                "CalendarEvent/get" => self.handle_calendar_event_get(&account, arguments).await,
                "CalendarEvent/query" => {
                    self.handle_calendar_event_query(&account, arguments).await
                }
                "CalendarEvent/changes" => {
                    self.handle_calendar_event_changes(&account, arguments)
                        .await
                }
                "CalendarEvent/set" => {
                    self.handle_calendar_event_set(&account, arguments, &mut created_ids)
                        .await
                }
                "Identity/get" => self.handle_identity_get(&account, arguments).await,
                "Thread/get" => self.handle_thread_get(&account, arguments).await,
                "Thread/changes" => self.handle_thread_changes(&account, arguments).await,
                "Quota/get" => self.handle_quota_get(&account, arguments).await,
                "SearchSnippet/get" => self.handle_search_snippet_get(&account, arguments).await,
                _ => Ok(method_error("unknownMethod", "method is not supported")),
            };

            let payload = match response {
                Ok(payload) => payload,
                Err(error) => method_error("invalidArguments", &error.to_string()),
            };
            method_responses.push(JmapMethodResponse(method_name, payload, call_id));
        }

        Ok(JmapApiResponse {
            method_responses,
            created_ids,
            session_state: SESSION_STATE.to_string(),
        })
    }

    async fn handle_mailbox_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: MailboxGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = mailbox_properties(arguments.properties);
        let mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;

        let requested_ids = parse_uuid_list(arguments.ids)?;
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();

        let list = mailboxes
            .iter()
            .filter(|mailbox| requested_ids.is_none() || requested_set.contains(&mailbox.id))
            .map(|mailbox| mailbox_to_value(mailbox, &properties))
            .collect::<Vec<_>>();

        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !mailboxes.iter().any(|mailbox| mailbox.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_mailbox_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: MailboxQueryArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;
        mailboxes.sort_by_key(|mailbox| (mailbox.sort_order, mailbox.name.to_lowercase()));
        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = mailboxes
            .iter()
            .skip(position)
            .take(limit)
            .map(|mailbox| mailbox.id.to_string())
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": SESSION_STATE,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": mailboxes.len(),
        }))
    }

    async fn handle_mailbox_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let ids = self.store.fetch_jmap_mailbox_ids(account_id).await?;
        Ok(changes_response(
            account_id,
            &arguments.since_state,
            arguments.max_changes,
            ids.into_iter().map(|id| id.to_string()).collect(),
        ))
    }

    async fn handle_mailbox_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: MailboxSetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_mailbox_create(value) {
                    Ok(input) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-mailbox-create".to_string(),
                            subject: creation_id.clone(),
                        };
                        match self
                            .store
                            .create_jmap_mailbox(
                                JmapMailboxCreateInput {
                                    account_id,
                                    name: input.name,
                                    sort_order: input.sort_order,
                                },
                                audit,
                            )
                            .await
                        {
                            Ok(mailbox) => {
                                created_ids.insert(creation_id.clone(), mailbox.id.to_string());
                                created.insert(creation_id, json!({"id": mailbox.id.to_string()}));
                            }
                            Err(error) => {
                                not_created.insert(creation_id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                match parse_uuid(&id).and_then(|mailbox_id| {
                    parse_mailbox_update(value).map(|input| (mailbox_id, input))
                }) {
                    Ok((mailbox_id, input)) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-mailbox-update".to_string(),
                            subject: id.clone(),
                        };
                        match self
                            .store
                            .update_jmap_mailbox(
                                JmapMailboxUpdateInput {
                                    account_id,
                                    mailbox_id,
                                    name: input.name,
                                    sort_order: input.sort_order,
                                },
                                audit,
                            )
                            .await
                        {
                            Ok(_) => {
                                updated.insert(id, Value::Object(Map::new()));
                            }
                            Err(error) => {
                                not_updated.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(mailbox_id) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-mailbox-destroy".to_string(),
                            subject: id.clone(),
                        };
                        match self
                            .store
                            .destroy_jmap_mailbox(account_id, mailbox_id, audit)
                            .await
                        {
                            Ok(()) => destroyed.push(Value::String(id)),
                            Err(error) => {
                                not_destroyed.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    async fn handle_address_book_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: AddressBookGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = address_book_properties(arguments.properties);
        let requested_ids = arguments.ids.unwrap_or_default();
        let include_default = requested_ids.is_empty()
            || requested_ids.iter().any(|id| id == DEFAULT_ADDRESS_BOOK_ID);
        let list = if include_default {
            vec![address_book_to_value(&properties)]
        } else {
            Vec::new()
        };
        let not_found = requested_ids
            .into_iter()
            .filter(|id| id != DEFAULT_ADDRESS_BOOK_ID)
            .map(Value::String)
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_address_book_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: AddressBookQueryArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let position = arguments.position.unwrap_or(0).min(1) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = [DEFAULT_ADDRESS_BOOK_ID.to_string()]
            .into_iter()
            .skip(position)
            .take(limit)
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": SESSION_STATE,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": 1,
        }))
    }

    async fn handle_address_book_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        Ok(changes_response(
            account_id,
            &arguments.since_state,
            arguments.max_changes,
            vec![DEFAULT_ADDRESS_BOOK_ID.to_string()],
        ))
    }

    async fn handle_contact_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ContactCardGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = contact_properties(arguments.properties);
        let requested_ids = parse_uuid_list(arguments.ids)?;
        let contacts = if let Some(ids) = requested_ids.as_ref() {
            self.store
                .fetch_client_contacts_by_ids(account_id, &ids)
                .await?
        } else {
            self.store.fetch_client_contacts(account_id).await?
        };
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();

        let list = contacts
            .iter()
            .filter(|contact| requested_ids.is_none() || requested_set.contains(&contact.id))
            .map(|contact| contact_to_value(contact, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !contacts.iter().any(|contact| contact.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_contact_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ContactCardQueryArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_entity_sort(arguments.sort.as_deref(), "name", true)?;
        validate_contact_filter(arguments.filter.as_ref())?;

        let mut contacts = self.store.fetch_client_contacts(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            contacts.retain(|contact| contact_matches_filter(contact, filter));
        }
        contacts.sort_by_key(|contact| contact.name.to_lowercase());

        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = contacts
            .iter()
            .skip(position)
            .take(limit)
            .map(|contact| contact.id.to_string())
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": SESSION_STATE,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": contacts.len(),
        }))
    }

    async fn handle_contact_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let ids = self
            .store
            .fetch_client_contacts(account_id)
            .await?
            .into_iter()
            .map(|contact| contact.id.to_string())
            .collect::<Vec<_>>();
        Ok(changes_response(
            account_id,
            &arguments.since_state,
            arguments.max_changes,
            ids,
        ))
    }

    async fn handle_contact_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: ContactCardSetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_contact_input(None, account_id, value) {
                    Ok(input) => match self.store.upsert_client_contact(input).await {
                        Ok(contact) => {
                            created_ids.insert(creation_id.clone(), contact.id.to_string());
                            created.insert(creation_id, json!({ "id": contact.id.to_string() }));
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                match parse_uuid(&id)
                    .and_then(|contact_id| parse_contact_input(Some(contact_id), account_id, value))
                {
                    Ok(input) => match self.store.upsert_client_contact(input).await {
                        Ok(_) => {
                            updated.insert(id, Value::Object(Map::new()));
                        }
                        Err(error) => {
                            not_updated.insert(id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(contact_id) => match self
                        .store
                        .delete_client_contact(account_id, contact_id)
                        .await
                    {
                        Ok(()) => destroyed.push(Value::String(id)),
                        Err(error) => {
                            not_destroyed.insert(id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    async fn handle_calendar_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: CalendarGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = calendar_properties(arguments.properties);
        let requested_ids = arguments.ids.unwrap_or_default();
        let include_default =
            requested_ids.is_empty() || requested_ids.iter().any(|id| id == DEFAULT_CALENDAR_ID);
        let list = if include_default {
            vec![calendar_to_value(&properties)]
        } else {
            Vec::new()
        };
        let not_found = requested_ids
            .into_iter()
            .filter(|id| id != DEFAULT_CALENDAR_ID)
            .map(Value::String)
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_calendar_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: CalendarQueryArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let position = arguments.position.unwrap_or(0).min(1) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = [DEFAULT_CALENDAR_ID.to_string()]
            .into_iter()
            .skip(position)
            .take(limit)
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": SESSION_STATE,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": 1,
        }))
    }

    async fn handle_calendar_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        Ok(changes_response(
            account_id,
            &arguments.since_state,
            arguments.max_changes,
            vec![DEFAULT_CALENDAR_ID.to_string()],
        ))
    }

    async fn handle_calendar_event_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: CalendarEventGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = calendar_event_properties(arguments.properties);
        let requested_ids = parse_uuid_list(arguments.ids)?;
        let events = if let Some(ids) = requested_ids.as_ref() {
            self.store
                .fetch_client_events_by_ids(account_id, ids)
                .await?
        } else {
            self.store.fetch_client_events(account_id).await?
        };
        let requested_set = requested_ids
            .as_ref()
            .map(|ids| ids.iter().copied().collect::<HashSet<Uuid>>())
            .unwrap_or_default();
        let list = events
            .iter()
            .filter(|event| requested_ids.is_none() || requested_set.contains(&event.id))
            .map(|event| calendar_event_to_value(event, &properties))
            .collect::<Vec<_>>();
        let not_found = requested_ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !events.iter().any(|event| event.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_calendar_event_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: CalendarEventQueryArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_entity_sort(arguments.sort.as_deref(), "start", true)?;
        validate_calendar_event_filter(arguments.filter.as_ref())?;

        let mut events = self.store.fetch_client_events(account_id).await?;
        if let Some(filter) = arguments.filter.as_ref() {
            events.retain(|event| event_matches_filter(event, filter));
        }
        events.sort_by_key(calendar_event_sort_key);

        let position = arguments.position.unwrap_or(0) as usize;
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT) as usize;
        let ids = events
            .iter()
            .skip(position)
            .take(limit)
            .map(|event| event.id.to_string())
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": SESSION_STATE,
            "canCalculateChanges": true,
            "position": position,
            "ids": ids,
            "total": events.len(),
        }))
    }

    async fn handle_calendar_event_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let ids = self
            .store
            .fetch_client_events(account_id)
            .await?
            .into_iter()
            .map(|event| event.id.to_string())
            .collect::<Vec<_>>();
        Ok(changes_response(
            account_id,
            &arguments.since_state,
            arguments.max_changes,
            ids,
        ))
    }

    async fn handle_calendar_event_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: CalendarEventSetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_calendar_event_input(None, account_id, value) {
                    Ok(input) => match self.store.upsert_client_event(input).await {
                        Ok(event) => {
                            created_ids.insert(creation_id.clone(), event.id.to_string());
                            created.insert(creation_id, json!({ "id": event.id.to_string() }));
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                match parse_uuid(&id).and_then(|event_id| {
                    parse_calendar_event_input(Some(event_id), account_id, value)
                }) {
                    Ok(input) => match self.store.upsert_client_event(input).await {
                        Ok(_) => {
                            updated.insert(id, Value::Object(Map::new()));
                        }
                        Err(error) => {
                            not_updated.insert(id, set_error(&error.to_string()));
                        }
                    },
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(event_id) => {
                        match self.store.delete_client_event(account_id, event_id).await {
                            Ok(()) => destroyed.push(Value::String(id)),
                            Err(error) => {
                                not_destroyed.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    async fn handle_email_query(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: EmailQueryArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        validate_query_sort(arguments.sort.as_deref())?;

        let mailbox_id = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.in_mailbox.as_deref())
            .map(|value| parse_uuid(&value))
            .transpose()?;
        let search_text = arguments
            .filter
            .as_ref()
            .and_then(|filter| filter.text.as_deref());
        let position = arguments.position.unwrap_or(0);
        let limit = arguments
            .limit
            .unwrap_or(DEFAULT_GET_LIMIT)
            .min(MAX_QUERY_LIMIT);
        let query = self
            .store
            .query_jmap_email_ids(account_id, mailbox_id, search_text, position, limit)
            .await?;

        Ok(json!({
            "accountId": account_id.to_string(),
            "queryState": SESSION_STATE,
            "canCalculateChanges": false,
            "position": position,
            "ids": query.ids.into_iter().map(|id| id.to_string()).collect::<Vec<_>>(),
            "total": query.total,
        }))
    }

    async fn handle_email_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: EmailGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = email_properties(arguments.properties);

        let ids = match parse_uuid_list(arguments.ids)? {
            Some(ids) => ids,
            None => {
                self.store
                    .query_jmap_email_ids(account_id, None, None, 0, DEFAULT_GET_LIMIT)
                    .await?
                    .ids
            }
        };

        let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
        let not_found = ids
            .iter()
            .filter(|id| !emails.iter().any(|email| email.id == **id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": emails.iter().map(|email| email_to_value(email, &properties)).collect::<Vec<_>>(),
            "notFound": not_found,
        }))
    }

    async fn handle_email_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
        Ok(changes_response(
            account_id,
            &arguments.since_state,
            arguments.max_changes,
            ids.into_iter().map(|id| id.to_string()).collect(),
        ))
    }

    async fn handle_email_copy(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailCopyArguments = serde_json::from_value(arguments)?;
        let from_account_id = requested_account_id(Some(&arguments.from_account_id), account)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        if from_account_id != account_id {
            bail!("cross-account Email/copy is not supported");
        }

        let mut created = Map::new();
        let mut not_created = Map::new();
        for (creation_id, value) in arguments.create {
            match parse_email_copy(value, created_ids) {
                Ok((email_id, mailbox_id)) => {
                    let audit = AuditEntryInput {
                        actor: account.email.clone(),
                        action: "jmap-email-copy".to_string(),
                        subject: creation_id.clone(),
                    };
                    match self
                        .store
                        .copy_jmap_email(account_id, email_id, mailbox_id, audit)
                        .await
                    {
                        Ok(email) => {
                            created_ids.insert(creation_id.clone(), email.id.to_string());
                            created.insert(
                                creation_id,
                                json!({
                                    "id": email.id.to_string(),
                                    "blobId": format!("message:{}", email.id),
                                    "threadId": email.thread_id.to_string(),
                                }),
                            );
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    }
                }
                Err(error) => {
                    not_created.insert(creation_id, set_error(&error.to_string()));
                }
            }
        }

        Ok(json!({
            "fromAccountId": from_account_id.to_string(),
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
        }))
    }

    async fn handle_email_import(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailImportArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut created = Map::new();
        let mut not_created = Map::new();

        for (creation_id, value) in arguments.emails {
            match self
                .parse_email_import(account, account_id, value, created_ids)
                .await
            {
                Ok(input) => {
                    let audit = AuditEntryInput {
                        actor: account.email.clone(),
                        action: "jmap-email-import".to_string(),
                        subject: creation_id.clone(),
                    };
                    match self.store.import_jmap_email(input, audit).await {
                        Ok(email) => {
                            created_ids.insert(creation_id.clone(), email.id.to_string());
                            created.insert(
                                creation_id,
                                json!({
                                    "id": email.id.to_string(),
                                    "blobId": format!("message:{}", email.id),
                                    "threadId": email.thread_id.to_string(),
                                }),
                            );
                        }
                        Err(error) => {
                            not_created.insert(creation_id, set_error(&error.to_string()));
                        }
                    }
                }
                Err(error) => {
                    not_created.insert(creation_id, set_error(&error.to_string()));
                }
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
        }))
    }

    async fn handle_email_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailSetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut created = Map::new();
        let mut not_created = Map::new();
        let mut updated = Map::new();
        let mut not_updated = Map::new();
        let mut destroyed = Vec::new();
        let mut not_destroyed = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match self
                    .create_draft(account, account_id, value, creation_id.as_str())
                    .await
                {
                    Ok(saved) => {
                        created_ids.insert(creation_id.clone(), saved.message_id.to_string());
                        created.insert(
                            creation_id,
                            json!({
                                "id": saved.message_id.to_string(),
                                "blobId": format!("draft:{}", saved.message_id),
                            }),
                        );
                    }
                    Err(error) => {
                        not_created.insert(creation_id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(update) = arguments.update {
            for (id, value) in update {
                match self.update_draft(account, account_id, &id, value).await {
                    Ok(_) => {
                        updated.insert(id, Value::Object(Map::new()));
                    }
                    Err(error) => {
                        not_updated.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        if let Some(ids) = arguments.destroy {
            for id in ids {
                match parse_uuid(&id) {
                    Ok(message_id) => {
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-email-draft-delete".to_string(),
                            subject: id.clone(),
                        };
                        match self
                            .store
                            .delete_draft_message(account_id, message_id, audit)
                            .await
                        {
                            Ok(()) => destroyed.push(Value::String(id)),
                            Err(error) => {
                                not_destroyed.insert(id, set_error(&error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        not_destroyed.insert(id, set_error(&error.to_string()));
                    }
                }
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
            "updated": Value::Object(updated),
            "notUpdated": Value::Object(not_updated),
            "destroyed": destroyed,
            "notDestroyed": Value::Object(not_destroyed),
        }))
    }

    async fn handle_email_submission_set(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
        created_ids: &mut HashMap<String, String>,
    ) -> Result<Value> {
        let arguments: EmailSubmissionSetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let mut created = Map::new();
        let mut not_created = Map::new();

        if let Some(create) = arguments.create {
            for (creation_id, value) in create {
                match parse_submission_email_id(&value, created_ids)? {
                    Some(email_id) => {
                        let message_id = parse_uuid(&email_id)?;
                        let audit = AuditEntryInput {
                            actor: account.email.clone(),
                            action: "jmap-email-submit".to_string(),
                            subject: email_id.clone(),
                        };
                        match self
                            .store
                            .submit_draft_message(account_id, message_id, "jmap", audit)
                            .await
                        {
                            Ok(result) => {
                                created_ids.insert(
                                    creation_id.clone(),
                                    result.outbound_queue_id.to_string(),
                                );
                                created.insert(
                                    creation_id,
                                    json!({
                                        "id": result.outbound_queue_id.to_string(),
                                        "emailId": result.message_id.to_string(),
                                        "threadId": result.thread_id.to_string(),
                                        "undoStatus": "final",
                                    }),
                                );
                            }
                            Err(error) => {
                                not_created.insert(creation_id, set_error(&error.to_string()));
                            }
                        }
                    }
                    None => {
                        not_created.insert(
                            creation_id,
                            method_error("invalidArguments", "emailId is required"),
                        );
                    }
                }
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "created": Value::Object(created),
            "notCreated": Value::Object(not_created),
        }))
    }

    async fn handle_email_submission_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: EmailSubmissionGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let ids = parse_uuid_list(arguments.ids)?;
        let properties = email_submission_properties(arguments.properties);
        let ids_ref = ids.as_deref().unwrap_or(&[]);
        let submissions = self
            .store
            .fetch_jmap_email_submissions(account_id, ids_ref)
            .await?;
        let not_found = ids
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !submissions.iter().any(|submission| submission.id == *id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": submissions.iter().map(|submission| email_submission_to_value(submission, &properties)).collect::<Vec<_>>(),
            "notFound": not_found,
        }))
    }

    async fn handle_identity_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: IdentityGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = identity_properties(arguments.properties);
        let identity_id = identity_id_for(account);
        let ids = arguments.ids.unwrap_or_else(|| vec![identity_id.clone()]);
        let mut list = Vec::new();
        let mut not_found = Vec::new();
        for id in ids {
            if id == identity_id {
                list.push(identity_to_value(account, &properties));
            } else {
                not_found.push(Value::String(id));
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_thread_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ThreadGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let properties = thread_properties(arguments.properties);
        let all_email_ids = self.store.fetch_all_jmap_email_ids(account_id).await?;
        let emails = self
            .store
            .fetch_jmap_emails(account_id, &all_email_ids)
            .await?;
        let ids = arguments.ids.unwrap_or_else(|| {
            emails
                .iter()
                .map(|email| email.thread_id.to_string())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
        });

        let mut list = Vec::new();
        let mut not_found = Vec::new();
        for id in ids {
            let thread_id = parse_uuid(&id)?;
            let thread_emails = emails
                .iter()
                .filter(|email| email.thread_id == thread_id)
                .map(|email| email.id.to_string())
                .collect::<Vec<_>>();
            if thread_emails.is_empty() {
                not_found.push(Value::String(id));
            } else {
                list.push(thread_to_value(thread_id, thread_emails, &properties));
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_thread_changes(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: ChangesArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let ids = self.store.fetch_all_jmap_thread_ids(account_id).await?;
        Ok(changes_response(
            account_id,
            &arguments.since_state,
            arguments.max_changes,
            ids.into_iter().map(|id| id.to_string()).collect(),
        ))
    }

    async fn handle_quota_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: QuotaGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let quota = self.store.fetch_jmap_quota(account_id).await?;
        let ids = arguments.ids.unwrap_or_else(|| vec![quota.id.clone()]);
        let mut list = Vec::new();
        let mut not_found = Vec::new();
        for id in ids {
            if id == quota.id {
                list.push(quota_to_value(&quota));
            } else {
                not_found.push(Value::String(id));
            }
        }

        Ok(json!({
            "accountId": account_id.to_string(),
            "state": SESSION_STATE,
            "list": list,
            "notFound": not_found,
        }))
    }

    async fn handle_search_snippet_get(
        &self,
        account: &AuthenticatedAccount,
        arguments: Value,
    ) -> Result<Value> {
        let arguments: SearchSnippetGetArguments = serde_json::from_value(arguments)?;
        let account_id = requested_account_id(arguments.account_id.as_deref(), account)?;
        let ids = parse_uuid_list(Some(arguments.email_ids))?.unwrap_or_default();
        let emails = self.store.fetch_jmap_emails(account_id, &ids).await?;
        let not_found = ids
            .iter()
            .filter(|id| !emails.iter().any(|email| email.id == **id))
            .map(|id| Value::String(id.to_string()))
            .collect::<Vec<_>>();

        Ok(json!({
            "accountId": account_id.to_string(),
            "list": emails.iter().map(search_snippet_to_value).collect::<Vec<_>>(),
            "notFound": not_found,
        }))
    }

    async fn handle_upload(
        &self,
        authorization: Option<&str>,
        account_id: &str,
        media_type: &str,
        body: &[u8],
    ) -> Result<Value> {
        let account = self.authenticate(authorization).await?;
        let requested_account_id = requested_account_id(Some(account_id), &account)?;
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

    async fn handle_download(
        &self,
        authorization: Option<&str>,
        account_id: &str,
        blob_id: &str,
    ) -> Result<JmapUploadBlob> {
        let account = self.authenticate(authorization).await?;
        let requested_account_id = requested_account_id(Some(account_id), &account)?;
        let blob_id = parse_uuid(blob_id)?;
        self.store
            .fetch_jmap_upload_blob(requested_account_id, blob_id)
            .await?
            .ok_or_else(|| anyhow!("blob not found"))
    }

    async fn create_draft(
        &self,
        account: &AuthenticatedAccount,
        account_id: Uuid,
        value: Value,
        creation_id: &str,
    ) -> Result<SavedDraftMessage> {
        let mutation = parse_draft_mutation(value)?;
        let from = select_from_address(mutation.from, account)?;
        let audit = AuditEntryInput {
            actor: account.email.clone(),
            action: "jmap-email-draft-create".to_string(),
            subject: creation_id.to_string(),
        };
        self.store
            .save_draft_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id,
                    source: "jmap".to_string(),
                    from_display: from.name,
                    from_address: from.email,
                    to: map_recipients(mutation.to.unwrap_or_default())?,
                    cc: map_recipients(mutation.cc.unwrap_or_default())?,
                    bcc: map_recipients(mutation.bcc.unwrap_or_default())?,
                    subject: mutation.subject.unwrap_or_default(),
                    body_text: mutation.text_body.unwrap_or_default(),
                    body_html_sanitized: mutation.html_body.unwrap_or(None),
                    internet_message_id: None,
                    mime_blob_ref: None,
                    size_octets: 0,
                    attachments: Vec::new(),
                },
                audit,
            )
            .await
    }

    async fn update_draft(
        &self,
        account: &AuthenticatedAccount,
        account_id: Uuid,
        id: &str,
        value: Value,
    ) -> Result<SavedDraftMessage> {
        let message_id = parse_uuid(id)?;
        let existing = self
            .store
            .fetch_jmap_draft(account_id, message_id)
            .await?
            .ok_or_else(|| anyhow!("draft not found"))?;
        let mutation = parse_draft_mutation(value)?;
        let from = match mutation.from {
            Some(from) => select_from_address(Some(from), account)?,
            None => EmailAddressInput {
                email: existing.from_address,
                name: existing.from_display,
            },
        };
        let audit = AuditEntryInput {
            actor: account.email.clone(),
            action: "jmap-email-draft-update".to_string(),
            subject: id.to_string(),
        };

        self.store
            .save_draft_message(
                SubmitMessageInput {
                    draft_message_id: Some(message_id),
                    account_id,
                    source: "jmap".to_string(),
                    from_display: from.name,
                    from_address: from.email,
                    to: mutation
                        .to
                        .map(map_recipients)
                        .transpose()?
                        .unwrap_or_else(|| map_existing_recipients(&existing.to)),
                    cc: mutation
                        .cc
                        .map(map_recipients)
                        .transpose()?
                        .unwrap_or_else(|| map_existing_recipients(&existing.cc)),
                    bcc: mutation
                        .bcc
                        .map(map_recipients)
                        .transpose()?
                        .unwrap_or_else(|| map_existing_recipients(&existing.bcc)),
                    subject: mutation.subject.unwrap_or(existing.subject),
                    body_text: mutation.text_body.unwrap_or(existing.body_text),
                    body_html_sanitized: mutation.html_body.unwrap_or(existing.body_html_sanitized),
                    internet_message_id: existing.internet_message_id,
                    mime_blob_ref: None,
                    size_octets: existing.size_octets,
                    attachments: Vec::new(),
                },
                audit,
            )
            .await
    }

    async fn parse_email_import(
        &self,
        account: &AuthenticatedAccount,
        account_id: Uuid,
        value: Value,
        created_ids: &HashMap<String, String>,
    ) -> Result<JmapImportedEmailInput> {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("import arguments must be an object"))?;
        let blob_id = object
            .get("blobId")
            .and_then(Value::as_str)
            .map(|value| resolve_creation_reference(value, created_ids))
            .ok_or_else(|| anyhow!("blobId is required"))?;
        let blob_id = parse_uuid(&blob_id)?;
        let mailbox_ids = object
            .get("mailboxIds")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("mailboxIds is required"))?;
        let target_mailbox_id = mailbox_ids
            .iter()
            .find(|(_, included)| included.as_bool().unwrap_or(false))
            .map(|(mailbox_id, _)| parse_uuid(mailbox_id))
            .transpose()?
            .ok_or_else(|| anyhow!("one target mailboxId is required"))?;
        let blob = self
            .store
            .fetch_jmap_upload_blob(account_id, blob_id)
            .await?
            .ok_or_else(|| anyhow!("uploaded blob not found"))?;
        let outcome = self.validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::JmapEmailImport,
                declared_mime: Some(blob.media_type.clone()),
                filename: None,
                expected_kind: ExpectedKind::Rfc822Message,
            },
            &blob.blob_bytes,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "JMAP email import blocked by Magika validation: {}",
                outcome.reason
            );
        }
        let parsed = parse_rfc822_message(&blob.blob_bytes)?;

        Ok(JmapImportedEmailInput {
            account_id,
            mailbox_id: target_mailbox_id,
            source: "jmap-import".to_string(),
            from_display: parsed
                .from
                .as_ref()
                .and_then(|from| from.display_name.clone())
                .or(Some(account.display_name.clone())),
            from_address: parsed
                .from
                .map(|from| from.email)
                .unwrap_or_else(|| account.email.clone()),
            to: map_parsed_recipients(parsed.to),
            cc: map_parsed_recipients(parsed.cc),
            bcc: Vec::new(),
            subject: parsed.subject,
            body_text: parsed.body_text,
            body_html_sanitized: None,
            internet_message_id: parsed.message_id,
            mime_blob_ref: format!("upload:{}", blob.id),
            size_octets: blob.octet_size as i64,
            received_at: None,
            attachments: parsed.attachments,
        })
    }

    async fn authenticate(&self, authorization: Option<&str>) -> Result<AuthenticatedAccount> {
        let token = bearer_token(authorization).ok_or_else(|| anyhow!("missing bearer token"))?;
        self.store
            .fetch_account_session(token)
            .await?
            .ok_or_else(|| anyhow!("invalid or expired account session"))
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

fn http_error(error: anyhow::Error) -> (StatusCode, String) {
    let message = error.to_string();
    let status = if message.contains("bearer token") || message.contains("expired account session")
    {
        StatusCode::UNAUTHORIZED
    } else if message.contains("Magika command")
        || message.contains("spawn Magika")
        || message.contains("Magika stdin")
    {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::BAD_REQUEST
    };
    (status, message)
}

fn session_capabilities() -> HashMap<String, Value> {
    HashMap::from([
        (
            JMAP_CORE_CAPABILITY.to_string(),
            json!({
                "maxSizeUpload": 0,
                "maxCallsInRequest": 16,
                "maxConcurrentUpload": 0,
                "maxObjectsInGet": 250,
                "maxObjectsInSet": 128,
                "collationAlgorithms": ["i;ascii-casemap"],
            }),
        ),
        (
            JMAP_MAIL_CAPABILITY.to_string(),
            json!({
                "maxMailboxesPerEmail": 1,
                "maxMailboxDepth": 1,
                "emailQuerySortOptions": ["receivedAt"],
            }),
        ),
        (
            JMAP_SUBMISSION_CAPABILITY.to_string(),
            json!({
                "maxDelayedSend": 0,
            }),
        ),
        (
            JMAP_CONTACTS_CAPABILITY.to_string(),
            json!({
                "maxAddressBooksPerCard": 1,
            }),
        ),
        (
            JMAP_CALENDARS_CAPABILITY.to_string(),
            json!({
                "maxCalendarsPerEvent": 1,
            }),
        ),
    ])
}

fn requested_account_id(
    requested_account_id: Option<&str>,
    account: &AuthenticatedAccount,
) -> Result<Uuid> {
    match requested_account_id {
        Some(value) => {
            let id = parse_uuid(value)?;
            if id == account.account_id {
                Ok(id)
            } else {
                bail!("accountId does not match authenticated account");
            }
        }
        None => Ok(account.account_id),
    }
}

fn parse_uuid(value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|_| anyhow!("invalid id: {value}"))
}

fn parse_uuid_list(value: Option<Vec<String>>) -> Result<Option<Vec<Uuid>>> {
    value
        .map(|values| values.into_iter().map(|value| parse_uuid(&value)).collect())
        .transpose()
}

fn validate_query_sort(sort: Option<&[EmailQuerySort]>) -> Result<()> {
    if let Some(sort) = sort {
        for item in sort {
            if item.property != "receivedAt" || item.is_ascending.unwrap_or(false) {
                bail!("only receivedAt descending sort is supported");
            }
        }
    }
    Ok(())
}

fn validate_entity_sort(
    sort: Option<&[EntityQuerySort]>,
    expected_property: &str,
    ascending: bool,
) -> Result<()> {
    if let Some(sort) = sort {
        for item in sort {
            if item.property != expected_property || item.is_ascending.unwrap_or(true) != ascending
            {
                let direction = if ascending { "ascending" } else { "descending" };
                bail!("only {expected_property} {direction} sort is supported");
            }
        }
    }
    Ok(())
}

fn validate_contact_filter(filter: Option<&ContactCardQueryFilter>) -> Result<()> {
    if let Some(filter) = filter {
        if let Some(address_book_id) = filter.in_address_book.as_deref() {
            require_default_collection_id(address_book_id, "addressBook")?;
        }
    }
    Ok(())
}

fn validate_calendar_event_filter(filter: Option<&CalendarEventQueryFilter>) -> Result<()> {
    if let Some(filter) = filter {
        if let Some(calendar_id) = filter.in_calendar.as_deref() {
            require_default_collection_id(calendar_id, "calendar")?;
        }
        if let Some(after) = filter.after.as_deref() {
            parse_local_datetime(after)?;
        }
        if let Some(before) = filter.before.as_deref() {
            parse_local_datetime(before)?;
        }
    }
    Ok(())
}

fn require_default_collection_id(value: &str, kind: &str) -> Result<()> {
    if value != DEFAULT_ADDRESS_BOOK_ID && value != DEFAULT_CALENDAR_ID {
        bail!("unsupported {kind} id");
    }
    Ok(())
}

fn mailbox_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "role".to_string(),
                "sortOrder".to_string(),
                "totalEmails".to_string(),
                "unreadEmails".to_string(),
                "isSubscribed".to_string(),
                "myRights".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn address_book_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "sortOrder".to_string(),
                "isSubscribed".to_string(),
                "myRights".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn address_book_to_value(properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", DEFAULT_ADDRESS_BOOK_ID);
    insert_if(properties, &mut object, "name", "Contacts");
    insert_if(properties, &mut object, "sortOrder", 0);
    insert_if(properties, &mut object, "isSubscribed", true);
    if properties.contains("myRights") {
        object.insert(
            "myRights".to_string(),
            json!({
                "mayRead": true,
                "mayAddItems": true,
                "mayModifyItems": true,
                "mayRemoveItems": true,
                "mayRename": false,
                "mayDelete": false,
                "mayAdmin": false,
            }),
        );
    }
    Value::Object(object)
}

fn calendar_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "sortOrder".to_string(),
                "isSubscribed".to_string(),
                "isVisible".to_string(),
                "myRights".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn calendar_to_value(properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", DEFAULT_CALENDAR_ID);
    insert_if(properties, &mut object, "name", "Calendar");
    insert_if(properties, &mut object, "sortOrder", 0);
    insert_if(properties, &mut object, "isSubscribed", true);
    insert_if(properties, &mut object, "isVisible", true);
    if properties.contains("myRights") {
        object.insert(
            "myRights".to_string(),
            json!({
                "mayRead": true,
                "mayAddItems": true,
                "mayModifyItems": true,
                "mayRemoveItems": true,
                "mayRename": false,
                "mayDelete": false,
                "mayAdmin": false,
            }),
        );
    }
    Value::Object(object)
}

fn mailbox_to_value(mailbox: &JmapMailbox, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", mailbox.id.to_string());
    insert_if(properties, &mut object, "name", mailbox.name.clone());
    insert_if(properties, &mut object, "role", mailbox.role.clone());
    insert_if(properties, &mut object, "sortOrder", mailbox.sort_order);
    insert_if(properties, &mut object, "totalEmails", mailbox.total_emails);
    insert_if(
        properties,
        &mut object,
        "unreadEmails",
        mailbox.unread_emails,
    );
    insert_if(properties, &mut object, "isSubscribed", true);
    if properties.contains("myRights") {
        object.insert(
            "myRights".to_string(),
            json!({
                "mayReadItems": true,
                "mayAddItems": mailbox.role == "drafts",
                "mayRemoveItems": mailbox.role == "drafts",
                "maySetSeen": true,
                "maySetKeywords": true,
                "mayCreateChild": false,
                "mayRename": false,
                "mayDelete": false,
                "maySubmit": mailbox.role == "drafts",
            }),
        );
    }
    Value::Object(object)
}

fn changes_response(
    account_id: Uuid,
    since_state: &str,
    max_changes: Option<u64>,
    ids: Vec<String>,
) -> Value {
    let max_changes = max_changes.unwrap_or(u64::MAX) as usize;
    if since_state == SESSION_STATE {
        json!({
            "accountId": account_id.to_string(),
            "oldState": SESSION_STATE,
            "newState": SESSION_STATE,
            "hasMoreChanges": false,
            "created": Vec::<String>::new(),
            "updated": Vec::<String>::new(),
            "destroyed": Vec::<String>::new(),
        })
    } else {
        let created = ids.into_iter().take(max_changes).collect::<Vec<_>>();
        json!({
            "accountId": account_id.to_string(),
            "oldState": since_state,
            "newState": SESSION_STATE,
            "hasMoreChanges": false,
            "created": created,
            "updated": Vec::<String>::new(),
            "destroyed": Vec::<String>::new(),
        })
    }
}

fn email_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "blobId".to_string(),
                "threadId".to_string(),
                "mailboxIds".to_string(),
                "keywords".to_string(),
                "size".to_string(),
                "receivedAt".to_string(),
                "sentAt".to_string(),
                "messageId".to_string(),
                "subject".to_string(),
                "from".to_string(),
                "to".to_string(),
                "cc".to_string(),
                "preview".to_string(),
                "hasAttachment".to_string(),
                "textBody".to_string(),
                "htmlBody".to_string(),
                "bodyValues".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn contact_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "uid".to_string(),
                "kind".to_string(),
                "name".to_string(),
                "emails".to_string(),
                "phones".to_string(),
                "organizations".to_string(),
                "titles".to_string(),
                "notes".to_string(),
                "addressBookIds".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn calendar_event_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "uid".to_string(),
                "@type".to_string(),
                "title".to_string(),
                "start".to_string(),
                "duration".to_string(),
                "timeZone".to_string(),
                "locations".to_string(),
                "participants".to_string(),
                "description".to_string(),
                "calendarIds".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn email_submission_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "emailId".to_string(),
                "threadId".to_string(),
                "identityId".to_string(),
                "envelope".to_string(),
                "sendAt".to_string(),
                "undoStatus".to_string(),
                "deliveryStatus".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn identity_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| {
            vec![
                "id".to_string(),
                "name".to_string(),
                "email".to_string(),
                "replyTo".to_string(),
                "bcc".to_string(),
                "textSignature".to_string(),
                "htmlSignature".to_string(),
                "mayDelete".to_string(),
            ]
        })
        .into_iter()
        .collect()
}

fn thread_properties(properties: Option<Vec<String>>) -> HashSet<String> {
    properties
        .unwrap_or_else(|| vec!["id".to_string(), "emailIds".to_string()])
        .into_iter()
        .collect()
}

fn email_to_value(email: &JmapEmail, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", email.id.to_string());
    insert_if(
        properties,
        &mut object,
        "blobId",
        format!("message:{}", email.id),
    );
    insert_if(
        properties,
        &mut object,
        "threadId",
        email.thread_id.to_string(),
    );
    if properties.contains("mailboxIds") {
        let mut mailbox_ids = Map::new();
        mailbox_ids.insert(email.mailbox_id.to_string(), Value::Bool(true));
        object.insert("mailboxIds".to_string(), Value::Object(mailbox_ids));
    }
    if properties.contains("keywords") {
        object.insert("keywords".to_string(), email_keywords(email));
    }
    insert_if(properties, &mut object, "size", email.size_octets);
    insert_if(
        properties,
        &mut object,
        "receivedAt",
        email.received_at.clone(),
    );
    if let Some(sent_at) = &email.sent_at {
        insert_if(properties, &mut object, "sentAt", sent_at.clone());
    }
    if properties.contains("messageId") {
        object.insert(
            "messageId".to_string(),
            Value::Array(
                email
                    .internet_message_id
                    .as_ref()
                    .map(|message_id| vec![Value::String(message_id.clone())])
                    .unwrap_or_default(),
            ),
        );
    }
    insert_if(properties, &mut object, "subject", email.subject.clone());
    if properties.contains("from") {
        object.insert(
            "from".to_string(),
            Value::Array(vec![address_value(
                &email.from_address,
                email.from_display.as_deref(),
            )]),
        );
    }
    if properties.contains("to") {
        object.insert(
            "to".to_string(),
            Value::Array(
                email
                    .to
                    .iter()
                    .map(|recipient| {
                        address_value(&recipient.address, recipient.display_name.as_deref())
                    })
                    .collect(),
            ),
        );
    }
    if properties.contains("cc") {
        object.insert(
            "cc".to_string(),
            Value::Array(
                email
                    .cc
                    .iter()
                    .map(|recipient| {
                        address_value(&recipient.address, recipient.display_name.as_deref())
                    })
                    .collect(),
            ),
        );
    }
    if properties.contains("bcc") && !email.bcc.is_empty() {
        object.insert(
            "bcc".to_string(),
            Value::Array(
                email
                    .bcc
                    .iter()
                    .map(|recipient| {
                        address_value(&recipient.address, recipient.display_name.as_deref())
                    })
                    .collect(),
            ),
        );
    }
    insert_if(properties, &mut object, "preview", email.preview.clone());
    insert_if(
        properties,
        &mut object,
        "hasAttachment",
        email.has_attachments,
    );

    let mut body_values = Map::new();
    if !email.body_text.is_empty() {
        body_values.insert(
            "textBody".to_string(),
            json!({
                "value": email.body_text.clone(),
                "isEncodingProblem": false,
                "isTruncated": false,
            }),
        );
        if properties.contains("textBody") {
            object.insert(
                "textBody".to_string(),
                json!([{ "partId": "textBody", "type": "text/plain" }]),
            );
        }
    }
    if let Some(html) = &email.body_html_sanitized {
        body_values.insert(
            "htmlBody".to_string(),
            json!({
                "value": html.clone(),
                "isEncodingProblem": false,
                "isTruncated": false,
            }),
        );
        if properties.contains("htmlBody") {
            object.insert(
                "htmlBody".to_string(),
                json!([{ "partId": "htmlBody", "type": "text/html" }]),
            );
        }
    }
    if properties.contains("bodyValues") {
        object.insert("bodyValues".to_string(), Value::Object(body_values));
    }

    Value::Object(object)
}

fn email_submission_to_value(
    submission: &JmapEmailSubmission,
    properties: &HashSet<String>,
) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", submission.id.to_string());
    insert_if(
        properties,
        &mut object,
        "emailId",
        submission.email_id.to_string(),
    );
    insert_if(
        properties,
        &mut object,
        "threadId",
        submission.thread_id.to_string(),
    );
    insert_if(
        properties,
        &mut object,
        "identityId",
        submission.identity_email.clone(),
    );
    if properties.contains("envelope") {
        object.insert(
            "envelope".to_string(),
            json!({
                "mailFrom": {"email": submission.envelope_mail_from},
                "rcptTo": submission.envelope_rcpt_to.iter().map(|address| json!({"email": address})).collect::<Vec<_>>(),
            }),
        );
    }
    insert_if(
        properties,
        &mut object,
        "sendAt",
        submission.send_at.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "undoStatus",
        submission.undo_status.clone(),
    );
    insert_if(
        properties,
        &mut object,
        "deliveryStatus",
        submission.delivery_status.clone(),
    );
    Value::Object(object)
}

fn identity_id_for(account: &AuthenticatedAccount) -> String {
    account.email.to_lowercase()
}

fn identity_to_value(account: &AuthenticatedAccount, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", identity_id_for(account));
    insert_if(
        properties,
        &mut object,
        "name",
        account.display_name.clone(),
    );
    insert_if(properties, &mut object, "email", account.email.clone());
    if properties.contains("replyTo") {
        object.insert("replyTo".to_string(), Value::Null);
    }
    if properties.contains("bcc") {
        object.insert("bcc".to_string(), Value::Null);
    }
    insert_if(properties, &mut object, "textSignature", "");
    insert_if(properties, &mut object, "htmlSignature", "");
    insert_if(properties, &mut object, "mayDelete", false);
    Value::Object(object)
}

fn thread_to_value(thread_id: Uuid, email_ids: Vec<String>, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", thread_id.to_string());
    if properties.contains("emailIds") {
        object.insert(
            "emailIds".to_string(),
            Value::Array(email_ids.into_iter().map(Value::String).collect()),
        );
    }
    Value::Object(object)
}

fn search_snippet_to_value(email: &JmapEmail) -> Value {
    let subject = if email.subject.is_empty() {
        email.preview.clone()
    } else {
        email.subject.clone()
    };
    let preview = if email.preview.is_empty() {
        trim_snippet(&email.body_text, 120)
    } else {
        trim_snippet(&email.preview, 120)
    };
    json!({
        "emailId": email.id.to_string(),
        "subject": subject,
        "preview": preview,
    })
}

fn trim_snippet(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        normalized
    } else {
        normalized.chars().take(max_chars).collect::<String>()
    }
}

fn quota_to_value(quota: &JmapQuota) -> Value {
    json!({
        "id": quota.id,
        "name": quota.name,
        "used": quota.used,
        "hardLimit": quota.hard_limit,
        "scope": "account",
    })
}

fn email_keywords(email: &JmapEmail) -> Value {
    let mut keywords = Map::new();
    if email.mailbox_role == "drafts" {
        keywords.insert("$draft".to_string(), Value::Bool(true));
    }
    if !email.unread {
        keywords.insert("$seen".to_string(), Value::Bool(true));
    }
    if email.flagged {
        keywords.insert("$flagged".to_string(), Value::Bool(true));
    }
    Value::Object(keywords)
}

fn contact_to_value(contact: &ClientContact, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", contact.id.to_string());
    insert_if(properties, &mut object, "uid", contact.id.to_string());
    insert_if(properties, &mut object, "kind", "individual");
    if properties.contains("name") {
        object.insert(
            "name".to_string(),
            json!({
                "@type": "Name",
                "full": contact.name,
            }),
        );
    }
    if properties.contains("emails") && !contact.email.trim().is_empty() {
        object.insert(
            "emails".to_string(),
            json!({
                "main": {
                    "@type": "EmailAddress",
                    "address": contact.email,
                    "contexts": {"work": true},
                    "pref": 1,
                }
            }),
        );
    }
    if properties.contains("phones") && !contact.phone.trim().is_empty() {
        object.insert(
            "phones".to_string(),
            json!({
                "main": {
                    "@type": "Phone",
                    "number": contact.phone,
                    "contexts": {"work": true},
                }
            }),
        );
    }
    if properties.contains("organizations") && !contact.team.trim().is_empty() {
        object.insert(
            "organizations".to_string(),
            json!({
                "main": {
                    "@type": "Organization",
                    "name": contact.team,
                    "contexts": {"work": true},
                }
            }),
        );
    }
    if properties.contains("titles") && !contact.role.trim().is_empty() {
        object.insert(
            "titles".to_string(),
            json!({
                "main": {
                    "@type": "Title",
                    "kind": "title",
                    "name": contact.role,
                }
            }),
        );
    }
    if properties.contains("notes") && !contact.notes.trim().is_empty() {
        object.insert(
            "notes".to_string(),
            json!({
                "main": {
                    "@type": "Note",
                    "note": contact.notes,
                }
            }),
        );
    }
    if properties.contains("addressBookIds") {
        object.insert(
            "addressBookIds".to_string(),
            json!({DEFAULT_ADDRESS_BOOK_ID: true}),
        );
    }
    Value::Object(object)
}

fn calendar_event_to_value(event: &ClientEvent, properties: &HashSet<String>) -> Value {
    let mut object = Map::new();
    insert_if(properties, &mut object, "id", event.id.to_string());
    insert_if(properties, &mut object, "uid", event.id.to_string());
    insert_if(properties, &mut object, "@type", "Event");
    insert_if(properties, &mut object, "title", event.title.clone());
    insert_if(
        properties,
        &mut object,
        "start",
        format!("{}T{}:00", event.date, event.time),
    );
    insert_if(
        properties,
        &mut object,
        "duration",
        if event.duration_minutes <= 0 {
            "PT0S".to_string()
        } else {
            format!("PT{}M", event.duration_minutes)
        },
    );
    if properties.contains("timeZone") {
        if event.time_zone.trim().is_empty() {
            object.insert("timeZone".to_string(), Value::Null);
        } else {
            object.insert("timeZone".to_string(), Value::String(event.time_zone.clone()));
        }
    }
    if properties.contains("locations") && !event.location.trim().is_empty() {
        object.insert(
            "locations".to_string(),
            json!({
                "main": {
                    "@type": "Location",
                    "name": event.location,
                }
            }),
        );
    }
    if properties.contains("participants") && !event.attendees.trim().is_empty() {
        object.insert(
            "participants".to_string(),
            participants_from_event(event),
        );
    }
    insert_if(properties, &mut object, "description", event.notes.clone());
    if properties.contains("calendarIds") {
        object.insert(
            "calendarIds".to_string(),
            json!({DEFAULT_CALENDAR_ID: true}),
        );
    }
    Value::Object(object)
}

fn participants_from_event(event: &ClientEvent) -> Value {
    if let Ok(value) = serde_json::from_str::<Value>(&event.attendees_json) {
        if value.is_object() {
            return value;
        }
        if let Some(entries) = value.as_array() {
            let participants = entries
                .iter()
                .filter_map(Value::as_object)
                .enumerate()
                .map(|(index, attendee)| {
                    let key = format!("p{}", index + 1);
                    let name = attendee
                        .get("common_name")
                        .and_then(Value::as_str)
                        .filter(|value| !value.trim().is_empty())
                        .or_else(|| attendee.get("email").and_then(Value::as_str))
                        .unwrap_or_default();
                    let email = attendee
                        .get("email")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let participant = json!({
                        "@type": "Participant",
                        "name": name,
                        "email": email,
                        "roles": {"attendee": true},
                        "participationStatus": attendee
                            .get("partstat")
                            .and_then(Value::as_str)
                            .unwrap_or("needs-action")
                            .to_ascii_lowercase(),
                    });
                    (key, participant)
                })
                .collect::<Map<String, Value>>();
            return Value::Object(participants);
        }
    }
    participants_from_attendees(&event.attendees)
}

fn participants_from_attendees(attendees: &str) -> Value {
    let participants = attendees
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .enumerate()
        .map(|(index, value)| {
            let key = format!("p{}", index + 1);
            let participant = if value.contains('@') {
                json!({
                    "@type": "Participant",
                    "name": value,
                    "email": value,
                    "roles": {"attendee": true},
                })
            } else {
                json!({
                    "@type": "Participant",
                    "name": value,
                    "roles": {"attendee": true},
                })
            };
            (key, participant)
        })
        .collect::<Map<String, Value>>();
    Value::Object(participants)
}

fn contact_matches_filter(contact: &ClientContact, filter: &ContactCardQueryFilter) -> bool {
    if filter.in_address_book.is_some() {
        // Only one virtual address book is exposed in the MVP.
    }
    if let Some(text) = filter.text.as_deref() {
        let needle = text.trim().to_lowercase();
        if !needle.is_empty() {
            let haystack = format!(
                "{} {} {} {} {} {}",
                contact.name,
                contact.email,
                contact.role,
                contact.phone,
                contact.team,
                contact.notes
            )
            .to_lowercase();
            if !haystack.contains(&needle) {
                return false;
            }
        }
    }
    true
}

fn event_matches_filter(event: &ClientEvent, filter: &CalendarEventQueryFilter) -> bool {
    if filter.in_calendar.is_some() {
        // Only one virtual calendar is exposed in the MVP.
    }
    if let Some(after) = filter.after.as_deref() {
        let start = calendar_event_start(event);
        if let Ok(after) = parse_local_datetime_value(after) {
            if start < after {
                return false;
            }
        } else {
            return false;
        }
    }
    if let Some(before) = filter.before.as_deref() {
        let start = calendar_event_start(event);
        if let Ok(before) = parse_local_datetime_value(before) {
            if start >= before {
                return false;
            }
        } else {
            return false;
        }
    }
    if let Some(text) = filter.text.as_deref() {
        let needle = text.trim().to_lowercase();
        if !needle.is_empty() {
            let haystack = format!(
                "{} {} {} {}",
                event.title, event.location, event.attendees, event.notes
            )
            .to_lowercase();
            if !haystack.contains(&needle) {
                return false;
            }
        }
    }
    true
}

fn calendar_event_sort_key(event: &ClientEvent) -> String {
    calendar_event_start(event)
}

fn calendar_event_start(event: &ClientEvent) -> String {
    format!("{}T{}:00", event.date, event.time)
}

fn insert_if<T: Serialize>(
    properties: &HashSet<String>,
    object: &mut Map<String, Value>,
    key: &str,
    value: T,
) {
    if properties.contains(key) {
        object.insert(
            key.to_string(),
            serde_json::to_value(value).unwrap_or(Value::Null),
        );
    }
}

fn address_value(email: &str, name: Option<&str>) -> Value {
    json!({
        "email": email,
        "name": name,
    })
}

fn method_error(kind: &str, description: &str) -> Value {
    json!({
        "type": kind,
        "description": description,
    })
}

fn set_error(description: &str) -> Value {
    method_error("invalidProperties", description)
}

fn parse_submission_email_id(
    value: &Value,
    created_ids: &HashMap<String, String>,
) -> Result<Option<String>> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("submission create arguments must be an object"))?;
    if let Some(email_id) = object.get("emailId").and_then(Value::as_str) {
        return Ok(Some(resolve_creation_reference(email_id, created_ids)));
    }
    if let Some(reference) = object.get("#emailId").and_then(Value::as_str) {
        return Ok(created_ids.get(reference).cloned());
    }
    Ok(None)
}

fn resolve_creation_reference(value: &str, created_ids: &HashMap<String, String>) -> String {
    if let Some(reference) = value.strip_prefix('#') {
        created_ids
            .get(reference)
            .cloned()
            .unwrap_or_else(|| value.to_string())
    } else {
        value.to_string()
    }
}

fn parse_draft_mutation(value: Value) -> Result<DraftMutation> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("email arguments must be an object"))?;
    reject_unknown_email_properties(object)?;

    if let Some(mailbox_ids) = object.get("mailboxIds").and_then(Value::as_object) {
        if mailbox_ids.len() > 1 {
            bail!("only one mailboxId is supported");
        }
    }
    if let Some(keywords) = object.get("keywords").and_then(Value::as_object) {
        for keyword in keywords.keys() {
            if keyword != "$draft" && keyword != "$seen" && keyword != "$flagged" {
                bail!("unsupported keyword: {keyword}");
            }
        }
    }

    Ok(DraftMutation {
        from: parse_address_list(object.get("from"))?,
        to: parse_address_list(object.get("to"))?,
        cc: parse_address_list(object.get("cc"))?,
        bcc: parse_address_list(object.get("bcc"))?,
        subject: parse_optional_string(object.get("subject"))?,
        text_body: parse_optional_string(object.get("textBody"))?,
        html_body: parse_optional_nullable_string(object.get("htmlBody"))?,
    })
}

fn parse_mailbox_create(value: Value) -> Result<MailboxCreateInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("mailbox create arguments must be an object"))?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("mailbox name is required"))?
        .to_string();
    let sort_order = object
        .get("sortOrder")
        .and_then(Value::as_i64)
        .map(|value| value as i32);
    Ok(MailboxCreateInput { name, sort_order })
}

fn parse_mailbox_update(value: Value) -> Result<MailboxUpdateInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("mailbox update arguments must be an object"))?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_string());
    let sort_order = object
        .get("sortOrder")
        .and_then(Value::as_i64)
        .map(|value| value as i32);
    Ok(MailboxUpdateInput { name, sort_order })
}

fn parse_contact_input(
    id: Option<Uuid>,
    account_id: Uuid,
    value: Value,
) -> Result<UpsertClientContactInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("contact card arguments must be an object"))?;
    reject_unknown_contact_properties(object)?;
    validate_address_book_ids(object.get("addressBookIds"))?;

    let kind = object
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("individual");
    if kind != "individual" {
        bail!("only kind=individual is supported");
    }

    Ok(UpsertClientContactInput {
        id,
        account_id,
        name: parse_contact_name(object.get("name"))?,
        role: parse_contact_title(object.get("titles"))?,
        email: parse_contact_email(object.get("emails"))?,
        phone: parse_contact_phone(object.get("phones"))?,
        team: parse_contact_organization(object.get("organizations"))?,
        notes: parse_contact_note(object.get("notes"))?,
    })
}

fn parse_calendar_event_input(
    id: Option<Uuid>,
    account_id: Uuid,
    value: Value,
) -> Result<UpsertClientEventInput> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("calendar event arguments must be an object"))?;
    reject_unknown_calendar_event_properties(object)?;
    validate_calendar_ids(object.get("calendarIds"))?;

    let event_type = object
        .get("@type")
        .and_then(Value::as_str)
        .unwrap_or("Event");
    if event_type != "Event" {
        bail!("only @type=Event is supported");
    }
    if let Some(uid) = object.get("uid").and_then(Value::as_str) {
        if uid.trim().is_empty() {
            bail!("uid must not be empty");
        }
    }

    let (date, time) = parse_local_datetime(
        object
            .get("start")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("start is required"))?,
    )?;

    Ok(UpsertClientEventInput {
        id,
        account_id,
        date,
        time,
        time_zone: parse_optional_string(object.get("timeZone"))?.unwrap_or_default(),
        duration_minutes: parse_calendar_duration(object.get("duration"))?,
        recurrence_rule: String::new(),
        title: parse_required_string(object.get("title"), "title")?,
        location: parse_calendar_location(object.get("locations"))?,
        attendees: parse_calendar_participants(object.get("participants"))?,
        attendees_json: parse_calendar_participants_json(object.get("participants"))?,
        notes: parse_optional_string(object.get("description"))?.unwrap_or_default(),
    })
}

fn parse_email_copy(value: Value, created_ids: &HashMap<String, String>) -> Result<(Uuid, Uuid)> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("Email/copy create arguments must be an object"))?;
    let email_id = object
        .get("emailId")
        .and_then(Value::as_str)
        .map(|value| resolve_creation_reference(value, created_ids))
        .ok_or_else(|| anyhow!("emailId is required"))?;
    let mailbox_ids = object
        .get("mailboxIds")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("mailboxIds is required"))?;
    let mailbox_id = mailbox_ids
        .iter()
        .find(|(_, value)| value.as_bool().unwrap_or(false))
        .map(|(id, _)| parse_uuid(id))
        .transpose()?
        .ok_or_else(|| anyhow!("one target mailboxId is required"))?;
    Ok((parse_uuid(&email_id)?, mailbox_id))
}

fn reject_unknown_contact_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "uid" | "kind" | "name" | "emails" | "phones" | "organizations" | "titles"
            | "notes" | "addressBookIds" => {}
            _ => bail!("unsupported contact card property: {key}"),
        }
    }
    Ok(())
}

fn reject_unknown_calendar_event_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "@type" | "uid" | "title" | "start" | "duration" | "timeZone" | "locations"
            | "participants" | "description" | "calendarIds" => {}
            _ => bail!("unsupported calendar event property: {key}"),
        }
    }
    Ok(())
}

fn validate_address_book_ids(value: Option<&Value>) -> Result<()> {
    if let Some(value) = value {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("addressBookIds must be an object"))?;
        if object.len() != 1
            || object.get(DEFAULT_ADDRESS_BOOK_ID).and_then(Value::as_bool) != Some(true)
        {
            bail!("only the default addressBookId is supported");
        }
    }
    Ok(())
}

fn validate_calendar_ids(value: Option<&Value>) -> Result<()> {
    if let Some(value) = value {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("calendarIds must be an object"))?;
        if object.len() != 1
            || object.get(DEFAULT_CALENDAR_ID).and_then(Value::as_bool) != Some(true)
        {
            bail!("only the default calendarId is supported");
        }
    }
    Ok(())
}

fn reject_unknown_email_properties(object: &Map<String, Value>) -> Result<()> {
    for key in object.keys() {
        match key.as_str() {
            "from" | "to" | "cc" | "bcc" | "subject" | "textBody" | "htmlBody" | "mailboxIds"
            | "keywords" => {}
            _ => bail!("unsupported email property: {key}"),
        }
    }
    Ok(())
}

fn normalize_email(value: &str) -> String {
    value.trim().to_lowercase()
}

fn parse_contact_name(value: Option<&Value>) -> Result<String> {
    let value = value.ok_or_else(|| anyhow!("name is required"))?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("name must be an object"))?;
    if let Some(full) = object.get("full").and_then(Value::as_str) {
        let full = full.trim();
        if full.is_empty() {
            bail!("name.full is required");
        }
        return Ok(full.to_string());
    }
    bail!("name.full is required")
}

fn parse_contact_email(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "emails", "address")
        .map(|email| normalize_email(&email))
}

fn parse_contact_phone(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "phones", "number")
}

fn parse_contact_organization(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "organizations", "name")
}

fn parse_contact_title(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "titles", "name")
}

fn parse_contact_note(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "notes", "note")
}

fn parse_calendar_location(value: Option<&Value>) -> Result<String> {
    parse_first_property_object_string(value, "locations", "name")
}

fn parse_calendar_participants(value: Option<&Value>) -> Result<String> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("participants must be an object"))?;
    let attendees = object
        .values()
        .filter_map(Value::as_object)
        .map(|participant| {
            participant
                .get("email")
                .and_then(Value::as_str)
                .or_else(|| participant.get("name").and_then(Value::as_str))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("participant name or email is required"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(attendees.join(", "))
}

fn parse_calendar_participants_json(value: Option<&Value>) -> Result<String> {
    let Some(value) = value else {
        return Ok("{}".to_string());
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("participants must be an object"))?;
    Ok(Value::Object(object.clone()).to_string())
}

fn parse_calendar_duration(value: Option<&Value>) -> Result<i32> {
    let Some(value) = value.and_then(Value::as_str) else {
        return Ok(0);
    };
    if value == "PT0S" {
        return Ok(0);
    }
    let Some(value) = value.strip_prefix("PT") else {
        bail!("duration must use PT...");
    };
    if let Some(hours) = value.strip_suffix('H') {
        return hours
            .parse::<i32>()
            .map(|value| value.max(0) * 60)
            .map_err(|_| anyhow!("invalid duration"));
    }
    if let Some(minutes) = value.strip_suffix('M') {
        return minutes
            .parse::<i32>()
            .map(|value| value.max(0))
            .map_err(|_| anyhow!("invalid duration"));
    }
    bail!("invalid duration")
}

fn parse_first_property_object_string(
    value: Option<&Value>,
    property_name: &str,
    field_name: &str,
) -> Result<String> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("{property_name} must be an object"))?;
    let Some(first) = object.values().next() else {
        return Ok(String::new());
    };
    let first = first
        .as_object()
        .ok_or_else(|| anyhow!("{property_name} entries must be objects"))?;
    Ok(first
        .get(field_name)
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default()
        .to_string())
}

fn parse_address_list(value: Option<&Value>) -> Result<Option<Vec<EmailAddressInput>>> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(Vec::new())),
        Some(value) => Ok(Some(serde_json::from_value(value.clone())?)),
    }
}

fn parse_optional_string(value: Option<&Value>) -> Result<Option<String>> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(String::new())),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => bail!("string property expected"),
    }
}

fn parse_required_string(value: Option<&Value>, field_name: &str) -> Result<String> {
    let value = value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{field_name} is required"))?;
    Ok(value.to_string())
}

fn parse_optional_nullable_string(value: Option<&Value>) -> Result<Option<Option<String>>> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(value)) => Ok(Some(Some(value.clone()))),
        _ => bail!("string or null property expected"),
    }
}

fn parse_local_datetime(value: &str) -> Result<(String, String)> {
    let trimmed = value.trim();
    let (date, time) = trimmed
        .split_once('T')
        .ok_or_else(|| anyhow!("invalid local date-time"))?;
    if date.len() != 10 || time.len() < 5 {
        bail!("invalid local date-time");
    }
    let time = time.trim_end_matches('Z');
    let hhmm = if let Some((hours_minutes, _seconds)) = time.split_once(':') {
        if hours_minutes.len() != 2 {
            bail!("invalid local date-time");
        }
        format!("{hours_minutes}:{}", &time[3..5])
    } else {
        bail!("invalid local date-time");
    };
    Ok((date.to_string(), hhmm))
}

fn parse_local_datetime_value(value: &str) -> Result<String> {
    let (date, time) = parse_local_datetime(value)?;
    Ok(format!("{date}T{time}:00"))
}

fn select_from_address(
    from: Option<Vec<EmailAddressInput>>,
    account: &AuthenticatedAccount,
) -> Result<EmailAddressInput> {
    match from {
        None => Ok(EmailAddressInput {
            email: account.email.clone(),
            name: Some(account.display_name.clone()),
        }),
        Some(mut addresses) => {
            if addresses.len() != 1 {
                bail!("exactly one from address is required");
            }
            let address = addresses.remove(0);
            if address.email.trim().eq_ignore_ascii_case(&account.email) {
                Ok(EmailAddressInput {
                    email: account.email.clone(),
                    name: address.name,
                })
            } else {
                bail!("from email must match authenticated account");
            }
        }
    }
}

fn map_recipients(input: Vec<EmailAddressInput>) -> Result<Vec<SubmittedRecipientInput>> {
    input
        .into_iter()
        .map(|recipient| {
            let address = recipient.email.trim().to_lowercase();
            if address.is_empty() {
                bail!("recipient email is required");
            }
            Ok(SubmittedRecipientInput {
                address,
                display_name: recipient.name.and_then(|name| {
                    let trimmed = name.trim().to_string();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed)
                    }
                }),
            })
        })
        .collect()
}

fn map_existing_recipients(recipients: &[JmapEmailAddress]) -> Vec<SubmittedRecipientInput> {
    recipients
        .iter()
        .map(|recipient| SubmittedRecipientInput {
            address: recipient.address.clone(),
            display_name: recipient.display_name.clone(),
        })
        .collect()
}

fn map_parsed_recipients(
    recipients: Vec<lpe_storage::mail::ParsedMailAddress>,
) -> Vec<SubmittedRecipientInput> {
    recipients
        .into_iter()
        .map(|recipient| SubmittedRecipientInput {
            address: recipient.email,
            display_name: recipient.display_name,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use lpe_magika::{DetectionSource, Detector, MagikaDetection};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct FakeStore {
        session: Option<AuthenticatedAccount>,
        mailboxes: Vec<JmapMailbox>,
        emails: Vec<JmapEmail>,
        contacts: Arc<Mutex<Vec<ClientContact>>>,
        events: Arc<Mutex<Vec<ClientEvent>>>,
        uploads: Arc<Mutex<Vec<JmapUploadBlob>>>,
        saved_drafts: Arc<Mutex<Vec<SubmitMessageInput>>>,
        submitted_drafts: Arc<Mutex<Vec<Uuid>>>,
    }

    #[derive(Clone)]
    struct FakeDetector {
        result: Result<MagikaDetection, String>,
    }

    #[test]
    fn parse_rfc822_message_collects_supported_attachment_parts() {
        let message = concat!(
            "From: Alice <alice@example.test>\r\n",
            "To: Bob <bob@example.test>\r\n",
            "Subject: Import\r\n",
            "Content-Type: multipart/mixed; boundary=\"b1\"\r\n",
            "\r\n",
            "--b1\r\n",
            "Content-Type: text/plain\r\n",
            "\r\n",
            "Hello\r\n",
            "--b1\r\n",
            "Content-Type: application/vnd.oasis.opendocument.text\r\n",
            "Content-Disposition: attachment; filename=\"notes.odt\"\r\n",
            "\r\n",
            "ODT-DATA\r\n",
            "--b1--\r\n"
        );

        let parsed = parse_rfc822_message(message.as_bytes()).unwrap();

        assert_eq!(parsed.subject, "Import");
        assert_eq!(parsed.attachments.len(), 1);
        assert_eq!(parsed.attachments[0].file_name, "notes.odt");
        assert_eq!(
            parsed.attachments[0].media_type,
            "application/vnd.oasis.opendocument.text"
        );
        assert_eq!(parsed.attachments[0].blob_bytes, b"ODT-DATA".to_vec());
    }

    impl Detector for FakeDetector {
        fn detect(&self, _source: DetectionSource<'_>) -> Result<MagikaDetection> {
            self.result.clone().map_err(anyhow::Error::msg)
        }
    }

    fn validator_ok(
        mime_type: &str,
        label: &str,
        extension: &str,
        score: f32,
    ) -> Validator<FakeDetector> {
        Validator::new(
            FakeDetector {
                result: Ok(MagikaDetection {
                    label: label.to_string(),
                    mime_type: mime_type.to_string(),
                    description: label.to_string(),
                    group: "document".to_string(),
                    extensions: vec![extension.to_string()],
                    score: Some(score),
                }),
            },
            0.80,
        )
    }

    fn validator_error(message: &str) -> Validator<FakeDetector> {
        Validator::new(
            FakeDetector {
                result: Err(message.to_string()),
            },
            0.80,
        )
    }

    impl FakeStore {
        fn account() -> AuthenticatedAccount {
            AuthenticatedAccount {
                account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                email: "alice@example.test".to_string(),
                display_name: "Alice".to_string(),
                expires_at: "2099-01-01T00:00:00Z".to_string(),
            }
        }

        fn draft_mailbox() -> JmapMailbox {
            JmapMailbox {
                id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
                role: "drafts".to_string(),
                name: "Drafts".to_string(),
                sort_order: 10,
                total_emails: 1,
                unread_emails: 0,
            }
        }

        fn draft_email() -> JmapEmail {
            JmapEmail {
                id: Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
                thread_id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
                mailbox_id: Self::draft_mailbox().id,
                mailbox_role: "drafts".to_string(),
                mailbox_name: "Drafts".to_string(),
                received_at: "2026-04-18T10:00:00Z".to_string(),
                sent_at: None,
                from_address: "alice@example.test".to_string(),
                from_display: Some("Alice".to_string()),
                to: vec![lpe_storage::JmapEmailAddress {
                    address: "bob@example.test".to_string(),
                    display_name: Some("Bob".to_string()),
                }],
                cc: Vec::new(),
                bcc: vec![lpe_storage::JmapEmailAddress {
                    address: "hidden@example.test".to_string(),
                    display_name: None,
                }],
                subject: "Draft subject".to_string(),
                preview: "Draft preview".to_string(),
                body_text: "Draft body".to_string(),
                body_html_sanitized: None,
                unread: false,
                flagged: false,
                has_attachments: false,
                size_octets: 42,
                internet_message_id: Some("<draft@example.test>".to_string()),
                delivery_status: "draft".to_string(),
            }
        }

        fn contact() -> ClientContact {
            ClientContact {
                id: Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap(),
                name: "Bob Example".to_string(),
                role: "Sales".to_string(),
                email: "bob@example.test".to_string(),
                phone: "+33123456789".to_string(),
                team: "North".to_string(),
                notes: "VIP".to_string(),
            }
        }

        fn event() -> ClientEvent {
            ClientEvent {
                id: Uuid::parse_str("34343434-3434-3434-3434-343434343434").unwrap(),
                date: "2026-04-20".to_string(),
                time: "09:30".to_string(),
                time_zone: "".to_string(),
                duration_minutes: 0,
                recurrence_rule: "".to_string(),
                title: "Standup".to_string(),
                location: "Room A".to_string(),
                attendees: "alice@example.test, bob@example.test".to_string(),
                attendees_json: "[]".to_string(),
                notes: "Daily sync".to_string(),
            }
        }
    }

    impl JmapStore for FakeStore {
        async fn fetch_account_session(&self, token: &str) -> Result<Option<AuthenticatedAccount>> {
            Ok(if token == "token" {
                self.session.clone()
            } else {
                None
            })
        }

        async fn fetch_jmap_mailboxes(&self, _account_id: Uuid) -> Result<Vec<JmapMailbox>> {
            Ok(self.mailboxes.clone())
        }

        async fn fetch_jmap_mailbox_ids(&self, _account_id: Uuid) -> Result<Vec<Uuid>> {
            Ok(self.mailboxes.iter().map(|mailbox| mailbox.id).collect())
        }

        async fn query_jmap_email_ids(
            &self,
            _account_id: Uuid,
            mailbox_id: Option<Uuid>,
            _search_text: Option<&str>,
            position: u64,
            limit: u64,
        ) -> Result<JmapEmailQuery> {
            let mut ids = self
                .emails
                .iter()
                .filter(|email| mailbox_id.is_none() || Some(email.mailbox_id) == mailbox_id)
                .map(|email| email.id)
                .collect::<Vec<_>>();
            let total = ids.len() as u64;
            ids = ids
                .into_iter()
                .skip(position as usize)
                .take(limit as usize)
                .collect();
            Ok(JmapEmailQuery { ids, total })
        }

        async fn fetch_all_jmap_email_ids(&self, _account_id: Uuid) -> Result<Vec<Uuid>> {
            Ok(self.emails.iter().map(|email| email.id).collect())
        }

        async fn fetch_all_jmap_thread_ids(&self, _account_id: Uuid) -> Result<Vec<Uuid>> {
            Ok(self
                .emails
                .iter()
                .map(|email| email.thread_id)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect())
        }

        async fn create_jmap_mailbox(
            &self,
            input: JmapMailboxCreateInput,
            _audit: AuditEntryInput,
        ) -> Result<JmapMailbox> {
            Ok(JmapMailbox {
                id: Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap(),
                role: "".to_string(),
                name: input.name,
                sort_order: input.sort_order.unwrap_or(99),
                total_emails: 0,
                unread_emails: 0,
            })
        }

        async fn update_jmap_mailbox(
            &self,
            input: JmapMailboxUpdateInput,
            _audit: AuditEntryInput,
        ) -> Result<JmapMailbox> {
            Ok(JmapMailbox {
                id: input.mailbox_id,
                role: "".to_string(),
                name: input.name.unwrap_or_else(|| "Updated".to_string()),
                sort_order: input.sort_order.unwrap_or(10),
                total_emails: 0,
                unread_emails: 0,
            })
        }

        async fn destroy_jmap_mailbox(
            &self,
            _account_id: Uuid,
            _mailbox_id: Uuid,
            _audit: AuditEntryInput,
        ) -> Result<()> {
            Ok(())
        }

        async fn fetch_jmap_emails(
            &self,
            _account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<JmapEmail>> {
            Ok(ids
                .iter()
                .filter_map(|id| self.emails.iter().find(|email| email.id == *id).cloned())
                .collect())
        }

        async fn fetch_jmap_draft(&self, _account_id: Uuid, id: Uuid) -> Result<Option<JmapEmail>> {
            Ok(self.emails.iter().find(|email| email.id == id).cloned())
        }

        async fn fetch_jmap_email_submissions(
            &self,
            _account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<JmapEmailSubmission>> {
            let submissions = vec![JmapEmailSubmission {
                id: Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
                email_id: Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap(),
                thread_id: FakeStore::draft_email().thread_id,
                identity_email: FakeStore::account().email,
                envelope_mail_from: "alice@example.test".to_string(),
                envelope_rcpt_to: vec!["bob@example.test".to_string()],
                send_at: "2026-04-18T10:01:00Z".to_string(),
                undo_status: "final".to_string(),
                delivery_status: "queued".to_string(),
            }];
            if ids.is_empty() {
                Ok(submissions)
            } else {
                Ok(submissions
                    .into_iter()
                    .filter(|submission| ids.contains(&submission.id))
                    .collect())
            }
        }

        async fn fetch_jmap_quota(&self, _account_id: Uuid) -> Result<JmapQuota> {
            Ok(JmapQuota {
                id: "mail".to_string(),
                name: "Mail".to_string(),
                used: 10,
                hard_limit: 100,
            })
        }

        async fn save_jmap_upload_blob(
            &self,
            account_id: Uuid,
            media_type: &str,
            blob_bytes: &[u8],
        ) -> Result<JmapUploadBlob> {
            let blob = JmapUploadBlob {
                id: Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap(),
                account_id,
                media_type: media_type.to_string(),
                octet_size: blob_bytes.len() as u64,
                blob_bytes: blob_bytes.to_vec(),
            };
            self.uploads.lock().unwrap().push(blob.clone());
            Ok(blob)
        }

        async fn fetch_jmap_upload_blob(
            &self,
            _account_id: Uuid,
            blob_id: Uuid,
        ) -> Result<Option<JmapUploadBlob>> {
            Ok(self
                .uploads
                .lock()
                .unwrap()
                .iter()
                .find(|blob| blob.id == blob_id)
                .cloned())
        }

        async fn save_draft_message(
            &self,
            input: SubmitMessageInput,
            _audit: AuditEntryInput,
        ) -> Result<SavedDraftMessage> {
            self.saved_drafts.lock().unwrap().push(input.clone());
            Ok(SavedDraftMessage {
                message_id: input.draft_message_id.unwrap_or_else(Uuid::new_v4),
                account_id: input.account_id,
                draft_mailbox_id: FakeStore::draft_mailbox().id,
                delivery_status: "draft".to_string(),
            })
        }

        async fn delete_draft_message(
            &self,
            _account_id: Uuid,
            _message_id: Uuid,
            _audit: AuditEntryInput,
        ) -> Result<()> {
            Ok(())
        }

        async fn submit_draft_message(
            &self,
            _account_id: Uuid,
            draft_message_id: Uuid,
            _source: &str,
            _audit: AuditEntryInput,
        ) -> Result<SubmittedMessage> {
            self.submitted_drafts.lock().unwrap().push(draft_message_id);
            Ok(SubmittedMessage {
                message_id: Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap(),
                thread_id: FakeStore::draft_email().thread_id,
                account_id: FakeStore::account().account_id,
                sent_mailbox_id: Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
                outbound_queue_id: Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
                delivery_status: "queued".to_string(),
            })
        }

        async fn copy_jmap_email(
            &self,
            _account_id: Uuid,
            _message_id: Uuid,
            target_mailbox_id: Uuid,
            _audit: AuditEntryInput,
        ) -> Result<JmapEmail> {
            let mut email = FakeStore::draft_email();
            email.id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
            email.mailbox_id = target_mailbox_id;
            email.mailbox_role = "".to_string();
            email.mailbox_name = "Archive".to_string();
            Ok(email)
        }

        async fn import_jmap_email(
            &self,
            input: JmapImportedEmailInput,
            _audit: AuditEntryInput,
        ) -> Result<JmapEmail> {
            Ok(JmapEmail {
                id: Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap(),
                thread_id: Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap(),
                mailbox_id: input.mailbox_id,
                mailbox_role: "".to_string(),
                mailbox_name: "Imported".to_string(),
                received_at: "2026-04-18T10:05:00Z".to_string(),
                sent_at: None,
                from_address: input.from_address,
                from_display: input.from_display,
                to: input
                    .to
                    .into_iter()
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address,
                        display_name: recipient.display_name,
                    })
                    .collect(),
                cc: input
                    .cc
                    .into_iter()
                    .map(|recipient| JmapEmailAddress {
                        address: recipient.address,
                        display_name: recipient.display_name,
                    })
                    .collect(),
                bcc: Vec::new(),
                subject: input.subject,
                preview: "Imported".to_string(),
                body_text: input.body_text,
                body_html_sanitized: None,
                unread: false,
                flagged: false,
                has_attachments: false,
                size_octets: input.size_octets,
                internet_message_id: input.internet_message_id,
                delivery_status: "stored".to_string(),
            })
        }

        async fn fetch_client_contacts(&self, _account_id: Uuid) -> Result<Vec<ClientContact>> {
            Ok(self.contacts.lock().unwrap().clone())
        }

        async fn fetch_client_contacts_by_ids(
            &self,
            _account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<ClientContact>> {
            Ok(self
                .contacts
                .lock()
                .unwrap()
                .iter()
                .filter(|contact| ids.contains(&contact.id))
                .cloned()
                .collect())
        }

        async fn upsert_client_contact(
            &self,
            input: UpsertClientContactInput,
        ) -> Result<ClientContact> {
            let contact = ClientContact {
                id: input.id.unwrap_or_else(Uuid::new_v4),
                name: input.name,
                role: input.role,
                email: input.email,
                phone: input.phone,
                team: input.team,
                notes: input.notes,
            };
            let mut contacts = self.contacts.lock().unwrap();
            contacts.retain(|entry| entry.id != contact.id);
            contacts.push(contact.clone());
            Ok(contact)
        }

        async fn delete_client_contact(&self, _account_id: Uuid, contact_id: Uuid) -> Result<()> {
            let mut contacts = self.contacts.lock().unwrap();
            let original_len = contacts.len();
            contacts.retain(|entry| entry.id != contact_id);
            if contacts.len() == original_len {
                bail!("contact not found");
            }
            Ok(())
        }

        async fn fetch_client_events(&self, _account_id: Uuid) -> Result<Vec<ClientEvent>> {
            Ok(self.events.lock().unwrap().clone())
        }

        async fn fetch_client_events_by_ids(
            &self,
            _account_id: Uuid,
            ids: &[Uuid],
        ) -> Result<Vec<ClientEvent>> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| ids.contains(&event.id))
                .cloned()
                .collect())
        }

        async fn upsert_client_event(&self, input: UpsertClientEventInput) -> Result<ClientEvent> {
            let event = ClientEvent {
                id: input.id.unwrap_or_else(Uuid::new_v4),
                date: input.date,
                time: input.time,
                time_zone: input.time_zone,
                duration_minutes: input.duration_minutes,
                recurrence_rule: input.recurrence_rule,
                title: input.title,
                location: input.location,
                attendees: input.attendees,
                attendees_json: input.attendees_json,
                notes: input.notes,
            };
            let mut events = self.events.lock().unwrap();
            events.retain(|entry| entry.id != event.id);
            events.push(event.clone());
            Ok(event)
        }

        async fn delete_client_event(&self, _account_id: Uuid, event_id: Uuid) -> Result<()> {
            let mut events = self.events.lock().unwrap();
            let original_len = events.len();
            events.retain(|entry| entry.id != event_id);
            if events.len() == original_len {
                bail!("event not found");
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn session_uses_existing_account_authentication() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        });

        let session = service
            .session_document(Some("Bearer token"))
            .await
            .unwrap();

        assert_eq!(session.username, "alice@example.test");
        assert_eq!(session.api_url, "/jmap/api");
        assert!(session.capabilities.contains_key(JMAP_MAIL_CAPABILITY));
    }

    #[tokio::test]
    async fn email_set_creates_draft_through_canonical_storage() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "Email/set".to_string(),
                        json!({
                            "create": {
                                "k1": {
                                    "from": [{"email": "alice@example.test", "name": "Alice"}],
                                    "to": [{"email": "bob@example.test"}],
                                    "bcc": [{"email": "hidden@example.test"}],
                                    "subject": "Hello",
                                    "textBody": "Draft body"
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let saved = store.saved_drafts.lock().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].from_address, "alice@example.test");
        assert_eq!(saved[0].bcc.len(), 1);
        assert!(response.created_ids.contains_key("k1"));
    }

    #[tokio::test]
    async fn email_submission_set_submits_existing_draft_and_returns_queued_state() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![JmapMethodCall(
                        "EmailSubmission/set".to_string(),
                        json!({
                            "create": {
                                "send1": {
                                    "emailId": FakeStore::draft_email().id.to_string()
                                }
                            }
                        }),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        let submitted = store.submitted_drafts.lock().unwrap();
        assert_eq!(submitted.as_slice(), &[FakeStore::draft_email().id]);
        let payload = &response.method_responses[0].1;
        assert_eq!(
            payload["created"]["send1"]["id"],
            Value::String("11111111-2222-3333-4444-555555555555".to_string())
        );
        assert_eq!(
            payload["created"]["send1"]["undoStatus"],
            Value::String("final".to_string())
        );
    }

    #[tokio::test]
    async fn mailbox_and_email_changes_return_existing_ids_from_initial_state() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "eml", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_CORE_CAPABILITY.to_string()],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/changes".to_string(),
                            json!({"sinceState": "0"}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/changes".to_string(),
                            json!({"sinceState": "0"}),
                            "c2".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["created"][0],
            Value::String(FakeStore::draft_mailbox().id.to_string())
        );
        assert_eq!(
            response.method_responses[1].1["created"][0],
            Value::String(FakeStore::draft_email().id.to_string())
        );
    }

    #[tokio::test]
    async fn identity_thread_and_submission_reads_are_available() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                        JMAP_SUBMISSION_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall("Identity/get".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall(
                            "Thread/get".to_string(),
                            json!({"ids": [FakeStore::draft_email().thread_id.to_string()]}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "EmailSubmission/get".to_string(),
                            json!({"ids": ["11111111-2222-3333-4444-555555555555"]}),
                            "c3".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["email"],
            Value::String("alice@example.test".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["list"][0]["emailIds"][0],
            Value::String(FakeStore::draft_email().id.to_string())
        );
        assert_eq!(
            response.method_responses[2].1["list"][0]["deliveryStatus"],
            Value::String("queued".to_string())
        );
    }

    #[tokio::test]
    async fn search_snippets_return_preview_for_requested_messages() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![JMAP_MAIL_CAPABILITY.to_string()],
                    method_calls: vec![JmapMethodCall(
                        "SearchSnippet/get".to_string(),
                        json!({"emailIds": [FakeStore::draft_email().id.to_string()]}),
                        "c1".to_string(),
                    )],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["preview"],
            Value::String("Draft preview".to_string())
        );
    }

    #[tokio::test]
    async fn mailbox_set_copy_import_and_quota_are_available() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: vec![FakeStore::draft_mailbox()],
            emails: vec![FakeStore::draft_email()],
            ..Default::default()
        };
        store.uploads.lock().unwrap().push(JmapUploadBlob {
            id: Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap(),
            account_id: FakeStore::account().account_id,
            media_type: "message/rfc822".to_string(),
            octet_size: 82,
            blob_bytes: b"From: Alice <alice@example.test>\r\nTo: Bob <bob@example.test>\r\nSubject: Imported\r\n\r\nHello".to_vec(),
        });
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "email", "eml", 0.99),
        );

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_MAIL_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall(
                            "Mailbox/set".to_string(),
                            json!({"create": {"m1": {"name": "Archive"}}}),
                            "c1".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/copy".to_string(),
                            json!({"fromAccountId": FakeStore::account().account_id.to_string(), "create": {"e1": {"emailId": FakeStore::draft_email().id.to_string(), "mailboxIds": {"99999999-9999-9999-9999-999999999999": true}}}}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "Email/import".to_string(),
                            json!({"emails": {"i1": {"blobId": "77777777-7777-7777-7777-777777777777", "mailboxIds": {"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb": true}}}}),
                            "c3".to_string(),
                        ),
                        JmapMethodCall("Quota/get".to_string(), json!({}), "c4".to_string()),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["created"]["m1"]["id"],
            Value::String("99999999-9999-9999-9999-999999999999".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["created"]["e1"]["id"],
            Value::String("66666666-6666-6666-6666-666666666666".to_string())
        );
        assert_eq!(
            response.method_responses[2].1["created"]["i1"]["id"],
            Value::String("55555555-5555-5555-5555-555555555555".to_string())
        );
        assert_eq!(
            response.method_responses[3].1["list"][0]["hardLimit"],
            Value::Number(100.into())
        );
    }

    #[tokio::test]
    async fn session_exposes_contacts_and_calendars_capabilities() {
        let service = JmapService::new(FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        });

        let session = service
            .session_document(Some("Bearer token"))
            .await
            .unwrap();

        assert!(session.capabilities.contains_key(JMAP_CONTACTS_CAPABILITY));
        assert!(session.capabilities.contains_key(JMAP_CALENDARS_CAPABILITY));
        assert_eq!(
            session.primary_accounts[JMAP_CONTACTS_CAPABILITY],
            FakeStore::account().account_id.to_string()
        );
        assert_eq!(
            session.primary_accounts[JMAP_CALENDARS_CAPABILITY],
            FakeStore::account().account_id.to_string()
        );
    }

    #[tokio::test]
    async fn contacts_methods_use_canonical_contact_store() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            contacts: Arc::new(Mutex::new(vec![FakeStore::contact()])),
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_CONTACTS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall("AddressBook/get".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall(
                            "ContactCard/get".to_string(),
                            json!({"ids": [FakeStore::contact().id.to_string()]}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "ContactCard/set".to_string(),
                            json!({
                                "create": {
                                    "new1": {
                                        "name": {"full": "Carol Example"},
                                        "emails": {"main": {"address": "carol@example.test"}},
                                        "phones": {"main": {"number": "+339999"}},
                                        "organizations": {"main": {"name": "Ops"}},
                                        "titles": {"main": {"name": "Manager"}},
                                        "notes": {"main": {"note": "Priority"}},
                                        "addressBookIds": {"default": true}
                                    }
                                }
                            }),
                            "c3".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["id"],
            Value::String("default".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["list"][0]["name"]["full"],
            Value::String("Bob Example".to_string())
        );
        assert!(response.created_ids.contains_key("new1"));
        let contacts = store.contacts.lock().unwrap();
        assert_eq!(contacts.len(), 2);
        assert!(contacts
            .iter()
            .any(|contact| contact.email == "carol@example.test"));
    }

    #[tokio::test]
    async fn calendar_methods_use_canonical_event_store() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            events: Arc::new(Mutex::new(vec![FakeStore::event()])),
            ..Default::default()
        };
        let service = JmapService::new(store.clone());

        let response = service
            .handle_api_request(
                Some("Bearer token"),
                JmapApiRequest {
                    using_capabilities: vec![
                        JMAP_CORE_CAPABILITY.to_string(),
                        JMAP_CALENDARS_CAPABILITY.to_string(),
                    ],
                    method_calls: vec![
                        JmapMethodCall("Calendar/get".to_string(), json!({}), "c1".to_string()),
                        JmapMethodCall(
                            "CalendarEvent/query".to_string(),
                            json!({"filter": {"inCalendar": "default"}}),
                            "c2".to_string(),
                        ),
                        JmapMethodCall(
                            "CalendarEvent/set".to_string(),
                            json!({
                                "create": {
                                    "ev1": {
                                        "@type": "Event",
                                        "title": "Planning",
                                        "start": "2026-04-21T11:00:00",
                                        "duration": "PT0S",
                                        "locations": {"main": {"name": "Room B"}},
                                        "participants": {"p1": {"name": "alice@example.test", "email": "alice@example.test"}},
                                        "description": "Weekly planning",
                                        "calendarIds": {"default": true}
                                    }
                                }
                            }),
                            "c3".to_string(),
                        ),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response.method_responses[0].1["list"][0]["id"],
            Value::String("default".to_string())
        );
        assert_eq!(
            response.method_responses[1].1["ids"][0],
            Value::String(FakeStore::event().id.to_string())
        );
        let events = store.events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().any(|event| event.title == "Planning"));
    }

    #[tokio::test]
    async fn upload_and_download_use_authenticated_account() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store.clone(),
            validator_ok("message/rfc822", "eml", "eml", 0.99),
        );

        let upload = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"Subject: Hello\r\n\r\nBody",
            )
            .await
            .unwrap();
        assert_eq!(
            upload["blobId"],
            Value::String("77777777-7777-7777-7777-777777777777".to_string())
        );

        let blob = service
            .handle_download(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "77777777-7777-7777-7777-777777777777",
            )
            .await
            .unwrap();
        assert_eq!(blob.media_type, "message/rfc822");
        assert_eq!(blob.blob_bytes, b"Subject: Hello\r\n\r\nBody".to_vec());
    }

    #[tokio::test]
    async fn upload_accepts_validated_matching_blob() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("message/rfc822", "eml", "eml", 0.99),
        );

        let upload = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"Subject: Hello\r\n\r\nBody",
            )
            .await
            .unwrap();

        assert_eq!(upload["type"], Value::String("message/rfc822".to_string()));
    }

    #[tokio::test]
    async fn upload_rejects_declared_mime_mismatch() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            validator_ok("application/pdf", "pdf", "pdf", 0.99),
        );

        let error = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"%PDF-1.7",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("JMAP upload blocked"));
    }

    #[tokio::test]
    async fn upload_rejects_unknown_type() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = JmapService::new_with_validator(
            store,
            Validator::new(
                FakeDetector {
                    result: Ok(MagikaDetection {
                        label: "unknown_binary".to_string(),
                        mime_type: "application/octet-stream".to_string(),
                        description: "unknown".to_string(),
                        group: "unknown".to_string(),
                        extensions: Vec::new(),
                        score: Some(0.99),
                    }),
                },
                0.80,
            ),
        );

        let error = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "application/octet-stream",
                b"\x00\x01\x02",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("JMAP upload blocked"));
    }

    #[tokio::test]
    async fn upload_surfaces_magika_failure_mode() {
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service =
            JmapService::new_with_validator(store, validator_error("Magika command failed"));

        let error = service
            .handle_upload(
                Some("Bearer token"),
                &FakeStore::account().account_id.to_string(),
                "message/rfc822",
                b"Subject: Hello\r\n\r\nBody",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("Magika command failed"));
    }
}
