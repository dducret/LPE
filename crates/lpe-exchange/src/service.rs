use anyhow::{anyhow, bail, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{
        header::{CONTENT_TYPE, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
    routing::{on, MethodFilter},
    Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use lpe_core::sieve::{Action, Statement};
use lpe_magika::{
    Detector, ExpectedKind, IngressContext, PolicyDecision, SystemDetector, ValidationRequest,
    Validator,
};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, ActiveSyncAttachment, ActiveSyncAttachmentContent,
    AttachmentUploadInput, AuditEntryInput, ClientTask, CollaborationCollection, JmapEmail,
    JmapEmailAddress, JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput, Storage,
    SubmitMessageInput, SubmittedRecipientInput, UpsertClientContactInput, UpsertClientEventInput,
    UpsertClientTaskInput,
};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::{
    mapi::{self, MapiEndpoint},
    store::ExchangeStore,
};

const EWS_PATH: &str = "/EWS/Exchange.asmx";
const EWS_LOWER_PATH: &str = "/ews/exchange.asmx";
const MAPI_EMSMDB_PATH: &str = "/mapi/emsmdb";
const MAPI_EMSMDB_TRAILING_PATH: &str = "/mapi/emsmdb/";
const MAPI_NSPI_PATH: &str = "/mapi/nspi";
const MAPI_NSPI_TRAILING_PATH: &str = "/mapi/nspi/";
const CONTACTS_FOLDER_ID: &str = "contacts";
const CALENDAR_FOLDER_ID: &str = "calendar";
const TASKS_FOLDER_ID: &str = "tasks";
const DEFAULT_COLLECTION_ID: &str = "default";
const MAILBOX_QUERY_LIMIT: u64 = 200;

pub fn router() -> Router<Storage> {
    Router::new()
        .route(
            EWS_PATH,
            on(MethodFilter::OPTIONS, options_handler).post(post_handler),
        )
        .route(
            EWS_LOWER_PATH,
            on(MethodFilter::OPTIONS, options_handler).post(post_handler),
        )
        .route(
            MAPI_EMSMDB_PATH,
            on(MethodFilter::OPTIONS, mapi_options_handler).post(mapi_emsmdb_post_handler),
        )
        .route(
            MAPI_EMSMDB_TRAILING_PATH,
            on(MethodFilter::OPTIONS, mapi_options_handler).post(mapi_emsmdb_post_handler),
        )
        .route(
            MAPI_NSPI_PATH,
            on(MethodFilter::OPTIONS, mapi_options_handler).post(mapi_nspi_post_handler),
        )
        .route(
            MAPI_NSPI_TRAILING_PATH,
            on(MethodFilter::OPTIONS, mapi_options_handler).post(mapi_nspi_post_handler),
        )
}

#[derive(Clone)]
pub(crate) struct ExchangeService<S, V = SystemDetector> {
    store: S,
    validator: Validator<V>,
}

impl<S> ExchangeService<S, SystemDetector> {
    pub(crate) fn new(store: S) -> Self {
        Self {
            store,
            validator: Validator::from_env(),
        }
    }
}

#[cfg(test)]
impl<S, V> ExchangeService<S, V> {
    pub(crate) fn new_with_validator(store: S, validator: Validator<V>) -> Self {
        Self { store, validator }
    }
}

async fn options_handler() -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .headers_mut()
        .insert("allow", HeaderValue::from_static("OPTIONS, POST"));
    response
}

async fn post_handler(State(storage): State<Storage>, headers: HeaderMap, body: Bytes) -> Response {
    let service = ExchangeService::new(storage);
    match service.handle(&headers, body.as_ref()).await {
        Ok(response) => response,
        Err(error) => error_response(&error),
    }
}

async fn mapi_options_handler() -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .headers_mut()
        .insert("allow", HeaderValue::from_static("OPTIONS, POST"));
    response.headers_mut().insert(
        "x-lpe-mapi-status",
        HeaderValue::from_static("transport-session-ready"),
    );
    response
}

async fn mapi_emsmdb_post_handler(
    State(storage): State<Storage>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    mapi_post_handler(MapiEndpoint::Emsmdb, storage, headers, body).await
}

async fn mapi_nspi_post_handler(
    State(storage): State<Storage>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    mapi_post_handler(MapiEndpoint::Nspi, storage, headers, body).await
}

async fn mapi_post_handler(
    endpoint: MapiEndpoint,
    storage: Storage,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let service = ExchangeService::new(storage);
    match service.handle_mapi(endpoint, &headers, body.as_ref()).await {
        Ok(response) => response,
        Err(error) => mapi::mapi_error_response(&error),
    }
}

impl<S: ExchangeStore, V: Detector> ExchangeService<S, V> {
    pub(crate) async fn handle(&self, headers: &HeaderMap, body: &[u8]) -> Result<Response> {
        let principal = authenticate_account(&self.store, None, headers, "ews").await?;
        let body = decode_ews_body(headers, body)?;
        let operation = operation_name(&body).ok_or_else(|| anyhow!("unsupported EWS request"))?;

        let payload = match operation.as_str() {
            "SyncFolderHierarchy" => self.sync_folder_hierarchy(&principal).await?,
            "FindFolder" => self.find_folder(&principal).await?,
            "GetFolder" => self.get_folder(&principal, &body).await?,
            "FindItem" => self.find_item(&principal, &body).await?,
            "GetItem" => self.get_item(&principal, &body).await?,
            "SyncFolderItems" => self
                .sync_folder_items(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "SyncFolderItems",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            "GetServerTimeZones" => get_server_time_zones_response(),
            "ResolveNames" => resolve_names_no_results_response(),
            "GetUserAvailability" => self.get_user_availability(&principal, &body).await?,
            "CreateItem" => self.create_item(&principal, &body).await?,
            "UpdateItem" => self.update_item(&principal, &body).await?,
            "DeleteItem" => self.delete_item(&principal, &body).await?,
            "MoveItem" => self.move_item(&principal, &body).await?,
            "CopyItem" => self.copy_item(&principal, &body).await?,
            "CreateFolder" => self.create_folder(&principal, &body).await?,
            "DeleteFolder" => self.delete_folder(&principal, &body).await?,
            "GetAttachment" => self.get_attachment(&principal, &body).await?,
            "CreateAttachment" => self.create_attachment(&principal, &body).await?,
            "DeleteAttachment" => self.delete_attachment(&principal, &body).await?,
            "GetUserOofSettings" => self.get_user_oof_settings(&principal).await?,
            "GetRoomLists" => unsupported_operation_response("GetRoomLists"),
            "FindPeople" => unsupported_operation_response("FindPeople"),
            "ExpandDL" => unsupported_operation_response("ExpandDL"),
            "Subscribe" => unsupported_operation_response("Subscribe"),
            "GetDelegate" => unsupported_operation_response("GetDelegate"),
            "GetUserConfiguration" => unsupported_operation_response("GetUserConfiguration"),
            "GetSharingMetadata" => unsupported_operation_response("GetSharingMetadata"),
            "GetSharingFolder" => unsupported_operation_response("GetSharingFolder"),
            "Unsubscribe" => unsupported_operation_response("Unsubscribe"),
            "GetEvents" => unsupported_operation_response("GetEvents"),
            _ => unsupported_operation_response(&operation),
        };

        Ok(soap_response(payload))
    }

    pub(crate) async fn handle_mapi(
        &self,
        endpoint: MapiEndpoint,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        mapi::handle_mapi(&self.store, endpoint, headers, body).await
    }

    async fn find_folder(&self, principal: &AccountPrincipal) -> Result<String> {
        let mut folders = String::new();
        for collection in self
            .store
            .fetch_accessible_contact_collections(principal.account_id)
            .await?
        {
            folders.push_str(&folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts"));
        }
        for collection in self
            .store
            .fetch_accessible_calendar_collections(principal.account_id)
            .await?
        {
            folders.push_str(&folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar"));
        }
        for collection in self
            .store
            .fetch_accessible_task_collections(principal.account_id)
            .await?
        {
            folders.push_str(&folder_xml(&collection, TASKS_FOLDER_ID, "Task"));
        }
        for mailbox in self
            .store
            .fetch_jmap_mailboxes(principal.account_id)
            .await?
        {
            folders.push_str(&mailbox_folder_xml(&mailbox));
        }

        Ok(format!(
            concat!(
                "<m:FindFolderResponse>",
                "<m:ResponseMessages>",
                "<m:FindFolderResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:RootFolder TotalItemsInView=\"{count}\" IncludesLastItemInRange=\"true\">",
                "<t:Folders>{folders}</t:Folders>",
                "</m:RootFolder>",
                "</m:FindFolderResponseMessage>",
                "</m:ResponseMessages>",
                "</m:FindFolderResponse>"
            ),
            folders = folders,
            count = count_folder_elements(&folders),
        ))
    }

    async fn sync_folder_hierarchy(&self, principal: &AccountPrincipal) -> Result<String> {
        let mut changes = String::new();
        let mut count = 0;
        for collection in self
            .store
            .fetch_accessible_contact_collections(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts"));
            changes.push_str("</t:Create>");
            count += 1;
        }
        for collection in self
            .store
            .fetch_accessible_calendar_collections(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar"));
            changes.push_str("</t:Create>");
            count += 1;
        }
        for collection in self
            .store
            .fetch_accessible_task_collections(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&folder_xml(&collection, TASKS_FOLDER_ID, "Task"));
            changes.push_str("</t:Create>");
            count += 1;
        }
        for mailbox in self
            .store
            .fetch_jmap_mailboxes(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&mailbox_folder_xml(&mailbox));
            changes.push_str("</t:Create>");
            count += 1;
        }
        let sync_state = format!("folder-hierarchy:{count}");

        Ok(format!(
            concat!(
                "<m:SyncFolderHierarchyResponse>",
                "<m:ResponseMessages>",
                "<m:SyncFolderHierarchyResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:SyncState>{sync_state}</m:SyncState>",
                "<m:IncludesLastFolderInRange>true</m:IncludesLastFolderInRange>",
                "<m:Changes>{changes}</m:Changes>",
                "</m:SyncFolderHierarchyResponseMessage>",
                "</m:ResponseMessages>",
                "</m:SyncFolderHierarchyResponse>"
            ),
            sync_state = escape_xml(&sync_state),
            changes = changes,
        ))
    }

    async fn get_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let mailbox_ids = self
            .requested_mailbox_folder_ids(principal, request)
            .await?;
        if !mailbox_ids.is_empty() {
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            let mut folders = String::new();
            for mailbox_id in &mailbox_ids {
                let Some(mailbox) = mailboxes.iter().find(|mailbox| mailbox.id == *mailbox_id)
                else {
                    return Ok(get_folder_error_response(
                        "ErrorFolderNotFound",
                        "requested mailbox folder is not exposed by EWS",
                    ));
                };
                folders.push_str(&mailbox_folder_xml(mailbox));
            }

            return Ok(format!(
                concat!(
                    "<m:GetFolderResponse>",
                    "<m:ResponseMessages>",
                    "<m:GetFolderResponseMessage ResponseClass=\"Success\">",
                    "<m:ResponseCode>NoError</m:ResponseCode>",
                    "<m:Folders>{folders}</m:Folders>",
                    "</m:GetFolderResponseMessage>",
                    "</m:ResponseMessages>",
                    "</m:GetFolderResponse>"
                ),
                folders = folders,
            ));
        }

        let requested = requested_folder_kinds(request);
        if requested.is_empty() && request_contains_folder_reference(request) {
            return Ok(get_folder_error_response(
                "ErrorFolderNotFound",
                "folder not found",
            ));
        }

        let mut folders = String::new();
        for kind in requested {
            match kind {
                FolderKind::Root => {
                    folders.push_str(&root_folder_xml(
                        self.root_child_folder_count(principal).await?,
                    ));
                }
                FolderKind::Contacts => {
                    folders.push_str(
                        &self
                            .store
                            .fetch_accessible_contact_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(|collection| {
                                folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts")
                            })
                            .collect::<String>(),
                    );
                }
                FolderKind::Calendar => {
                    folders.push_str(
                        &self
                            .store
                            .fetch_accessible_calendar_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(|collection| {
                                folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar")
                            })
                            .collect::<String>(),
                    );
                }
                FolderKind::Tasks => {
                    folders.push_str(
                        &self
                            .store
                            .fetch_accessible_task_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(|collection| folder_xml(&collection, TASKS_FOLDER_ID, "Task"))
                            .collect::<String>(),
                    );
                }
                FolderKind::Mailbox => {
                    let mailbox_ids = self
                        .requested_mailbox_folder_ids(principal, request)
                        .await?;
                    let mailboxes = self
                        .store
                        .fetch_jmap_mailboxes(principal.account_id)
                        .await?;
                    for mailbox in mailboxes.into_iter().filter(|mailbox| {
                        mailbox_ids.is_empty() || mailbox_ids.contains(&mailbox.id)
                    }) {
                        folders.push_str(&mailbox_folder_xml(&mailbox));
                    }
                }
            }
        }

        if folders.is_empty() {
            return Ok(get_folder_error_response(
                "ErrorFolderNotFound",
                "folder not found",
            ));
        }

        Ok(format!(
            concat!(
                "<m:GetFolderResponse>",
                "<m:ResponseMessages>",
                "<m:GetFolderResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:Folders>{folders}</m:Folders>",
                "</m:GetFolderResponseMessage>",
                "</m:ResponseMessages>",
                "</m:GetFolderResponse>"
            ),
            folders = folders,
        ))
    }

    async fn find_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        match requested_folder_kind(request).unwrap_or(FolderKind::Contacts) {
            FolderKind::Root => Ok(find_item_response(String::new())),
            FolderKind::Contacts => {
                let collection_id = requested_collection_id(request).unwrap_or(CONTACTS_FOLDER_ID);
                let contacts = self
                    .store
                    .fetch_accessible_contacts_in_collection(principal.account_id, collection_id)
                    .await?;
                Ok(find_item_response(
                    contacts.iter().map(contact_summary_xml).collect(),
                ))
            }
            FolderKind::Calendar => {
                let collection_id = requested_collection_id(request).unwrap_or(CALENDAR_FOLDER_ID);
                let events = self
                    .store
                    .fetch_accessible_events_in_collection(principal.account_id, collection_id)
                    .await?;
                Ok(find_item_response(
                    events.iter().map(calendar_item_summary_xml).collect(),
                ))
            }
            FolderKind::Tasks => {
                let collection_id = requested_collection_id(request).unwrap_or(TASKS_FOLDER_ID);
                let tasks = self
                    .store
                    .fetch_accessible_tasks_in_collection(principal.account_id, collection_id)
                    .await?;
                Ok(find_item_response(
                    tasks.iter().map(task_item_summary_xml).collect(),
                ))
            }
            FolderKind::Mailbox => {
                let Some(mailbox_id) = self
                    .requested_mailbox_folder_ids(principal, request)
                    .await?
                    .into_iter()
                    .next()
                else {
                    return Ok(find_item_response(String::new()));
                };
                let query = self
                    .store
                    .query_jmap_email_ids(
                        principal.account_id,
                        Some(mailbox_id),
                        None,
                        0,
                        MAILBOX_QUERY_LIMIT,
                    )
                    .await?;
                let emails = self
                    .store
                    .fetch_jmap_emails(principal.account_id, &query.ids)
                    .await?;
                Ok(find_item_response(
                    emails
                        .iter()
                        .filter(|email| email.mailbox_id == mailbox_id)
                        .map(message_summary_xml)
                        .collect(),
                ))
            }
        }
    }

    async fn get_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let include_mime_content = requested_mime_content(request);
        let ids = requested_item_ids(request);
        let contact_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("contact:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();
        let event_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("event:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();
        let task_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("task:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();
        let message_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("message:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();
        let supported_id_count =
            contact_ids.len() + event_ids.len() + task_ids.len() + message_ids.len();

        let mut items = String::new();
        for contact in self
            .store
            .fetch_accessible_contacts_by_ids(principal.account_id, &contact_ids)
            .await?
        {
            items.push_str(&contact_item_xml(&contact));
        }
        for event in self
            .store
            .fetch_accessible_events_by_ids(principal.account_id, &event_ids)
            .await?
        {
            items.push_str(&calendar_item_xml(&event));
        }
        for task in self
            .store
            .fetch_accessible_tasks_by_ids(principal.account_id, &task_ids)
            .await?
        {
            items.push_str(&task_item_xml(&task));
        }
        for email in self
            .store
            .fetch_jmap_emails(principal.account_id, &message_ids)
            .await?
            .into_iter()
        {
            let attachments = if email.has_attachments {
                self.store
                    .fetch_message_attachments(principal.account_id, email.id)
                    .await?
            } else {
                Vec::new()
            };
            let mut attachment_contents = Vec::new();
            if include_mime_content {
                for attachment in &attachments {
                    let Some(content) = self
                        .store
                        .fetch_attachment_content(principal.account_id, &attachment.file_reference)
                        .await?
                    else {
                        return Ok(get_item_error_response(
                            "ErrorItemNotFound",
                            "The requested item attachment content was not found.",
                        ));
                    };
                    attachment_contents.push(content);
                }
            }
            items.push_str(&message_item_xml_with_details(
                &email,
                &attachments,
                include_mime_content.then_some(attachment_contents.as_slice()),
            ));
        }

        if !ids.is_empty()
            && (supported_id_count != ids.len()
                || count_tag_occurrences(&items, "<t:ItemId") != supported_id_count)
        {
            return Ok(get_item_error_response(
                "ErrorItemNotFound",
                "The requested item was not found or is not exposed by the EWS MVP.",
            ));
        }

        Ok(format!(
            concat!(
                "<m:GetItemResponse>",
                "<m:ResponseMessages>",
                "<m:GetItemResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:Items>{items}</m:Items>",
                "</m:GetItemResponseMessage>",
                "</m:ResponseMessages>",
                "</m:GetItemResponse>"
            ),
            items = items,
        ))
    }

    async fn get_attachment(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let ids = requested_attachment_ids(request);
        if ids.is_empty() {
            return Ok(operation_error_response(
                "GetAttachment",
                "ErrorInvalidOperation",
                "GetAttachment requires at least one AttachmentId.",
            ));
        }

        let mut attachments = String::new();
        for id in ids {
            let Some(content) = self
                .store
                .fetch_attachment_content(principal.account_id, &id)
                .await?
            else {
                return Ok(operation_error_response(
                    "GetAttachment",
                    "ErrorAttachmentNotFound",
                    "The requested attachment was not found or is not exposed by EWS.",
                ));
            };
            attachments.push_str(&file_attachment_content_xml(&content));
        }

        Ok(get_attachment_success_response(attachments))
    }

    async fn create_attachment(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let ids = requested_item_ids(request);
        let message_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("message:"))
            .map(Uuid::parse_str)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if ids.len() != 1 || message_ids.len() != 1 {
            return Ok(operation_error_response(
                "CreateAttachment",
                "ErrorInvalidOperation",
                "CreateAttachment currently supports exactly one canonical message parent id.",
            ));
        }
        if element_content(request, "ItemAttachment").is_some() {
            return Ok(operation_error_response(
                "CreateAttachment",
                "ErrorInvalidOperation",
                "CreateAttachment currently supports only FileAttachment payloads.",
            ));
        }

        let file_attachments = element_contents(request, "FileAttachment");
        if file_attachments.is_empty() {
            return Ok(operation_error_response(
                "CreateAttachment",
                "ErrorInvalidOperation",
                "CreateAttachment requires at least one FileAttachment.",
            ));
        }

        let message_id = message_ids[0];
        let mut attachments = String::new();
        let mut root_item = String::new();
        for file_attachment in file_attachments {
            let mut attachment = match parse_file_attachment_upload(file_attachment) {
                Ok(attachment) => attachment,
                Err(error) => {
                    return Ok(operation_error_response(
                        "CreateAttachment",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    ));
                }
            };

            let declared_mime = element_text(file_attachment, "ContentType")
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            let outcome = self.validator.validate_bytes(
                ValidationRequest {
                    ingress_context: IngressContext::ExchangeAttachment,
                    declared_mime: declared_mime.clone(),
                    filename: Some(attachment.file_name.clone()),
                    expected_kind: expected_attachment_kind(
                        &attachment.media_type,
                        &attachment.file_name,
                    ),
                },
                &attachment.blob_bytes,
            )?;
            if outcome.policy_decision != PolicyDecision::Accept {
                return Ok(operation_error_response(
                    "CreateAttachment",
                    "ErrorInvalidOperation",
                    &outcome.reason,
                ));
            }
            if declared_mime.is_none() && !outcome.detected_mime.trim().is_empty() {
                attachment.media_type = outcome.detected_mime.clone();
            }

            let Some((email, stored_attachment)) = self
                .store
                .add_message_attachment(
                    principal.account_id,
                    message_id,
                    attachment,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-create-attachment".to_string(),
                        subject: format!("message:{message_id}"),
                    },
                )
                .await?
            else {
                return Ok(operation_error_response(
                    "CreateAttachment",
                    "ErrorItemNotFound",
                    "The requested parent message was not found or is not exposed by EWS.",
                ));
            };
            root_item = root_item_id_xml(&email);
            attachments.push_str(&file_attachment_reference_xml(&stored_attachment));
        }

        Ok(create_attachment_success_response(attachments, root_item))
    }

    async fn delete_attachment(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let ids = requested_attachment_ids(request);
        if ids.is_empty() {
            return Ok(operation_error_response(
                "DeleteAttachment",
                "ErrorInvalidOperation",
                "DeleteAttachment requires at least one AttachmentId.",
            ));
        }

        let mut root_items = String::new();
        for id in ids {
            let Some(email) = self
                .store
                .delete_message_attachment(
                    principal.account_id,
                    &id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-delete-attachment".to_string(),
                        subject: id.clone(),
                    },
                )
                .await?
            else {
                return Ok(operation_error_response(
                    "DeleteAttachment",
                    "ErrorAttachmentNotFound",
                    "The requested attachment was not found or is not exposed by EWS.",
                ));
            };
            root_items.push_str(&root_item_id_xml(&email));
        }

        Ok(delete_attachment_success_response(root_items))
    }

    async fn get_user_oof_settings(&self, principal: &AccountPrincipal) -> Result<String> {
        let script = self
            .store
            .fetch_active_sieve_script(principal.account_id)
            .await?;
        Ok(get_user_oof_settings_response(&oof_projection_from_script(
            script.as_ref().map(|script| script.content.as_str()),
        )))
    }

    async fn get_user_availability(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        if let Some(mailbox) = element_content(request, "MailboxData")
            .and_then(|mailbox_data| element_content(mailbox_data, "Email"))
            .and_then(parse_mailbox)
        {
            if !mailbox.address.eq_ignore_ascii_case(&principal.email) {
                return Ok(get_user_availability_error_response(
                    "Free/busy is currently available only for the authenticated mailbox.",
                ));
            }
        }

        let (window_start, window_end) = requested_availability_window(request);
        let mut events = self
            .store
            .fetch_accessible_events_in_collection(principal.account_id, DEFAULT_COLLECTION_ID)
            .await?;
        events.retain(|event| {
            event_overlaps_window(event, window_start.as_deref(), window_end.as_deref())
        });
        events.sort_by(|left, right| {
            ews_datetime(&left.date, &left.time).cmp(&ews_datetime(&right.date, &right.time))
        });
        Ok(get_user_availability_success_response(&events))
    }

    async fn root_child_folder_count(&self, principal: &AccountPrincipal) -> Result<usize> {
        Ok(self
            .store
            .fetch_accessible_contact_collections(principal.account_id)
            .await?
            .len()
            + self
                .store
                .fetch_accessible_calendar_collections(principal.account_id)
                .await?
                .len()
            + self
                .store
                .fetch_accessible_task_collections(principal.account_id)
                .await?
                .len()
            + self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?
                .len())
    }

    async fn sync_folder_items(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let mut changes = String::new();
        let sync_state = match requested_folder_kind(request).unwrap_or(FolderKind::Contacts) {
            FolderKind::Root => "root:0".to_string(),
            FolderKind::Contacts => {
                let collection_id = requested_collection_id(request).unwrap_or(CONTACTS_FOLDER_ID);
                let contacts = self
                    .store
                    .fetch_accessible_contacts_in_collection(principal.account_id, collection_id)
                    .await?;
                let sync_versions = sync_version_by_id(
                    self.store
                        .fetch_contact_sync_versions(principal.account_id, collection_id)
                        .await?,
                );
                let current_items = contacts
                    .iter()
                    .map(|contact| {
                        (
                            contact.id,
                            contact_change_key(
                                contact,
                                sync_versions.get(&contact.id).map(String::as_str),
                            ),
                        )
                    })
                    .collect::<Vec<_>>();
                let current_set = current_items
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>();
                let previous_state = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_items(&state, "contacts", collection_id))
                    .unwrap_or_default();
                let previous_by_id = sync_state_items_by_id(&previous_state.items);
                for contact in &contacts {
                    let current_change_key = contact_change_key(
                        contact,
                        sync_versions.get(&contact.id).map(String::as_str),
                    );
                    match previous_by_id.get(&contact.id) {
                        None => {
                            changes.push_str("<t:Create>");
                            changes.push_str(&contact_item_xml_with_change_key(
                                contact,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Create>");
                        }
                        Some(None) => {
                            changes.push_str("<t:Update>");
                            changes.push_str(&contact_item_xml_with_change_key(
                                contact,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        Some(Some(previous_change_key))
                            if !previous_state.is_current_version
                                || previous_change_key != &current_change_key =>
                        {
                            changes.push_str("<t:Update>");
                            changes.push_str(&contact_item_xml_with_change_key(
                                contact,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let contact_id = item.id;
                    if !current_set.contains(&contact_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"contact:{contact_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                collaboration_sync_state("contacts", collection_id, &current_items)
            }
            FolderKind::Calendar => {
                let collection_id = requested_collection_id(request).unwrap_or(CALENDAR_FOLDER_ID);
                let events = self
                    .store
                    .fetch_accessible_events_in_collection(principal.account_id, collection_id)
                    .await?;
                let sync_versions = sync_version_by_id(
                    self.store
                        .fetch_event_sync_versions(principal.account_id, collection_id)
                        .await?,
                );
                let current_items = events
                    .iter()
                    .map(|event| {
                        (
                            event.id,
                            calendar_change_key(
                                event,
                                sync_versions.get(&event.id).map(String::as_str),
                            ),
                        )
                    })
                    .collect::<Vec<_>>();
                let current_set = current_items
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>();
                let previous_state = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_items(&state, "calendar", collection_id))
                    .unwrap_or_default();
                let previous_by_id = sync_state_items_by_id(&previous_state.items);
                for event in &events {
                    let current_change_key = calendar_change_key(
                        event,
                        sync_versions.get(&event.id).map(String::as_str),
                    );
                    match previous_by_id.get(&event.id) {
                        None => {
                            changes.push_str("<t:Create>");
                            changes.push_str(&calendar_item_xml_with_change_key(
                                event,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Create>");
                        }
                        Some(None) => {
                            changes.push_str("<t:Update>");
                            changes.push_str(&calendar_item_xml_with_change_key(
                                event,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        Some(Some(previous_change_key))
                            if !previous_state.is_current_version
                                || previous_change_key != &current_change_key =>
                        {
                            changes.push_str("<t:Update>");
                            changes.push_str(&calendar_item_xml_with_change_key(
                                event,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let event_id = item.id;
                    if !current_set.contains(&event_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"event:{event_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                collaboration_sync_state("calendar", collection_id, &current_items)
            }
            FolderKind::Tasks => {
                let collection_id = requested_collection_id(request).unwrap_or(TASKS_FOLDER_ID);
                let tasks = self
                    .store
                    .fetch_accessible_tasks_in_collection(principal.account_id, collection_id)
                    .await?;
                let sync_versions = sync_version_by_id(
                    self.store
                        .fetch_task_sync_versions(principal.account_id, collection_id)
                        .await?,
                );
                let current_items = tasks
                    .iter()
                    .map(|task| {
                        (
                            task.id,
                            task_change_key(task, sync_versions.get(&task.id).map(String::as_str)),
                        )
                    })
                    .collect::<Vec<_>>();
                let current_set = current_items
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>();
                let previous_state = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_items(&state, "tasks", collection_id))
                    .unwrap_or_default();
                let previous_by_id = sync_state_items_by_id(&previous_state.items);
                for task in &tasks {
                    let current_change_key =
                        task_change_key(task, sync_versions.get(&task.id).map(String::as_str));
                    match previous_by_id.get(&task.id) {
                        None => {
                            changes.push_str("<t:Create>");
                            changes.push_str(&task_item_xml_with_change_key(
                                task,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Create>");
                        }
                        Some(None) => {
                            changes.push_str("<t:Update>");
                            changes.push_str(&task_item_xml_with_change_key(
                                task,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        Some(Some(previous_change_key))
                            if !previous_state.is_current_version
                                || previous_change_key != &current_change_key =>
                        {
                            changes.push_str("<t:Update>");
                            changes.push_str(&task_item_xml_with_change_key(
                                task,
                                &current_change_key,
                            ));
                            changes.push_str("</t:Update>");
                        }
                        _ => {}
                    }
                }
                for item in previous_state.items {
                    let task_id = item.id;
                    if !current_set.contains(&task_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"task:{task_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                collaboration_sync_state("tasks", collection_id, &current_items)
            }
            FolderKind::Mailbox => {
                let Some(mailbox_id) = self
                    .requested_mailbox_folder_ids(principal, request)
                    .await?
                    .into_iter()
                    .next()
                else {
                    return Ok(sync_folder_items_response("mailbox:0", String::new()));
                };
                let query = self
                    .store
                    .query_jmap_email_ids(
                        principal.account_id,
                        Some(mailbox_id),
                        None,
                        0,
                        MAILBOX_QUERY_LIMIT,
                    )
                    .await?;
                let emails = self
                    .store
                    .fetch_jmap_emails(principal.account_id, &query.ids)
                    .await?
                    .into_iter()
                    .filter(|email| email.mailbox_id == mailbox_id)
                    .collect::<Vec<_>>();
                let current_ids = emails.iter().map(|email| email.id).collect::<Vec<_>>();
                let current_set = current_ids.iter().copied().collect::<HashSet<_>>();
                let previous_ids = requested_sync_state(request)
                    .map(|state| mailbox_sync_state_ids(&state, mailbox_id))
                    .unwrap_or_default();
                let previous_set = previous_ids.iter().copied().collect::<HashSet<_>>();

                for email in &emails {
                    if !previous_set.contains(&email.id) {
                        changes.push_str("<t:Create>");
                        changes.push_str(&message_summary_xml(email));
                        changes.push_str("</t:Create>");
                    }
                }
                for message_id in previous_ids {
                    if !current_set.contains(&message_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"message:{message_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                mailbox_sync_state(mailbox_id, &current_ids)
            }
        };

        Ok(sync_folder_items_response(&sync_state, changes))
    }

    async fn create_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            if element_content(request, "Contact").is_some() {
                let collection_id = requested_collection_id(request);
                let contact = self
                    .store
                    .create_accessible_contact(
                        principal.account_id,
                        collection_id,
                        parse_create_contact_input(principal, request)?,
                    )
                    .await?;
                return Ok(create_contact_success_response(&contact));
            }
            if element_content(request, "CalendarItem").is_some() {
                let collection_id = requested_collection_id(request);
                let event = self
                    .store
                    .create_accessible_event(
                        principal.account_id,
                        collection_id,
                        parse_create_event_input(principal, request)?,
                    )
                    .await?;
                return Ok(create_event_success_response(&event));
            }
            if element_content(request, "Task").is_some() {
                let task = self
                    .store
                    .create_accessible_task(
                        principal.account_id,
                        parse_create_task_input(principal, request)?,
                    )
                    .await?;
                return Ok(create_task_success_response(&task));
            }

            let input = parse_create_message_input(principal, request)?;
            let subject_for_audit = input.subject.clone();
            let disposition = attribute_value_after(request, "CreateItem", "MessageDisposition")
                .unwrap_or("SaveOnly");

            match disposition {
                "SaveOnly" => {
                    if let Some(mailbox_id) = self
                        .requested_mailbox_folder_ids(principal, request)
                        .await?
                        .into_iter()
                        .next()
                    {
                        let imported = self
                            .store
                            .import_jmap_email(
                                imported_email_input(input, mailbox_id),
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "ews-import-custom-mailbox-message".to_string(),
                                    subject: subject_for_audit,
                                },
                            )
                            .await?;
                        return Ok(create_item_success_response(
                            imported.id,
                            &imported.delivery_status,
                        ));
                    }
                    let draft = self
                        .store
                        .save_draft_message(
                            input,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-save-draft-message".to_string(),
                                subject: subject_for_audit,
                            },
                        )
                        .await?;
                    Ok(create_item_success_response(draft.message_id, "draft"))
                }
                "SendOnly" | "SendAndSaveCopy" => {
                    let submitted = self
                        .store
                        .submit_message(
                            input,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-submit-message".to_string(),
                                subject: subject_for_audit,
                            },
                        )
                        .await?;
                    Ok(create_item_success_response(submitted.message_id, "queued"))
                }
                other => Ok(operation_error_response(
                    "CreateItem",
                    "ErrorInvalidOperation",
                    &format!("unsupported CreateItem MessageDisposition {other}"),
                )),
            }
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("CreateItem", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    async fn update_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let ids = requested_item_ids(request);
            let contact_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("contact:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let event_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("event:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let task_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("task:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty()
                || contact_ids.len() + event_ids.len() + task_ids.len() + message_ids.len()
                    != ids.len()
            {
                return Ok(operation_error_response(
                    "UpdateItem",
                    "ErrorInvalidOperation",
                    "UpdateItem currently supports only contact, calendar, task, and read/flag message item ids.",
                ));
            }

            let mut items = String::new();
            if !message_ids.is_empty() {
                let Some((unread, flagged)) = parse_update_message_flags(request)? else {
                    return Ok(operation_error_response(
                        "UpdateItem",
                        "ErrorInvalidOperation",
                        "UpdateItem message updates currently support only IsRead and FlagStatus.",
                    ));
                };
                for message_id in message_ids {
                    let updated = self
                        .store
                        .update_jmap_email_flags(
                            principal.account_id,
                            message_id,
                            unread,
                            flagged,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-update-message-flags".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                    items.push_str(&message_item_xml(&updated));
                }
            }
            for contact_id in contact_ids {
                let existing = self
                    .store
                    .fetch_accessible_contacts_by_ids(principal.account_id, &[contact_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("contact not found"))?;
                let updated = self
                    .store
                    .update_accessible_contact(
                        principal.account_id,
                        contact_id,
                        parse_update_contact_input(principal, &existing, request),
                    )
                    .await?;
                items.push_str(&contact_item_xml(&updated));
            }
            for event_id in event_ids {
                let existing = self
                    .store
                    .fetch_accessible_events_by_ids(principal.account_id, &[event_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("event not found"))?;
                let updated = self
                    .store
                    .update_accessible_event(
                        principal.account_id,
                        event_id,
                        parse_update_event_input(principal, &existing, request)?,
                    )
                    .await?;
                items.push_str(&calendar_item_xml(&updated));
            }
            for task_id in task_ids {
                let existing = self
                    .store
                    .fetch_accessible_tasks_by_ids(principal.account_id, &[task_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("task not found"))?;
                let updated = self
                    .store
                    .update_accessible_task(
                        principal.account_id,
                        task_id,
                        parse_update_task_input(principal, &existing, request)?,
                    )
                    .await?;
                items.push_str(&task_item_xml(&updated));
            }

            Ok(update_item_success_response(items))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("UpdateItem", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    async fn delete_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let ids = requested_item_ids(request);
            let contact_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("contact:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let event_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("event:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let task_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("task:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty()
                || contact_ids.len() + event_ids.len() + task_ids.len() + message_ids.len()
                    != ids.len()
            {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorInvalidOperation",
                    "DeleteItem currently supports only contact, calendar, task, and message ids.",
                ));
            }
            for contact_id in contact_ids {
                self.store
                    .delete_accessible_contact(principal.account_id, contact_id)
                    .await?;
            }
            for event_id in event_ids {
                self.store
                    .delete_accessible_event(principal.account_id, event_id)
                    .await?;
            }
            for task_id in task_ids {
                self.store
                    .delete_accessible_task(principal.account_id, task_id)
                    .await?;
            }
            let delete_type = attribute_value_after(request, "DeleteItem", "DeleteType")
                .unwrap_or("MoveToDeletedItems");
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            let trash_mailbox_id = mailboxes
                .iter()
                .find(|mailbox| mailbox.role == "trash")
                .map(|mailbox| mailbox.id);

            for message_id in message_ids {
                let existing = self
                    .store
                    .fetch_jmap_emails(principal.account_id, &[message_id])
                    .await?;
                let Some(email) = existing.into_iter().next() else {
                    return Ok(operation_error_response(
                        "DeleteItem",
                        "ErrorItemNotFound",
                        "message not found",
                    ));
                };

                if delete_type == "HardDelete" || email.mailbox_role == "trash" {
                    self.store
                        .delete_jmap_email(
                            principal.account_id,
                            message_id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-delete-message".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                } else if let Some(trash_mailbox_id) = trash_mailbox_id {
                    self.store
                        .move_jmap_email(
                            principal.account_id,
                            message_id,
                            trash_mailbox_id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-move-message-to-trash".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                } else {
                    self.store
                        .delete_jmap_email(
                            principal.account_id,
                            message_id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-delete-message-without-trash".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                }
            }

            Ok(delete_item_success_response())
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("DeleteItem", "ErrorItemNotFound", &error.to_string())
        }))
    }

    async fn move_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let ids = requested_item_ids(request);
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty() || message_ids.len() != ids.len() {
                return Ok(operation_error_response(
                    "MoveItem",
                    "ErrorInvalidOperation",
                    "MoveItem currently supports only canonical message ids.",
                ));
            }

            let target_mailbox_ids = self
                .requested_mailbox_folder_ids(principal, request)
                .await?;
            if target_mailbox_ids.len() != 1 {
                return Ok(operation_error_response(
                    "MoveItem",
                    "ErrorInvalidOperation",
                    "MoveItem requires exactly one canonical mailbox target folder.",
                ));
            }
            let target_mailbox_id = target_mailbox_ids[0];
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            if !mailboxes
                .iter()
                .any(|mailbox| mailbox.id == target_mailbox_id)
            {
                return Ok(operation_error_response(
                    "MoveItem",
                    "ErrorFolderNotFound",
                    "target mailbox folder not found",
                ));
            }

            let mut items = String::new();
            for message_id in message_ids {
                let moved = self
                    .store
                    .move_jmap_email(
                        principal.account_id,
                        message_id,
                        target_mailbox_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-move-message".to_string(),
                            subject: format!("{message_id}->{target_mailbox_id}"),
                        },
                    )
                    .await?;
                items.push_str(&message_item_xml(&moved));
            }

            Ok(move_item_success_response(items))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("MoveItem", "ErrorItemNotFound", &error.to_string())
        }))
    }

    async fn copy_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let ids = requested_item_ids(request);
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty() || message_ids.len() != ids.len() {
                return Ok(operation_error_response(
                    "CopyItem",
                    "ErrorInvalidOperation",
                    "CopyItem currently supports only canonical message ids.",
                ));
            }

            let target_mailbox_ids = self
                .requested_mailbox_folder_ids(principal, request)
                .await?;
            if target_mailbox_ids.len() != 1 {
                return Ok(operation_error_response(
                    "CopyItem",
                    "ErrorInvalidOperation",
                    "CopyItem requires exactly one canonical mailbox target folder.",
                ));
            }
            let target_mailbox_id = target_mailbox_ids[0];
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            if !mailboxes
                .iter()
                .any(|mailbox| mailbox.id == target_mailbox_id)
            {
                return Ok(operation_error_response(
                    "CopyItem",
                    "ErrorFolderNotFound",
                    "target mailbox folder not found",
                ));
            }

            let mut items = String::new();
            for message_id in message_ids {
                let copied = self
                    .store
                    .copy_jmap_email(
                        principal.account_id,
                        message_id,
                        target_mailbox_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-copy-message".to_string(),
                            subject: format!("{message_id}->{target_mailbox_id}"),
                        },
                    )
                    .await?;
                items.push_str(&message_item_xml(&copied));
            }

            Ok(copy_item_success_response(items))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("CopyItem", "ErrorItemNotFound", &error.to_string())
        }))
    }

    async fn requested_mailbox_folder_ids(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<Vec<Uuid>> {
        let mut ids = requested_mailbox_folder_ids(request);
        if ids.is_empty() {
            if let Some(mailbox_id) =
                requested_sync_state(request).and_then(|state| mailbox_sync_state_folder_id(&state))
            {
                ids.push(mailbox_id);
            }
        }
        if ids.is_empty() {
            if let Some(role) = requested_mailbox_role(request) {
                ids.extend(
                    self.store
                        .fetch_jmap_mailboxes(principal.account_id)
                        .await?
                        .into_iter()
                        .filter(|mailbox| mailbox.role == role)
                        .map(|mailbox| mailbox.id),
                );
            }
        }
        Ok(ids)
    }

    async fn create_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let display_name = element_text(request, "DisplayName")
                .ok_or_else(|| anyhow!("CreateFolder is missing DisplayName"))?;
            let mailbox = self
                .store
                .create_jmap_mailbox(
                    JmapMailboxCreateInput {
                        account_id: principal.account_id,
                        name: display_name.clone(),
                        sort_order: None,
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-create-folder".to_string(),
                        subject: display_name,
                    },
                )
                .await?;

            Ok(create_folder_success_response(&mailbox))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("CreateFolder", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    async fn delete_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let folder_ids = requested_mailbox_folder_ids(request);
            if folder_ids.is_empty() {
                return Ok(operation_error_response(
                    "DeleteFolder",
                    "ErrorInvalidOperation",
                    "DeleteFolder currently supports only mailbox folder ids.",
                ));
            }

            for folder_id in folder_ids {
                self.store
                    .destroy_jmap_mailbox(
                        principal.account_id,
                        folder_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-delete-folder".to_string(),
                            subject: folder_id.to_string(),
                        },
                    )
                    .await?;
            }

            Ok(delete_folder_success_response())
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("DeleteFolder", "ErrorFolderNotFound", &error.to_string())
        }))
    }
}

fn decode_ews_body(headers: &HeaderMap, body: &[u8]) -> Result<String> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .chars()
        .filter(|character| {
            !character.is_ascii_whitespace() && *character != '"' && *character != '\''
        })
        .collect::<String>();

    if body.starts_with(&[0xff, 0xfe]) {
        return decode_utf16_body(&body[2..], true);
    }
    if body.starts_with(&[0xfe, 0xff]) {
        return decode_utf16_body(&body[2..], false);
    }
    if content_type.contains("charset=utf-16be") {
        return decode_utf16_body(body, false);
    }
    if content_type.contains("charset=utf-16le") || content_type.contains("charset=utf-16") {
        return decode_utf16_body(body, true);
    }

    std::str::from_utf8(body)
        .map(str::to_string)
        .map_err(|_| anyhow!("EWS request body is not valid UTF-8 or UTF-16"))
}

fn decode_utf16_body(body: &[u8], little_endian: bool) -> Result<String> {
    let mut chunks = body.chunks_exact(2);
    let words = chunks
        .by_ref()
        .map(|chunk| {
            if little_endian {
                u16::from_le_bytes([chunk[0], chunk[1]])
            } else {
                u16::from_be_bytes([chunk[0], chunk[1]])
            }
        })
        .collect::<Vec<_>>();
    if !chunks.remainder().is_empty() {
        return Err(anyhow!("EWS UTF-16 request body has an odd byte length"));
    }
    String::from_utf16(&words).map_err(|_| anyhow!("EWS request body is not valid UTF-16"))
}

#[derive(Debug, Clone)]
struct ParsedMailbox {
    address: String,
    display_name: Option<String>,
}

fn parse_create_message_input(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<SubmitMessageInput> {
    let message = element_content(request, "Message")
        .ok_or_else(|| anyhow!("CreateItem currently supports only Message items"))?;
    let body_tag = open_tag_text(message, "Body").unwrap_or_default();
    let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
    let body_value = element_text(message, "Body").unwrap_or_default();
    let body_text = if body_type.eq_ignore_ascii_case("HTML") {
        html_to_text(&body_value)
    } else {
        body_value
    };
    let from = element_content(message, "From").and_then(parse_first_mailbox);
    let sender = element_content(message, "Sender").and_then(parse_first_mailbox);
    let from_display = from
        .as_ref()
        .and_then(|mailbox| mailbox.display_name.clone())
        .or_else(|| Some(principal.display_name.clone()));
    let from_address = from
        .map(|mailbox| mailbox.address)
        .unwrap_or_else(|| principal.email.clone());

    Ok(SubmitMessageInput {
        draft_message_id: None,
        account_id: principal.account_id,
        submitted_by_account_id: principal.account_id,
        source: "ews-createitem".to_string(),
        from_display,
        from_address,
        sender_display: sender
            .as_ref()
            .and_then(|mailbox| mailbox.display_name.clone()),
        sender_address: sender.map(|mailbox| mailbox.address),
        to: parse_recipients(message, "ToRecipients"),
        cc: parse_recipients(message, "CcRecipients"),
        bcc: parse_recipients(message, "BccRecipients"),
        subject: element_text(message, "Subject").unwrap_or_default(),
        body_text,
        body_html_sanitized: None,
        internet_message_id: element_text(message, "InternetMessageId"),
        mime_blob_ref: Some(format!("ews-createitem:{}", Uuid::new_v4())),
        size_octets: message.len() as i64,
        unread: Some(false),
        flagged: Some(false),
        attachments: Vec::new(),
    })
}

fn parse_create_contact_input(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<UpsertClientContactInput> {
    let contact = element_content(request, "Contact")
        .ok_or_else(|| anyhow!("CreateItem is missing Contact"))?;
    let email = contact_entry_value(contact, "EmailAddresses", "EmailAddress1")
        .or_else(|| element_text(contact, "EmailAddress"))
        .unwrap_or_else(|| principal.email.clone());
    let given_name = element_text(contact, "GivenName").unwrap_or_default();
    let surname = element_text(contact, "Surname").unwrap_or_default();
    let fallback_name = [given_name.as_str(), surname.as_str()]
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let name = element_text(contact, "DisplayName")
        .or_else(|| element_text(contact, "FileAs"))
        .or_else(|| (!fallback_name.trim().is_empty()).then_some(fallback_name))
        .unwrap_or_else(|| email.clone());
    let body_tag = open_tag_text(contact, "Body").unwrap_or_default();
    let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
    let body_value = element_text(contact, "Body").unwrap_or_default();
    let notes = if body_type.eq_ignore_ascii_case("HTML") {
        html_to_text(&body_value)
    } else {
        body_value
    };

    Ok(UpsertClientContactInput {
        id: None,
        account_id: principal.account_id,
        name,
        role: element_text(contact, "JobTitle").unwrap_or_default(),
        email,
        phone: contact_entry_value(contact, "PhoneNumbers", "MobilePhone")
            .or_else(|| contact_entry_value(contact, "PhoneNumbers", "BusinessPhone"))
            .or_else(|| contact_entry_value(contact, "PhoneNumbers", "HomePhone"))
            .unwrap_or_default(),
        team: element_text(contact, "CompanyName").unwrap_or_default(),
        notes,
    })
}

fn parse_update_contact_input(
    principal: &AccountPrincipal,
    existing: &AccessibleContact,
    request: &str,
) -> UpsertClientContactInput {
    let contact = element_content(request, "Contact").unwrap_or(request);
    let given_name = element_text(contact, "GivenName");
    let surname = element_text(contact, "Surname");
    let existing_given = first_name(&existing.name);
    let existing_surname = last_name(&existing.name);
    let name_from_parts = (given_name.is_some() || surname.is_some()).then(|| {
        [
            given_name.as_deref().unwrap_or(&existing_given),
            surname.as_deref().unwrap_or(&existing_surname),
        ]
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ")
    });
    let name = element_text(contact, "DisplayName")
        .or_else(|| element_text(contact, "FileAs"))
        .or(name_from_parts)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| existing.name.clone());
    let email = contact_entry_value(contact, "EmailAddresses", "EmailAddress1")
        .or_else(|| element_text(contact, "EmailAddress"))
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| existing.email.clone());
    let notes = if field_deleted(request, "item:Body") {
        String::new()
    } else if let Some(body_value) = element_text(contact, "Body") {
        let body_tag = open_tag_text(contact, "Body").unwrap_or_default();
        let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
        if body_type.eq_ignore_ascii_case("HTML") {
            html_to_text(&body_value)
        } else {
            body_value
        }
    } else {
        existing.notes.clone()
    };

    UpsertClientContactInput {
        id: Some(existing.id),
        account_id: principal.account_id,
        name,
        role: deleted_or_updated_text(
            request,
            contact,
            "contacts:JobTitle",
            "JobTitle",
            &existing.role,
        ),
        email,
        phone: deleted_or_updated_contact_entry(
            request,
            contact,
            &[
                "contacts:PhoneNumber:MobilePhone",
                "contacts:PhoneNumber:BusinessPhone",
                "contacts:PhoneNumber:HomePhone",
            ],
            "PhoneNumbers",
            &["MobilePhone", "BusinessPhone", "HomePhone"],
            &existing.phone,
        ),
        team: deleted_or_updated_text(
            request,
            contact,
            "contacts:CompanyName",
            "CompanyName",
            &existing.team,
        ),
        notes,
    }
}

fn parse_create_event_input(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<UpsertClientEventInput> {
    let event = element_content(request, "CalendarItem")
        .ok_or_else(|| anyhow!("CreateItem is missing CalendarItem"))?;
    let start = element_text(event, "Start").unwrap_or_default();
    let end = element_text(event, "End").unwrap_or_default();
    let (date, time) = ews_datetime_parts(&start)
        .ok_or_else(|| anyhow!("CalendarItem is missing a valid Start value"))?;
    let duration_minutes = ews_duration_minutes(&start, &end).unwrap_or(60);
    let body_tag = open_tag_text(event, "Body").unwrap_or_default();
    let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
    let body_value = element_text(event, "Body").unwrap_or_default();
    let notes = if body_type.eq_ignore_ascii_case("HTML") {
        html_to_text(&body_value)
    } else {
        body_value
    };
    let attendees = parse_event_attendees(event);

    Ok(UpsertClientEventInput {
        id: None,
        account_id: principal.account_id,
        date,
        time,
        time_zone: requested_time_zone(request).unwrap_or_else(|| "UTC".to_string()),
        duration_minutes,
        recurrence_rule: parse_ews_recurrence(event)?,
        title: element_text(event, "Subject").unwrap_or_else(|| "Untitled event".to_string()),
        location: element_text(event, "Location").unwrap_or_default(),
        attendees: attendees.join(", "),
        attendees_json: "[]".to_string(),
        notes,
    })
}

fn parse_update_event_input(
    principal: &AccountPrincipal,
    existing: &AccessibleEvent,
    request: &str,
) -> Result<UpsertClientEventInput> {
    let event = element_content(request, "CalendarItem").unwrap_or(request);
    let start = element_text(event, "Start");
    let end = element_text(event, "End");
    let (date, time) = start
        .as_deref()
        .and_then(ews_datetime_parts)
        .unwrap_or_else(|| (existing.date.clone(), existing.time.clone()));
    let duration_minutes = match (start.as_deref(), end.as_deref()) {
        (Some(start), Some(end)) => {
            ews_duration_minutes(start, end).unwrap_or(existing.duration_minutes)
        }
        (Some(start), None) => {
            ews_duration_minutes(start, &format!("{}T{}:00Z", existing.date, existing.time))
                .unwrap_or(existing.duration_minutes)
        }
        _ => existing.duration_minutes,
    };
    let notes = if field_deleted(request, "item:Body") {
        String::new()
    } else if let Some(body_value) = element_text(event, "Body") {
        let body_tag = open_tag_text(event, "Body").unwrap_or_default();
        let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
        if body_type.eq_ignore_ascii_case("HTML") {
            html_to_text(&body_value)
        } else {
            body_value
        }
    } else {
        existing.notes.clone()
    };
    let attendees = parse_event_attendees(event);

    Ok(UpsertClientEventInput {
        id: Some(existing.id),
        account_id: principal.account_id,
        date,
        time,
        time_zone: requested_time_zone(request).unwrap_or_else(|| existing.time_zone.clone()),
        duration_minutes,
        recurrence_rule: if field_deleted(request, "calendar:Recurrence") {
            String::new()
        } else if element_content(event, "Recurrence").is_some() {
            parse_ews_recurrence(event)?
        } else {
            existing.recurrence_rule.clone()
        },
        title: deleted_or_updated_text(
            request,
            event,
            "calendar:Subject",
            "Subject",
            &existing.title,
        )
        .if_empty(existing.title.clone()),
        location: deleted_or_updated_text(
            request,
            event,
            "calendar:Location",
            "Location",
            &existing.location,
        ),
        attendees: if attendees.is_empty() {
            existing.attendees.clone()
        } else {
            attendees.join(", ")
        },
        attendees_json: existing.attendees_json.clone(),
        notes,
    })
}

fn parse_create_task_input(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<UpsertClientTaskInput> {
    let task =
        element_content(request, "Task").ok_or_else(|| anyhow!("CreateItem is missing Task"))?;
    let body_tag = open_tag_text(task, "Body").unwrap_or_default();
    let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
    let body_value = element_text(task, "Body").unwrap_or_default();
    let description = if body_type.eq_ignore_ascii_case("HTML") {
        html_to_text(&body_value)
    } else {
        body_value
    };
    let status = element_text(task, "Status")
        .map(|value| ews_task_status_to_canonical(&value))
        .transpose()?
        .unwrap_or("needs-action")
        .to_string();

    Ok(UpsertClientTaskInput {
        id: None,
        principal_account_id: principal.account_id,
        account_id: principal.account_id,
        task_list_id: requested_task_list_id(request)?,
        title: element_text(task, "Subject").unwrap_or_else(|| "Untitled task".to_string()),
        description,
        status,
        due_at: element_text(task, "DueDate"),
        completed_at: element_text(task, "CompleteDate"),
        sort_order: 0,
    })
}

fn parse_update_task_input(
    principal: &AccountPrincipal,
    existing: &ClientTask,
    request: &str,
) -> Result<UpsertClientTaskInput> {
    let task = element_content(request, "Task").unwrap_or(request);
    let description = if field_deleted(request, "item:Body") {
        String::new()
    } else if let Some(body_value) = element_text(task, "Body") {
        let body_tag = open_tag_text(task, "Body").unwrap_or_default();
        let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
        if body_type.eq_ignore_ascii_case("HTML") {
            html_to_text(&body_value)
        } else {
            body_value
        }
    } else {
        existing.description.clone()
    };
    let status = element_text(task, "Status")
        .map(|value| ews_task_status_to_canonical(&value))
        .transpose()?
        .unwrap_or(existing.status.as_str())
        .to_string();

    Ok(UpsertClientTaskInput {
        id: Some(existing.id),
        principal_account_id: principal.account_id,
        account_id: principal.account_id,
        task_list_id: requested_task_list_id(request)?.or(Some(existing.task_list_id)),
        title: deleted_or_updated_text(request, task, "task:Subject", "Subject", &existing.title)
            .if_empty(existing.title.clone()),
        description,
        status,
        due_at: if field_deleted(request, "task:DueDate") {
            None
        } else {
            element_text(task, "DueDate").or_else(|| existing.due_at.clone())
        },
        completed_at: if field_deleted(request, "task:CompleteDate") {
            None
        } else {
            element_text(task, "CompleteDate").or_else(|| existing.completed_at.clone())
        },
        sort_order: existing.sort_order,
    })
}

fn requested_task_list_id(request: &str) -> Result<Option<Uuid>> {
    match requested_collection_id(request) {
        Some("default") | Some("tasks") | None => Ok(None),
        Some(id) => Uuid::parse_str(id)
            .map(Some)
            .map_err(|_| anyhow!("Task folder id is not a canonical task-list id")),
    }
}

fn ews_task_status_to_canonical(value: &str) -> Result<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "notstarted" | "needs-action" => Ok("needs-action"),
        "inprogress" | "waitingonothers" | "in-progress" => Ok("in-progress"),
        "completed" => Ok("completed"),
        "deferred" | "cancelled" | "canceled" => Ok("cancelled"),
        other => bail!("unsupported task Status {other}"),
    }
}

#[derive(Debug, Clone)]
struct OofProjection {
    is_enabled: bool,
    text_body: String,
}

impl OofProjection {
    fn disabled() -> Self {
        Self {
            is_enabled: false,
            text_body: String::new(),
        }
    }
}

fn oof_projection_from_script(content: Option<&str>) -> OofProjection {
    let Some(content) = content else {
        return OofProjection::disabled();
    };
    let Ok(script) = lpe_core::sieve::parse_script(content) else {
        return OofProjection::disabled();
    };
    let Some(text_body) = find_vacation_reason(&script.statements) else {
        return OofProjection::disabled();
    };

    OofProjection {
        is_enabled: true,
        text_body,
    }
}

fn find_vacation_reason(statements: &[Statement]) -> Option<String> {
    for statement in statements {
        match statement {
            Statement::Action(Action::Vacation { reason, .. }) => return Some(reason.clone()),
            Statement::If {
                branches,
                else_block,
            } => {
                for (_, branch) in branches {
                    if let Some(reason) = find_vacation_reason(branch) {
                        return Some(reason);
                    }
                }
                if let Some(else_block) = else_block {
                    if let Some(reason) = find_vacation_reason(else_block) {
                        return Some(reason);
                    }
                }
            }
            _ => {}
        }
    }
    None
}

trait EmptyStringFallback {
    fn if_empty(self, fallback: String) -> String;
}

impl EmptyStringFallback for String {
    fn if_empty(self, fallback: String) -> String {
        if self.trim().is_empty() {
            fallback
        } else {
            self
        }
    }
}

fn parse_event_attendees(event: &str) -> Vec<String> {
    ["RequiredAttendees", "OptionalAttendees"]
        .into_iter()
        .filter_map(|collection_name| element_content(event, collection_name))
        .flat_map(|collection| {
            element_contents(collection, "Attendee")
                .into_iter()
                .filter_map(parse_attendee)
        })
        .collect()
}

fn parse_attendee(attendee: &str) -> Option<String> {
    let mailbox = element_content(attendee, "Mailbox").and_then(parse_mailbox)?;
    Some(match mailbox.display_name {
        Some(display_name) if !display_name.trim().is_empty() => {
            format!("{display_name} <{}>", mailbox.address)
        }
        _ => mailbox.address,
    })
}

fn parse_ews_recurrence(event: &str) -> Result<String> {
    let Some(recurrence) = element_content(event, "Recurrence") else {
        return Ok(String::new());
    };

    let mut parts = Vec::new();
    if let Some(daily) = element_content(recurrence, "DailyRecurrence") {
        parts.push("FREQ=DAILY".to_string());
        push_interval_part(&mut parts, daily);
    } else if let Some(weekly) = element_content(recurrence, "WeeklyRecurrence") {
        parts.push("FREQ=WEEKLY".to_string());
        push_interval_part(&mut parts, weekly);
        if let Some(days) = element_text(weekly, "DaysOfWeek") {
            let byday = days
                .split_whitespace()
                .map(ews_weekday_to_rrule)
                .collect::<Result<Vec<_>>>()?;
            if !byday.is_empty() {
                parts.push(format!("BYDAY={}", byday.join(",")));
            }
        }
    } else if let Some(monthly) = element_content(recurrence, "AbsoluteMonthlyRecurrence") {
        parts.push("FREQ=MONTHLY".to_string());
        push_interval_part(&mut parts, monthly);
        if let Some(day) = element_text(monthly, "DayOfMonth") {
            parts.push(format!(
                "BYMONTHDAY={}",
                parse_positive_number(&day, "DayOfMonth")?
            ));
        }
    } else if let Some(yearly) = element_content(recurrence, "AbsoluteYearlyRecurrence") {
        parts.push("FREQ=YEARLY".to_string());
        if let Some(day) = element_text(yearly, "DayOfMonth") {
            parts.push(format!(
                "BYMONTHDAY={}",
                parse_positive_number(&day, "DayOfMonth")?
            ));
        }
        if let Some(month) = element_text(yearly, "Month") {
            parts.push(format!("BYMONTH={}", ews_month_to_number(&month)?));
        }
    } else {
        bail!("unsupported EWS recurrence pattern");
    }

    if let Some(numbered) = element_content(recurrence, "NumberedRecurrence") {
        if let Some(count) = element_text(numbered, "NumberOfOccurrences") {
            parts.push(format!(
                "COUNT={}",
                parse_positive_number(&count, "NumberOfOccurrences")?
            ));
        }
    } else if let Some(end_date) = element_content(recurrence, "EndDateRecurrence") {
        if let Some(end) = element_text(end_date, "EndDate") {
            parts.push(format!("UNTIL={}", rrule_date(&end)?));
        }
    }

    Ok(parts.join(";"))
}

fn push_interval_part(parts: &mut Vec<String>, recurrence: &str) {
    if let Some(interval) = element_text(recurrence, "Interval")
        .and_then(|value| parse_positive_number(&value, "Interval").ok())
        .filter(|value| *value > 1)
    {
        parts.push(format!("INTERVAL={interval}"));
    }
}

fn parse_positive_number(value: &str, field: &str) -> Result<u32> {
    let number = value
        .trim()
        .parse::<u32>()
        .map_err(|_| anyhow!("{field} must be a positive integer"))?;
    if number == 0 {
        bail!("{field} must be a positive integer");
    }
    Ok(number)
}

fn ews_weekday_to_rrule(value: &str) -> Result<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "monday" => Ok("MO"),
        "tuesday" => Ok("TU"),
        "wednesday" => Ok("WE"),
        "thursday" => Ok("TH"),
        "friday" => Ok("FR"),
        "saturday" => Ok("SA"),
        "sunday" => Ok("SU"),
        other => bail!("unsupported recurrence weekday {other}"),
    }
}

fn ews_month_to_number(value: &str) -> Result<u32> {
    match value.trim().to_ascii_lowercase().as_str() {
        "january" => Ok(1),
        "february" => Ok(2),
        "march" => Ok(3),
        "april" => Ok(4),
        "may" => Ok(5),
        "june" => Ok(6),
        "july" => Ok(7),
        "august" => Ok(8),
        "september" => Ok(9),
        "october" => Ok(10),
        "november" => Ok(11),
        "december" => Ok(12),
        other => bail!("unsupported recurrence month {other}"),
    }
}

fn rrule_date(value: &str) -> Result<String> {
    let date = value.trim().split('T').next().unwrap_or_default();
    let mut parts = date.split('-');
    let (Some(year), Some(month), Some(day), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        bail!("recurrence end date must be YYYY-MM-DD");
    };
    Ok(format!("{year}{month}{day}"))
}

fn parse_update_message_flags(request: &str) -> Result<Option<(Option<bool>, Option<bool>)>> {
    let unread = element_text(request, "IsRead")
        .map(|value| parse_xml_bool(&value).map(|is_read| !is_read))
        .transpose()?;
    let mut flagged = element_text(request, "FlagStatus")
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "notflagged" => Ok(false),
            "flagged" | "complete" => Ok(true),
            other => bail!("unsupported message FlagStatus {other}"),
        })
        .transpose()?;
    if field_deleted(request, "message:Flag") || field_deleted(request, "message:FlagStatus") {
        flagged = Some(false);
    }

    Ok((unread.is_some() || flagged.is_some()).then_some((unread, flagged)))
}

fn parse_xml_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => bail!("unsupported boolean value {other}"),
    }
}

fn requested_time_zone(request: &str) -> Option<String> {
    let time_zone = open_tag_text(request, "TimeZoneDefinition")?;
    attribute_value(time_zone, "Id").map(str::to_string)
}

fn requested_availability_window(request: &str) -> (Option<String>, Option<String>) {
    let time_window = element_content(request, "TimeWindow").unwrap_or(request);
    (
        element_text(time_window, "StartTime"),
        element_text(time_window, "EndTime"),
    )
}

fn event_overlaps_window(event: &AccessibleEvent, start: Option<&str>, end: Option<&str>) -> bool {
    let event_start = ews_datetime(&event.date, &event.time);
    let event_end = event_end_datetime(event);
    start.is_none_or(|start| event_end.as_str() > start)
        && end.is_none_or(|end| event_start.as_str() < end)
}

fn ews_datetime_parts(value: &str) -> Option<(String, String)> {
    let trimmed = value.trim();
    if trimmed.len() < 16 {
        return None;
    }
    let date = trimmed.get(0..10)?;
    let time = trimmed.get(11..16)?;
    Some((date.to_string(), time.to_string()))
}

fn ews_duration_minutes(start: &str, end: &str) -> Option<i32> {
    let (_, start_time) = ews_datetime_parts(start)?;
    let (_, end_time) = ews_datetime_parts(end)?;
    let start_minutes = time_minutes(&start_time)?;
    let end_minutes = time_minutes(&end_time)?;
    (end_minutes > start_minutes).then_some(end_minutes - start_minutes)
}

fn time_minutes(value: &str) -> Option<i32> {
    let (hour, minute) = value.split_once(':')?;
    Some(hour.parse::<i32>().ok()? * 60 + minute.parse::<i32>().ok()?)
}

fn contact_entry_value(contact: &str, collection_name: &str, key: &str) -> Option<String> {
    let collection = element_content(contact, collection_name)?;
    let mut rest = collection;
    while let Some(tag_start) = rest.find('<') {
        let raw_tag_text = &rest[tag_start + 1..];
        let tag_text = raw_tag_text.trim_start();
        let open_tag_start = tag_start + 1 + (raw_tag_text.len() - tag_text.len());
        if tag_text.starts_with('/') || tag_text.starts_with('?') || tag_text.starts_with('!') {
            rest = &tag_text[1..];
            continue;
        }
        let tag_end = tag_text.find('>')?;
        let open_tag = &tag_text[..tag_end];
        let qualified_name = open_tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()?;
        let content_start = open_tag_start + tag_end + 1;
        if qualified_name.rsplit(':').next() == Some("Entry")
            && attribute_value(open_tag, "Key") == Some(key)
        {
            let close_pattern = format!("</{qualified_name}>");
            let content = &rest[content_start..];
            let content_end = content.find(&close_pattern)?;
            return Some(xml_text(&content[..content_end]));
        }
        rest = &rest[content_start..];
    }
    element_text(collection, "Entry")
}

fn deleted_or_updated_text(
    request: &str,
    container: &str,
    field_uri: &str,
    local_name: &str,
    existing: &str,
) -> String {
    if field_deleted(request, field_uri) {
        String::new()
    } else {
        element_text(container, local_name).unwrap_or_else(|| existing.to_string())
    }
}

fn deleted_or_updated_contact_entry(
    request: &str,
    contact: &str,
    field_uris: &[&str],
    collection_name: &str,
    keys: &[&str],
    existing: &str,
) -> String {
    if field_uris
        .iter()
        .any(|field_uri| field_deleted(request, field_uri))
    {
        return String::new();
    }
    keys.iter()
        .find_map(|key| contact_entry_value(contact, collection_name, key))
        .unwrap_or_else(|| existing.to_string())
}

fn field_deleted(request: &str, field_uri: &str) -> bool {
    element_contents(request, "DeleteItemField")
        .into_iter()
        .any(|delete| field_block_matches(delete, field_uri))
}

fn field_block_matches(block: &str, field_uri: &str) -> bool {
    if attribute_values_for_tag(block, "FieldURI", "FieldURI")
        .into_iter()
        .any(|value| value == field_uri)
    {
        return true;
    }

    let Some((base_field_uri, field_index)) = field_uri.rsplit_once(':') else {
        return false;
    };
    let indexed_fields = attribute_values_for_tag(block, "IndexedFieldURI", "FieldURI");
    let field_indexes = attribute_values_for_tag(block, "IndexedFieldURI", "FieldIndex");
    indexed_fields.iter().any(|value| *value == base_field_uri)
        && field_indexes.iter().any(|value| *value == field_index)
}

fn imported_email_input(input: SubmitMessageInput, mailbox_id: Uuid) -> JmapImportedEmailInput {
    JmapImportedEmailInput {
        account_id: input.account_id,
        submitted_by_account_id: input.submitted_by_account_id,
        mailbox_id,
        source: input.source,
        from_display: input.from_display,
        from_address: input.from_address,
        sender_display: input.sender_display,
        sender_address: input.sender_address,
        to: input.to,
        cc: input.cc,
        bcc: input.bcc,
        subject: input.subject,
        body_text: input.body_text,
        body_html_sanitized: input.body_html_sanitized,
        internet_message_id: input.internet_message_id,
        mime_blob_ref: input
            .mime_blob_ref
            .unwrap_or_else(|| format!("ews-createitem:{}", Uuid::new_v4())),
        size_octets: input.size_octets,
        received_at: None,
        attachments: input.attachments,
    }
}

fn parse_recipients(message: &str, collection_name: &str) -> Vec<SubmittedRecipientInput> {
    element_content(message, collection_name)
        .map(|collection| {
            element_contents(collection, "Mailbox")
                .into_iter()
                .filter_map(parse_mailbox)
                .map(|mailbox| SubmittedRecipientInput {
                    address: mailbox.address,
                    display_name: mailbox.display_name,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_file_attachment_upload(value: &str) -> Result<AttachmentUploadInput> {
    let file_name = element_text(value, "Name")
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| anyhow!("FileAttachment Name is required"))?;
    let media_type = element_text(value, "ContentType")
        .map(|content_type| content_type.trim().to_string())
        .filter(|content_type| !content_type.is_empty())
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let content = element_text(value, "Content")
        .map(|content| content.trim().to_string())
        .filter(|content| !content.is_empty())
        .ok_or_else(|| anyhow!("FileAttachment Content is required"))?;
    let blob_bytes = BASE64_STANDARD
        .decode(content.as_bytes())
        .map_err(|_| anyhow!("FileAttachment Content must be valid base64"))?;

    Ok(AttachmentUploadInput {
        file_name,
        media_type,
        blob_bytes,
    })
}

fn expected_attachment_kind(media_type: &str, file_name: &str) -> ExpectedKind {
    let media_type = media_type.trim().to_ascii_lowercase();
    let file_name = file_name.trim().to_ascii_lowercase();
    if matches!(
        media_type.as_str(),
        "application/pdf"
            | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            | "application/vnd.oasis.opendocument.text"
    ) || file_name.ends_with(".pdf")
        || file_name.ends_with(".docx")
        || file_name.ends_with(".odt")
    {
        ExpectedKind::SupportedAttachmentText
    } else {
        ExpectedKind::Any
    }
}

fn parse_first_mailbox(value: &str) -> Option<ParsedMailbox> {
    element_contents(value, "Mailbox")
        .into_iter()
        .find_map(parse_mailbox)
}

fn parse_mailbox(value: &str) -> Option<ParsedMailbox> {
    let address = element_text(value, "EmailAddress")?;
    if address.trim().is_empty() {
        return None;
    }
    Some(ParsedMailbox {
        address: address.trim().to_string(),
        display_name: element_text(value, "Name").filter(|name| !name.trim().is_empty()),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FolderKind {
    Root,
    Contacts,
    Calendar,
    Tasks,
    Mailbox,
}

fn operation_name(body: &str) -> Option<String> {
    let body_start = body.find(":Body").or_else(|| body.find("<Body"))?;
    let body_content_start = body[body_start..].find('>')? + body_start + 1;
    let mut remaining = &body[body_content_start..];

    loop {
        let tag_start = remaining.find('<')?;
        let tag_text = remaining[tag_start + 1..].trim_start();
        if tag_text.starts_with('/') {
            return None;
        }
        if tag_text.starts_with('?') || tag_text.starts_with('!') {
            remaining = &tag_text[1..];
            continue;
        }

        let tag_end = tag_text
            .find(|value: char| value.is_whitespace() || value == '/' || value == '>')
            .unwrap_or(tag_text.len());
        let qualified_name = &tag_text[..tag_end];
        let local_name = qualified_name.rsplit(':').next()?;
        if local_name
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || value == '_')
        {
            return Some(match local_name {
                "GetUserAvailabilityRequest" => "GetUserAvailability".to_string(),
                value if value.ends_with("Request") => {
                    value.trim_end_matches("Request").to_string()
                }
                _ => local_name.to_string(),
            });
        }
        return None;
    }
}

fn requested_folder_kind(request: &str) -> Option<FolderKind> {
    if let Some(kind) =
        requested_sync_state(request).and_then(|state| sync_state_folder_kind(&state))
    {
        return Some(kind);
    }
    if request.contains("DistinguishedFolderId Id=\"msgfolderroot\"")
        || request.contains("DistinguishedFolderId Id='msgfolderroot'")
        || request.contains("DistinguishedFolderId Id=\"root\"")
        || request.contains("DistinguishedFolderId Id='root'")
        || request.contains("FolderId Id=\"msgfolderroot\"")
        || request.contains("FolderId Id='msgfolderroot'")
        || request.contains("FolderId Id=\"root\"")
        || request.contains("FolderId Id='root'")
    {
        return Some(FolderKind::Root);
    }
    if request.contains("DistinguishedFolderId Id=\"calendar\"")
        || request.contains("DistinguishedFolderId Id='calendar'")
        || request.contains("FolderId Id=\"calendar\"")
        || request.contains("FolderId Id='calendar'")
    {
        return Some(FolderKind::Calendar);
    }
    if request.contains("DistinguishedFolderId Id=\"contacts\"")
        || request.contains("DistinguishedFolderId Id='contacts'")
        || request.contains("FolderId Id=\"contacts\"")
        || request.contains("FolderId Id='contacts'")
    {
        return Some(FolderKind::Contacts);
    }
    if request.contains("DistinguishedFolderId Id=\"tasks\"")
        || request.contains("DistinguishedFolderId Id='tasks'")
        || request.contains("FolderId Id=\"tasks\"")
        || request.contains("FolderId Id='tasks'")
    {
        return Some(FolderKind::Tasks);
    }
    if request.contains("mailbox:") || !requested_mailbox_folder_ids(request).is_empty() {
        return Some(FolderKind::Mailbox);
    }
    if requested_mailbox_role(request).is_some() {
        return Some(FolderKind::Mailbox);
    }
    requested_collection_id(request).and_then(|id| {
        if id.starts_with("shared-calendar-") {
            Some(FolderKind::Calendar)
        } else if id.starts_with("shared-contacts-") {
            Some(FolderKind::Contacts)
        } else if id.starts_with("shared-tasks-") {
            Some(FolderKind::Tasks)
        } else if id.starts_with("mailbox:") || Uuid::parse_str(id).is_ok() {
            Some(FolderKind::Mailbox)
        } else if id == "msgfolderroot" || id == "root" {
            Some(FolderKind::Root)
        } else {
            None
        }
    })
}

fn sync_state_folder_kind(sync_state: &str) -> Option<FolderKind> {
    if sync_state.starts_with("contacts:") {
        Some(FolderKind::Contacts)
    } else if sync_state.starts_with("calendar:") {
        Some(FolderKind::Calendar)
    } else if sync_state.starts_with("tasks:") {
        Some(FolderKind::Tasks)
    } else if sync_state.starts_with("mailbox:") {
        Some(FolderKind::Mailbox)
    } else if sync_state.starts_with("root:") {
        Some(FolderKind::Root)
    } else {
        None
    }
}

fn requested_folder_kinds(request: &str) -> Vec<FolderKind> {
    let mut kinds = Vec::new();
    if request.contains("DistinguishedFolderId Id=\"msgfolderroot\"")
        || request.contains("DistinguishedFolderId Id='msgfolderroot'")
        || request.contains("DistinguishedFolderId Id=\"root\"")
        || request.contains("DistinguishedFolderId Id='root'")
        || request.contains("FolderId Id=\"msgfolderroot\"")
        || request.contains("FolderId Id='msgfolderroot'")
        || request.contains("FolderId Id=\"root\"")
        || request.contains("FolderId Id='root'")
    {
        kinds.push(FolderKind::Root);
    }
    if request.contains("DistinguishedFolderId Id=\"contacts\"")
        || request.contains("DistinguishedFolderId Id='contacts'")
        || request.contains("FolderId Id=\"contacts\"")
        || request.contains("FolderId Id='contacts'")
        || request.contains("shared-contacts-")
    {
        kinds.push(FolderKind::Contacts);
    }
    if request.contains("DistinguishedFolderId Id=\"calendar\"")
        || request.contains("DistinguishedFolderId Id='calendar'")
        || request.contains("FolderId Id=\"calendar\"")
        || request.contains("FolderId Id='calendar'")
        || request.contains("shared-calendar-")
    {
        kinds.push(FolderKind::Calendar);
    }
    if request.contains("DistinguishedFolderId Id=\"tasks\"")
        || request.contains("DistinguishedFolderId Id='tasks'")
        || request.contains("FolderId Id=\"tasks\"")
        || request.contains("FolderId Id='tasks'")
        || request.contains("shared-tasks-")
    {
        kinds.push(FolderKind::Tasks);
    }
    if request.contains("mailbox:") || !requested_mailbox_folder_ids(request).is_empty() {
        kinds.push(FolderKind::Mailbox);
    }
    if requested_mailbox_role(request).is_some() {
        kinds.push(FolderKind::Mailbox);
    }
    kinds.dedup();
    kinds
}

fn request_contains_folder_reference(request: &str) -> bool {
    request.contains("FolderId") || request.contains("DistinguishedFolderId")
}

fn requested_collection_id(request: &str) -> Option<&str> {
    attribute_values_for_tag(request, "FolderId", "Id")
        .into_iter()
        .next()
        .or_else(|| {
            attribute_values_for_tag(request, "DistinguishedFolderId", "Id")
                .into_iter()
                .next()
        })
        .map(|value| match value {
            "contacts" | "calendar" | "tasks" => DEFAULT_COLLECTION_ID,
            other => other,
        })
}

fn requested_mailbox_role(request: &str) -> Option<&'static str> {
    requested_distinguished_folder_id(request).and_then(ews_distinguished_mailbox_role)
}

fn requested_distinguished_folder_id(request: &str) -> Option<&str> {
    attribute_values_for_tag(request, "DistinguishedFolderId", "Id")
        .into_iter()
        .next()
        .or_else(|| {
            attribute_values_for_tag(request, "FolderId", "Id")
                .into_iter()
                .next()
        })
}

fn ews_distinguished_mailbox_role(value: &str) -> Option<&'static str> {
    match value.to_ascii_lowercase().as_str() {
        "inbox" => Some("inbox"),
        "drafts" => Some("drafts"),
        "sentitems" | "sent" => Some("sent"),
        "deleteditems" | "trash" => Some("trash"),
        _ => None,
    }
}

fn requested_sync_state(request: &str) -> Option<String> {
    element_text(request, "SyncState").filter(|value| !value.trim().is_empty())
}

fn mailbox_sync_state(mailbox_id: Uuid, message_ids: &[Uuid]) -> String {
    format!(
        "mailbox:{mailbox_id}:{}",
        message_ids
            .iter()
            .map(Uuid::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
}

const COLLABORATION_SYNC_STATE_VERSION: &str = "v2";

fn collaboration_sync_state(kind: &str, collection_id: &str, items: &[(Uuid, String)]) -> String {
    let item_list = items
        .iter()
        .map(|(id, change_key)| format!("{id}={change_key}"))
        .collect::<Vec<_>>()
        .join(",");
    if item_list.is_empty() {
        format!("{kind}:{collection_id}:{COLLABORATION_SYNC_STATE_VERSION}:0")
    } else {
        format!("{kind}:{collection_id}:{COLLABORATION_SYNC_STATE_VERSION}:{item_list}")
    }
}

#[derive(Debug, Clone)]
struct SyncStateItem {
    id: Uuid,
    change_key: Option<String>,
}

#[derive(Debug, Clone)]
struct CollaborationSyncState {
    is_current_version: bool,
    items: Vec<SyncStateItem>,
}

impl Default for CollaborationSyncState {
    fn default() -> Self {
        Self {
            is_current_version: true,
            items: Vec::new(),
        }
    }
}

fn collaboration_sync_state_items(
    sync_state: &str,
    kind: &str,
    collection_id: &str,
) -> CollaborationSyncState {
    let prefix = format!("{kind}:{collection_id}:");
    let Some(values) = sync_state.strip_prefix(&prefix) else {
        return CollaborationSyncState::default();
    };
    let (is_current_version, values) = if let Some(values) =
        values.strip_prefix(&format!("{COLLABORATION_SYNC_STATE_VERSION}:"))
    {
        (true, values)
    } else {
        (false, values)
    };
    let items = values
        .split(',')
        .filter(|value| !value.is_empty() && *value != "0")
        .filter_map(|value| {
            if let Some((id, change_key)) = value.split_once('=') {
                return Uuid::parse_str(id).ok().map(|id| SyncStateItem {
                    id,
                    change_key: Some(change_key.to_string()),
                });
            }
            Uuid::parse_str(value).ok().map(|id| SyncStateItem {
                id,
                change_key: None,
            })
        })
        .collect();
    CollaborationSyncState {
        is_current_version,
        items,
    }
}

fn sync_state_items_by_id(items: &[SyncStateItem]) -> HashMap<Uuid, Option<String>> {
    items
        .iter()
        .map(|item| (item.id, item.change_key.clone()))
        .collect()
}

fn sync_version_by_id(items: Vec<(Uuid, String)>) -> HashMap<Uuid, String> {
    items.into_iter().collect()
}

fn mailbox_sync_state_ids(sync_state: &str, mailbox_id: Uuid) -> Vec<Uuid> {
    let prefix = format!("mailbox:{mailbox_id}:");
    sync_state
        .strip_prefix(&prefix)
        .unwrap_or_default()
        .split(',')
        .filter(|value| !value.is_empty() && *value != "0")
        .filter_map(|value| Uuid::parse_str(value).ok())
        .collect()
}

fn mailbox_sync_state_folder_id(sync_state: &str) -> Option<Uuid> {
    let rest = sync_state.strip_prefix("mailbox:")?;
    let folder_id = rest.split_once(':')?.0;
    Uuid::parse_str(folder_id).ok()
}

fn requested_item_ids(request: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut rest = request;
    while let Some(index) = rest.find("<t:ItemId").or_else(|| rest.find("<ItemId")) {
        rest = &rest[index..];
        if let Some(id) = attribute_value_after(rest, "ItemId", "Id") {
            ids.push(id.to_string());
        }
        rest = &rest[1..];
    }
    ids
}

fn requested_attachment_ids(request: &str) -> Vec<String> {
    attribute_values_for_tag(request, "AttachmentId", "Id")
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn requested_mime_content(request: &str) -> bool {
    request.contains("item:MimeContent") || request.contains("MimeContent")
}

fn requested_mailbox_folder_ids(request: &str) -> Vec<Uuid> {
    requested_folder_ids(request)
        .into_iter()
        .filter_map(|id| {
            id.strip_prefix("mailbox:")
                .or(Some(id.as_str()))
                .and_then(|value| Uuid::parse_str(value).ok())
        })
        .collect()
}

fn requested_folder_ids(request: &str) -> Vec<String> {
    attribute_values_for_tag(request, "FolderId", "Id")
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn attribute_values_for_tag<'a>(xml: &'a str, local_name: &str, attr: &str) -> Vec<&'a str> {
    let mut values = Vec::new();
    let mut rest = xml;
    while let Some(tag_start) = rest.find('<') {
        let tag_text = rest[tag_start + 1..].trim_start();
        if tag_text.starts_with('/') || tag_text.starts_with('?') || tag_text.starts_with('!') {
            rest = &tag_text[1..];
            continue;
        }
        let Some(tag_end) = tag_text.find('>') else {
            break;
        };
        let open_tag = &tag_text[..tag_end];
        let Some(qualified_name) = open_tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()
        else {
            rest = &tag_text[tag_end + 1..];
            continue;
        };
        if qualified_name.rsplit(':').next() == Some(local_name) {
            if let Some(value) = attribute_value(open_tag, attr) {
                values.push(value);
            }
        }
        rest = &tag_text[tag_end + 1..];
    }
    values
}

fn attribute_value_after<'a>(body: &'a str, tag: &str, attr: &str) -> Option<&'a str> {
    let index = body.find(tag)?;
    let rest = &body[index..];
    let end = rest.find('>')?;
    let tag_text = &rest[..end];
    attribute_value(tag_text, attr)
}

fn attribute_value<'a>(tag_text: &'a str, attr: &str) -> Option<&'a str> {
    let pattern = format!("{attr}=");
    let start = tag_text.find(&pattern)? + pattern.len();
    let quote = tag_text[start..].chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let value_start = start + quote.len_utf8();
    let value_end = tag_text[value_start..].find(quote)? + value_start;
    Some(&tag_text[value_start..value_end])
}

fn open_tag_text<'a>(xml: &'a str, local_name: &str) -> Option<&'a str> {
    let mut rest = xml;
    while let Some(tag_start) = rest.find('<') {
        let tag_text = rest[tag_start + 1..].trim_start();
        if tag_text.starts_with('/') || tag_text.starts_with('?') || tag_text.starts_with('!') {
            rest = &tag_text[1..];
            continue;
        }
        let tag_end = tag_text.find('>')?;
        let open_tag = &tag_text[..tag_end];
        let qualified_name = open_tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()?;
        if qualified_name.rsplit(':').next()? == local_name {
            return Some(open_tag);
        }
        rest = &tag_text[tag_end + 1..];
    }
    None
}

fn element_text(xml: &str, local_name: &str) -> Option<String> {
    element_content(xml, local_name).map(xml_text)
}

fn element_content<'a>(xml: &'a str, local_name: &str) -> Option<&'a str> {
    element_contents(xml, local_name).into_iter().next()
}

fn element_contents<'a>(xml: &'a str, local_name: &str) -> Vec<&'a str> {
    let mut values = Vec::new();
    let mut rest = xml;
    while let Some(tag_start) = rest.find('<') {
        let tag_text = rest[tag_start + 1..].trim_start();
        if tag_text.starts_with('/') || tag_text.starts_with('?') || tag_text.starts_with('!') {
            rest = &tag_text[1..];
            continue;
        }
        let Some(tag_end) = tag_text.find('>') else {
            break;
        };
        let open_tag = &tag_text[..tag_end];
        let Some(qualified_name) = open_tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()
        else {
            break;
        };
        if qualified_name.rsplit(':').next() != Some(local_name) {
            rest = &tag_text[tag_end + 1..];
            continue;
        }
        if open_tag.trim_end().ends_with('/') {
            values.push("");
            rest = &tag_text[tag_end + 1..];
            continue;
        }

        let content_start = tag_start + 1 + tag_text[..tag_end + 1].len();
        let closing_tag = format!("</{qualified_name}>");
        let Some(relative_end) = rest[content_start..].find(&closing_tag) else {
            break;
        };
        let content_end = content_start + relative_end;
        values.push(&rest[content_start..content_end]);
        rest = &rest[content_end + closing_tag.len()..];
    }
    values
}

fn xml_text(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .trim()
        .to_string()
}

fn html_to_text(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut in_tag = false;
    for ch in value.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                output.push(' ');
            }
            _ if !in_tag => output.push(ch),
            _ => {}
        }
    }
    output.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn find_item_response(items: String) -> String {
    format!(
        concat!(
            "<m:FindItemResponse>",
            "<m:ResponseMessages>",
            "<m:FindItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:RootFolder TotalItemsInView=\"{count}\" IncludesLastItemInRange=\"true\">",
            "<t:Items>{items}</t:Items>",
            "</m:RootFolder>",
            "</m:FindItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:FindItemResponse>"
        ),
        items = items,
        count = count_tag_occurrences(&items, "<t:ItemId")
    )
}

fn sync_folder_items_response(sync_state: &str, changes: String) -> String {
    format!(
        concat!(
            "<m:SyncFolderItemsResponse>",
            "<m:ResponseMessages>",
            "<m:SyncFolderItemsResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:SyncState>{sync_state}</m:SyncState>",
            "<m:IncludesLastItemInRange>true</m:IncludesLastItemInRange>",
            "<m:Changes>{changes}</m:Changes>",
            "</m:SyncFolderItemsResponseMessage>",
            "</m:ResponseMessages>",
            "</m:SyncFolderItemsResponse>"
        ),
        sync_state = escape_xml(sync_state),
        changes = changes,
    )
}

fn message_summary_xml(email: &JmapEmail) -> String {
    format!(
        concat!(
            "<t:Message>",
            "<t:ItemId Id=\"message:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"mailbox:{mailbox_id}\"/>",
            "<t:Subject>{subject}</t:Subject>",
            "<t:DateTimeReceived>{received_at}</t:DateTimeReceived>",
            "<t:Size>{size}</t:Size>",
            "<t:HasAttachments>{has_attachments}</t:HasAttachments>",
            "<t:IsRead>{is_read}</t:IsRead>",
            "</t:Message>"
        ),
        id = email.id,
        change_key = escape_xml(&email.delivery_status),
        mailbox_id = email.mailbox_id,
        subject = escape_xml(&email.subject),
        received_at = escape_xml(&email.received_at),
        size = email.size_octets.max(0),
        has_attachments = email.has_attachments,
        is_read = !email.unread,
    )
}

fn message_item_xml(email: &JmapEmail) -> String {
    message_item_xml_with_attachments(email, &[])
}

fn message_item_xml_with_attachments(
    email: &JmapEmail,
    attachments: &[ActiveSyncAttachment],
) -> String {
    message_item_xml_with_details(email, attachments, None)
}

fn message_item_xml_with_details(
    email: &JmapEmail,
    attachments: &[ActiveSyncAttachment],
    mime_attachment_contents: Option<&[ActiveSyncAttachmentContent]>,
) -> String {
    let mut xml = message_summary_xml(email);
    let mime_content = mime_attachment_contents
        .map(|contents| {
            format!(
                "<t:MimeContent CharacterSet=\"UTF-8\">{}</t:MimeContent>",
                BASE64_STANDARD.encode(render_mime_message(email, contents))
            )
        })
        .unwrap_or_default();
    xml.insert_str(
        xml.len() - "</t:Message>".len(),
        &format!(
            "{}<t:Body BodyType=\"Text\">{}</t:Body>{}",
            mime_content,
            escape_xml(&email.body_text),
            message_attachments_xml(attachments),
        ),
    );
    xml
}

fn render_mime_message(email: &JmapEmail, attachments: &[ActiveSyncAttachmentContent]) -> Vec<u8> {
    let mut message = render_mime_header(email, attachments.is_empty());
    if attachments.is_empty() {
        message.push_str(&render_standalone_body_mime(email));
    } else {
        let boundary = mixed_boundary(email);
        message.push_str(&format!("--{boundary}\r\n"));
        message.push_str(&render_body_mime_part(email));
        if !message.ends_with("\r\n") {
            message.push_str("\r\n");
        }
        for attachment in attachments {
            message.push_str(&format!("--{boundary}\r\n"));
            message.push_str(&render_attachment_mime_part(attachment));
        }
        message.push_str(&format!("--{boundary}--\r\n"));
    }
    message.into_bytes()
}

fn render_standalone_body_mime(email: &JmapEmail) -> String {
    if let Some(html) = email.body_html_sanitized.as_deref() {
        let boundary = alternative_boundary(email);
        return format!(
            concat!(
                "--{boundary}\r\n",
                "Content-Type: text/plain; charset=UTF-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "{text}\r\n",
                "--{boundary}\r\n",
                "Content-Type: text/html; charset=UTF-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "{html}\r\n",
                "--{boundary}--\r\n"
            ),
            boundary = boundary,
            text = email.body_text,
            html = html,
        );
    }

    email.body_text.clone()
}

fn render_mime_header(email: &JmapEmail, without_attachments: bool) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "Date: {}",
        sanitize_header_value(&email.received_at)
    ));
    lines.push(format!(
        "From: {}",
        render_mime_address(email.from_display.as_deref(), email.from_address.as_str())
    ));
    if !email.to.is_empty() {
        lines.push(format!("To: {}", render_mime_recipients(&email.to)));
    }
    if !email.cc.is_empty() {
        lines.push(format!("Cc: {}", render_mime_recipients(&email.cc)));
    }
    if !email.bcc.is_empty() && matches!(email.mailbox_role.as_str(), "drafts" | "sent") {
        lines.push(format!("Bcc: {}", render_mime_recipients(&email.bcc)));
    }
    lines.push(format!(
        "Subject: {}",
        sanitize_header_value(&email.subject)
    ));
    if let Some(message_id) = email.internet_message_id.as_deref() {
        lines.push(format!("Message-Id: {}", sanitize_header_value(message_id)));
    }
    lines.push("MIME-Version: 1.0".to_string());
    let content_type = if without_attachments {
        body_content_type(email)
    } else {
        format!("multipart/mixed; boundary=\"{}\"", mixed_boundary(email))
    };
    lines.push(format!("Content-Type: {content_type}"));
    lines.join("\r\n") + "\r\n\r\n"
}

fn render_body_mime_part(email: &JmapEmail) -> String {
    if let Some(html) = email.body_html_sanitized.as_deref() {
        let boundary = alternative_boundary(email);
        return format!(
            concat!(
                "Content-Type: multipart/alternative; boundary=\"{boundary}\"\r\n",
                "\r\n",
                "--{boundary}\r\n",
                "Content-Type: text/plain; charset=UTF-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "{text}\r\n",
                "--{boundary}\r\n",
                "Content-Type: text/html; charset=UTF-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "{html}\r\n",
                "--{boundary}--\r\n"
            ),
            boundary = boundary,
            text = email.body_text,
            html = html,
        );
    }

    format!(
        concat!(
            "Content-Type: text/plain; charset=UTF-8\r\n",
            "Content-Transfer-Encoding: 7bit\r\n",
            "\r\n",
            "{}\r\n"
        ),
        email.body_text,
    )
}

fn render_attachment_mime_part(attachment: &ActiveSyncAttachmentContent) -> String {
    let file_name = quote_mime_parameter(&attachment.file_name);
    format!(
        concat!(
            "Content-Type: {content_type}; name=\"{file_name}\"\r\n",
            "Content-Transfer-Encoding: base64\r\n",
            "Content-Disposition: attachment; filename=\"{file_name}\"\r\n",
            "\r\n",
            "{body}\r\n"
        ),
        content_type = sanitize_header_value(&attachment.media_type),
        file_name = file_name,
        body = base64_mime_lines(&attachment.blob_bytes),
    )
}

fn body_content_type(email: &JmapEmail) -> String {
    if email.body_html_sanitized.is_some() {
        format!(
            "multipart/alternative; boundary=\"{}\"",
            alternative_boundary(email)
        )
    } else {
        "text/plain; charset=UTF-8".to_string()
    }
}

fn mixed_boundary(email: &JmapEmail) -> String {
    format!("lpe-ews-mixed-{}", email.id.simple())
}

fn alternative_boundary(email: &JmapEmail) -> String {
    format!("lpe-ews-alt-{}", email.id.simple())
}

fn render_mime_recipients(recipients: &[JmapEmailAddress]) -> String {
    recipients
        .iter()
        .map(|recipient| render_mime_address(recipient.display_name.as_deref(), &recipient.address))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_mime_address(display_name: Option<&str>, address: &str) -> String {
    let address = sanitize_header_value(address);
    match display_name
        .map(sanitize_header_value)
        .filter(|value| !value.trim().is_empty() && value != &address)
    {
        Some(display_name) => format!("{} <{}>", quote_display_name(&display_name), address),
        None => address,
    }
}

fn quote_display_name(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, ' ' | '.' | '_' | '-'))
    {
        value.to_string()
    } else {
        format!("\"{}\"", quote_mime_parameter(value))
    }
}

fn quote_mime_parameter(value: &str) -> String {
    sanitize_header_value(value)
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

fn sanitize_header_value(value: &str) -> String {
    value
        .replace(['\r', '\n'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn base64_mime_lines(bytes: &[u8]) -> String {
    bytes
        .chunks(57)
        .map(|chunk| BASE64_STANDARD.encode(chunk))
        .collect::<Vec<_>>()
        .join("\r\n")
}

fn message_attachments_xml(attachments: &[ActiveSyncAttachment]) -> String {
    if attachments.is_empty() {
        return String::new();
    }

    format!(
        "<t:Attachments>{}</t:Attachments>",
        attachments
            .iter()
            .map(file_attachment_reference_xml)
            .collect::<String>()
    )
}

fn file_attachment_reference_xml(attachment: &ActiveSyncAttachment) -> String {
    format!(
        concat!(
            "<t:FileAttachment>",
            "<t:AttachmentId Id=\"{file_reference}\"/>",
            "<t:Name>{name}</t:Name>",
            "<t:ContentType>{content_type}</t:ContentType>",
            "<t:Size>{size}</t:Size>",
            "<t:IsInline>false</t:IsInline>",
            "</t:FileAttachment>"
        ),
        file_reference = escape_xml(&attachment.file_reference),
        name = escape_xml(&attachment.file_name),
        content_type = escape_xml(&attachment.media_type),
        size = attachment.size_octets,
    )
}

fn file_attachment_content_xml(content: &ActiveSyncAttachmentContent) -> String {
    format!(
        concat!(
            "<t:FileAttachment>",
            "<t:AttachmentId Id=\"{file_reference}\"/>",
            "<t:Name>{name}</t:Name>",
            "<t:ContentType>{content_type}</t:ContentType>",
            "<t:Size>{size}</t:Size>",
            "<t:IsInline>false</t:IsInline>",
            "<t:Content>{body}</t:Content>",
            "</t:FileAttachment>"
        ),
        file_reference = escape_xml(&content.file_reference),
        name = escape_xml(&content.file_name),
        content_type = escape_xml(&content.media_type),
        size = content.blob_bytes.len(),
        body = BASE64_STANDARD.encode(&content.blob_bytes),
    )
}

fn root_item_id_xml(email: &JmapEmail) -> String {
    format!(
        "<m:RootItemId RootItemId=\"message:{id}\" RootItemChangeKey=\"{change_key}\"/>",
        id = email.id,
        change_key = escape_xml(&email.delivery_status),
    )
}

fn create_item_success_response(message_id: Uuid, delivery_status: &str) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:Message>",
            "<t:ItemId Id=\"message:{message_id}\" ChangeKey=\"{delivery_status}\"/>",
            "</t:Message>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        message_id = message_id,
        delivery_status = escape_xml(delivery_status),
    )
}

fn create_contact_success_response(contact: &AccessibleContact) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:Contact>",
            "<t:ItemId Id=\"contact:{id}\" ChangeKey=\"created\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{name}</t:Subject>",
            "<t:DisplayName>{name}</t:DisplayName>",
            "</t:Contact>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        id = contact.id,
        folder_id = escape_xml(&contact.collection_id),
        name = escape_xml(&contact.name),
    )
}

fn create_event_success_response(event: &AccessibleEvent) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\" ChangeKey=\"created\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Start>{start}</t:Start>",
            "<t:End>{end}</t:End>",
            "</t:CalendarItem>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        id = event.id,
        folder_id = escape_xml(&event.collection_id),
        title = escape_xml(&event.title),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
    )
}

fn create_task_success_response(task: &ClientTask) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:Task>",
            "<t:ItemId Id=\"task:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Status>{status}</t:Status>",
            "{due_date}",
            "{complete_date}",
            "</t:Task>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        id = task.id,
        change_key = escape_xml(&task_change_key(task, None)),
        folder_id = task.task_list_id,
        title = escape_xml(&task.title),
        status = ews_task_status(&task.status),
        due_date = optional_text_element("t:DueDate", task.due_at.as_deref()),
        complete_date = optional_text_element("t:CompleteDate", task.completed_at.as_deref()),
    )
}

fn update_item_success_response(items: String) -> String {
    format!(
        concat!(
            "<m:UpdateItemResponse>",
            "<m:ResponseMessages>",
            "<m:UpdateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>{items}</m:Items>",
            "</m:UpdateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:UpdateItemResponse>"
        ),
        items = items,
    )
}

fn delete_item_success_response() -> String {
    concat!(
        "<m:DeleteItemResponse>",
        "<m:ResponseMessages>",
        "<m:DeleteItemResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "</m:DeleteItemResponseMessage>",
        "</m:ResponseMessages>",
        "</m:DeleteItemResponse>"
    )
    .to_string()
}

fn move_item_success_response(items: String) -> String {
    format!(
        concat!(
            "<m:MoveItemResponse>",
            "<m:ResponseMessages>",
            "<m:MoveItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>{items}</m:Items>",
            "</m:MoveItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:MoveItemResponse>"
        ),
        items = items,
    )
}

fn copy_item_success_response(items: String) -> String {
    format!(
        concat!(
            "<m:CopyItemResponse>",
            "<m:ResponseMessages>",
            "<m:CopyItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>{items}</m:Items>",
            "</m:CopyItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CopyItemResponse>"
        ),
        items = items,
    )
}

fn get_attachment_success_response(attachments: String) -> String {
    format!(
        concat!(
            "<m:GetAttachmentResponse>",
            "<m:ResponseMessages>",
            "<m:GetAttachmentResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Attachments>{attachments}</m:Attachments>",
            "</m:GetAttachmentResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetAttachmentResponse>"
        ),
        attachments = attachments,
    )
}

fn create_attachment_success_response(attachments: String, root_item: String) -> String {
    format!(
        concat!(
            "<m:CreateAttachmentResponse>",
            "<m:ResponseMessages>",
            "<m:CreateAttachmentResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Attachments>{attachments}</m:Attachments>",
            "{root_item}",
            "</m:CreateAttachmentResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateAttachmentResponse>"
        ),
        attachments = attachments,
        root_item = root_item,
    )
}

fn delete_attachment_success_response(root_items: String) -> String {
    format!(
        concat!(
            "<m:DeleteAttachmentResponse>",
            "<m:ResponseMessages>",
            "<m:DeleteAttachmentResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "{root_items}",
            "</m:DeleteAttachmentResponseMessage>",
            "</m:ResponseMessages>",
            "</m:DeleteAttachmentResponse>"
        ),
        root_items = root_items,
    )
}

fn create_folder_success_response(mailbox: &JmapMailbox) -> String {
    format!(
        concat!(
            "<m:CreateFolderResponse>",
            "<m:ResponseMessages>",
            "<m:CreateFolderResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Folders>{folder}</m:Folders>",
            "</m:CreateFolderResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateFolderResponse>"
        ),
        folder = mailbox_folder_xml(mailbox),
    )
}

fn delete_folder_success_response() -> String {
    concat!(
        "<m:DeleteFolderResponse>",
        "<m:ResponseMessages>",
        "<m:DeleteFolderResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "</m:DeleteFolderResponseMessage>",
        "</m:ResponseMessages>",
        "</m:DeleteFolderResponse>"
    )
    .to_string()
}

fn get_item_error_response(code: &str, message: &str) -> String {
    format!(
        concat!(
            "<m:GetItemResponse>",
            "<m:ResponseMessages>",
            "<m:GetItemResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "<m:Items/>",
            "</m:GetItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetItemResponse>"
        ),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}

fn get_folder_error_response(code: &str, message: &str) -> String {
    format!(
        concat!(
            "<m:GetFolderResponse>",
            "<m:ResponseMessages>",
            "<m:GetFolderResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "<m:Folders/>",
            "</m:GetFolderResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetFolderResponse>"
        ),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}

fn get_server_time_zones_response() -> String {
    concat!(
        "<m:GetServerTimeZonesResponse>",
        "<m:ResponseMessages>",
        "<m:GetServerTimeZonesResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "<m:TimeZoneDefinitions>",
        "<t:TimeZoneDefinition Id=\"UTC\" Name=\"(UTC) Coordinated Universal Time\"/>",
        "<t:TimeZoneDefinition Id=\"W. Europe Standard Time\" Name=\"(UTC+01:00) Amsterdam, Berlin, Bern, Rome, Stockholm, Vienna\"/>",
        "</m:TimeZoneDefinitions>",
        "</m:GetServerTimeZonesResponseMessage>",
        "</m:ResponseMessages>",
        "</m:GetServerTimeZonesResponse>"
    )
    .to_string()
}

fn resolve_names_no_results_response() -> String {
    concat!(
        "<m:ResolveNamesResponse>",
        "<m:ResponseMessages>",
        "<m:ResolveNamesResponseMessage ResponseClass=\"Error\">",
        "<m:MessageText>No results were found.</m:MessageText>",
        "<m:ResponseCode>ErrorNameResolutionNoResults</m:ResponseCode>",
        "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
        "</m:ResolveNamesResponseMessage>",
        "</m:ResponseMessages>",
        "</m:ResolveNamesResponse>"
    )
    .to_string()
}

fn get_user_availability_success_response(events: &[AccessibleEvent]) -> String {
    let events = events
        .iter()
        .map(|event| {
            format!(
                concat!(
                    "<t:CalendarEvent>",
                    "<t:StartTime>{}</t:StartTime>",
                    "<t:EndTime>{}</t:EndTime>",
                    "<t:BusyType>Busy</t:BusyType>",
                    "</t:CalendarEvent>"
                ),
                escape_xml(&ews_datetime(&event.date, &event.time)),
                escape_xml(&event_end_datetime(event)),
            )
        })
        .collect::<String>();
    format!(
        concat!(
            "<m:GetUserAvailabilityResponse>",
            "<m:FreeBusyResponseArray>",
            "<m:FreeBusyResponse>",
            "<m:ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "</m:ResponseMessage>",
            "<m:FreeBusyView>",
            "<t:FreeBusyViewType>Detailed</t:FreeBusyViewType>",
            "<t:CalendarEventArray>{events}</t:CalendarEventArray>",
            "</m:FreeBusyView>",
            "</m:FreeBusyResponse>",
            "</m:FreeBusyResponseArray>",
            "</m:GetUserAvailabilityResponse>"
        ),
        events = events,
    )
}

fn get_user_availability_error_response(message: &str) -> String {
    format!(
        concat!(
            "<m:GetUserAvailabilityResponse>",
            "<m:FreeBusyResponseArray>",
            "<m:FreeBusyResponse>",
            "<m:ResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>ErrorFreeBusyGenerationFailed</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "</m:ResponseMessage>",
            "</m:FreeBusyResponse>",
            "</m:FreeBusyResponseArray>",
            "</m:GetUserAvailabilityResponse>"
        ),
        message = escape_xml(message),
    )
}

fn get_user_oof_settings_response(projection: &OofProjection) -> String {
    let state = if projection.is_enabled {
        "Enabled"
    } else {
        "Disabled"
    };
    let audience = if projection.is_enabled { "All" } else { "None" };
    let message = escape_xml(&projection.text_body);
    format!(
        concat!(
            "<m:GetUserOofSettingsResponse>",
            "<m:ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "</m:ResponseMessage>",
            "<m:OofSettings>",
            "<t:OofState>{state}</t:OofState>",
            "<t:ExternalAudience>{audience}</t:ExternalAudience>",
            "<t:InternalReply><t:Message>{message}</t:Message></t:InternalReply>",
            "<t:ExternalReply><t:Message>{message}</t:Message></t:ExternalReply>",
            "</m:OofSettings>",
            "<m:AllowExternalOof>{audience}</m:AllowExternalOof>",
            "</m:GetUserOofSettingsResponse>"
        ),
        state = state,
        audience = audience,
        message = message,
    )
}

fn unsupported_operation_response(operation: &str) -> String {
    operation_error_response(
        operation,
        "ErrorInvalidOperation",
        &format!("{operation} is not implemented by the EWS MVP."),
    )
}

fn operation_error_response(operation: &str, code: &str, message: &str) -> String {
    format!(
        concat!(
            "<m:{operation}Response>",
            "<m:ResponseMessages>",
            "<m:{operation}ResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "</m:{operation}ResponseMessage>",
            "</m:ResponseMessages>",
            "</m:{operation}Response>"
        ),
        operation = escape_xml(operation),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}

fn root_folder_xml(child_folder_count: usize) -> String {
    format!(
        concat!(
            "<t:Folder>",
            "<t:FolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>",
            "<t:FolderClass>IPF.Note</t:FolderClass>",
            "<t:DisplayName>Root</t:DisplayName>",
            "<t:TotalCount>0</t:TotalCount>",
            "<t:ChildFolderCount>{child_folder_count}</t:ChildFolderCount>",
            "<t:EffectiveRights>",
            "<t:CreateAssociated>true</t:CreateAssociated>",
            "<t:CreateContents>true</t:CreateContents>",
            "<t:CreateHierarchy>true</t:CreateHierarchy>",
            "<t:Delete>true</t:Delete>",
            "<t:Modify>true</t:Modify>",
            "<t:Read>true</t:Read>",
            "<t:ViewPrivateItems>true</t:ViewPrivateItems>",
            "</t:EffectiveRights>",
            "<t:UnreadCount>0</t:UnreadCount>",
            "</t:Folder>"
        ),
        child_folder_count = child_folder_count,
    )
}

fn folder_xml(collection: &CollaborationCollection, distinguished_id: &str, class: &str) -> String {
    let element = match distinguished_id {
        CONTACTS_FOLDER_ID => "ContactsFolder",
        CALENDAR_FOLDER_ID => "CalendarFolder",
        TASKS_FOLDER_ID => "TasksFolder",
        _ => "Folder",
    };
    format!(
        concat!(
            "<t:{element}>",
            "<t:FolderId Id=\"{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>",
            "<t:FolderClass>IPF.{class}</t:FolderClass>",
            "<t:DisplayName>{display}</t:DisplayName>",
            "<t:TotalCount>0</t:TotalCount>",
            "<t:ChildFolderCount>0</t:ChildFolderCount>",
            "<t:EffectiveRights>",
            "<t:CreateAssociated>true</t:CreateAssociated>",
            "<t:CreateContents>true</t:CreateContents>",
            "<t:CreateHierarchy>true</t:CreateHierarchy>",
            "<t:Delete>true</t:Delete>",
            "<t:Modify>true</t:Modify>",
            "<t:Read>true</t:Read>",
            "<t:ViewPrivateItems>true</t:ViewPrivateItems>",
            "</t:EffectiveRights>",
            "<t:UnreadCount>0</t:UnreadCount>",
            "</t:{element}>"
        ),
        element = element,
        id = escape_xml(&collection.id),
        change_key = escape_xml(&folder_change_key(&collection.id)),
        display = escape_xml(&collection.display_name),
        class = class,
    )
}

fn mailbox_folder_xml(mailbox: &JmapMailbox) -> String {
    format!(
        concat!(
            "<t:Folder>",
            "<t:FolderId Id=\"mailbox:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>",
            "<t:FolderClass>IPF.Note</t:FolderClass>",
            "<t:DisplayName>{display}</t:DisplayName>",
            "<t:TotalCount>{total_count}</t:TotalCount>",
            "<t:ChildFolderCount>0</t:ChildFolderCount>",
            "<t:EffectiveRights>",
            "<t:CreateAssociated>true</t:CreateAssociated>",
            "<t:CreateContents>true</t:CreateContents>",
            "<t:CreateHierarchy>true</t:CreateHierarchy>",
            "<t:Delete>true</t:Delete>",
            "<t:Modify>true</t:Modify>",
            "<t:Read>true</t:Read>",
            "<t:ViewPrivateItems>true</t:ViewPrivateItems>",
            "</t:EffectiveRights>",
            "<t:UnreadCount>{unread_count}</t:UnreadCount>",
            "</t:Folder>"
        ),
        id = mailbox.id,
        change_key = folder_change_key(&mailbox.id.to_string()),
        display = escape_xml(&mailbox.name),
        total_count = mailbox.total_emails,
        unread_count = mailbox.unread_emails,
    )
}

fn folder_change_key(id: &str) -> String {
    format!("ck-{id}")
}

fn contact_change_key(contact: &AccessibleContact, sync_version: Option<&str>) -> String {
    stable_change_key(&[
        "contact",
        &contact.id.to_string(),
        sync_version.unwrap_or_default(),
        &contact.collection_id,
        &contact.name,
        &contact.role,
        &contact.email,
        &contact.phone,
        &contact.team,
        &contact.notes,
    ])
}

fn calendar_change_key(event: &AccessibleEvent, sync_version: Option<&str>) -> String {
    stable_change_key(&[
        "calendar",
        &event.id.to_string(),
        sync_version.unwrap_or_default(),
        &event.collection_id,
        &event.date,
        &event.time,
        &event.time_zone,
        &event.duration_minutes.to_string(),
        &event.recurrence_rule,
        &event.title,
        &event.location,
        &event.attendees,
        &event.attendees_json,
        &event.notes,
    ])
}

fn task_change_key(task: &ClientTask, sync_version: Option<&str>) -> String {
    stable_change_key(&[
        "task",
        &task.id.to_string(),
        sync_version.unwrap_or_default(),
        &task.task_list_id.to_string(),
        &task.title,
        &task.description,
        &task.status,
        task.due_at.as_deref().unwrap_or_default(),
        task.completed_at.as_deref().unwrap_or_default(),
        &task.sort_order.to_string(),
    ])
}

fn stable_change_key(parts: &[&str]) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("ck-{hash:016x}")
}

fn count_folder_elements(value: &str) -> usize {
    count_tag_occurrences(value, "<t:Folder>")
        + count_tag_occurrences(value, "<t:ContactsFolder>")
        + count_tag_occurrences(value, "<t:CalendarFolder>")
        + count_tag_occurrences(value, "<t:TasksFolder>")
}

fn contact_summary_xml(contact: &AccessibleContact) -> String {
    let change_key = contact_change_key(contact, None);
    contact_summary_xml_with_change_key(contact, &change_key)
}

fn contact_summary_xml_with_change_key(contact: &AccessibleContact, change_key: &str) -> String {
    format!(
        concat!(
            "<t:Contact>",
            "<t:ItemId Id=\"contact:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:Subject>{name}</t:Subject>",
            "<t:DisplayName>{name}</t:DisplayName>",
            "</t:Contact>"
        ),
        id = contact.id,
        change_key = escape_xml(change_key),
        name = escape_xml(&contact.name),
    )
}

fn contact_item_xml(contact: &AccessibleContact) -> String {
    let change_key = contact_change_key(contact, None);
    contact_item_xml_with_change_key(contact, &change_key)
}

fn contact_item_xml_with_change_key(contact: &AccessibleContact, change_key: &str) -> String {
    format!(
        concat!(
            "<t:Contact>",
            "<t:ItemId Id=\"contact:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{name}</t:Subject>",
            "<t:DisplayName>{name}</t:DisplayName>",
            "<t:GivenName>{given}</t:GivenName>",
            "<t:Surname>{surname}</t:Surname>",
            "<t:JobTitle>{role}</t:JobTitle>",
            "<t:CompanyName>{team}</t:CompanyName>",
            "<t:EmailAddresses><t:Entry Key=\"EmailAddress1\">{email}</t:Entry></t:EmailAddresses>",
            "<t:PhoneNumbers><t:Entry Key=\"MobilePhone\">{phone}</t:Entry></t:PhoneNumbers>",
            "<t:Body BodyType=\"Text\">{notes}</t:Body>",
            "</t:Contact>"
        ),
        id = contact.id,
        change_key = escape_xml(change_key),
        folder_id = escape_xml(&contact.collection_id),
        name = escape_xml(&contact.name),
        given = escape_xml(&first_name(&contact.name)),
        surname = escape_xml(&last_name(&contact.name)),
        role = escape_xml(&contact.role),
        team = escape_xml(&contact.team),
        email = escape_xml(&contact.email),
        phone = escape_xml(&contact.phone),
        notes = escape_xml(&contact.notes),
    )
}

fn calendar_item_summary_xml(event: &AccessibleEvent) -> String {
    let change_key = calendar_change_key(event, None);
    calendar_item_summary_xml_with_change_key(event, &change_key)
}

fn calendar_item_summary_xml_with_change_key(event: &AccessibleEvent, change_key: &str) -> String {
    format!(
        concat!(
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Start>{start}</t:Start>",
            "<t:End>{end}</t:End>",
            "</t:CalendarItem>"
        ),
        id = event.id,
        change_key = escape_xml(change_key),
        title = escape_xml(&event.title),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
    )
}

fn calendar_item_xml(event: &AccessibleEvent) -> String {
    let change_key = calendar_change_key(event, None);
    calendar_item_xml_with_change_key(event, &change_key)
}

fn calendar_item_xml_with_change_key(event: &AccessibleEvent, change_key: &str) -> String {
    format!(
        concat!(
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Location>{location}</t:Location>",
            "<t:Start>{start}</t:Start>",
            "<t:End>{end}</t:End>",
            "{recurrence}",
            "<t:LegacyFreeBusyStatus>Busy</t:LegacyFreeBusyStatus>",
            "<t:Body BodyType=\"Text\">{notes}</t:Body>",
            "</t:CalendarItem>"
        ),
        id = event.id,
        change_key = escape_xml(change_key),
        folder_id = escape_xml(&event.collection_id),
        title = escape_xml(&event.title),
        location = escape_xml(&event.location),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
        recurrence = ews_recurrence_xml(event),
        notes = escape_xml(&event.notes),
    )
}

fn ews_recurrence_xml(event: &AccessibleEvent) -> String {
    let Some(recurrence) = rrule_to_ews_recurrence(&event.recurrence_rule, &event.date) else {
        return String::new();
    };
    recurrence
}

fn rrule_to_ews_recurrence(rrule: &str, start_date: &str) -> Option<String> {
    let fields = rrule_fields(rrule);
    let freq = fields.get("FREQ")?.as_str();
    let interval = fields
        .get("INTERVAL")
        .cloned()
        .unwrap_or_else(|| "1".to_string());
    let pattern = match freq {
        "DAILY" => format!(
            "<t:DailyRecurrence><t:Interval>{}</t:Interval></t:DailyRecurrence>",
            escape_xml(&interval)
        ),
        "WEEKLY" => {
            let days = fields
                .get("BYDAY")
                .map(|value| {
                    value
                        .split(',')
                        .filter_map(rrule_weekday_to_ews)
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "Monday".to_string());
            format!(
                concat!(
                    "<t:WeeklyRecurrence>",
                    "<t:Interval>{interval}</t:Interval>",
                    "<t:DaysOfWeek>{days}</t:DaysOfWeek>",
                    "</t:WeeklyRecurrence>"
                ),
                interval = escape_xml(&interval),
                days = escape_xml(&days),
            )
        }
        "MONTHLY" => {
            let day = fields.get("BYMONTHDAY")?;
            format!(
                concat!(
                    "<t:AbsoluteMonthlyRecurrence>",
                    "<t:Interval>{interval}</t:Interval>",
                    "<t:DayOfMonth>{day}</t:DayOfMonth>",
                    "</t:AbsoluteMonthlyRecurrence>"
                ),
                interval = escape_xml(&interval),
                day = escape_xml(day),
            )
        }
        "YEARLY" => {
            let day = fields.get("BYMONTHDAY")?;
            let month = fields.get("BYMONTH").and_then(|value| {
                value
                    .parse::<u32>()
                    .ok()
                    .and_then(rrule_month_number_to_ews)
            })?;
            format!(
                concat!(
                    "<t:AbsoluteYearlyRecurrence>",
                    "<t:DayOfMonth>{day}</t:DayOfMonth>",
                    "<t:Month>{month}</t:Month>",
                    "</t:AbsoluteYearlyRecurrence>"
                ),
                day = escape_xml(day),
                month = month,
            )
        }
        _ => return None,
    };
    let range = if let Some(count) = fields.get("COUNT") {
        format!(
            concat!(
                "<t:NumberedRecurrence>",
                "<t:StartDate>{}</t:StartDate>",
                "<t:NumberOfOccurrences>{}</t:NumberOfOccurrences>",
                "</t:NumberedRecurrence>"
            ),
            escape_xml(start_date),
            escape_xml(count),
        )
    } else if let Some(until) = fields.get("UNTIL") {
        format!(
            concat!(
                "<t:EndDateRecurrence>",
                "<t:StartDate>{}</t:StartDate>",
                "<t:EndDate>{}</t:EndDate>",
                "</t:EndDateRecurrence>"
            ),
            escape_xml(start_date),
            escape_xml(&rrule_until_to_ews_date(until)),
        )
    } else {
        format!(
            "<t:NoEndRecurrence><t:StartDate>{}</t:StartDate></t:NoEndRecurrence>",
            escape_xml(start_date)
        )
    };
    Some(format!("<t:Recurrence>{pattern}{range}</t:Recurrence>"))
}

fn rrule_fields(rrule: &str) -> HashMap<String, String> {
    rrule
        .split(';')
        .filter_map(|part| part.split_once('='))
        .map(|(key, value)| (key.trim().to_ascii_uppercase(), value.trim().to_string()))
        .collect()
}

fn rrule_weekday_to_ews(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_uppercase().as_str() {
        "MO" => Some("Monday"),
        "TU" => Some("Tuesday"),
        "WE" => Some("Wednesday"),
        "TH" => Some("Thursday"),
        "FR" => Some("Friday"),
        "SA" => Some("Saturday"),
        "SU" => Some("Sunday"),
        _ => None,
    }
}

fn rrule_month_number_to_ews(value: u32) -> Option<&'static str> {
    match value {
        1 => Some("January"),
        2 => Some("February"),
        3 => Some("March"),
        4 => Some("April"),
        5 => Some("May"),
        6 => Some("June"),
        7 => Some("July"),
        8 => Some("August"),
        9 => Some("September"),
        10 => Some("October"),
        11 => Some("November"),
        12 => Some("December"),
        _ => None,
    }
}

fn rrule_until_to_ews_date(value: &str) -> String {
    let date = value.split('T').next().unwrap_or(value);
    if date.len() == 8 {
        format!("{}-{}-{}", &date[0..4], &date[4..6], &date[6..8])
    } else {
        date.to_string()
    }
}

fn task_item_summary_xml(task: &ClientTask) -> String {
    let change_key = task_change_key(task, None);
    task_item_summary_xml_with_change_key(task, &change_key)
}

fn task_item_summary_xml_with_change_key(task: &ClientTask, change_key: &str) -> String {
    format!(
        concat!(
            "<t:Task>",
            "<t:ItemId Id=\"task:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Status>{status}</t:Status>",
            "{due_date}",
            "{complete_date}",
            "</t:Task>"
        ),
        id = task.id,
        change_key = escape_xml(change_key),
        title = escape_xml(&task.title),
        status = ews_task_status(&task.status),
        due_date = optional_text_element("t:DueDate", task.due_at.as_deref()),
        complete_date = optional_text_element("t:CompleteDate", task.completed_at.as_deref()),
    )
}

fn task_item_xml(task: &ClientTask) -> String {
    let change_key = task_change_key(task, None);
    task_item_xml_with_change_key(task, &change_key)
}

fn task_item_xml_with_change_key(task: &ClientTask, change_key: &str) -> String {
    format!(
        concat!(
            "<t:Task>",
            "<t:ItemId Id=\"task:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Body BodyType=\"Text\">{description}</t:Body>",
            "<t:Status>{status}</t:Status>",
            "{due_date}",
            "{complete_date}",
            "</t:Task>"
        ),
        id = task.id,
        change_key = escape_xml(change_key),
        folder_id = task.task_list_id,
        title = escape_xml(&task.title),
        description = escape_xml(&task.description),
        status = ews_task_status(&task.status),
        due_date = optional_text_element("t:DueDate", task.due_at.as_deref()),
        complete_date = optional_text_element("t:CompleteDate", task.completed_at.as_deref()),
    )
}

fn ews_task_status(status: &str) -> &'static str {
    match status {
        "in-progress" => "InProgress",
        "completed" => "Completed",
        "cancelled" => "Deferred",
        _ => "NotStarted",
    }
}

fn optional_text_element(name: &str, value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("<{name}>{}</{name}>", escape_xml(value)))
        .unwrap_or_default()
}

fn ews_datetime(date: &str, time: &str) -> String {
    format!("{}T{}:00Z", date.trim(), time.trim())
}

fn event_end_datetime(event: &AccessibleEvent) -> String {
    let (hour, minute) = event
        .time
        .split_once(':')
        .and_then(|(hour, minute)| Some((hour.parse::<i32>().ok()?, minute.parse::<i32>().ok()?)))
        .unwrap_or((0, 0));
    let total = hour
        .saturating_mul(60)
        .saturating_add(minute)
        .saturating_add(event.duration_minutes.max(0));
    let end_hour = (total / 60).min(23);
    let end_minute = total % 60;
    ews_datetime(&event.date, &format!("{end_hour:02}:{end_minute:02}"))
}

fn first_name(name: &str) -> String {
    name.split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string()
}

fn last_name(name: &str) -> String {
    name.split_whitespace()
        .skip(1)
        .collect::<Vec<_>>()
        .join(" ")
}

fn count_tag_occurrences(value: &str, needle: &str) -> usize {
    value.match_indices(needle).count()
}

fn soap_response(body: String) -> Response {
    let envelope = format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
            "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" ",
            "xmlns:m=\"http://schemas.microsoft.com/exchange/services/2006/messages\" ",
            "xmlns:t=\"http://schemas.microsoft.com/exchange/services/2006/types\">",
            "<s:Header><t:ServerVersionInfo MajorVersion=\"15\" MinorVersion=\"0\" MajorBuildNumber=\"0\" MinorBuildNumber=\"0\" Version=\"Exchange2013\"/></s:Header>",
            "<s:Body>{body}</s:Body>",
            "</s:Envelope>"
        ),
        body = body,
    );
    xml_response(StatusCode::OK, envelope)
}

pub(crate) fn error_response(error: &anyhow::Error) -> Response {
    let message = error.to_string();
    if is_authentication_error(&message) {
        return soap_auth_challenge(&message);
    }
    soap_error(StatusCode::BAD_REQUEST, &message)
}

fn is_authentication_error(message: &str) -> bool {
    matches!(
        message,
        "missing account authentication" | "invalid credentials"
    ) || message.contains("oauth access token")
}

fn soap_auth_challenge(message: &str) -> Response {
    let mut response = soap_error(StatusCode::UNAUTHORIZED, message);
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"LPE EWS\""),
    );
    response
}

fn soap_error(status: StatusCode, message: &str) -> Response {
    let envelope = format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
            "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">",
            "<s:Body><s:Fault>",
            "<faultcode>s:Client</faultcode>",
            "<faultstring>{}</faultstring>",
            "</s:Fault></s:Body>",
            "</s:Envelope>"
        ),
        escape_xml(message)
    );
    xml_response(status, envelope)
}

fn xml_response(status: StatusCode, body: String) -> Response {
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/xml; charset=utf-8"),
    );
    response
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
