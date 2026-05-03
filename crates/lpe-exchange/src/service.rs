use anyhow::{anyhow, Result};
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
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, AuditEntryInput, CollaborationCollection, JmapEmail,
    JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput, Storage, SubmitMessageInput,
    SubmittedRecipientInput, UpsertClientContactInput,
};
use std::collections::HashSet;
use uuid::Uuid;

use crate::store::ExchangeStore;

const EWS_PATH: &str = "/EWS/Exchange.asmx";
const EWS_LOWER_PATH: &str = "/ews/exchange.asmx";
const CONTACTS_FOLDER_ID: &str = "contacts";
const CALENDAR_FOLDER_ID: &str = "calendar";
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
}

#[derive(Clone)]
pub(crate) struct ExchangeService<S> {
    store: S,
}

impl<S> ExchangeService<S> {
    pub(crate) fn new(store: S) -> Self {
        Self { store }
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

impl<S: ExchangeStore> ExchangeService<S> {
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
            "GetUserAvailability" => get_user_availability_unavailable_response(),
            "CreateItem" => self.create_item(&principal, &body).await?,
            "UpdateItem" => unsupported_operation_response("UpdateItem"),
            "DeleteItem" => self.delete_item(&principal, &body).await?,
            "CreateFolder" => self.create_folder(&principal, &body).await?,
            "DeleteFolder" => self.delete_folder(&principal, &body).await?,
            "GetUserOofSettings" => unsupported_operation_response("GetUserOofSettings"),
            "GetRoomLists" => unsupported_operation_response("GetRoomLists"),
            "FindPeople" => unsupported_operation_response("FindPeople"),
            "ExpandDL" => unsupported_operation_response("ExpandDL"),
            "Subscribe" => unsupported_operation_response("Subscribe"),
            "GetDelegate" => unsupported_operation_response("GetDelegate"),
            "GetUserConfiguration" => unsupported_operation_response("GetUserConfiguration"),
            "GetSharingMetadata" => unsupported_operation_response("GetSharingMetadata"),
            "GetSharingFolder" => unsupported_operation_response("GetSharingFolder"),
            "GetAttachment" => unsupported_operation_response("GetAttachment"),
            "Unsubscribe" => unsupported_operation_response("Unsubscribe"),
            "GetEvents" => unsupported_operation_response("GetEvents"),
            _ => unsupported_operation_response(&operation),
        };

        Ok(soap_response(payload))
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
            count = count_tag_occurrences(&folders, "<t:Folder>"),
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
                FolderKind::Root => folders.push_str(&root_folder_xml()),
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
        let message_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("message:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();
        let supported_id_count = contact_ids.len() + event_ids.len() + message_ids.len();

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
        for email in self
            .store
            .fetch_jmap_emails(principal.account_id, &message_ids)
            .await?
            .into_iter()
        {
            items.push_str(&message_item_xml(&email));
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
                let current_ids = contacts
                    .iter()
                    .map(|contact| contact.id)
                    .collect::<Vec<_>>();
                let current_set = current_ids.iter().copied().collect::<HashSet<_>>();
                let previous_ids = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_ids(&state, "contacts", collection_id))
                    .unwrap_or_default();
                let previous_set = previous_ids.iter().copied().collect::<HashSet<_>>();
                for contact in &contacts {
                    if !previous_set.contains(&contact.id) {
                        changes.push_str("<t:Create>");
                        changes.push_str(&contact_item_xml(contact));
                        changes.push_str("</t:Create>");
                    }
                }
                for contact_id in previous_ids {
                    if !current_set.contains(&contact_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"contact:{contact_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                collaboration_sync_state("contacts", collection_id, &current_ids)
            }
            FolderKind::Calendar => {
                let collection_id = requested_collection_id(request).unwrap_or(CALENDAR_FOLDER_ID);
                let events = self
                    .store
                    .fetch_accessible_events_in_collection(principal.account_id, collection_id)
                    .await?;
                let current_ids = events.iter().map(|event| event.id).collect::<Vec<_>>();
                let current_set = current_ids.iter().copied().collect::<HashSet<_>>();
                let previous_ids = requested_sync_state(request)
                    .map(|state| collaboration_sync_state_ids(&state, "calendar", collection_id))
                    .unwrap_or_default();
                let previous_set = previous_ids.iter().copied().collect::<HashSet<_>>();
                for event in &events {
                    if !previous_set.contains(&event.id) {
                        changes.push_str("<t:Create>");
                        changes.push_str(&calendar_item_xml(event));
                        changes.push_str("</t:Create>");
                    }
                }
                for event_id in previous_ids {
                    if !current_set.contains(&event_id) {
                        changes.push_str("<t:Delete>");
                        changes.push_str(&format!(
                            "<t:ItemId Id=\"event:{event_id}\" ChangeKey=\"deleted\"/>"
                        ));
                        changes.push_str("</t:Delete>");
                    }
                }
                collaboration_sync_state("calendar", collection_id, &current_ids)
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

    async fn delete_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let ids = requested_item_ids(request);
            let contact_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("contact:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty() || contact_ids.len() + message_ids.len() != ids.len() {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorInvalidOperation",
                    "DeleteItem currently supports only contact and message ids.",
                ));
            }
            for contact_id in contact_ids {
                self.store
                    .delete_accessible_contact(principal.account_id, contact_id)
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
        } else if id.starts_with("mailbox:") || Uuid::parse_str(id).is_ok() {
            Some(FolderKind::Mailbox)
        } else if id == "msgfolderroot" || id == "root" {
            Some(FolderKind::Root)
        } else {
            None
        }
    })
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
            "contacts" | "calendar" => DEFAULT_COLLECTION_ID,
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

fn collaboration_sync_state(kind: &str, collection_id: &str, item_ids: &[Uuid]) -> String {
    let id_list = item_ids
        .iter()
        .map(Uuid::to_string)
        .collect::<Vec<_>>()
        .join(",");
    if id_list.is_empty() {
        format!("{kind}:{collection_id}:0")
    } else {
        format!("{kind}:{collection_id}:{id_list}")
    }
}

fn collaboration_sync_state_ids(sync_state: &str, kind: &str, collection_id: &str) -> Vec<Uuid> {
    let prefix = format!("{kind}:{collection_id}:");
    sync_state
        .strip_prefix(&prefix)
        .unwrap_or_default()
        .split(',')
        .filter(|value| !value.is_empty() && *value != "0")
        .filter_map(|value| Uuid::parse_str(value).ok())
        .collect()
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
    let mut xml = message_summary_xml(email);
    xml.insert_str(
        xml.len() - "</t:Message>".len(),
        &format!(
            "<t:Body BodyType=\"Text\">{}</t:Body>",
            escape_xml(&email.body_text)
        ),
    );
    xml
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

fn get_user_availability_unavailable_response() -> String {
    concat!(
        "<m:GetUserAvailabilityResponse>",
        "<m:FreeBusyResponseArray>",
        "<m:FreeBusyResponse>",
        "<m:ResponseMessage ResponseClass=\"Error\">",
        "<m:MessageText>Free/busy is not implemented by the EWS MVP.</m:MessageText>",
        "<m:ResponseCode>ErrorFreeBusyGenerationFailed</m:ResponseCode>",
        "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
        "</m:ResponseMessage>",
        "</m:FreeBusyResponse>",
        "</m:FreeBusyResponseArray>",
        "</m:GetUserAvailabilityResponse>"
    )
    .to_string()
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

fn root_folder_xml() -> String {
    concat!(
        "<t:Folder>",
        "<t:FolderId Id=\"msgfolderroot\"/>",
        "<t:DisplayName>Root</t:DisplayName>",
        "<t:FolderClass>IPF.Note</t:FolderClass>",
        "<t:TotalCount>0</t:TotalCount>",
        "<t:ChildFolderCount>0</t:ChildFolderCount>",
        "<t:DistinguishedFolderId Id=\"msgfolderroot\"/>",
        "</t:Folder>"
    )
    .to_string()
}

fn folder_xml(collection: &CollaborationCollection, distinguished_id: &str, class: &str) -> String {
    format!(
        concat!(
            "<t:Folder>",
            "<t:FolderId Id=\"{id}\"/>",
            "<t:DisplayName>{display}</t:DisplayName>",
            "<t:FolderClass>IPF.{class}</t:FolderClass>",
            "<t:TotalCount>0</t:TotalCount>",
            "<t:ChildFolderCount>0</t:ChildFolderCount>",
            "<t:DistinguishedFolderId Id=\"{distinguished_id}\"/>",
            "</t:Folder>"
        ),
        id = escape_xml(&collection.id),
        display = escape_xml(&collection.display_name),
        class = class,
        distinguished_id = distinguished_id,
    )
}

fn mailbox_folder_xml(mailbox: &JmapMailbox) -> String {
    format!(
        concat!(
            "<t:Folder>",
            "<t:FolderId Id=\"mailbox:{id}\"/>",
            "<t:DisplayName>{display}</t:DisplayName>",
            "<t:FolderClass>IPF.Note</t:FolderClass>",
            "<t:TotalCount>{total_count}</t:TotalCount>",
            "<t:ChildFolderCount>0</t:ChildFolderCount>",
            "</t:Folder>"
        ),
        id = mailbox.id,
        display = escape_xml(&mailbox.name),
        total_count = mailbox.total_emails,
    )
}

fn contact_summary_xml(contact: &AccessibleContact) -> String {
    format!(
        concat!(
            "<t:Contact>",
            "<t:ItemId Id=\"contact:{id}\"/>",
            "<t:Subject>{name}</t:Subject>",
            "<t:DisplayName>{name}</t:DisplayName>",
            "</t:Contact>"
        ),
        id = contact.id,
        name = escape_xml(&contact.name),
    )
}

fn contact_item_xml(contact: &AccessibleContact) -> String {
    format!(
        concat!(
            "<t:Contact>",
            "<t:ItemId Id=\"contact:{id}\"/>",
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
    format!(
        concat!(
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Start>{start}</t:Start>",
            "<t:End>{end}</t:End>",
            "</t:CalendarItem>"
        ),
        id = event.id,
        title = escape_xml(&event.title),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
    )
}

fn calendar_item_xml(event: &AccessibleEvent) -> String {
    format!(
        concat!(
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Location>{location}</t:Location>",
            "<t:Start>{start}</t:Start>",
            "<t:End>{end}</t:End>",
            "<t:LegacyFreeBusyStatus>Busy</t:LegacyFreeBusyStatus>",
            "<t:Body BodyType=\"Text\">{notes}</t:Body>",
            "</t:CalendarItem>"
        ),
        id = event.id,
        folder_id = escape_xml(&event.collection_id),
        title = escape_xml(&event.title),
        location = escape_xml(&event.location),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
        notes = escape_xml(&event.notes),
    )
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
