use anyhow::{anyhow, bail, Result};
use axum::{
    body::{to_bytes, Body, Bytes},
    extract::State,
    http::{
        header::{CONTENT_LENGTH, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, Method, StatusCode, Uri,
    },
    response::{IntoResponse, Response},
    Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use lpe_domain::mail_format::{
    format_mailbox_address, quote_header_parameter, sanitize_header_value, DisplayNamePolicy,
};
use lpe_domain::normalization;
use lpe_magika::{
    Detector, ExpectedKind, IngressContext, PolicyDecision, SystemDetector, ValidationRequest,
    Validator,
};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{
    calendar_attendee_labels, parse_calendar_participants_metadata,
    serialize_calendar_participants_metadata, AccessibleContact, AccessibleEvent,
    ActiveSyncAttachment, ActiveSyncAttachmentContent, AttachmentUploadInput, AuditEntryInput,
    CalendarOrganizerMetadata, CalendarParticipantMetadata, CalendarParticipantsMetadata,
    ClientReminder, ClientTask, CollaborationCollection, CollaborationGrant,
    CollaborationResourceKind, CollaborationRights, ContactNameFields, CreatePublicFolderInput,
    JmapEmail, JmapEmailAddress, JmapEmailFollowupUpdate, JmapImportedEmailInput, JmapMailbox,
    JmapMailboxCreateInput, JmapMailboxUpdateInput, MailboxRule, ManagedRetentionFolderCreateInput,
    PublicFolder, PublicFolderItem, ReminderQuery, Storage, SubmitMessageInput,
    SubmittedRecipientInput, UpdatePublicFolderInput, UpsertClientContactInput,
    UpsertClientEventInput, UpsertClientTaskInput, UpsertPublicFolderItemInput,
};
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use uuid::Uuid;

use crate::{
    build_info,
    ews_types::{EwsDeleteType, EwsDistinguishedFolderIdName, EwsOofState, EwsTaskStatus},
    mapi::{self, MapiEndpoint},
    store::{
        EwsDelegate, EwsDelegatePreferences, EwsDiscoverySearchConfig, EwsDiscoverySearchResult,
        EwsHoldMailbox, EwsImGroup, EwsImGroupMember, EwsImList, EwsImMemberInput,
        EwsMailAppManifest, EwsMailAppTokenEvent, EwsMessageTrackingReport,
        EwsMessageTrackingReportDetail, EwsNonIndexableReport, EwsRetentionPolicyTag,
        EwsSearchableMailbox, EwsTransferJob, EwsUnifiedMessagingCall, EwsUserConfiguration,
        EwsUserConfigurationKey, ExchangeAddressBookDirectoryKind, ExchangeAddressBookEntry,
        ExchangeAddressBookEntryDetails, ExchangeAddressBookEntryKind, ExchangeStore,
        UpsertEwsDelegateInput, UpsertEwsUserConfigurationInput,
    },
};

mod ews {
    pub(super) mod attachments;
    pub(super) mod availability;
    pub(super) mod bulk_transfer;
    pub(super) mod calendar;
    pub(super) mod compliance;
    pub(super) mod contacts;
    pub(super) mod conversations;
    pub(super) mod delegation;
    pub(super) mod diagnostics;
    pub(super) mod directory;
    pub(super) mod errors;
    pub(super) mod fields;
    pub(super) mod folders;
    pub(super) mod ids;
    pub(super) mod items;
    pub(super) mod mail;
    pub(super) mod mail_apps;
    pub(super) mod mail_tips;
    pub(super) mod mailboxes;
    pub(super) mod message_tracking;
    pub(super) mod mime;
    pub(super) mod notifications;
    pub(super) mod oof;
    pub(super) mod public_folders;
    pub(super) mod reminders;
    pub(super) mod request_ids;
    pub(super) mod responses;
    pub(super) mod retention;
    pub(super) mod rooms;
    pub(super) mod rules;
    pub(super) mod sharing;
    pub(super) mod sync_state;
    pub(super) mod tasks;
    pub(super) mod ucs;
    pub(super) mod unified_messaging;
    pub(super) mod user_configuration;
    pub(super) mod xml;
}

mod http_routes;
mod http_utils;
mod mapi_http;
mod rpc_proxy_auth;
mod rpc_proxy_channels;
mod rpc_proxy_codec;
mod rpc_proxy_dce;
mod rpc_proxy_endpoints;
mod rpc_proxy_requests;
mod rpc_proxy_rts;
mod rpc_proxy_stream;
mod transport_diagnostics;

use ews::availability::*;
use ews::bulk_transfer::*;
use ews::calendar::*;
use ews::compliance::*;
use ews::contacts::*;
use ews::diagnostics::*;
pub(crate) use ews::errors::error_response;
use ews::fields::*;
use ews::folders::*;
use ews::ids::*;
use ews::mail::*;
use ews::mail_tips::*;
use ews::mailboxes::*;
use ews::message_tracking::*;
use ews::mime::*;
use ews::notifications::*;
use ews::oof::*;
use ews::public_folders::*;
use ews::reminders::*;
use ews::request_ids::*;
use ews::responses::*;
use ews::rules::*;
use ews::sync_state::*;
use ews::tasks::*;
use ews::xml::*;
use http_routes::*;
use http_utils::*;
use rpc_proxy_auth::*;
#[cfg(test)]
pub(crate) use rpc_proxy_channels::mark_rpc_proxy_out_endpoint_bind_ack;
pub(crate) use rpc_proxy_requests::is_rpc_proxy_in_data_channel_request;
use rpc_proxy_requests::{is_rpc_proxy_echo_request, is_rpc_proxy_endpoint_ping};
use rpc_proxy_rts::*;
use rpc_proxy_stream::*;
#[cfg(test)]
pub(crate) use rpc_proxy_stream::{
    rpc_proxy_in_channel_response_for_buffer, rpc_proxy_in_channel_response_for_endpoint_query,
    rpc_proxy_in_channel_response_for_endpoint_query_with_store,
};
use transport_diagnostics::*;

const RPC_PROXY_COMPAT_STATUS: &str = "x-lpe-rpc-proxy-status";
const RPC_PROXY_MAX_FINITE_BODY_BYTES: usize = 1024 * 1024;
const RPC_PROXY_RECEIVE_WINDOW_SIZE: u32 = 0x0001_0000;
const RPC_PROXY_CONNECTION_TIMEOUT_MS: u32 = 120_000;
const EWS_MAX_MAIL_TIPS_RECIPIENTS: usize = 100;
const CONTACTS_FOLDER_ID: &str = "contacts";
const CALENDAR_FOLDER_ID: &str = "calendar";
const TASKS_FOLDER_ID: &str = "tasks";
const DEFAULT_COLLECTION_ID: &str = "default";
const MAILBOX_QUERY_LIMIT: u64 = 200;
pub fn router() -> Router<Storage> {
    exchange_router()
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

async fn post_handler(
    State(storage): State<Storage>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started_at = Instant::now();
    let operation = ews_operation_hint(&headers, body.as_ref());
    let service = ExchangeService::new(storage);
    let response = match service.handle(&headers, body.as_ref()).await {
        Ok(response) => response,
        Err(error) => {
            let response = error_response(&error);
            log_ews_connection(
                &uri,
                &headers,
                body.len(),
                operation.as_deref().unwrap_or("unknown"),
                ews_response_code(&response),
                &response,
                started_at.elapsed().as_secs_f64() * 1000.0,
                Some(error.to_string().as_str()),
                ews_response_debug_detail(&response),
            );
            return response;
        }
    };
    log_ews_connection(
        &uri,
        &headers,
        body.len(),
        operation.as_deref().unwrap_or("unknown"),
        ews_response_code(&response),
        &response,
        started_at.elapsed().as_secs_f64() * 1000.0,
        None,
        ews_response_debug_detail(&response),
    );
    response
}

pub(crate) async fn mapi_options_handler() -> Response {
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
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    mapi_post_handler(MapiEndpoint::Emsmdb, storage, uri, headers, body).await
}

async fn mapi_nspi_post_handler(
    State(storage): State<Storage>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    mapi_post_handler(MapiEndpoint::Nspi, storage, uri, headers, body).await
}

async fn mapi_post_handler(
    endpoint: MapiEndpoint,
    storage: Storage,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started_at = Instant::now();
    let service = ExchangeService::new(storage);
    let response = match service.handle_mapi(endpoint, &headers, body.as_ref()).await {
        Ok(response) => response,
        Err(error) => {
            let response = mapi::mapi_error_response(&error);
            log_mapi_transport_connection(
                endpoint,
                &uri,
                &headers,
                body.as_ref(),
                &response,
                started_at.elapsed().as_secs_f64() * 1000.0,
                Some(error.to_string().as_str()),
            );
            return response;
        }
    };
    log_mapi_transport_connection(
        endpoint,
        &uri,
        &headers,
        body.as_ref(),
        &response,
        started_at.elapsed().as_secs_f64() * 1000.0,
        None,
    );
    response
}

async fn rpc_proxy_handler(
    State(storage): State<Storage>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let started_at = Instant::now();
    let service = ExchangeService::new(storage);

    if is_rpc_proxy_in_data_channel_request(&method, &uri, &headers) {
        let response = service
            .handle_rpc_proxy_in_data_channel(&method, &uri, &headers, body)
            .await;
        log_rpc_proxy_connection(
            &method,
            &uri,
            &headers,
            b"",
            &response,
            started_at.elapsed().as_secs_f64() * 1000.0,
        );
        return response;
    }

    let body = match to_bytes(body, RPC_PROXY_MAX_FINITE_BODY_BYTES).await {
        Ok(body) => body,
        Err(error) => {
            let response = (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("LPE RPC proxy request body rejected: {error}\n"),
            )
                .into_response();
            log_rpc_proxy_connection(
                &method,
                &uri,
                &headers,
                b"",
                &response,
                started_at.elapsed().as_secs_f64() * 1000.0,
            );
            return response;
        }
    };
    let response = service
        .handle_rpc_proxy(&method, &uri, &headers, body.as_ref())
        .await;
    log_rpc_proxy_connection(
        &method,
        &uri,
        &headers,
        body.as_ref(),
        &response,
        started_at.elapsed().as_secs_f64() * 1000.0,
    );
    response
}

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle(&self, headers: &HeaderMap, body: &[u8]) -> Result<Response> {
        let principal = authenticate_account(&self.store, None, headers, "ews").await?;
        let body = decode_ews_body(headers, body)?;
        let operation = operation_name(&body).ok_or_else(|| anyhow!("unsupported EWS request"))?;

        let payload = match operation.as_str() {
            "SyncFolderHierarchy" => self.sync_folder_hierarchy(&principal).await?,
            "FindFolder" => self.find_folder(&principal).await?,
            "GetFolder" => self.get_folder(&principal, &body).await?,
            "FindItem" => self
                .find_item(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "FindItem",
                        ews_error_code_or(&error, "ErrorInvalidOperation"),
                        &error.to_string(),
                    )
                }),
            "GetItem" => self
                .get_item(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    get_item_error_response(
                        ews_error_code_or(&error, "ErrorInvalidOperation"),
                        &error.to_string(),
                    )
                }),
            "SyncFolderItems" => self
                .sync_folder_items(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "SyncFolderItems",
                        ews_error_code_or(&error, "ErrorInvalidOperation"),
                        &error.to_string(),
                    )
                }),
            "FindConversation" => self.find_conversation(&principal, &body).await?,
            "GetConversationItems" => self.get_conversation_items(&principal, &body).await?,
            "ApplyConversationAction" => self.apply_conversation_action(&principal, &body).await?,
            "GetServerTimeZones" => get_server_time_zones_response(),
            "ResolveNames" => self.resolve_names(&principal, &body).await?,
            "GetUserAvailability" => self.get_user_availability(&principal, &body).await?,
            "CreateItem" => self.create_item(&principal, &body).await?,
            "SendItem" => self.send_item(&principal, &body).await?,
            "UpdateItem" => self.update_item(&principal, &body).await?,
            "DeleteItem" => self.delete_item(&principal, &body).await?,
            "ArchiveItem" => self.archive_item(&principal, &body).await?,
            "MoveItem" => self.move_item(&principal, &body).await?,
            "CopyItem" => self.copy_item(&principal, &body).await?,
            "MarkAllItemsAsRead" => self.mark_all_items_as_read(&principal, &body).await?,
            "CreateFolder" => self.create_folder(&principal, &body).await?,
            "CreateFolderPath" => self.create_folder_path(&principal, &body).await?,
            "CreateManagedFolder" => self.create_managed_folder(&principal, &body).await?,
            "CopyFolder" => self.copy_folder(&principal, &body).await?,
            "EmptyFolder" => self.empty_folder(&principal, &body).await?,
            "MoveFolder" => self.move_folder(&principal, &body).await?,
            "UpdateFolder" => self.update_folder(&principal, &body).await?,
            "DeleteFolder" => self.delete_folder(&principal, &body).await?,
            "GetAttachment" => self.get_attachment(&principal, &body).await?,
            "CreateAttachment" => self.create_attachment(&principal, &body).await?,
            "DeleteAttachment" => self.delete_attachment(&principal, &body).await?,
            "GetUserOofSettings" => self.get_user_oof_settings(&principal).await?,
            "SetUserOofSettings" => self.set_user_oof_settings(&principal, &body).await?,
            "GetInboxRules" => self.get_inbox_rules(&principal).await?,
            "UpdateInboxRules" => self.update_inbox_rules(&principal, &body).await?,
            "GetReminders" => self.get_reminders(&principal, &body).await?,
            "PerformReminderAction" => self.perform_reminder_action(&principal, &body).await?,
            "Subscribe" => self.subscribe(&principal, &body).await?,
            "GetEvents" => self.get_events(&principal, &body).await?,
            "GetStreamingEvents" => self.get_streaming_events(&principal, &body).await?,
            "Unsubscribe" => self.unsubscribe(&body).await?,
            "GetRooms" => self.get_rooms(&principal, &body).await?,
            "GetRoomLists" => self.get_room_lists(&principal).await?,
            "ConvertId" => self.convert_id(&body).await?,
            "GetMailTips" => self.get_mail_tips(&principal, &body).await?,
            "GetServiceConfiguration" => self.get_service_configuration(&body).await?,
            "GetUserRetentionPolicyTags" => self.get_user_retention_policy_tags(&principal).await?,
            "GetDiscoverySearchConfiguration" => {
                self.get_discovery_search_configuration(&principal).await?
            }
            "GetSearchableMailboxes" => self.get_searchable_mailboxes(&principal).await?,
            "SearchMailboxes" => self.search_mailboxes(&principal, &body).await?,
            "FindMessageTrackingReport" => {
                self.find_message_tracking_report(&principal, &body).await?
            }
            "GetMessageTrackingReport" => {
                self.get_message_tracking_report(&principal, &body).await?
            }
            "GetHoldOnMailboxes" => self.get_hold_on_mailboxes(&principal, &body).await?,
            "SetHoldOnMailboxes" => self.set_hold_on_mailboxes(&principal, &body).await?,
            "GetNonIndexableItemDetails" => self.get_non_indexable_item_details(&principal).await?,
            "GetNonIndexableItemStatistics" => {
                self.get_non_indexable_item_statistics(&principal).await?
            }
            "UploadItems" => self.upload_items(&principal, &body).await?,
            "ExportItems" => self.export_items(&principal, &body).await?,
            "GetAppManifests" => self.get_app_manifests(&principal).await?,
            "GetAppMarketplaceUrl" => self.get_app_marketplace_url(&principal).await?,
            "InstallApp" => self.install_app(&principal, &body).await?,
            "DisableApp" => self.disable_app(&principal, &body).await?,
            "UninstallApp" => self.uninstall_app(&principal, &body).await?,
            "GetClientAccessToken" => self.get_client_access_token(&principal, &body).await?,
            "PlayOnPhone" => self.play_on_phone(&principal, &body).await?,
            "GetPhoneCallInformation" => self.get_phone_call_information(&principal, &body).await?,
            "DisconnectPhoneCall" => self.disconnect_phone_call(&principal, &body).await?,
            "FindPeople" => self.find_people(&principal, &body).await?,
            "GetPersona" => self.get_persona(&principal, &body).await?,
            "ExpandDL" => self.expand_dl(&principal, &body).await?,
            "AddDelegate" => self.add_delegate(&principal, &body).await?,
            "GetDelegate" => self.get_delegate(&principal, &body).await?,
            "UpdateDelegate" => self.update_delegate(&principal, &body).await?,
            "RemoveDelegate" => self.remove_delegate(&principal, &body).await?,
            "GetUserConfiguration" => self.get_user_configuration(&principal, &body).await?,
            "CreateUserConfiguration" => self.create_user_configuration(&principal, &body).await?,
            "UpdateUserConfiguration" => self.update_user_configuration(&principal, &body).await?,
            "DeleteUserConfiguration" => self.delete_user_configuration(&principal, &body).await?,
            "GetSharingMetadata" => self.get_sharing_metadata(&principal, &body).await?,
            "GetSharingFolder" => self.get_sharing_folder(&principal, &body).await?,
            "RefreshSharingFolder" => self.refresh_sharing_folder(&principal, &body).await?,
            "GetUserPhoto" => self.get_user_photo(&principal, &body).await?,
            "GetPasswordExpirationDate" => {
                self.get_password_expiration_date(&principal, &body).await?
            }
            "MarkAsJunk" => self.mark_as_junk(&principal, &body).await?,
            "GetImItemList" => self
                .get_im_item_list(&principal)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "GetImItemList",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            "GetImItems" => self
                .get_im_items(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "GetImItems",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            "AddImGroup" => self
                .add_im_group(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "AddImGroup",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            "SetImGroup" => self
                .set_im_group(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "SetImGroup",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            "RemoveImGroup" => self
                .remove_im_group(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "RemoveImGroup",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            "AddImContactToGroup" => self
                .add_im_contact_to_group(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "AddImContactToGroup",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            "AddNewImContactToGroup" => self
                .add_new_im_contact_to_group(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "AddNewImContactToGroup",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            "AddNewTelUriContactToGroup" => self
                .add_new_tel_uri_contact_to_group(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "AddNewTelUriContactToGroup",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            "RemoveContactFromImList" => self
                .remove_contact_from_im_list(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "RemoveContactFromImList",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            "RemoveImContactFromGroup" => self
                .remove_im_contact_from_group(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "RemoveImContactFromGroup",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            "AddDistributionGroupToImList" => self
                .add_distribution_group_to_im_list(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "AddDistributionGroupToImList",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            "RemoveDistributionGroupFromImList" => self
                .remove_distribution_group_from_im_list(&principal, &body)
                .await
                .unwrap_or_else(|error| {
                    operation_error_response(
                        "RemoveDistributionGroupFromImList",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    )
                }),
            _ => unsupported_operation_response(&operation),
        };

        let response_code = element_text(&payload, "ResponseCode").unwrap_or_default();
        let detail = ews_payload_debug_detail(&operation, &payload);
        let mut response = soap_response(payload);
        response.extensions_mut().insert(EwsResponseDebug {
            response_code,
            detail,
        });
        Ok(response)
    }

    async fn create_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            if element_content(request, "AcceptSharingInvitation").is_some() {
                return self.accept_sharing_invitation(principal, request).await;
            }
            if element_content(request, "Contact").is_some() {
                let collection_id = requested_collection_id_in(request, "SavedItemFolderId");
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
                let collection_id = requested_collection_id_in(request, "SavedItemFolderId");
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
                    if let Some(public_folder_id) =
                        requested_public_folder_ids(request).into_iter().next()
                    {
                        let item = self
                            .store
                            .upsert_public_folder_item(
                                UpsertPublicFolderItemInput {
                                    id: None,
                                    account_id: principal.account_id,
                                    public_folder_id,
                                    item_kind: "post".to_string(),
                                    message_class: "IPM.Post".to_string(),
                                    subject: input.subject,
                                    body_text: input.body_text,
                                    body_html_sanitized: input.body_html_sanitized,
                                    source_payload_json: "{}".to_string(),
                                },
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "ews-create-public-folder-item".to_string(),
                                    subject: subject_for_audit,
                                },
                            )
                            .await?;
                        return Ok(create_public_folder_item_success_response(&item));
                    }
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
            operation_error_response(
                "CreateItem",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )
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
            let public_folder_item_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("public-folder-item:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty()
                || contact_ids.len()
                    + event_ids.len()
                    + task_ids.len()
                    + message_ids.len()
                    + public_folder_item_ids.len()
                    != ids.len()
            {
                return Ok(operation_error_response(
                    "UpdateItem",
                    "ErrorInvalidOperation",
                    "UpdateItem currently supports only contact, calendar, task, public folder item, and read/flag message item ids.",
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
            let public_folder_items = self
                .store
                .fetch_public_folder_items_by_ids(principal.account_id, &public_folder_item_ids)
                .await?;
            if public_folder_items.len() != public_folder_item_ids.len() {
                return Ok(operation_error_response(
                    "UpdateItem",
                    "ErrorItemNotFound",
                    "public folder item not found",
                ));
            }
            for existing in public_folder_items {
                let updated = self
                    .store
                    .upsert_public_folder_item(
                        parse_update_public_folder_item_input(principal, &existing, request),
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-update-public-folder-item".to_string(),
                            subject: existing.id.to_string(),
                        },
                    )
                    .await?;
                items.push_str(&public_folder_item_xml(&updated));
            }

            Ok(update_item_success_response(items))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "UpdateItem",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )
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
            let public_folder_item_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("public-folder-item:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty()
                || contact_ids.len()
                    + event_ids.len()
                    + task_ids.len()
                    + message_ids.len()
                    + public_folder_item_ids.len()
                    != ids.len()
            {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorInvalidOperation",
                    "DeleteItem currently supports only contact, calendar, task, message, and public folder item ids.",
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
                .map(EwsDeleteType::parse)
                .transpose()?
                .unwrap_or(EwsDeleteType::MoveToDeletedItems);
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
                if delete_type == EwsDeleteType::HardDelete || email.mailbox_role == "trash" {
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
            let public_folder_items = self
                .store
                .fetch_public_folder_items_by_ids(principal.account_id, &public_folder_item_ids)
                .await?;
            if public_folder_items.len() != public_folder_item_ids.len() {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorItemNotFound",
                    "public folder item not found",
                ));
            }
            for item in public_folder_items {
                self.store
                    .delete_public_folder_item(
                        principal.account_id,
                        item.public_folder_id,
                        item.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-delete-public-folder-item".to_string(),
                            subject: item.id.to_string(),
                        },
                    )
                    .await?;
            }

            Ok(delete_item_success_response())
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "DeleteItem",
                ews_error_code_or(&error, "ErrorItemNotFound"),
                &error.to_string(),
            )
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
            let public_folder_item_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("public-folder-item:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty()
                || message_ids.len() + public_folder_item_ids.len() != ids.len()
                || (!message_ids.is_empty() && !public_folder_item_ids.is_empty())
            {
                return Ok(operation_error_response(
                    "MoveItem",
                    "ErrorInvalidOperation",
                    "MoveItem currently supports only canonical message ids or public folder item ids.",
                ));
            }
            if !public_folder_item_ids.is_empty() {
                let target_public_folder_ids = requested_public_folder_ids(request);
                if target_public_folder_ids.len() != 1 {
                    return Ok(operation_error_response(
                        "MoveItem",
                        "ErrorInvalidOperation",
                        "MoveItem requires exactly one canonical public-folder target for public folder items.",
                    ));
                }
                let target_public_folder_id = target_public_folder_ids[0];
                let existing_items = self
                    .store
                    .fetch_public_folder_items_by_ids(principal.account_id, &public_folder_item_ids)
                    .await?;
                if existing_items.len() != public_folder_item_ids.len() {
                    return Ok(operation_error_response(
                        "MoveItem",
                        "ErrorItemNotFound",
                        "public folder item not found",
                    ));
                }
                let mut items = String::new();
                for existing in existing_items {
                    let moved = self
                        .store
                        .upsert_public_folder_item(
                            public_folder_item_clone_input(
                                principal,
                                &existing,
                                target_public_folder_id,
                            ),
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-move-public-folder-item-copy".to_string(),
                                subject: format!("{}->{target_public_folder_id}", existing.id),
                            },
                        )
                        .await?;
                    self.store
                        .delete_public_folder_item(
                            principal.account_id,
                            existing.public_folder_id,
                            existing.id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-move-public-folder-item-delete".to_string(),
                                subject: existing.id.to_string(),
                            },
                        )
                        .await?;
                    items.push_str(&public_folder_item_xml(&moved));
                }
                return Ok(move_item_success_response(items));
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
            operation_error_response(
                "MoveItem",
                ews_error_code_or(&error, "ErrorItemNotFound"),
                &error.to_string(),
            )
        }))
    }

    async fn archive_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let ids = requested_item_ids(request);
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if ids.is_empty() || message_ids.len() != ids.len() {
                return Ok(operation_error_response(
                    "ArchiveItem",
                    "ErrorInvalidOperation",
                    "ArchiveItem currently supports only canonical message item ids.",
                ));
            }

            let mailboxes = self
                .store
                .ensure_jmap_system_mailboxes(principal.account_id)
                .await?;
            let Some(archive_mailbox_id) = mailboxes
                .iter()
                .find(|mailbox| mailbox.role == "archive")
                .map(|mailbox| mailbox.id)
            else {
                return Ok(operation_error_response(
                    "ArchiveItem",
                    "ErrorFolderNotFound",
                    "The canonical Archive mailbox was not found.",
                ));
            };

            let existing = self
                .store
                .fetch_jmap_emails(principal.account_id, &message_ids)
                .await?;
            if existing.len() != message_ids.len() {
                return Ok(operation_error_response(
                    "ArchiveItem",
                    "ErrorItemNotFound",
                    "message not found",
                ));
            }

            let mut items = String::new();
            for message_id in message_ids {
                let moved = self
                    .store
                    .move_jmap_email(
                        principal.account_id,
                        message_id,
                        archive_mailbox_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-archive-message".to_string(),
                            subject: format!("{message_id}->{archive_mailbox_id}"),
                        },
                    )
                    .await?;
                items.push_str(&message_item_xml(&moved));
            }

            Ok(archive_item_success_response(items))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "ArchiveItem",
                ews_error_code_or(&error, "ErrorItemNotFound"),
                &error.to_string(),
            )
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
            let public_folder_item_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("public-folder-item:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty()
                || message_ids.len() + public_folder_item_ids.len() != ids.len()
                || (!message_ids.is_empty() && !public_folder_item_ids.is_empty())
            {
                return Ok(operation_error_response(
                    "CopyItem",
                    "ErrorInvalidOperation",
                    "CopyItem currently supports only canonical message ids or public folder item ids.",
                ));
            }
            if !public_folder_item_ids.is_empty() {
                let target_public_folder_ids = requested_public_folder_ids(request);
                if target_public_folder_ids.len() != 1 {
                    return Ok(operation_error_response(
                        "CopyItem",
                        "ErrorInvalidOperation",
                        "CopyItem requires exactly one canonical public-folder target for public folder items.",
                    ));
                }
                let target_public_folder_id = target_public_folder_ids[0];
                let existing_items = self
                    .store
                    .fetch_public_folder_items_by_ids(principal.account_id, &public_folder_item_ids)
                    .await?;
                if existing_items.len() != public_folder_item_ids.len() {
                    return Ok(operation_error_response(
                        "CopyItem",
                        "ErrorItemNotFound",
                        "public folder item not found",
                    ));
                }
                let mut items = String::new();
                for existing in existing_items {
                    let copied = self
                        .store
                        .upsert_public_folder_item(
                            public_folder_item_clone_input(
                                principal,
                                &existing,
                                target_public_folder_id,
                            ),
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-copy-public-folder-item".to_string(),
                                subject: format!("{}->{target_public_folder_id}", existing.id),
                            },
                        )
                        .await?;
                    items.push_str(&public_folder_item_xml(&copied));
                }
                return Ok(copy_item_success_response(items));
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
            operation_error_response(
                "CopyItem",
                ews_error_code_or(&error, "ErrorItemNotFound"),
                &error.to_string(),
            )
        }))
    }

    async fn mark_all_items_as_read(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            if !requested_public_folder_ids(request).is_empty() {
                bail!("MarkAllItemsAsRead currently supports canonical mailbox folders only.");
            }
            let folder_ids = self
                .requested_mailbox_folder_ids(principal, request)
                .await?;
            if folder_ids.is_empty() {
                bail!("MarkAllItemsAsRead requires a mailbox folder id.");
            }
            let read_flag = element_text(request, "ReadFlag")
                .map(|value| !value.eq_ignore_ascii_case("false"))
                .unwrap_or(true);
            for folder_id in folder_ids {
                let message_ids = self
                    .store
                    .query_jmap_email_ids(principal.account_id, Some(folder_id), None, 0, 10_000)
                    .await?
                    .ids;
                for message_id in message_ids {
                    self.store
                        .update_jmap_email_flags(
                            principal.account_id,
                            message_id,
                            Some(!read_flag),
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-mark-all-items-as-read".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                }
            }
            Ok(simple_operation_success_response("MarkAllItemsAsRead"))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "MarkAllItemsAsRead",
                "ErrorInvalidOperation",
                &error.to_string(),
            )
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
            if let Some(parent_folder_id) = requested_public_folder_ids(request).into_iter().next()
            {
                let folder = self
                    .store
                    .create_public_folder_child(
                        CreatePublicFolderInput {
                            account_id: principal.account_id,
                            parent_folder_id,
                            display_name: display_name.clone(),
                            folder_class: element_text(request, "FolderClass")
                                .unwrap_or_else(|| "IPF.Note".to_string()),
                            sort_order: 0,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-create-public-folder".to_string(),
                            subject: parent_folder_id.to_string(),
                        },
                    )
                    .await?;
                return Ok(create_public_folder_success_response(&folder));
            }
            let mailbox = self
                .store
                .create_jmap_mailbox(
                    JmapMailboxCreateInput {
                        account_id: principal.account_id,
                        name: display_name.clone(),
                        parent_id: None,
                        sort_order: None,
                        is_subscribed: true,
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

    async fn create_managed_folder(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let folder_names = element_contents(request, "FolderName")
                .into_iter()
                .map(xml_text)
                .filter(|name| !name.is_empty())
                .collect::<Vec<_>>();
            if folder_names.is_empty() {
                bail!("CreateManagedFolder requires at least one FolderName.");
            }

            let mut folders = String::new();
            for folder_name in folder_names {
                let mailbox = self
                    .store
                    .create_managed_retention_folder(
                        ManagedRetentionFolderCreateInput {
                            account_id: principal.account_id,
                            folder_name: folder_name.clone(),
                            is_subscribed: true,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-create-managed-folder".to_string(),
                            subject: folder_name,
                        },
                    )
                    .await?;
                folders.push_str(&mailbox_folder_xml(&mailbox));
            }

            Ok(folders_operation_success_response(
                "CreateManagedFolder",
                folders,
            ))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "CreateManagedFolder",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )
        }))
    }

    async fn create_folder_path(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let segments = requested_folder_path_segments(request);
            if segments.is_empty() {
                bail!("CreateFolderPath requires at least one folder DisplayName.");
            }
            if let Some(parent_folder_id) =
                requested_public_folder_ids_in(request, "ParentFolderId")
                    .into_iter()
                    .next()
            {
                let mut created = Vec::new();
                let mut parent_id = parent_folder_id;
                for segment in segments {
                    let existing = self
                        .store
                        .fetch_public_folder_children(principal.account_id, parent_id)
                        .await?
                        .into_iter()
                        .find(|folder| folder.display_name.eq_ignore_ascii_case(&segment));
                    let folder = match existing {
                        Some(folder) => folder,
                        None => {
                            self.store
                                .create_public_folder_child(
                                    CreatePublicFolderInput {
                                        account_id: principal.account_id,
                                        parent_folder_id: parent_id,
                                        display_name: segment.clone(),
                                        folder_class: "IPF.Note".to_string(),
                                        sort_order: 0,
                                    },
                                    AuditEntryInput {
                                        actor: principal.email.clone(),
                                        action: "ews-create-public-folder-path".to_string(),
                                        subject: segment,
                                    },
                                )
                                .await?
                        }
                    };
                    parent_id = folder.id;
                    created.push(public_folder_xml(&folder, folder.parent_folder_id, 0, 0));
                }
                return Ok(folders_operation_success_response(
                    "CreateFolderPath",
                    created.join(""),
                ));
            }

            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            let mut parent_id = requested_mailbox_folder_ids_in(request, "ParentFolderId")
                .into_iter()
                .next()
                .or_else(|| {
                    requested_mailbox_role_in(request, "ParentFolderId").and_then(|role| {
                        mailboxes
                            .iter()
                            .find(|mailbox| mailbox.role == role)
                            .map(|mailbox| mailbox.id)
                    })
                });
            let mut current = mailboxes;
            let mut created = Vec::new();
            for segment in segments {
                let existing = current
                    .iter()
                    .find(|mailbox| {
                        mailbox.parent_id == parent_id
                            && mailbox.name.eq_ignore_ascii_case(&segment)
                    })
                    .cloned();
                let mailbox = match existing {
                    Some(mailbox) => mailbox,
                    None => {
                        self.store
                            .create_jmap_mailbox(
                                JmapMailboxCreateInput {
                                    account_id: principal.account_id,
                                    name: segment.clone(),
                                    parent_id,
                                    sort_order: None,
                                    is_subscribed: true,
                                },
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "ews-create-folder-path".to_string(),
                                    subject: segment,
                                },
                            )
                            .await?
                    }
                };
                parent_id = Some(mailbox.id);
                created.push(mailbox_folder_xml(&mailbox));
                current = self
                    .store
                    .fetch_jmap_mailboxes(principal.account_id)
                    .await?;
            }

            Ok(folders_operation_success_response(
                "CreateFolderPath",
                created.join(""),
            ))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "CreateFolderPath",
                "ErrorInvalidOperation",
                &error.to_string(),
            )
        }))
    }

    async fn copy_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            if !requested_public_folder_ids(request).is_empty() {
                let target_parent_id = requested_public_folder_ids_in(request, "ToFolderId")
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("CopyFolder requires a public folder target."))?;
                let mut copied = Vec::new();
                for source_id in requested_public_folder_ids_in(request, "FolderIds") {
                    let folder = self
                        .copy_public_folder_tree(principal, source_id, target_parent_id)
                        .await?;
                    copied.push(public_folder_xml(&folder, folder.parent_folder_id, 0, 0));
                }
                return Ok(folders_operation_success_response(
                    "CopyFolder",
                    copied.join(""),
                ));
            }

            let target_parent_id = requested_mailbox_folder_ids_in(request, "ToFolderId")
                .into_iter()
                .next();
            let mailbox_ids = requested_mailbox_folder_ids_in(request, "FolderIds");
            if mailbox_ids.is_empty() {
                bail!("CopyFolder requires at least one mailbox FolderId.");
            }
            let mut copied = Vec::new();
            for source_id in mailbox_ids {
                let mailbox = self
                    .copy_mailbox_folder_tree(principal, source_id, target_parent_id)
                    .await?;
                copied.push(mailbox_folder_xml(&mailbox));
            }
            Ok(folders_operation_success_response(
                "CopyFolder",
                copied.join(""),
            ))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("CopyFolder", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    async fn empty_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let delete_subfolders = request.contains("DeleteSubFolders=\"true\"")
                || request.contains("DeleteSubFolders=\"1\"");
            let public_folder_ids = requested_public_folder_ids_in(request, "FolderIds");
            if !public_folder_ids.is_empty() {
                for folder_id in public_folder_ids {
                    self.empty_public_folder(principal, folder_id, delete_subfolders)
                        .await?;
                }
                return Ok(simple_operation_success_response("EmptyFolder"));
            }

            let mailbox_ids = requested_mailbox_folder_ids_in(request, "FolderIds");
            if mailbox_ids.is_empty() {
                bail!("EmptyFolder requires at least one mailbox or public folder id.");
            }
            for mailbox_id in mailbox_ids {
                self.empty_mailbox_folder(principal, mailbox_id, delete_subfolders)
                    .await?;
            }
            Ok(simple_operation_success_response("EmptyFolder"))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("EmptyFolder", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    async fn move_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            if !requested_public_folder_ids(request).is_empty() {
                bail!("MoveFolder for public folders is unsupported until canonical public-folder reparenting exists.");
            }
            let target_parent_id = requested_mailbox_folder_ids_in(request, "ToFolderId")
                .into_iter()
                .next();
            let mailbox_ids = requested_mailbox_folder_ids_in(request, "FolderIds");
            if mailbox_ids.is_empty() {
                bail!("MoveFolder requires at least one mailbox FolderId.");
            }
            let mailboxes = self.store.fetch_jmap_mailboxes(principal.account_id).await?;
            let mut moved = Vec::new();
            for mailbox_id in mailbox_ids {
                let mailbox = mailbox_by_id(&mailboxes, mailbox_id)?;
                ensure_custom_mailbox(mailbox)?;
                let updated = self
                    .store
                    .update_jmap_mailbox(
                        JmapMailboxUpdateInput {
                            account_id: principal.account_id,
                            mailbox_id,
                            name: None,
                            parent_id: Some(target_parent_id),
                            sort_order: None,
                            is_subscribed: None,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-move-folder".to_string(),
                            subject: mailbox_id.to_string(),
                        },
                    )
                    .await?;
                moved.push(mailbox_folder_xml(&updated));
            }
            Ok(folders_operation_success_response("MoveFolder", moved.join("")))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("MoveFolder", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    async fn update_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let display_name = element_text(request, "DisplayName")
                .ok_or_else(|| anyhow!("UpdateFolder currently requires DisplayName."))?;
            if let Some(folder_id) = requested_public_folder_ids(request).into_iter().next() {
                let folder = self
                    .store
                    .update_public_folder(
                        UpdatePublicFolderInput {
                            account_id: principal.account_id,
                            folder_id,
                            parent_folder_id: None,
                            display_name: Some(display_name),
                            folder_class: None,
                            sort_order: None,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-update-public-folder".to_string(),
                            subject: folder_id.to_string(),
                        },
                    )
                    .await?;
                return Ok(folders_operation_success_response(
                    "UpdateFolder",
                    public_folder_xml(&folder, folder.parent_folder_id, 0, 0),
                ));
            }

            let folder_id = requested_mailbox_folder_ids(request)
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("UpdateFolder requires a mailbox FolderId."))?;
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            ensure_custom_mailbox(mailbox_by_id(&mailboxes, folder_id)?)?;
            let mailbox = self
                .store
                .update_jmap_mailbox(
                    JmapMailboxUpdateInput {
                        account_id: principal.account_id,
                        mailbox_id: folder_id,
                        name: Some(display_name),
                        parent_id: None,
                        sort_order: None,
                        is_subscribed: None,
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-update-folder".to_string(),
                        subject: folder_id.to_string(),
                    },
                )
                .await?;
            Ok(folders_operation_success_response(
                "UpdateFolder",
                mailbox_folder_xml(&mailbox),
            ))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("UpdateFolder", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    async fn copy_mailbox_folder_tree(
        &self,
        principal: &AccountPrincipal,
        source_id: Uuid,
        target_parent_id: Option<Uuid>,
    ) -> Result<JmapMailbox> {
        let mailboxes = self
            .store
            .fetch_jmap_mailboxes(principal.account_id)
            .await?;
        ensure_custom_mailbox(mailbox_by_id(&mailboxes, source_id)?)?;
        let mut stack = vec![(source_id, target_parent_id)];
        let mut root = None;
        while let Some((current_id, parent_id)) = stack.pop() {
            let current = mailbox_by_id(&mailboxes, current_id)?.clone();
            ensure_custom_mailbox(&current)?;
            let copied = self
                .store
                .create_jmap_mailbox(
                    JmapMailboxCreateInput {
                        account_id: principal.account_id,
                        name: current.name.clone(),
                        parent_id,
                        sort_order: Some(current.sort_order),
                        is_subscribed: current.is_subscribed,
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-copy-folder".to_string(),
                        subject: current_id.to_string(),
                    },
                )
                .await?;
            if current_id == source_id {
                root = Some(copied.clone());
            }
            let message_ids = self
                .store
                .query_jmap_email_ids(principal.account_id, Some(current_id), None, 0, 10_000)
                .await?
                .ids;
            for message_id in message_ids {
                self.store
                    .copy_jmap_email(
                        principal.account_id,
                        message_id,
                        copied.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-copy-folder-message".to_string(),
                            subject: message_id.to_string(),
                        },
                    )
                    .await?;
            }
            for child in mailboxes
                .iter()
                .filter(|mailbox| mailbox.parent_id == Some(current_id))
            {
                stack.push((child.id, Some(copied.id)));
            }
        }
        root.ok_or_else(|| anyhow!("mailbox folder not found"))
    }

    async fn empty_mailbox_folder(
        &self,
        principal: &AccountPrincipal,
        folder_id: Uuid,
        delete_subfolders: bool,
    ) -> Result<()> {
        let mailboxes = self
            .store
            .fetch_jmap_mailboxes(principal.account_id)
            .await?;
        mailbox_by_id(&mailboxes, folder_id)?;
        let mut folder_ids = vec![folder_id];
        if delete_subfolders {
            let mut index = 0;
            while index < folder_ids.len() {
                let current_id = folder_ids[index];
                for child in mailboxes
                    .iter()
                    .filter(|mailbox| mailbox.parent_id == Some(current_id))
                {
                    ensure_custom_mailbox(child)?;
                    folder_ids.push(child.id);
                }
                index += 1;
            }
        }
        for current_id in &folder_ids {
            let message_ids = self
                .store
                .query_jmap_email_ids(principal.account_id, Some(*current_id), None, 0, 10_000)
                .await?
                .ids;
            for message_id in message_ids {
                self.store
                    .delete_jmap_email_from_mailbox(
                        principal.account_id,
                        *current_id,
                        message_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-empty-folder-message".to_string(),
                            subject: message_id.to_string(),
                        },
                    )
                    .await?;
            }
        }
        if delete_subfolders {
            for child_id in folder_ids.into_iter().skip(1).rev() {
                self.store
                    .destroy_jmap_mailbox(
                        principal.account_id,
                        child_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-empty-folder-delete-subfolder".to_string(),
                            subject: child_id.to_string(),
                        },
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn copy_public_folder_tree(
        &self,
        principal: &AccountPrincipal,
        source_id: Uuid,
        target_parent_id: Uuid,
    ) -> Result<PublicFolder> {
        let mut stack = vec![(source_id, target_parent_id)];
        let mut root = None;
        while let Some((current_id, parent_id)) = stack.pop() {
            let current = self
                .store
                .fetch_public_folder(principal.account_id, current_id)
                .await?;
            let copied = self
                .store
                .create_public_folder_child(
                    CreatePublicFolderInput {
                        account_id: principal.account_id,
                        parent_folder_id: parent_id,
                        display_name: current.display_name.clone(),
                        folder_class: current.folder_class.clone(),
                        sort_order: current.sort_order,
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-copy-public-folder".to_string(),
                        subject: current_id.to_string(),
                    },
                )
                .await?;
            if current_id == source_id {
                root = Some(copied.clone());
            }
            let items = self
                .store
                .fetch_public_folder_items(principal.account_id, current_id)
                .await?;
            for item in items {
                self.store
                    .upsert_public_folder_item(
                        UpsertPublicFolderItemInput {
                            id: None,
                            account_id: principal.account_id,
                            public_folder_id: copied.id,
                            item_kind: item.item_kind,
                            message_class: item.message_class,
                            subject: item.subject,
                            body_text: item.body_text,
                            body_html_sanitized: item.body_html_sanitized,
                            source_payload_json: item.source_payload_json,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-copy-public-folder-item".to_string(),
                            subject: item.id.to_string(),
                        },
                    )
                    .await?;
            }
            for child in self
                .store
                .fetch_public_folder_children(principal.account_id, current_id)
                .await?
            {
                stack.push((child.id, copied.id));
            }
        }
        root.ok_or_else(|| anyhow!("public folder not found"))
    }

    async fn empty_public_folder(
        &self,
        principal: &AccountPrincipal,
        folder_id: Uuid,
        delete_subfolders: bool,
    ) -> Result<()> {
        let mut folder_ids = vec![folder_id];
        if delete_subfolders {
            let mut index = 0;
            while index < folder_ids.len() {
                let current_id = folder_ids[index];
                for child in self
                    .store
                    .fetch_public_folder_children(principal.account_id, current_id)
                    .await?
                {
                    folder_ids.push(child.id);
                }
                index += 1;
            }
        }
        for current_id in &folder_ids {
            let items = self
                .store
                .fetch_public_folder_items(principal.account_id, *current_id)
                .await?;
            for item in items {
                self.store
                    .delete_public_folder_item(
                        principal.account_id,
                        *current_id,
                        item.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-empty-public-folder-item".to_string(),
                            subject: item.id.to_string(),
                        },
                    )
                    .await?;
            }
        }
        if delete_subfolders {
            for child_id in folder_ids.into_iter().skip(1).rev() {
                self.store
                    .delete_public_folder(
                        principal.account_id,
                        child_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-empty-public-folder-delete-subfolder".to_string(),
                            subject: child_id.to_string(),
                        },
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn delete_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let public_folder_ids = requested_public_folder_ids(request);
            if !public_folder_ids.is_empty() {
                for folder_id in public_folder_ids {
                    self.store
                        .delete_public_folder(
                            principal.account_id,
                            folder_id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-delete-public-folder".to_string(),
                                subject: folder_id.to_string(),
                            },
                        )
                        .await?;
                }
                return Ok(delete_folder_success_response());
            }
            let folder_ids = requested_mailbox_folder_ids(request);
            if folder_ids.is_empty() {
                return Ok(operation_error_response(
                    "DeleteFolder",
                    "ErrorInvalidOperation",
                    "DeleteFolder currently supports only mailbox or public folder ids.",
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

    async fn send_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let draft_ids = requested_item_ids(request)
                .into_iter()
                .filter_map(|id| canonical_message_id_from_ews_id(&id))
                .collect::<Vec<_>>();
            if draft_ids.is_empty() {
                bail!("SendItem requires at least one message ItemId.");
            }
            for draft_id in draft_ids {
                self.store
                    .submit_draft_message(
                        principal.account_id,
                        draft_id,
                        principal.account_id,
                        "ews-senditem",
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-senditem".to_string(),
                            subject: draft_id.to_string(),
                        },
                    )
                    .await?;
            }
            Ok(simple_operation_success_response("SendItem"))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("SendItem", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    async fn get_inbox_rules(&self, principal: &AccountPrincipal) -> Result<String> {
        let rules = self.store.list_mailbox_rules(principal.account_id).await?;
        Ok(get_inbox_rules_response(&rules))
    }

    async fn update_inbox_rules(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let mut mutations = Vec::new();
            for operation in element_contents(request, "DeleteRuleOperation") {
                let rule_id = element_text(operation, "RuleId")
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| anyhow!("DeleteRuleOperation requires RuleId."))?;
                mutations.push(EwsInboxRuleMutation::Delete { rule_id });
            }
            for operation in element_contents(request, "CreateRuleOperation") {
                let rule = element_content(operation, "Rule").unwrap_or(operation);
                let (name, active, sieve) = bounded_ews_rule_to_sieve(rule)?;
                mutations.push(EwsInboxRuleMutation::Put {
                    name,
                    active,
                    sieve,
                    audit_action: "ews-update-inbox-rules-create",
                });
            }
            for operation in element_contents(request, "SetRuleOperation") {
                let rule = element_content(operation, "Rule").unwrap_or(operation);
                let (name, active, sieve) = bounded_ews_rule_to_sieve(rule)?;
                mutations.push(EwsInboxRuleMutation::Put {
                    name,
                    active,
                    sieve,
                    audit_action: "ews-update-inbox-rules-set",
                });
            }
            if mutations.is_empty() && !request.contains("RemoveOutlookRuleBlob") {
                bail!("UpdateInboxRules supports bounded create, set, and delete rule operations.");
            }

            for mutation in mutations {
                match mutation {
                    EwsInboxRuleMutation::Delete { rule_id } => {
                        self.store
                            .delete_sieve_script(
                                principal.account_id,
                                &rule_id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "ews-update-inbox-rules-delete".to_string(),
                                    subject: rule_id.clone(),
                                },
                            )
                            .await?;
                    }
                    EwsInboxRuleMutation::Put {
                        name,
                        active,
                        sieve,
                        audit_action,
                    } => {
                        self.store
                            .put_sieve_script(
                                principal.account_id,
                                &name,
                                &sieve,
                                active,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: audit_action.to_string(),
                                    subject: name.clone(),
                                },
                            )
                            .await?;
                    }
                }
            }
            Ok(simple_operation_success_response("UpdateInboxRules"))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "UpdateInboxRules",
                "ErrorInvalidOperation",
                &error.to_string(),
            )
        }))
    }

    async fn get_reminders(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let include_inactive = element_text(request, "IncludeDismissedReminders")
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let reminders = self
            .store
            .query_client_reminders(principal.account_id, ReminderQuery { include_inactive })
            .await?;
        Ok(get_reminders_response(&reminders))
    }

    async fn perform_reminder_action(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let action = element_text(request, "ActionType")
                .or_else(|| element_text(request, "ReminderItemActionType"))
                .or_else(|| element_text(request, "ReminderAction"))
                .unwrap_or_default();
            let action = if action.is_empty() {
                "Dismiss".to_string()
            } else {
                action
            };
            if !action.eq_ignore_ascii_case("Dismiss") && !action.eq_ignore_ascii_case("Snooze") {
                bail!("PerformReminderAction currently supports only Dismiss and Snooze.");
            }
            let snooze_until = if action.eq_ignore_ascii_case("Snooze") {
                Some(
                    element_text(request, "NewReminderTime")
                        .or_else(|| element_text(request, "SnoozeUntil"))
                        .or_else(|| element_text(request, "ReminderTime"))
                        .filter(|value| !value.trim().is_empty())
                        .ok_or_else(|| {
                            anyhow!("PerformReminderAction Snooze requires a new reminder time.")
                        })?,
                )
            } else {
                None
            };
            let reminder_ids = requested_item_ids(request);
            if reminder_ids.is_empty() {
                bail!("PerformReminderAction requires reminder ItemId values.");
            }
            for reminder_id in reminder_ids {
                let parsed = parse_reminder_item_id(&reminder_id)
                    .ok_or_else(|| anyhow!("unsupported reminder ItemId `{reminder_id}`"))?;
                match parsed.source_type.as_str() {
                    "mail" | "message" => {
                        self.store
                            .update_jmap_email_followup_flags(
                                principal.account_id,
                                parsed.source_id,
                                JmapEmailFollowupUpdate {
                                    reminder_dismissed_at: if snooze_until.is_none() {
                                        Some("now".to_string())
                                    } else {
                                        None
                                    },
                                    reminder_at: snooze_until.clone(),
                                    reminder_set: snooze_until.as_ref().map(|_| true),
                                    ..JmapEmailFollowupUpdate::default()
                                },
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "ews-perform-reminder-action".to_string(),
                                    subject: parsed.source_id.to_string(),
                                },
                            )
                            .await?;
                    }
                    "calendar" => {
                        if let Some(reminder_at) = snooze_until.clone() {
                            self.store
                                .update_accessible_event_reminder(
                                    principal.account_id,
                                    parsed.source_id,
                                    Some(true),
                                    Some(reminder_at),
                                    None,
                                )
                                .await?;
                        } else {
                            self.store
                                .dismiss_reminder_occurrence(
                                    principal.account_id,
                                    &parsed.source_type,
                                    parsed.source_id,
                                    parsed.occurrence_start_at.as_deref(),
                                    "now",
                                )
                                .await?;
                        }
                    }
                    "task" => {
                        if let Some(reminder_at) = snooze_until.clone() {
                            self.store
                                .update_accessible_task_reminder(
                                    principal.account_id,
                                    parsed.source_id,
                                    Some(true),
                                    Some(reminder_at),
                                    None,
                                    Some(true),
                                )
                                .await?;
                        } else {
                            self.store
                                .dismiss_reminder_occurrence(
                                    principal.account_id,
                                    &parsed.source_type,
                                    parsed.source_id,
                                    parsed.occurrence_start_at.as_deref(),
                                    "now",
                                )
                                .await?;
                        }
                    }
                    _ => bail!("unsupported reminder source `{}`", parsed.source_type),
                }
            }
            Ok(simple_operation_success_response("PerformReminderAction"))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "PerformReminderAction",
                "ErrorInvalidOperation",
                &error.to_string(),
            )
        }))
    }

    async fn get_mail_tips(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let result = async {
            let recipients = requested_mail_tips_recipients(request);
            if recipients.is_empty() {
                bail!("GetMailTips requires at least one recipient Mailbox.");
            }
            if recipients.len() > EWS_MAX_MAIL_TIPS_RECIPIENTS {
                bail!(
                    "GetMailTips supports at most {} recipients per request.",
                    EWS_MAX_MAIL_TIPS_RECIPIENTS
                );
            }
            let requested_tips = requested_mail_tips(request);
            let entries = self.store.fetch_address_book_entries(principal).await?;
            let mut recipient_tips = Vec::new();
            for recipient in recipients {
                let entry = entries
                    .iter()
                    .find(|entry| entry.email.eq_ignore_ascii_case(&recipient));
                let oof = if requested_tips.contains("OutOfOfficeMessage") {
                    if let Some(entry) = entry
                        .filter(|entry| entry.entry_kind == ExchangeAddressBookEntryKind::Account)
                    {
                        self.store
                            .fetch_active_sieve_script(entry.id)
                            .await?
                            .and_then(|script| {
                                let projection =
                                    oof_projection_from_script(Some(script.content.as_str()));
                                (projection.state != EwsOofState::Disabled)
                                    .then_some(projection.text_body)
                            })
                    } else {
                        None
                    }
                } else {
                    None
                };
                recipient_tips.push(MailTipProjection {
                    recipient,
                    display_name: entry.map(|entry| entry.display_name.clone()),
                    recipient_type: entry.map(|entry| entry.entry_kind),
                    invalid_recipient: entry.is_none()
                        && requested_tips.contains("InvalidRecipient"),
                    out_of_office_message: oof,
                });
            }
            Ok(get_mail_tips_response(&recipient_tips))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("GetMailTips", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    async fn get_discovery_search_configuration(
        &self,
        principal: &AccountPrincipal,
    ) -> Result<String> {
        let searches = self
            .store
            .fetch_ews_discovery_search_configurations(principal)
            .await?;
        Ok(get_discovery_search_configuration_response(&searches))
    }

    async fn get_searchable_mailboxes(&self, principal: &AccountPrincipal) -> Result<String> {
        let mailboxes = self.store.fetch_ews_searchable_mailboxes(principal).await?;
        Ok(get_searchable_mailboxes_response(&mailboxes))
    }

    async fn search_mailboxes(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let query_text = discovery_query_text(request);
        let mailbox_emails = requested_mailbox_emails(request);
        let result = self
            .store
            .search_ews_mailboxes(principal, &query_text, &mailbox_emails, 100)
            .await?;
        Ok(search_mailboxes_response(&result))
    }

    async fn find_message_tracking_report(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let query_text = message_tracking_query_text(request);
        let reports = self
            .store
            .fetch_ews_message_tracking_reports(principal, &query_text, 100)
            .await?;
        Ok(find_message_tracking_report_response(&reports))
    }

    async fn get_message_tracking_report(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let report_id = requested_message_tracking_report_id(request);
        let Some(report_id) = report_id else {
            return Ok(operation_error_response(
                "GetMessageTrackingReport",
                "ErrorInvalidOperation",
                "MessageTrackingReportId is required.",
            ));
        };
        let detail = self
            .store
            .fetch_ews_message_tracking_report_detail(principal, &report_id)
            .await?;
        match detail {
            Some(detail) => Ok(get_message_tracking_report_response(&detail)),
            None => Ok(operation_error_response(
                "GetMessageTrackingReport",
                "ErrorItemNotFound",
                "The requested message tracking report was not found.",
            )),
        }
    }

    async fn get_hold_on_mailboxes(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let mailbox_emails = requested_mailbox_emails(request);
        let holds = self
            .store
            .fetch_ews_hold_mailboxes(principal, &mailbox_emails)
            .await?;
        Ok(get_hold_on_mailboxes_response(&holds))
    }

    async fn set_hold_on_mailboxes(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let mailbox_emails = requested_mailbox_emails(request);
        let hold_name = element_text(request, "HoldId")
            .or_else(|| element_text(request, "HoldName"))
            .unwrap_or_else(|| "EWS Litigation Hold".to_string());
        let query_text = discovery_query_text(request);
        let enable = !element_text(request, "Action")
            .unwrap_or_else(|| "CreateHold".to_string())
            .to_ascii_lowercase()
            .contains("release");
        let holds = self
            .store
            .set_ews_hold_mailboxes(
                principal,
                &hold_name,
                &query_text,
                &mailbox_emails,
                enable,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: if enable {
                        "ews-set-hold".to_string()
                    } else {
                        "ews-release-hold".to_string()
                    },
                    subject: hold_name.clone(),
                },
            )
            .await?;
        Ok(set_hold_on_mailboxes_response(&holds, enable))
    }

    async fn get_non_indexable_item_details(&self, principal: &AccountPrincipal) -> Result<String> {
        let reports = self
            .store
            .fetch_ews_non_indexable_reports(principal)
            .await?;
        Ok(get_non_indexable_item_details_response(&reports))
    }

    async fn get_non_indexable_item_statistics(
        &self,
        principal: &AccountPrincipal,
    ) -> Result<String> {
        let reports = self
            .store
            .fetch_ews_non_indexable_reports(principal)
            .await?;
        Ok(get_non_indexable_item_statistics_response(&reports))
    }

    async fn upload_items(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let item_ids = requested_transfer_item_ids(request);
        let job = self
            .store
            .create_ews_transfer_job(
                principal,
                "import",
                &item_ids,
                serde_json::json!({ "operation": "UploadItems", "itemCount": item_ids.len() }),
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-upload-items".to_string(),
                    subject: format!("{} items", item_ids.len()),
                },
            )
            .await?;
        Ok(transfer_job_response("UploadItems", &job))
    }

    async fn export_items(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let item_ids = requested_item_ids(request);
        let job = self
            .store
            .create_ews_transfer_job(
                principal,
                "export",
                &item_ids,
                serde_json::json!({ "operation": "ExportItems", "itemCount": item_ids.len() }),
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-export-items".to_string(),
                    subject: format!("{} items", item_ids.len()),
                },
            )
            .await?;
        Ok(transfer_job_response("ExportItems", &job))
    }

    async fn subscribe(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        if element_content(request, "PullSubscriptionRequest").is_none() {
            return Ok(operation_error_response(
                "Subscribe",
                "ErrorInvalidOperation",
                "Subscribe currently supports only EWS pull subscriptions.",
            ));
        }

        let subscription = self.register_pull_subscription(principal, request).await?;
        Ok(subscribe_success_response(&subscription.0, &subscription.1))
    }

    async fn get_events(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let subscription_id = element_text(request, "SubscriptionId")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| notification_subscription_id(principal.account_id, request));
        let previous_watermark = element_text(request, "Watermark")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| notification_watermark(&subscription_id, None, 0));

        self.durable_events_response(
            "GetEvents",
            principal,
            &subscription_id,
            &previous_watermark,
        )
        .await
    }

    async fn get_streaming_events(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let subscription_id = element_text(request, "SubscriptionId")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| notification_subscription_id(principal.account_id, request));
        let previous_watermark = element_text(request, "Watermark")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| notification_watermark(&subscription_id, None, 0));
        self.durable_events_response(
            "GetStreamingEvents",
            principal,
            &subscription_id,
            &previous_watermark,
        )
        .await
    }

    async fn durable_events_response(
        &self,
        operation: &str,
        principal: &AccountPrincipal,
        subscription_id: &str,
        previous_watermark: &str,
    ) -> Result<String> {
        let after_cursor = notification_watermark_sequence(previous_watermark).unwrap_or(0) as i64;
        let poll = self
            .store
            .poll_mapi_notifications(principal.account_id, after_cursor)
            .await?;
        let event_pending = poll.event_pending;
        let cursor = poll.cursor;
        if after_cursor > 0 && cursor.is_none() {
            return Ok(operation_error_response(
                operation,
                "ErrorInvalidWatermark",
                "The requested EWS notification watermark is no longer available in canonical change-log retention.",
            ));
        }
        let mut notifications = Vec::new();
        for event in poll.events {
            let Some(mailbox_id) = event.canonical_folder_id() else {
                continue;
            };
            let Some(item_id) = event.canonical_message_id() else {
                continue;
            };
            let sequence = event
                .change_cursor()
                .unwrap_or_else(|| cursor.unwrap_or(after_cursor))
                .max(0) as u64;
            let kind = match event.change_kind().unwrap_or_default() {
                "deleted" | "destroyed" | "removed" => EwsNotificationKind::Deleted,
                "created" | "inserted" | "new" => EwsNotificationKind::NewMail,
                _ => EwsNotificationKind::Created,
            };
            notifications.push(EwsQueuedNotification {
                sequence,
                kind,
                item_id,
                mailbox_id,
                change_key: sequence.to_string(),
                timestamp: "1970-01-01T00:00:00Z".to_string(),
            });
        }
        if !notifications.is_empty() {
            return Ok(match operation {
                "GetStreamingEvents" => get_streaming_events_queued_response(
                    subscription_id,
                    previous_watermark,
                    &notifications,
                    event_pending && notifications.len() >= 100,
                ),
                _ => get_events_queued_response(
                    subscription_id,
                    previous_watermark,
                    &notifications,
                    event_pending && notifications.len() >= 100,
                ),
            });
        }
        Ok(match operation {
            "GetStreamingEvents" => {
                get_streaming_events_status_response(subscription_id, previous_watermark)
            }
            _ => get_events_status_response(subscription_id, previous_watermark),
        })
    }

    async fn unsubscribe(&self, request: &str) -> Result<String> {
        let subscription_id = element_text(request, "SubscriptionId").unwrap_or_default();
        if subscription_id.trim().is_empty() {
            return Ok(operation_error_response(
                "Unsubscribe",
                "ErrorInvalidSubscription",
                "Unsubscribe requires a SubscriptionId.",
            ));
        }

        Ok(unsubscribe_success_response())
    }

    async fn register_pull_subscription(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<(String, String)> {
        let subscription_id = notification_subscription_id(principal.account_id, request);
        let folder_marker = self
            .notification_request_folder_marker(principal, request)
            .await?;
        let requested_watermark =
            element_text(request, "Watermark").filter(|value| !value.trim().is_empty());
        let current_cursor = self
            .store
            .fetch_mapi_notification_cursor(principal.account_id)
            .await?
            .unwrap_or(0)
            .max(0) as u64;
        let watermark = requested_watermark.clone().unwrap_or_else(|| {
            notification_watermark(&subscription_id, folder_marker.as_deref(), current_cursor)
        });
        Ok((subscription_id, watermark))
    }

    async fn notification_request_folder_marker(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<Option<String>> {
        if let Some(mailbox_id) = requested_mailbox_folder_ids(request).into_iter().next() {
            return Ok(Some(format!("mailbox:{mailbox_id}")));
        }
        if let Some(role) = requested_mailbox_role(request) {
            return Ok(self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?
                .into_iter()
                .find(|mailbox| mailbox.role == role)
                .map(|mailbox| format!("mailbox:{}", mailbox.id))
                .or_else(|| Some(format!("role:{role}"))));
        }
        if pull_subscription_subscribes_to_all_folders(request) {
            return Ok(Some("all".to_string()));
        }
        Ok(None)
    }
}
