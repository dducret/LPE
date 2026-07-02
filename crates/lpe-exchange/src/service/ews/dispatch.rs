use super::super::*;
use lpe_core::outlook_trace::{write_outlook_trace, OutlookTraceDirection, OutlookTraceEvent};

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle(&self, headers: &HeaderMap, body: &[u8]) -> Result<Response> {
        let principal = authenticate_account(&self.store, None, headers, "ews").await?;
        let body = decode_ews_body(headers, body)?;
        let operation = operation_name(&body).ok_or_else(|| anyhow!("unsupported EWS request"))?;
        trace_ews_event(
            headers,
            &principal,
            &operation,
            OutlookTraceDirection::Inbound,
            None,
            Some(body.as_bytes()),
        );

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
        trace_ews_event(
            headers,
            &principal,
            &operation,
            OutlookTraceDirection::Outbound,
            Some(&response_code),
            Some(payload.as_bytes()),
        );
        let mut response = soap_response(payload);
        response.extensions_mut().insert(EwsResponseDebug {
            response_code,
            detail,
        });
        Ok(response)
    }
}

fn trace_ews_event(
    headers: &HeaderMap,
    principal: &AccountPrincipal,
    operation: &str,
    direction: OutlookTraceDirection,
    response_code: Option<&str>,
    payload: Option<&[u8]>,
) {
    let session_key = mapi::safe_header(headers, "client-request-id")
        .or_else(|| mapi::safe_header(headers, "x-requestid"))
        .or_else(|| mapi::safe_header(headers, "x-trace-id"))
        .unwrap_or_else(|| format!("ews:{}:{operation}", principal.email));
    let remote_peer = mapi::safe_header(headers, "x-forwarded-for")
        .and_then(|value| value.split(',').next().map(|part| part.trim().to_string()))
        .filter(|value| !value.is_empty())
        .or_else(|| mapi::safe_header(headers, "x-real-ip"));
    let tenant_id = principal.tenant_id.to_string();
    let account_id = principal.account_id.to_string();
    let mut metadata = vec![
        ("account_id", account_id),
        ("operation", operation.to_string()),
        (
            "trace_id",
            mapi::safe_header(headers, "x-trace-id").unwrap_or_default(),
        ),
        (
            "client_request_id",
            mapi::safe_header(headers, "client-request-id").unwrap_or_default(),
        ),
        (
            "user_agent",
            mapi::safe_header(headers, "user-agent").unwrap_or_default(),
        ),
    ];
    if let Some(response_code) = response_code {
        metadata.push(("ews_response_code", response_code.to_string()));
    }

    write_outlook_trace(&OutlookTraceEvent {
        component: "ews",
        endpoint: "Exchange.asmx",
        session_key: &session_key,
        direction,
        phase: operation,
        remote_peer: remote_peer.as_deref(),
        tenant_id: Some(&tenant_id),
        account: Some(&principal.email),
        status: None,
        metadata,
        payload,
    });
}
