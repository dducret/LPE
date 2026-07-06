use super::*;

pub(super) async fn append_stream_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    request_id: &str,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::OpenStream) => {
            append_open_stream_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                request_id,
                mailboxes,
                emails,
                snapshot,
                responses,
                output_handles,
            )
            .await;
        }
        Some(RopId::ReadStream) => {
            append_read_stream_response(
                principal,
                session,
                handle_slots,
                request,
                request_id,
                responses,
            );
        }
        Some(RopId::SeekStream) => {
            append_seek_stream_response(principal, session, handle_slots, request, responses);
        }
        Some(RopId::SetStreamSize) => {
            append_set_stream_size_response(
                principal,
                session,
                handle_slots,
                request,
                request_id,
                responses,
            );
        }
        Some(RopId::WriteStream | RopId::WriteAndCommitStream | RopId::WriteStreamExtended) => {
            append_write_stream_response(
                principal,
                session,
                handle_slots,
                request,
                request_id,
                responses,
            );
        }
        Some(RopId::CopyToStream) => {
            append_copy_to_stream_response(session, handle_slots, request, responses);
        }
        Some(RopId::GetStreamSize) => {
            append_get_stream_size_response(session, handle_slots, request, responses);
        }
        Some(RopId::CloneStream) => {
            append_clone_stream_response(session, handle_slots, request, responses, output_handles);
        }
        Some(RopId::LockRegionStream | RopId::UnlockRegionStream) => {
            append_stream_region_response(session, handle_slots, request, responses);
        }
        _ => unreachable!("append_stream_response called for non-stream ROP"),
    }
}

pub(super) async fn append_get_properties_specific_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    created_emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    let is_inbox_folder_type_probe = matches!(
        input_object(session, handle_slots, request),
        Some(MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            ..
        })
    ) && request
        .property_tags()
        .iter()
        .any(|tag| canonical_property_storage_tag(*tag) == PID_TAG_FOLDER_TYPE);
    if is_inbox_folder_type_probe {
        let input_handle_value = input_handle(handle_slots, request);
        session.record_inbox_folder_type_getprops_probe();
        session.record_recent_probe_action(format!(
            "GetPropertiesSpecific(in={},handle={},tags={})",
            request.input_handle_index().unwrap_or(0),
            format_optional_debug_handle(input_handle_value),
            format_debug_property_tags(&request.property_tags())
        ));
        if let Some(summary) = format_inbox_open_loop_summary(&session.post_hierarchy_actions) {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x07",
                input_handle_index = request.input_handle_index().unwrap_or(0),
                input_handle_value = %format_optional_debug_handle(input_handle_value),
                folder_id = format!("0x{INBOX_FOLDER_ID:016x}"),
                loop_summary = %summary,
                "rca debug mapi repeated inbox open folder loop summary"
            );
        }
    }
    let object_owned = input_object(session, handle_slots, request).cloned();
    let object = object_owned.as_ref();
    let visible_emails;
    let emails_for_request = if created_emails.is_empty() {
        emails
    } else {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x07",
            object_kind = mapi_object_debug_kind(object),
            folder_id = %mapi_object_debug_folder_id(object),
            same_execute_created_email_count = created_emails.len(),
            base_snapshot_email_count = emails.len(),
            "rca debug mapi same execute created message visibility"
        );
        visible_emails = emails
            .iter()
            .chain(created_emails.iter())
            .cloned()
            .collect::<Vec<_>>();
        &visible_emails
    };
    let custom_values = fetch_custom_property_values_for_request(
        store,
        principal,
        object,
        mailboxes,
        emails_for_request,
        snapshot,
        &request.property_tags(),
    )
    .await
    .unwrap_or_default();
    let inbox_folder_type_getprops_context = if let (
        true,
        Some(MapiObject::Folder { properties, .. }),
    ) = (is_inbox_folder_type_probe, object)
    {
        Some(format!(
                "input_index={};input_handle={};requested_tags={};folder_type={};display_name={};container_class={};content_count={};unread_count={};associated_count={}",
                request.input_handle_index().unwrap_or(0),
                format_optional_debug_handle(input_handle(handle_slots, request)),
                format_debug_property_tags(&request.property_tags()),
                mapi_value_debug_u32(properties, PID_TAG_FOLDER_TYPE),
                mapi_value_debug_string(properties, PID_TAG_DISPLAY_NAME_W),
                mapi_value_debug_string(properties, PID_TAG_CONTAINER_CLASS_W),
                mapi_value_debug_u32(properties, PID_TAG_CONTENT_COUNT),
                mapi_value_debug_u32(properties, PID_TAG_CONTENT_UNREAD_COUNT),
                mapi_value_debug_u32(properties, PID_TAG_ASSOCIATED_CONTENT_COUNT)
            ))
    } else {
        None
    };
    let named_property_context =
        format_debug_named_property_context(session, &request.property_tags());
    let inbox_config_getprops_trace = if let Some(MapiObject::AssociatedConfig {
        folder_id: INBOX_FOLDER_ID,
        config_id,
        saved_message,
    }) = object
    {
        let (message_class, subject) = saved_message
            .as_ref()
            .map(|message| (message.message_class.as_str(), message.subject.as_str()))
            .unwrap_or(("missing_saved_message", ""));
        Some(format!(
            "getprops_inbox_config:request_id={request_id};handle={};config=0x{config_id:016x};class={message_class};subject={subject};tags={};named_properties={}",
            format_optional_debug_handle(input_handle(handle_slots, request)),
            format_debug_property_tags(&request.property_tags()),
            named_property_context
        ))
    } else {
        None
    };
    if !named_property_context.is_empty() {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x07",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            response_handle_index = request.response_handle_index(),
            object_kind = mapi_object_debug_kind(object),
            folder_id = %mapi_object_debug_folder_id(object),
            requested_property_tags = %format_debug_property_tags(&request.property_tags()),
            named_property_context = %named_property_context,
            "rca debug mapi get properties named property context"
        );
    }
    let property_response = rop_get_properties_specific_response_with_custom(
        request,
        object,
        principal,
        mailboxes,
        emails_for_request,
        snapshot,
        &custom_values,
    );
    log_message_getprops_response_debug(
        principal,
        session,
        request_id,
        request,
        object,
        mailboxes,
        emails_for_request,
        snapshot,
        &property_response,
    );
    log_get_properties_specific_response_debug(
        principal,
        session,
        request_id,
        request,
        object,
        &property_response,
    );
    log_get_properties_view_response_debug(
        principal,
        request_id,
        request,
        object,
        &property_response,
    );
    log_get_properties_default_folder_response_debug(
        principal,
        request_id,
        request,
        object,
        mailboxes,
        emails_for_request,
        snapshot,
        &property_response,
    );
    if request
        .property_tags()
        .iter()
        .any(|tag| property_ids_match(*tag, PID_TAG_DEFAULT_VIEW_ENTRY_ID))
    {
        if let Some(MapiObject::Folder { folder_id, .. }) = object {
            if let Some(view) = debug_advertised_default_named_view(snapshot, *folder_id) {
                session.record_default_view_advertised(
                    request_id,
                    *folder_id,
                    view.folder_id,
                    view.id,
                    &view.name,
                );
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    mapi_request_id = request_id,
                    request_rop_id = "0x07",
                    folder_id = %format!("0x{folder_id:016x}"),
                    folder_role = debug_role_for_folder_id(*folder_id),
                    advertised_default_view_folder_id = %format!("0x{:016x}", view.folder_id),
                    advertised_default_view_message_id = %format!("0x{:016x}", view.id),
                    advertised_default_view_name = %view.name,
                    default_view_advertisement_state =
                        %session.default_view_advertisement_state(),
                    "rca debug mapi default view advertised"
                );
            }
        }
    }
    let post_hierarchy_contract =
        post_hierarchy_getprops_contract(request, object, &property_response);
    let outlook_surface_folder_getprops_trace = format_outlook_surface_folder_getprops_trace(
        request_id,
        request,
        object,
        &property_response,
    );
    if should_log_outlook_surface_getprops_info(object) {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            request_rop_id = "0x07",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            response_handle_index = request.response_handle_index(),
            object_kind = mapi_object_debug_kind(object),
            folder_id = %mapi_object_debug_folder_id(object),
            getprops_contract = %post_hierarchy_contract,
            "rca debug mapi outlook surface getprops contract"
        );
    }
    session.record_post_hierarchy_getprops_contract(post_hierarchy_contract.clone());
    session.record_post_hierarchy_request_contract(format!("{post_hierarchy_contract}->ok"));
    responses.extend_from_slice(&property_response);
    if let Some(trace) = outlook_surface_folder_getprops_trace {
        session.record_outlook_view_failure_trace_event(trace);
    }
    if is_inbox_folder_type_probe {
        let folder_type_probe_succeeded = property_response
            .get(2..6)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            == Some(0);
        if folder_type_probe_succeeded {
            session.record_receive_folder_verification_passed();
        }
        if let Some(context) = inbox_folder_type_getprops_context {
            session.record_last_inbox_folder_type_getprops_context(format!(
                "{};{}",
                context,
                format_inbox_folder_type_getprops_response_context(&property_response)
            ));
        }
        if let Some(context) =
            format_post_fai_folder_type_probe_loop_context(&session.post_hierarchy_actions)
        {
            record_mapi_outlook_view_bootstrap_stall(3);
            session.record_outlook_view_failure_trace_event(format!(
                "post_fai_folder_type_probe_loop:{context}"
            ));
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x07",
                mapi_request_id = request_id,
                input_handle_index = request.input_handle_index().unwrap_or(0),
                input_handle_value =
                    %format_optional_debug_handle(input_handle(handle_slots, request)),
                folder_id = format!("0x{INBOX_FOLDER_ID:016x}"),
                probe_loop_context = %context,
                "rca debug mapi post fai inbox folder type probe loop"
            );
            session.mark_post_inbox_fai_folder_type_probe_loop_logged();
        }
        if let Some(summary) = format_inbox_open_loop_summary(&session.post_hierarchy_actions) {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x07",
                input_handle_index = request.input_handle_index().unwrap_or(0),
                input_handle_value =
                    %format_optional_debug_handle(input_handle(handle_slots, request)),
                folder_id = format!("0x{INBOX_FOLDER_ID:016x}"),
                loop_summary = %summary,
                "rca debug mapi repeated inbox open folder loop summary"
            );
        }
    }
    if let Some(trace) = inbox_config_getprops_trace {
        session.record_outlook_view_failure_trace_event(trace);
    }
}

pub(super) fn append_get_properties_all_response(
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&rop_get_properties_all_response(
        request,
        input_object(session, handle_slots, request),
        principal,
        mailboxes,
        emails,
        snapshot,
    ));
}

pub(super) fn append_get_properties_list_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&rop_get_properties_list_response(
        request,
        input_object(session, handle_slots, request),
    ));
}

pub(super) fn append_get_stream_size_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    let requested_handle = input_handle(handle_slots, request);
    let stream_handle =
        requested_handle.and_then(|handle| resolve_writable_stream_handle(session, handle));
    match stream_handle.and_then(|handle| session.handles.get(&handle)) {
        Some(MapiObject::AttachmentStream { data, .. }) => {
            responses.extend_from_slice(&rop_get_stream_size_response(request, data.len()));
        }
        _ => responses.extend_from_slice(&rop_error_response(
            0x5E,
            request.response_handle_index(),
            0x8004_010F,
        )),
    }
}

pub(super) async fn append_open_stream_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    request_id: &str,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) where
    S: ExchangeStore,
{
    let Some(input_handle) = input_handle(handle_slots, request) else {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x2b",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            input_handle_value = "missing",
            response_handle_index = request.response_handle_index(),
            output_handle_index = request.output_handle_index.unwrap_or(0),
            stream_property_tag = %format!("0x{:08x}", request.stream_property_tag().unwrap_or(0)),
            stream_open_mode = %format!("0x{:02x}", request.stream_open_mode().unwrap_or(0)),
            stream_open_result = "missing_input_handle",
            message = "rca debug mapi open stream"
        );
        responses.extend_from_slice(&rop_error_response(
            0x2B,
            request.output_handle_index.unwrap_or(0),
            0x8004_010F,
        ));
        return;
    };
    let input_object_kind = mapi_object_debug_kind(session.handles.get(&input_handle));
    let input_folder_id = mapi_object_debug_folder_id(session.handles.get(&input_handle));
    let (associated_config_id, associated_config_class, associated_config_subject) =
        associated_config_debug_fields(session, snapshot, input_handle);
    let is_inbox_associated_config_stream = matches!(
        session.handles.get(&input_handle),
        Some(MapiObject::AssociatedConfig {
            folder_id: INBOX_FOLDER_ID,
            ..
        })
    );
    let is_inbox_rule_organizer_stream = is_inbox_associated_config_stream
        && associated_config_class == crate::mapi_store::OUTLOOK_INBOX_RULE_ORGANIZER_CONFIG_CLASS
        && request.stream_property_tag().unwrap_or(0) == OUTLOOK_RULE_ORGANIZER_BINARY_6802;
    if is_inbox_associated_config_stream {
        session.record_inbox_associated_config_stream_open();
        session.record_outlook_view_failure_trace_event(format!(
            "open_inbox_config_stream:request_id={request_id};input_handle={input_handle};tag=0x{:08x};mode=0x{:02x};class={associated_config_class};subject={associated_config_subject}",
            request.stream_property_tag().unwrap_or(0),
            request.stream_open_mode().unwrap_or(0)
        ));
        session.record_recent_probe_action(format!(
            "OpenAssociatedConfigStream(in={},tag=0x{:08x},mode=0x{:02x})",
            input_handle,
            request.stream_property_tag().unwrap_or(0),
            request.stream_open_mode().unwrap_or(0)
        ));
    }
    let Some((stream_data, writable_target)) = open_stream_data(
        store,
        principal,
        session,
        input_handle,
        request.stream_property_tag().unwrap_or(0),
        request.stream_open_mode().unwrap_or(0),
        mailboxes,
        emails,
        snapshot,
    )
    .await
    else {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x2b",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            input_handle_value = input_handle,
            response_handle_index = request.response_handle_index(),
            output_handle_index = request.output_handle_index.unwrap_or(0),
            object_kind = input_object_kind,
            folder_id = %input_folder_id,
            associated_config_id = %associated_config_id,
            associated_config_class = %associated_config_class,
            associated_config_subject = %associated_config_subject,
            stream_property_tag = %format!("0x{:08x}", request.stream_property_tag().unwrap_or(0)),
            stream_open_mode = %format!("0x{:02x}", request.stream_open_mode().unwrap_or(0)),
            stream_open_result = "missing_stream_data",
            inbox_associated_config_stream = is_inbox_associated_config_stream,
            message = "rca debug mapi open stream"
        );
        responses.extend_from_slice(&rop_error_response(
            0x2B,
            request.output_handle_index.unwrap_or(0),
            0x8004_010F,
        ));
        return;
    };
    let stream_size = stream_data.len();
    let handle = session.allocate_output_handle(
        request.output_handle_index,
        MapiObject::AttachmentStream {
            data: stream_data,
            position: 0,
            writable_target,
        },
    );
    if is_inbox_associated_config_stream {
        session.record_inbox_associated_config_stream_handle(handle);
        session.record_outlook_view_failure_trace_event(format!(
            "open_inbox_config_stream_result:request_id={request_id};input_handle={input_handle};output_handle={handle};size={stream_size};writable={}",
            writable_target.is_some()
        ));
    }
    if is_inbox_rule_organizer_stream {
        session.record_inbox_rule_organizer_stream_handle(handle);
        session.record_recent_probe_action(format!(
            "OpenRuleOrganizerStream(in={},out={},size={})",
            input_handle, handle, stream_size
        ));
    }
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x2b",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        input_handle_value = input_handle,
        response_handle_index = request.response_handle_index(),
        output_handle_index = request.output_handle_index.unwrap_or(0),
        output_handle_value = handle,
        object_kind = input_object_kind,
        folder_id = %input_folder_id,
        associated_config_id = %associated_config_id,
        associated_config_class = %associated_config_class,
        associated_config_subject = %associated_config_subject,
        stream_property_tag = %format!("0x{:08x}", request.stream_property_tag().unwrap_or(0)),
        stream_open_mode = %format!("0x{:02x}", request.stream_open_mode().unwrap_or(0)),
        stream_size,
        stream_empty = stream_size == 0,
        stream_preview = %hex_preview(
            match session.handles.get(&handle) {
                Some(MapiObject::AttachmentStream { data, .. }) => data.as_slice(),
                _ => &[],
            },
            32
        ),
        stream_open_result = "success",
        inbox_associated_config_stream = is_inbox_associated_config_stream,
        inbox_rule_organizer_stream = is_inbox_rule_organizer_stream,
        message = "rca debug mapi open stream"
    );
    responses.extend_from_slice(&rop_open_stream_response(request, stream_size));
    output_handles.push(handle);
}

pub(super) fn append_read_stream_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    responses: &mut Vec<u8>,
) {
    let read_input_handle = input_handle(handle_slots, request);
    let resolved_stream_handle =
        read_input_handle.and_then(|handle| resolve_writable_stream_handle(session, handle));
    let is_rule_organizer_stream_read = resolved_stream_handle
        .is_some_and(|handle| session.is_inbox_rule_organizer_stream_handle(handle));
    if let Some(stream_handle) = resolved_stream_handle {
        if session.is_inbox_associated_config_stream_handle(stream_handle) {
            session.record_inbox_associated_config_stream_read();
            session.record_outlook_view_failure_trace_event(format!(
                "read_inbox_config_stream:request_id={request_id};handle={stream_handle};requested_bytes={}",
                request.read_byte_count().unwrap_or(0)
            ));
            session.record_recent_probe_action(format!(
                "ReadAssociatedConfigStream(in={},max={})",
                stream_handle,
                request.read_byte_count().unwrap_or(0)
            ));
        }
    }
    let Some(stream) = resolved_stream_handle.and_then(|handle| session.handles.get_mut(&handle))
    else {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x2c",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            input_handle_value = read_input_handle
                .map(|handle| handle.to_string())
                .unwrap_or_else(|| "missing".to_string()),
            resolved_stream_handle = resolved_stream_handle
                .map(|handle| handle.to_string())
                .unwrap_or_else(|| "none".to_string()),
            response_handle_index = request.response_handle_index(),
            requested_byte_count = request.read_byte_count().unwrap_or(0),
            stream_read_result = "missing_input_object",
            message = "rca debug mapi read stream"
        );
        responses.extend_from_slice(&rop_error_response(
            0x2C,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let (before_position, stream_len) = match stream {
        MapiObject::AttachmentStream { data, position, .. } => (*position, data.len()),
        _ => (0, 0),
    };
    let response = rop_read_stream_response(request, stream);
    let after_position = match stream {
        MapiObject::AttachmentStream { position, .. } => *position,
        _ => 0,
    };
    let returned_byte_count = after_position.saturating_sub(before_position);
    let end_of_stream = after_position >= stream_len;
    if is_rule_organizer_stream_read {
        let context = format!(
            "input_handle={};requested_byte_count={};stream_size={};position_before={};position_after={};returned_byte_count={};end_of_stream={};response_bytes={};response_preview={}",
            read_input_handle
                .map(|handle| handle.to_string())
                .unwrap_or_else(|| "missing".to_string()),
            request.read_byte_count().unwrap_or(0),
            stream_len,
            before_position,
            after_position,
            returned_byte_count,
            end_of_stream,
            response.len(),
            hex_preview(&response, 48)
        );
        session.record_inbox_rule_organizer_stream_read(context.clone());
        session.record_recent_probe_action(format!(
            "ReadRuleOrganizerStream(in={},returned={},eos={})",
            read_input_handle
                .map(|handle| handle.to_string())
                .unwrap_or_else(|| "missing".to_string()),
            returned_byte_count,
            end_of_stream
        ));
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x2c",
            rule_organizer_stream_context = %context,
            inbox_loop_summary =
                %format_inbox_open_loop_summary(&session.post_hierarchy_actions)
                .unwrap_or_else(|| "none".to_string()),
            "rca debug outlook rule organizer stream read checkpoint"
        );
    }
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x2c",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        input_handle_value = read_input_handle
            .map(|handle| handle.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        resolved_stream_handle = resolved_stream_handle
            .map(|handle| handle.to_string())
            .unwrap_or_else(|| "none".to_string()),
        response_handle_index = request.response_handle_index(),
        requested_byte_count = request.read_byte_count().unwrap_or(0),
        stream_position_before = before_position,
        stream_position_after = after_position,
        returned_byte_count,
        end_of_stream,
        response_bytes = response.len(),
        response_preview = %hex_preview(&response, 48),
        stream_read_result = "success",
        inbox_rule_organizer_stream = is_rule_organizer_stream_read,
        message = "rca debug mapi read stream"
    );
    responses.extend_from_slice(&response);
}

pub(super) fn append_clone_stream_response(
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let requested_handle = input_handle(handle_slots, request);
    let stream_handle =
        requested_handle.and_then(|handle| resolve_writable_stream_handle(session, handle));
    match stream_handle
        .and_then(|handle| session.handles.get(&handle))
        .cloned()
    {
        Some(MapiObject::AttachmentStream {
            data,
            position,
            writable_target: None,
        }) => {
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                MapiObject::AttachmentStream {
                    data,
                    position,
                    writable_target: None,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            responses.extend_from_slice(&rop_simple_success_response(request));
            output_handles.push(handle);
        }
        Some(MapiObject::AttachmentStream { .. }) => responses.extend_from_slice(
            &rop_error_response(0x3B, request.response_handle_index(), 0x8004_0102),
        ),
        _ => responses.extend_from_slice(&rop_error_response(
            0x3B,
            request.response_handle_index(),
            0x8004_010F,
        )),
    }
}

pub(super) fn append_stream_region_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    let requested_handle = input_handle(handle_slots, request);
    match requested_handle.and_then(|handle| session.handles.get(&handle)) {
        Some(MapiObject::AttachmentStream { .. }) => {
            responses.extend_from_slice(&rop_simple_success_response(request));
        }
        _ => responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            0x8004_010F,
        )),
    }
}

pub(super) fn append_seek_stream_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    let requested_handle = input_handle(handle_slots, request);
    let stream_handle =
        requested_handle.and_then(|handle| resolve_writable_stream_handle(session, handle));
    let Some(stream) = stream_handle.and_then(|handle| session.handles.get_mut(&handle)) else {
        responses.extend_from_slice(&rop_error_response(
            0x2E,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x2e",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        requested_handle = requested_handle
            .map(|handle| handle.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        resolved_stream_handle = stream_handle
            .map(|handle| handle.to_string())
            .unwrap_or_else(|| "none".to_string()),
        message = "rca debug mapi seek stream"
    );
    responses.extend_from_slice(&rop_seek_stream_response(request, stream));
}

pub(super) fn append_set_stream_size_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    responses: &mut Vec<u8>,
) {
    let Some(requested_handle) = input_handle(handle_slots, request) else {
        responses.extend_from_slice(&rop_error_response(
            0x2F,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let stream_handle = resolve_writable_stream_handle(session, requested_handle);
    if stream_handle.is_some_and(|handle| session.is_inbox_associated_config_stream_handle(handle))
    {
        session.record_outlook_view_failure_trace_event(format!(
            "set_inbox_config_stream_size:request_id={request_id};requested_handle={requested_handle};resolved_handle={};size={}",
            stream_handle
                .map(|handle| handle.to_string())
                .unwrap_or_else(|| "none".to_string()),
            request.stream_size().unwrap_or(u64::MAX)
        ));
    }
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x2f",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        requested_handle,
        resolved_stream_handle = stream_handle
            .map(|handle| handle.to_string())
            .unwrap_or_else(|| "none".to_string()),
        requested_stream_size = request.stream_size().unwrap_or(u64::MAX),
        requested_object_kind = mapi_object_debug_kind(session.handles.get(&requested_handle)),
        resolved_object_kind = stream_handle
            .and_then(|handle| session.handles.get(&handle))
            .map(|object| mapi_object_debug_kind(Some(object)))
            .unwrap_or("none"),
        message = "rca debug mapi set stream size"
    );
    match set_attachment_stream_size(
        session,
        stream_handle.unwrap_or(requested_handle),
        request.stream_size().unwrap_or(u64::MAX),
    ) {
        Some(()) => responses.extend_from_slice(&rop_simple_success_response(request)),
        None => responses.extend_from_slice(&rop_error_response(
            0x2F,
            request.response_handle_index(),
            0x8004_0102,
        )),
    }
}

pub(super) fn append_write_stream_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    responses: &mut Vec<u8>,
) {
    let Some(requested_handle) = input_handle(handle_slots, request) else {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let stream_handle = resolve_writable_stream_handle(session, requested_handle);
    if stream_handle.is_some_and(|handle| session.is_inbox_associated_config_stream_handle(handle))
    {
        session.record_outlook_view_failure_trace_event(format!(
            "write_inbox_config_stream:request_id={request_id};requested_handle={requested_handle};resolved_handle={};bytes={}",
            stream_handle
                .map(|handle| handle.to_string())
                .unwrap_or_else(|| "none".to_string()),
            request.stream_write_data().len()
        ));
    }
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = %format!("0x{:02x}", request.rop_id),
        input_handle_index = request.input_handle_index().unwrap_or(0),
        requested_handle,
        resolved_stream_handle = stream_handle
            .map(|handle| handle.to_string())
            .unwrap_or_else(|| "none".to_string()),
        write_byte_count = request.stream_write_data().len(),
        requested_object_kind = mapi_object_debug_kind(session.handles.get(&requested_handle)),
        resolved_object_kind = stream_handle
            .and_then(|handle| session.handles.get(&handle))
            .map(|object| mapi_object_debug_kind(Some(object)))
            .unwrap_or("none"),
        message = "rca debug mapi write stream"
    );
    let stream_handle = stream_handle.unwrap_or(requested_handle);
    match write_stream(session, stream_handle, request.stream_write_data()) {
        Some(written) => responses.extend_from_slice(&rop_write_stream_response(request, written)),
        None => {
            let error_code = stream_write_error_code(
                stream_write_error(session, stream_handle).unwrap_or(StreamWriteError::NotFound),
            );
            responses.extend_from_slice(&rop_error_response(
                request.rop_id,
                request.response_handle_index(),
                error_code,
            ))
        }
    }
}

pub(super) fn append_copy_to_stream_response(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    let Some(source_handle) = input_handle(handle_slots, request) else {
        responses.extend_from_slice(&rop_error_response(
            0x3A,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let source_handle =
        resolve_writable_stream_handle(session, source_handle).unwrap_or(source_handle);
    let Some(destination_handle) = request.move_copy_target_handle(handle_slots) else {
        responses.extend_from_slice(&rop_error_response(
            0x3A,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let destination_handle =
        resolve_writable_stream_handle(session, destination_handle).unwrap_or(destination_handle);
    match copy_stream(
        session,
        source_handle,
        destination_handle,
        request.stream_size().unwrap_or(u64::MAX),
    ) {
        Some((read, written)) => {
            responses.extend_from_slice(&rop_copy_to_stream_response(request, read, written));
        }
        None => responses.extend_from_slice(&rop_error_response(
            0x3A,
            request.response_handle_index(),
            0x8004_0102,
        )),
    }
}

pub(super) async fn append_copy_to_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    if !matches!(request.copy_to_want_asynchronous(), Some(0x00 | 0x01))
        || !matches!(request.copy_to_want_subobjects(), Some(0x00 | 0x01))
    {
        responses.extend_from_slice(&rop_error_response(
            0x39,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    let Some(destination_handle) = request.move_copy_target_handle(handle_slots) else {
        responses.extend_from_slice(&rop_copy_to_null_destination_response(request));
        return;
    };
    let destination_object = session.handles.get(&destination_handle).cloned();
    if destination_object.is_none() {
        responses.extend_from_slice(&rop_copy_to_null_destination_response(request));
        return;
    }
    let source_object = input_object(session, handle_slots, request).cloned();
    if source_object.is_none() {
        responses.extend_from_slice(&rop_error_response(
            0x39,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    if copy_all_custom_property_values_for_request(
        store,
        principal,
        source_object.as_ref(),
        destination_object.as_ref(),
        mailboxes,
        emails,
        snapshot,
        &request.copy_to_excluded_property_tags(),
    )
    .await
    .unwrap_or(false)
    {
        responses.extend_from_slice(&rop_set_properties_response(request));
        return;
    }
    if copy_all_message_followup_property_values_for_request(
        store,
        principal,
        source_object.as_ref(),
        destination_object.as_ref(),
        mailboxes,
        emails,
        snapshot,
        &request.copy_to_excluded_property_tags(),
    )
    .await
    .unwrap_or(false)
    {
        responses.extend_from_slice(&rop_set_properties_response(request));
        return;
    }
    responses.extend_from_slice(&unsupported_rop_response(
        0x39,
        request.response_handle_index(),
    ));
}

pub(super) async fn append_copy_properties_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    if !matches!(
        request.copy_properties_want_asynchronous(),
        Some(0x00 | 0x01)
    ) {
        responses.extend_from_slice(&rop_error_response(
            0x67,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    if input_handle(handle_slots, request).is_none() {
        responses.extend_from_slice(&rop_error_response(
            0x67,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    let Some(destination_handle) = request.move_copy_target_handle(handle_slots) else {
        responses.extend_from_slice(&rop_copy_properties_null_destination_response(request));
        return;
    };
    if !session.handles.contains_key(&destination_handle) {
        responses.extend_from_slice(&rop_copy_properties_null_destination_response(request));
        return;
    }
    if request.copy_properties_property_tags().is_empty() {
        responses.extend_from_slice(&rop_copy_properties_success_response(request));
        return;
    }
    let source_object = input_object(session, handle_slots, request).cloned();
    let destination_object = session.handles.get(&destination_handle).cloned();
    if let Some(problems) = copy_message_followup_property_values_for_request(
        store,
        principal,
        source_object.as_ref(),
        destination_object.as_ref(),
        mailboxes,
        emails,
        snapshot,
        &request.copy_properties_property_tags(),
    )
    .await
    .unwrap_or_default()
    {
        if problems.is_empty() {
            responses.extend_from_slice(&rop_copy_properties_success_response(request));
        } else {
            responses.extend_from_slice(&rop_set_properties_problem_response(request, &problems));
        }
        return;
    }
    if let Some(problems) = copy_custom_property_values_for_request(
        store,
        principal,
        source_object.as_ref(),
        destination_object.as_ref(),
        mailboxes,
        emails,
        snapshot,
        &request.copy_properties_property_tags(),
    )
    .await
    .unwrap_or_default()
    {
        if problems.is_empty() {
            responses.extend_from_slice(&rop_copy_properties_success_response(request));
        } else {
            responses.extend_from_slice(&rop_set_properties_problem_response(request, &problems));
        }
        return;
    }
    responses.extend_from_slice(&unsupported_rop_response(
        0x67,
        request.response_handle_index(),
    ));
}

pub(super) async fn append_commit_stream_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    request_id: &str,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    let requested_handle = input_handle(handle_slots, request);
    let stream_handle =
        requested_handle.and_then(|handle| resolve_writable_stream_handle(session, handle));
    if stream_handle.is_some_and(|handle| session.is_inbox_associated_config_stream_handle(handle))
    {
        session.record_outlook_view_failure_trace_event(format!(
            "commit_inbox_config_stream:request_id={request_id};requested_handle={};resolved_handle={}",
            requested_handle
                .map(|handle| handle.to_string())
                .unwrap_or_else(|| "missing".to_string()),
            stream_handle
                .map(|handle| handle.to_string())
                .unwrap_or_else(|| "none".to_string())
        ));
    }
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x5d",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        requested_handle = requested_handle
            .map(|handle| handle.to_string())
            .unwrap_or_else(|| "missing".to_string()),
        resolved_stream_handle = stream_handle
            .map(|handle| handle.to_string())
            .unwrap_or_else(|| "none".to_string()),
        message = "rca debug mapi commit stream"
    );
    let commit_object = stream_handle
        .and_then(|handle| session.handles.get(&handle))
        .or_else(|| input_object(session, handle_slots, request))
        .cloned();
    let commit_result = match commit_object {
        Some(MapiObject::AttachmentStream {
            writable_target: Some(StreamWriteTarget::AssociatedConfigProperty { handle, .. }),
            ..
        }) => {
            let message = match session.handles.get(&handle) {
                Some(MapiObject::AssociatedConfig {
                    folder_id,
                    saved_message: Some(message),
                    ..
                }) => Some((*folder_id, message.clone())),
                _ => None,
            };
            match message {
                Some((folder_id, message)) => {
                    persist_associated_config_stream_message(store, principal, folder_id, &message)
                        .await
                }
                None => Err(anyhow!(
                    "MAPI associated config stream commit target was not found"
                )),
            }
        }
        Some(MapiObject::AttachmentStream { .. }) => Ok(()),
        _ => Err(anyhow!("MAPI stream commit target was not found")),
    };
    match commit_result {
        Ok(()) => responses.extend_from_slice(&rop_simple_success_response(request)),
        Err(_) => responses.extend_from_slice(&rop_error_response(
            0x5D,
            request.response_handle_index(),
            0x8004_010F,
        )),
    }
}

pub(super) async fn apply_supported_object_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    object: &MapiObject,
    values: Vec<(u32, MapiValue)>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let (canonical_values, custom_values) = split_object_property_values(object, values);
    if let Some(folder_id) = object.folder_id() {
        if !snapshot
            .folder_access_for_principal(folder_id, principal.account_id)
            .map(|access| access.may_write)
            .unwrap_or(true)
        {
            return Err(anyhow!(
                "MAPI object mutation denied by canonical folder rights"
            ));
        }
    }
    if !canonical_values.is_empty() {
        match object {
            MapiObject::Message {
                folder_id,
                message_id,
                ..
            } => {
                apply_canonical_message_property_values(
                    store,
                    principal,
                    *folder_id,
                    *message_id,
                    canonical_values,
                    mailboxes,
                    emails,
                )
                .await?;
            }
            MapiObject::Contact {
                folder_id,
                contact_id,
            } => {
                apply_canonical_contact_property_values(
                    store,
                    principal,
                    *folder_id,
                    *contact_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::Event {
                folder_id,
                event_id,
            } => {
                apply_canonical_event_property_values(
                    store,
                    principal,
                    *folder_id,
                    *event_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::Task { folder_id, task_id } => {
                apply_canonical_task_property_values(
                    store,
                    principal,
                    *folder_id,
                    *task_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::Note { folder_id, note_id } => {
                apply_canonical_note_property_values(
                    store,
                    principal,
                    *folder_id,
                    *note_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::JournalEntry {
                folder_id,
                journal_entry_id,
            } => {
                apply_canonical_journal_entry_property_values(
                    store,
                    principal,
                    *folder_id,
                    *journal_entry_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::ConversationAction {
                folder_id,
                conversation_action_id,
            } => {
                let Some(existing) = snapshot
                    .conversation_action_message_for_id(*conversation_action_id)
                    .filter(|message| message.folder_id == *folder_id)
                else {
                    return Err(anyhow!("canonical MAPI conversation action was not found"));
                };
                let mut properties = conversation_action_properties(&existing.action);
                apply_mapi_property_values_to_map(&mut properties, canonical_values);
                let action = conversation_action_from_mapi_properties(&properties);
                let move_target_mailbox_id =
                    conversation_action_target_mailbox_id(&action, mailboxes);
                let input = lpe_storage::UpsertConversationActionInput {
                    account_id: principal.account_id,
                    conversation_id: action.conversation_id,
                    subject: action.subject,
                    categories_json: action.categories_json,
                    move_folder_entry_id: action.move_folder_entry_id,
                    move_store_entry_id: action.move_store_entry_id,
                    move_target_mailbox_id,
                    max_delivery_time: action.max_delivery_time,
                    last_applied_time: action.last_applied_time,
                    version: Some(action.version),
                    processed: Some(action.processed),
                };
                let saved = store.upsert_conversation_action(input).await?;
                apply_conversation_action_to_existing_messages(
                    store, principal, &saved, mailboxes, emails,
                )
                .await?;
            }
            MapiObject::NavigationShortcut {
                folder_id,
                shortcut_id,
            } => {
                let Some(existing) = snapshot
                    .navigation_shortcut_message_for_id(*shortcut_id)
                    .filter(|message| message.folder_id == *folder_id)
                else {
                    return Err(anyhow!("canonical MAPI navigation shortcut was not found"));
                };
                let mut properties = HashMap::new();
                for tag in [
                    PID_TAG_SUBJECT_W,
                    PID_TAG_NORMALIZED_SUBJECT_W,
                    PID_TAG_WLINK_ENTRY_ID,
                    PID_TAG_WLINK_SAVE_STAMP,
                    PID_TAG_WLINK_TYPE,
                    PID_TAG_WLINK_FLAGS,
                    PID_TAG_WLINK_SECTION,
                    PID_TAG_WLINK_ORDINAL,
                ] {
                    if let Some(value) =
                        navigation_shortcut_property_value(&existing, principal.account_id, tag)
                    {
                        properties.insert(tag, value);
                    }
                }
                apply_mapi_property_values_to_map(&mut properties, canonical_values);
                let shortcut = navigation_shortcut_from_mapi_properties(
                    principal.account_id,
                    Some(existing.canonical_id),
                    &properties,
                );
                store
                    .upsert_mapi_navigation_shortcut(UpsertMapiNavigationShortcutInput {
                        id: Some(shortcut.canonical_id),
                        account_id: principal.account_id,
                        subject: shortcut.subject,
                        target_folder_id: shortcut.target_folder_id,
                        shortcut_type: shortcut.shortcut_type,
                        flags: shortcut.flags,
                        save_stamp: shortcut.save_stamp,
                        section: shortcut.section,
                        ordinal: shortcut.ordinal,
                        group_header_id: shortcut.group_header_id,
                        group_name: shortcut.group_name,
                    })
                    .await?;
            }
            MapiObject::AssociatedConfig {
                folder_id,
                config_id,
                saved_message,
            } => {
                let Some(existing) = associated_config_message_for_mutation(
                    snapshot,
                    *folder_id,
                    *config_id,
                    saved_message.as_ref(),
                ) else {
                    return Err(anyhow!("MAPI associated config message was not found"));
                };
                let mut properties = associated_config_mutation_base_properties(&existing);
                apply_mapi_property_values_to_map(&mut properties, canonical_values);
                let (message_class, subject) = associated_config_class_and_subject(&properties);
                store
                    .upsert_mapi_associated_config(UpsertMapiAssociatedConfigInput {
                        id: Some(existing.canonical_id),
                        account_id: principal.account_id,
                        folder_id: *folder_id,
                        message_class,
                        subject,
                        properties_json: mapi_properties_to_json(&properties),
                    })
                    .await?;
            }
            MapiObject::PublicFolderItem {
                folder_id, item_id, ..
            } => {
                apply_canonical_public_folder_item_property_values(
                    store,
                    principal,
                    *folder_id,
                    *item_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::DelegateFreeBusyMessage { .. } | MapiObject::RecoverableItem { .. } => {}
            _ => return Err(anyhow!("MAPI object does not support property mutation")),
        }
    }
    if custom_values.is_empty() {
        return Ok(());
    }
    let (object_kind, canonical_id) =
        custom_property_object_identity(Some(object), mailboxes, emails, snapshot)
            .ok_or_else(|| anyhow!("canonical MAPI object was not found"))?;
    upsert_custom_property_values(store, principal, object_kind, canonical_id, custom_values).await
}
