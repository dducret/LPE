use super::*;

pub(super) fn append_open_message_response(
    principal: &AccountPrincipal,
    request_id: &str,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let message_id = request.message_id().unwrap_or(0);
    let folder_id = open_message_folder_id(request, message_id);
    if let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) {
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::Message {
                folder_id,
                message_id,
                saved_email: Some(MapiSavedEmail {
                    email: email.clone(),
                }),
                pending_properties: HashMap::new(),
            },
        );
        session.record_message_handle_generation(handle, folder_id, message_id);
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        let response = rop_open_message_response_with_recipients(request, &email.subject, email);
        log_open_message_debug(
            principal,
            request,
            handle,
            folder_id,
            message_id,
            "mailbox",
            email,
            response.len(),
        );
        record_visible_inbox_message_open(
            session,
            request_id,
            request,
            handle,
            folder_id,
            message_id,
            "mailbox",
            email,
            response.len(),
        );
        responses.extend_from_slice(&response);
        output_handles.push(handle);
    } else if let Some(message) = search_folder_message_for_id(snapshot, folder_id, message_id) {
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::Message {
                folder_id,
                message_id,
                saved_email: Some(MapiSavedEmail {
                    email: message.email.clone(),
                }),
                pending_properties: HashMap::new(),
            },
        );
        session.record_message_handle_generation(handle, folder_id, message_id);
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        let response = rop_open_message_response_with_recipients(
            request,
            &message.email.subject,
            &message.email,
        );
        log_open_message_debug(
            principal,
            request,
            handle,
            folder_id,
            message_id,
            "search_folder",
            &message.email,
            response.len(),
        );
        responses.extend_from_slice(&response);
        output_handles.push(handle);
    } else if let Some(email) = unique_message_for_id(message_id, emails) {
        let canonical_folder_id = canonical_message_folder_id(email, mailboxes);
        let handle_folder_id = fallback_open_message_folder_id(folder_id, email, mailboxes);
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x03",
            requested_folder_id = %format!("0x{folder_id:016x}"),
            canonical_folder_id = %format!("0x{canonical_folder_id:016x}"),
            handle_folder_id = %format!("0x{handle_folder_id:016x}"),
            message_id = %format!("0x{message_id:016x}"),
            message_subject = %email.subject,
            fallback_reason = "unique_message_id_folder_mismatch",
            "rca debug mapi open message folder fallback"
        );
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::Message {
                folder_id: handle_folder_id,
                message_id,
                saved_email: Some(MapiSavedEmail {
                    email: email.clone(),
                }),
                pending_properties: HashMap::new(),
            },
        );
        session.record_message_handle_generation(handle, handle_folder_id, message_id);
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        let response = rop_open_message_response_with_recipients(request, &email.subject, email);
        log_open_message_debug(
            principal,
            request,
            handle,
            handle_folder_id,
            message_id,
            "unique_message_id_folder_mismatch",
            email,
            response.len(),
        );
        record_visible_inbox_message_open(
            session,
            request_id,
            request,
            handle,
            handle_folder_id,
            message_id,
            "unique_message_id_folder_mismatch",
            email,
            response.len(),
        );
        responses.extend_from_slice(&response);
        output_handles.push(handle);
    } else if let Some(contact) = snapshot.contact_for_id(folder_id, message_id) {
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::Contact {
                folder_id,
                contact_id: message_id,
            },
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&rop_open_message_response(
            request,
            &contact.contact.name,
            0,
        ));
        output_handles.push(handle);
    } else if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::Event {
                folder_id,
                event_id: message_id,
                transaction: MapiEventTransaction::new(
                    request.payload.get(8).copied().unwrap_or(0),
                    event.version.canonical_modseq,
                ),
            },
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&rop_open_message_response(request, &event.event.title, 0));
        output_handles.push(handle);
    } else if let Some(task) = snapshot.task_for_id(folder_id, message_id) {
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::Task {
                folder_id,
                task_id: message_id,
            },
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&rop_open_message_response(request, &task.task.title, 0));
        output_handles.push(handle);
    } else if let Some(note) = snapshot.note_for_id(folder_id, message_id) {
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::Note {
                folder_id,
                note_id: message_id,
            },
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&rop_open_message_response(request, &note.note.title, 0));
        output_handles.push(handle);
    } else if let Some(entry) = snapshot.journal_entry_for_id(folder_id, message_id) {
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::JournalEntry {
                folder_id,
                journal_entry_id: message_id,
            },
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&rop_open_message_response(request, &entry.entry.subject, 0));
        output_handles.push(handle);
    } else if let Some(message) =
        common_view_named_view_message_for_open(snapshot, folder_id, message_id)
    {
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::CommonViewNamedView {
                folder_id,
                view_id: message_id,
            },
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        log_outlook_view_handoff(
            principal, request, folder_id, message_id, handle, &message, snapshot,
        );
        session.record_outlook_view_failure_trace_event(format!(
            "view_handoff:request_id={request_id};folder=0x{folder_id:016x};view=0x{message_id:016x};handle={handle};class={};name={}",
            "IPM.Microsoft.FolderDesign.NamedView",
            message.name
        ));
        let opened_advertised_default_view =
            session.record_default_view_opened(request_id, folder_id, message_id);
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x03",
            mapi_request_id = request_id,
            folder_id = %format!("0x{folder_id:016x}"),
            view_message_id = %format!("0x{message_id:016x}"),
            view_name = %message.name,
            opened_advertised_default_view,
            default_view_advertisement_state = %session.default_view_advertisement_state(),
            "rca debug mapi advertised default view open state"
        );
        responses.extend_from_slice(&rop_open_message_response(request, &message.name, 0));
        output_handles.push(handle);
    } else if let Some(definition) =
        search_folder_definition_message_for_open(snapshot, folder_id, message_id)
    {
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::SearchFolderDefinitionMessage {
                folder_id,
                message_id,
            },
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&rop_open_message_response(
            request,
            &definition.display_name,
            0,
        ));
        output_handles.push(handle);
    } else if folder_id == COMMON_VIEWS_FOLDER_ID {
        if let Some(message) = navigation_shortcut_message_for_open(snapshot, folder_id, message_id)
        {
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                MapiObject::NavigationShortcut {
                    folder_id,
                    shortcut_id: message_id,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            responses.extend_from_slice(&rop_open_message_response(request, &message.subject, 0));
            output_handles.push(handle);
        } else {
            responses.extend_from_slice(&rop_error_response(
                0x03,
                request.output_handle_index.unwrap_or(0),
                0x8004_010F,
            ));
        }
    } else if folder_id == FREEBUSY_DATA_FOLDER_ID {
        if let Some(message) = delegate_freebusy_message_for_open(snapshot, folder_id, message_id) {
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                MapiObject::DelegateFreeBusyMessage {
                    folder_id,
                    message_id,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            responses.extend_from_slice(&rop_open_message_response(
                request,
                &message.message.subject,
                0,
            ));
            output_handles.push(handle);
        } else {
            responses.extend_from_slice(&rop_error_response(
                0x03,
                request.output_handle_index.unwrap_or(0),
                0x8004_010F,
            ));
        }
    } else if folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
        if let Some(message) = conversation_action_message_for_open(snapshot, folder_id, message_id)
        {
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                MapiObject::ConversationAction {
                    folder_id,
                    conversation_action_id: message_id,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            responses.extend_from_slice(&rop_open_message_response(
                request,
                &conversation_action_subject(&message.action),
                0,
            ));
            output_handles.push(handle);
        } else {
            responses.extend_from_slice(&rop_error_response(
                0x03,
                request.output_handle_index.unwrap_or(0),
                0x8004_010F,
            ));
        }
    } else if let Some(message) = snapshot
        .associated_config_message_for_id(message_id)
        .filter(|message| message.folder_id == folder_id)
        .or_else(|| {
            snapshot
                .associated_config_message_for_identity_id(message_id)
                .filter(|message| message.folder_id == folder_id)
                .inspect(|message| {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = format_args!("0x{:02x}", request.rop_id),
                        folder_id = format_args!("0x{folder_id:016x}"),
                        requested_message_id = format_args!("0x{message_id:016x}"),
                        canonical_config_id = %message.canonical_id,
                        modeled_config_id = format_args!("0x{:016x}", message.id),
                        message_class = %message.message_class,
                        "rca debug mapi opened virtual associated config identity"
                    );
                })
        })
        .or_else(|| {
            snapshot.associated_config_message_for_folder_and_source_key_id(folder_id, message_id)
        })
    {
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::AssociatedConfig {
                folder_id,
                config_id: message.id,
                saved_message: Some(message.clone()),
            },
        );
        if folder_id == INBOX_FOLDER_ID
            && (crate::mapi_store::is_outlook_configuration_message_class(&message.message_class)
                || message.message_class == "IPM.ExtendedRule.Message")
        {
            session.record_inbox_associated_config_open();
            session.record_outlook_view_failure_trace_event(format!(
                "open_inbox_config:request_id={request_id};folder=0x{folder_id:016x};config=0x{:016x};handle={handle};class={};subject={}",
                message.id,
                message.message_class,
                message.subject
            ));
            session.record_recent_probe_action(format!(
                "OpenAssociatedConfig(out={},folder=0x{folder_id:016x},id=0x{:016x},class={})",
                request.output_handle_index.unwrap_or(0),
                message.id,
                message.message_class
            ));
        }
        let response = rop_open_message_response(request, &message.subject, 0);
        if is_contact_link_timestamp_config(folder_id, &message.message_class) {
            session.record_outlook_view_failure_trace_event(format!(
                "open_contact_link_timestamp_config:request_id={request_id};folder=0x{folder_id:016x};config=0x{:016x};handle={handle};subject={}",
                message.id,
                message.subject
            ));
        }
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x03",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            output_handle_index = request.output_handle_index.unwrap_or(0),
            output_handle = handle,
            folder_id = format_args!("0x{folder_id:016x}"),
            associated_config_id = format_args!("0x{:016x}", message.id),
            associated_config_canonical_id = %message.canonical_id,
            associated_config_class = %message.message_class,
            associated_config_subject = %message.subject,
            open_message_payload_preview = %hex_preview(&request.payload, 48),
            open_message_response_bytes = response.len(),
            open_message_response_preview = %hex_preview(&response, 96),
            associated_config_shape = %associated_config_open_shape(&message),
            contacts_surface = mapi_folder_is_outlook_contacts_surface(folder_id),
            "rca debug mapi open associated config"
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&response);
        output_handles.push(handle);
    } else if snapshot.associated_config_identity_matches_folder(folder_id, message_id) {
        let handle = session.allocate_output_handle(
            request.output_handle_index,
            MapiObject::PendingAssociatedMessage {
                folder_id,
                properties: HashMap::new(),
            },
        );
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&rop_open_message_response(request, "IPM.Configuration", 0));
        output_handles.push(handle);
    } else if crate::mapi_store::recoverable_storage_folder(folder_id).is_some() {
        if let Some(item) = snapshot.recoverable_item_for_id(folder_id, message_id) {
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                MapiObject::RecoverableItem {
                    folder_id,
                    item_id: message_id,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            responses.extend_from_slice(&rop_open_message_response(request, &item.item.subject, 0));
            output_handles.push(handle);
        } else {
            responses.extend_from_slice(&rop_error_response(
                0x03,
                request.output_handle_index.unwrap_or(0),
                0x8004_010F,
            ));
        }
    } else if snapshot.public_folder_for_id(folder_id).is_some() {
        if let Some(item) = snapshot.public_folder_item_for_id(folder_id, message_id) {
            let handle = session.allocate_output_handle(
                request.output_handle_index,
                MapiObject::PublicFolderItem {
                    folder_id,
                    item_id: message_id,
                    properties: HashMap::new(),
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
            responses.extend_from_slice(&rop_open_message_response(request, &item.item.subject, 0));
            output_handles.push(handle);
        } else {
            responses.extend_from_slice(&rop_error_response(
                0x03,
                request.output_handle_index.unwrap_or(0),
                0x8004_010F,
            ));
        }
    } else {
        if folder_id == INBOX_FOLDER_ID {
            session
                .post_hierarchy_actions
                .visible_inbox_message_open_missing_count = session
                .post_hierarchy_actions
                .visible_inbox_message_open_missing_count
                .saturating_add(1);
            session
                .post_hierarchy_actions
                .last_visible_inbox_message_open_context = format!(
                "request_id={request_id};folder=0x{folder_id:016x};message=0x{message_id:016x};source=missing;loaded_email_count={};same_id_email_count={}",
                emails.len(),
                emails
                    .iter()
                    .filter(|email| mapi_item_id_matches(&email.id, message_id))
                    .count()
            );
            session.record_outlook_view_failure_trace_event(format!(
                "visible_inbox_message_open:{}",
                session
                    .post_hierarchy_actions
                    .last_visible_inbox_message_open_context
            ));
        }
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x03",
            requested_folder_id = %format!("0x{folder_id:016x}"),
            requested_folder_role = debug_role_for_folder_id(folder_id),
            message_id = %format!("0x{message_id:016x}"),
            loaded_email_count = emails.len(),
            same_id_email_count = emails
                .iter()
                .filter(|email| mapi_item_id_matches(&email.id, message_id))
                .count(),
            failure_reason = "open_message_not_found",
            "rca debug mapi open message failure"
        );
        responses.extend_from_slice(&rop_error_response(
            0x03,
            request.output_handle_index.unwrap_or(0),
            0x8004_010F,
        ));
    }
}

fn record_visible_inbox_message_open(
    session: &mut MapiSession,
    request_id: &str,
    request: &RopRequest,
    handle: u32,
    folder_id: u64,
    message_id: u64,
    source: &str,
    email: &JmapEmail,
    response_len: usize,
) {
    if folder_id != INBOX_FOLDER_ID {
        return;
    }
    session
        .post_hierarchy_actions
        .last_visible_inbox_message_open_context = format!(
        "request_id={request_id};input_index={};output_index={};handle={handle};folder=0x{folder_id:016x};message=0x{message_id:016x};source={source};subject_chars={};class={};body_text_bytes={};body_html_bytes={};recipient_count={};has_attachments={};unread={};size_octets={};response_rop_bytes={response_len}",
        request.input_handle_index().unwrap_or(0),
        request.output_handle_index.unwrap_or(0),
        email.subject.chars().count(),
        message_class_for_email(email),
        email.body_text.len(),
        email.body_html_sanitized
            .as_ref()
            .map(|body| body.len())
            .unwrap_or(0),
        message_recipients(email).len(),
        email.has_attachments,
        email.unread,
        email.size_octets,
    );
    session.record_outlook_view_failure_trace_event(format!(
        "visible_inbox_message_open:{}",
        session
            .post_hierarchy_actions
            .last_visible_inbox_message_open_context
    ));
}
