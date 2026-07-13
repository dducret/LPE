use super::super::*;

pub(in crate::mapi::dispatch) fn log_open_message_debug(
    principal: &AccountPrincipal,
    request: &RopRequest,
    handle: u32,
    folder_id: u64,
    message_id: u64,
    source: &str,
    email: &JmapEmail,
    response_len: usize,
) {
    let recipient_count = message_recipients(email).len();
    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x03",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        output_handle_index = request.output_handle_index.unwrap_or(0),
        output_handle_id = handle,
        object_kind = "message",
        open_message_source = source,
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = debug_role_for_folder_id(folder_id),
        item_id = %format!("0x{message_id:016x}"),
        subject_chars = email.subject.chars().count(),
        body_text_bytes = email.body_text.len(),
        body_text_chars = email.body_text.chars().count(),
        body_html_bytes = email
            .body_html_sanitized
            .as_ref()
            .map(|body| body.len())
            .unwrap_or(0),
        recipient_count,
        open_recipient_column_count = 0,
        open_recipient_row_count = recipient_count.min(u8::MAX as usize),
        has_attachments = email.has_attachments,
        unread = email.unread,
        size_octets = email.size_octets,
        response_rop_bytes = response_len,
        "rca debug mapi open message"
    );
}

pub(in crate::mapi::dispatch) fn log_message_getprops_response_debug(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    request_id: &str,
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    property_response: &[u8],
) {
    let Some(MapiObject::Message {
        folder_id,
        message_id,
        saved_email,
        ..
    }) = object
    else {
        return;
    };
    let (message_source, email) = if let Some(email) =
        message_for_id(*folder_id, *message_id, mailboxes, emails)
    {
        ("mailbox", Some(email))
    } else if let Some(message) = search_folder_message_for_id(snapshot, *folder_id, *message_id) {
        ("search_folder", Some(&message.email))
    } else if let Some(saved) = saved_email.as_ref() {
        ("saved_handle", Some(&saved.email))
    } else {
        ("missing", None)
    };
    let property_tags = request.property_tags();
    let materialization =
        summarize_message_getprops_materialization(&property_tags, property_response);
    if *folder_id == INBOX_FOLDER_ID {
        session
            .post_hierarchy_actions
            .visible_inbox_message_getprops_not_found_count = materialization.not_found_count;
        session
            .post_hierarchy_actions
            .last_visible_inbox_message_getprops_context = format!(
            "request_id={request_id};handle={};folder=0x{folder_id:016x};message=0x{message_id:016x};source={message_source};property_tag_count={};returned_value_count={};problem_count={};not_found_count={};first_problem_tags={};response_bytes={}",
            request.input_handle_index().unwrap_or(0),
            property_tags.len(),
            materialization.returned_value_count,
            materialization.problem_count,
            materialization.not_found_count,
            materialization.first_problem_tags,
            property_response.len(),
        );
        session.record_outlook_view_failure_trace_event(format!(
            "visible_inbox_message_getprops:{}",
            session
                .post_hierarchy_actions
                .last_visible_inbox_message_getprops_context
        ));
    }
    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x07",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        object_kind = "message",
        message_source = message_source,
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = debug_role_for_folder_id(*folder_id),
        item_id = %format!("0x{message_id:016x}"),
        requested_property_tag_count = property_tags.len(),
        requested_property_tags = %format_debug_property_tags(&property_tags),
        returned_value_count = materialization.returned_value_count,
        property_problem_count = materialization.problem_count,
        not_found_property_problem_count = materialization.not_found_count,
        first_problem_tags = %materialization.first_problem_tags,
        requested_body_or_rendering_property_count = property_tags
            .iter()
            .filter(|tag| matches!(
                **tag,
                PID_TAG_BODY_STRING8
                    | PID_TAG_BODY_W
                    | PID_TAG_RTF_COMPRESSED
                    | PID_TAG_BODY_HTML_W
                    | PID_TAG_HTML_BINARY
                    | PID_TAG_NATIVE_BODY
                    | PID_TAG_RTF_IN_SYNC
            ))
            .count(),
        subject_chars = email.map(|email| email.subject.chars().count()).unwrap_or(0),
        body_text_bytes = email.map(|email| email.body_text.len()).unwrap_or(0),
        body_html_bytes = email
            .and_then(|email| email.body_html_sanitized.as_ref().map(|body| body.len()))
            .unwrap_or(0),
        recipient_count = email
            .map(message_recipients)
            .map(|recipients| recipients.len())
            .unwrap_or(0),
        has_attachments = email.map(|email| email.has_attachments).unwrap_or(false),
        unread = email.map(|email| email.unread).unwrap_or(false),
        response_rop_bytes = property_response.len(),
        "rca debug mapi message get properties response"
    );
}

#[derive(Debug, Default, PartialEq, Eq)]
struct MessageGetPropsMaterializationSummary {
    returned_value_count: usize,
    problem_count: usize,
    not_found_count: usize,
    first_problem_tags: String,
}

fn summarize_message_getprops_materialization(
    property_tags: &[u32],
    response: &[u8],
) -> MessageGetPropsMaterializationSummary {
    let mut summary = MessageGetPropsMaterializationSummary::default();
    let Some(row_kind) = response.get(6).copied() else {
        summary.first_problem_tags = "truncated".to_string();
        return summary;
    };
    if row_kind == 0 {
        summary.returned_value_count = property_tags.len();
        return summary;
    }
    if row_kind != 1 {
        summary.first_problem_tags = format!("unsupported_row_kind={row_kind}");
        return summary;
    }
    let mut cursor = Cursor::new(response.get(7..).unwrap_or_default());
    let mut first_problem_tags = Vec::new();
    for tag in property_tags {
        let Ok(flag) = cursor.read_u8() else {
            first_problem_tags.push(format!("{tag:#010x}:truncated"));
            break;
        };
        match flag {
            0 => match parse_property_value_for_tag(&mut cursor, *tag) {
                Ok(_) => summary.returned_value_count += 1,
                Err(error) => {
                    summary.problem_count += 1;
                    if first_problem_tags.len() < 12 {
                        first_problem_tags.push(format!("{tag:#010x}:parse_error={error}"));
                    }
                    break;
                }
            },
            0x0A => {
                let error = cursor.read_u32().unwrap_or(0);
                summary.problem_count += 1;
                if error == 0x8004_010F {
                    summary.not_found_count += 1;
                }
                if first_problem_tags.len() < 12 {
                    first_problem_tags.push(format!("{tag:#010x}:{error:#010x}"));
                }
            }
            other => {
                summary.problem_count += 1;
                if first_problem_tags.len() < 12 {
                    first_problem_tags.push(format!("{tag:#010x}:flag={other:#04x}"));
                }
                break;
            }
        }
    }
    summary.first_problem_tags = first_problem_tags.join(",");
    summary
}

pub(in crate::mapi::dispatch) fn normal_message_debug_property_value(
    email: &JmapEmail,
    property_tag: u32,
) -> Option<MapiValue> {
    if property_tag == PID_TAG_HTML_BINARY {
        return email
            .body_html_sanitized
            .as_ref()
            .map(|value| MapiValue::Binary(value.clone().into_bytes()));
    }

    let property_tag = if property_tag == OUTLOOK_VIEW_DESCRIPTOR_NAMED_STRING_PLACEHOLDER_TAG {
        PID_NAME_KEYWORDS_TAG
    } else {
        property_tag
    };

    match canonical_property_storage_tag(property_tag) {
        PID_TAG_MID | PID_TAG_INST_ID => Some(MapiValue::U64(mapi_message_id(email))),
        PID_TAG_INSTANCE_NUM | PID_TAG_DEPTH => Some(MapiValue::U32(0)),
        PID_TAG_ROW_TYPE => Some(MapiValue::U32(1)),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
            Some(MapiValue::String(email.subject.clone()))
        }
        PID_TAG_MESSAGE_CLASS_W | PID_TAG_ORIGINAL_MESSAGE_CLASS_W => Some(MapiValue::String(
            message_class_for_email(email).to_string(),
        )),
        PID_TAG_CREATION_TIME
        | PID_TAG_MESSAGE_DELIVERY_TIME
        | PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&email.received_at),
        )),
        PID_TAG_CLIENT_SUBMIT_TIME => {
            Some(MapiValue::U64(email_client_submit_time_filetime(email)))
        }
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_ACCESS_LEVEL => Some(MapiValue::U32(1)),
        PID_TAG_IMPORTANCE => Some(MapiValue::U32(1)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(message_flags(email))),
        PID_TAG_READ => Some(MapiValue::Bool(!email.unread)),
        PID_TAG_MESSAGE_SIZE => Some(MapiValue::U32(
            email.size_octets.clamp(0, u32::MAX as i64) as u32
        )),
        PID_TAG_SENDER_NAME_W => Some(MapiValue::String(email_sender_name(email).to_string())),
        PID_TAG_SENDER_ADDRESS_TYPE_W => Some(MapiValue::String("SMTP".to_string())),
        PID_TAG_SENDER_EMAIL_ADDRESS_W | PID_TAG_SENDER_SMTP_ADDRESS_W => {
            Some(MapiValue::String(email_sender_address(email).to_string()))
        }
        PID_TAG_SENT_REPRESENTING_NAME_W => Some(MapiValue::String(
            email_sent_representing_name(email).to_string(),
        )),
        PID_TAG_SENT_REPRESENTING_ENTRY_ID => {
            Some(MapiValue::Binary(sent_representing_entry_id(email)))
        }
        PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W => Some(MapiValue::String("SMTP".to_string())),
        PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W | PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W => {
            Some(MapiValue::String(
                email_sent_representing_address(email).to_string(),
            ))
        }
        PID_TAG_DISPLAY_TO_W => Some(MapiValue::String(display_to(email))),
        PID_TAG_DISPLAY_CC_W => Some(MapiValue::String(display_cc(email))),
        PID_TAG_DISPLAY_BCC_W => Some(MapiValue::String(display_bcc(email))),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(email.has_attachments)),
        PID_TAG_RTF_IN_SYNC => Some(MapiValue::Bool(false)),
        PID_TAG_BODY_W => Some(MapiValue::String(email.body_text.clone())),
        PID_TAG_RTF_COMPRESSED => Some(MapiValue::Binary(uncompressed_rtf_body(&email.body_text))),
        PID_TAG_BODY_HTML_W => email.body_html_sanitized.clone().map(MapiValue::String),
        PID_TAG_NATIVE_BODY => Some(MapiValue::U32(native_body_format(email))),
        PID_TAG_INTERNET_CODEPAGE => Some(MapiValue::U32(65001)),
        PID_TAG_MESSAGE_LOCALE_ID => Some(MapiValue::U32(0x0409)),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(mapi_message_id(email)),
        )),
        PID_TAG_INTERNET_MESSAGE_ID_W => Some(MapiValue::String(
            email.internet_message_id.clone().unwrap_or_default(),
        )),
        PID_NAME_CONTENT_CLASS_W_TAG => {
            Some(MapiValue::String("urn:content-classes:message".to_string()))
        }
        tag => email_property_value(email, tag),
    }
}

pub(in crate::mapi::dispatch) fn format_normal_message_debug_value(
    property_tag: u32,
    value: &MapiValue,
) -> String {
    match (canonical_property_storage_tag(property_tag), value) {
        (PID_TAG_RTF_COMPRESSED | PID_TAG_HTML_BINARY, MapiValue::Binary(value)) => {
            format!("binary:bytes={}", value.len())
        }
        _ => format_debug_mapi_value(value),
    }
}
