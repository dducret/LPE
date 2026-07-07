use super::*;

pub(in crate::mapi) fn email_property_value(
    email: &JmapEmail,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    if let Some(value) = rss_email_named_property_value(email, property_tag) {
        return Some(value);
    }
    if named_property_id_matches(property_tag, PID_NAME_KEYWORDS_TAG) {
        return Some(MapiValue::MultiString(email.categories.clone()));
    }
    match property_tag {
        PID_TAG_FOLDER_ID | PID_TAG_PARENT_FOLDER_ID => {
            Some(MapiValue::U64(mapi_folder_id_for_email(email)))
        }
        PID_TAG_MID => Some(MapiValue::U64(mapi_message_id(email))),
        PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W | PID_TAG_CONVERSATION_TOPIC_W => {
            Some(MapiValue::String(email.subject.clone()))
        }
        PID_TAG_ORIGINAL_SUBJECT_W => Some(MapiValue::String(email.subject.clone())),
        PID_TAG_MESSAGE_CLASS_W | PID_TAG_ORIGINAL_MESSAGE_CLASS_W => Some(MapiValue::String(
            message_class_for_email(email).to_string(),
        )),
        PID_TAG_CREATION_TIME
        | PID_TAG_MESSAGE_DELIVERY_TIME
        | PID_TAG_LAST_MODIFICATION_TIME
        | PID_TAG_LOCAL_COMMIT_TIME => Some(MapiValue::U64(
            mapi_mailstore::filetime_from_rfc3339_utc(&email.received_at),
        )),
        PID_TAG_CLIENT_SUBMIT_TIME => email
            .sent_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_ACCESS_LEVEL => Some(MapiValue::U32(1)),
        PID_TAG_IMPORTANCE => Some(MapiValue::U32(1)),
        PID_TAG_PRIORITY | PID_TAG_SENSITIVITY | PID_TAG_ORIGINAL_SENSITIVITY => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_ARCHIVE_PERIOD | PID_TAG_RETENTION_PERIOD | PID_TAG_RETENTION_FLAGS => {
            Some(MapiValue::U32(0))
        }
        PID_TAG_ALTERNATE_RECIPIENT_ALLOWED
        | PID_TAG_AUTO_FORWARDED
        | PID_TAG_DELETE_AFTER_SUBMIT
        | PID_TAG_ORIGINATOR_DELIVERY_REPORT_REQUESTED
        | PID_TAG_READ_RECEIPT_REQUESTED
        | PID_TAG_RECIPIENT_REASSIGNMENT_PROHIBITED => Some(MapiValue::Bool(false)),
        PID_TAG_REPLY_REQUESTED | PID_TAG_RESPONSE_REQUESTED => Some(MapiValue::Bool(false)),
        PID_TAG_PROCESSED => Some(MapiValue::Bool(false)),
        PID_TAG_DEFERRED_DELIVERY_TIME
        | PID_TAG_DEFERRED_SEND_TIME
        | PID_TAG_END_DATE
        | PID_TAG_EXPIRY_TIME
        | PID_TAG_ARCHIVE_DATE
        | PID_TAG_LAST_VERB_EXECUTION_TIME
        | PID_TAG_ORIGINAL_SUBMIT_TIME
        | PID_TAG_RETENTION_DATE
        | PID_TAG_REPLY_TIME
        | PID_TAG_REPORT_TIME => Some(MapiValue::U64(0)),
        PID_TAG_START_DATE => Some(MapiValue::U64(0)),
        PID_TAG_ORIGINAL_AUTHOR_ENTRY_ID
        | PID_TAG_ARCHIVE_TAG
        | PID_TAG_PARENT_KEY
        | PID_TAG_POLICY_TAG
        | PID_TAG_REPLY_RECIPIENT_ENTRIES
        | PID_TAG_REPORT_TAG
        | PID_TAG_START_DATE_ETC => Some(MapiValue::Binary(Vec::new())),
        PID_TAG_ORIGINAL_AUTHOR_NAME_W
        | PID_TAG_ORIGINAL_DISPLAY_BCC_W
        | PID_TAG_ORIGINAL_DISPLAY_CC_W
        | PID_TAG_ORIGINAL_DISPLAY_TO_W
        | PID_TAG_ORIGINAL_SENDER_NAME_W
        | PID_TAG_LAST_MODIFIER_NAME_W
        | PID_TAG_IN_REPLY_TO_ID_W
        | PID_TAG_INTERNET_REFERENCES_W
        | PID_TAG_NEXT_SEND_ACCOUNT_W
        | PID_TAG_PRIMARY_SEND_ACCOUNT_W
        | PID_TAG_REPORT_DISPOSITION_W
        | PID_TAG_REPLY_RECIPIENT_NAMES_W => Some(MapiValue::String(String::new())),
        PID_TAG_ICON_INDEX
        | PID_TAG_INTERNET_MAIL_OVERRIDE_FORMAT
        | PID_TAG_BLOCK_STATUS
        | PID_TAG_LAST_VERB_EXECUTED
        | PID_TAG_MESSAGE_EDITOR_FORMAT
        | PID_TAG_OWNER_APPOINTMENT_ID => Some(MapiValue::U32(0)),
        PID_TAG_SUBJECT_PREFIX_W => Some(MapiValue::String(String::new())),
        PID_TAG_MESSAGE_STATUS => Some(MapiValue::U32(0)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(message_flags(email))),
        PID_TAG_READ => Some(MapiValue::Bool(!email.unread)),
        PID_TAG_FLAG_STATUS => Some(MapiValue::U32(mapi_mailstore::canonical_flag_status(email))),
        PID_LID_OUTLOOK_APPOINTMENT_8F07_TAG | 0x8017_000B => Some(MapiValue::Bool(false)),
        PID_LID_OUTLOOK_COMMON_8514_TAG | PID_LID_OUTLOOK_COMMON_85EF_TAG => {
            Some(MapiValue::Bool(false))
        }
        PID_LID_PERCENT_COMPLETE_TAG => {
            Some(MapiValue::F64(email_percent_complete(email).to_bits()))
        }
        PID_TAG_FLAG_COMPLETE_TIME => email
            .followup_completed_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_TAG_FOLLOWUP_ICON => Some(MapiValue::I32(email.followup_icon)),
        PID_TAG_TODO_ITEM_FLAGS => Some(MapiValue::I32(email.todo_item_flags)),
        PID_TAG_SWAPPED_TODO_STORE => email
            .swapped_todo_store_id
            .map(|id| MapiValue::Binary(id.as_bytes().to_vec())),
        PID_TAG_SWAPPED_TODO_DATA => email
            .swapped_todo_data
            .as_ref()
            .map(|data| MapiValue::Binary(data.clone())),
        PID_LID_FLAG_REQUEST_W_TAG => Some(MapiValue::String(email.followup_request.clone())),
        PID_LID_TASK_START_DATE_TAG => email
            .followup_start_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_LID_TASK_DUE_DATE_TAG => email
            .followup_due_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_LID_REMINDER_SET_TAG => Some(MapiValue::Bool(email.reminder_set)),
        PID_LID_REMINDER_TIME_TAG | PID_LID_REMINDER_SIGNAL_TIME_TAG => email
            .reminder_at
            .as_deref()
            .map(|value| MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value))),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(email.size_octets)),
        PID_TAG_MESSAGE_SIZE_EXTENDED => Some(mapi_message_size_extended_value(email.size_octets)),
        OUTLOOK_COMPACT_VIEW_AUXILIARY_FLAGS_TAG => Some(MapiValue::U32(0)),
        OUTLOOK_MESSAGES_VIEW_BINARY_0F03_TAG => Some(MapiValue::Binary(Vec::new())),
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
        PID_TAG_BODY_HTML_W => email
            .body_html_sanitized
            .clone()
            .or_else(|| html_body_from_plain_text(&email.body_text))
            .map(MapiValue::String),
        PID_TAG_HTML_BINARY => email
            .body_html_sanitized
            .clone()
            .or_else(|| html_body_from_plain_text(&email.body_text))
            .map(|value| MapiValue::Binary(value.into_bytes())),
        PID_TAG_NATIVE_BODY => Some(MapiValue::U32(native_body_format(email))),
        PID_TAG_INTERNET_CODEPAGE => Some(MapiValue::U32(65001)),
        PID_TAG_MESSAGE_LOCALE_ID => Some(MapiValue::U32(0x0409)),
        PID_TAG_CONVERSATION_INDEX => Some(MapiValue::Binary(conversation_index_for_uuid(
            email.thread_id,
        ))),
        PID_TAG_CONVERSATION_ID => Some(MapiValue::Binary(email.thread_id.as_bytes().to_vec())),
        PID_TAG_CONVERSATION_INDEX_TRACKING => Some(MapiValue::Bool(false)),
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => {
            let object_id = mapi_message_id(email);
            Some(MapiValue::Binary(
                crate::mapi::identity::instance_key_for_object_id(object_id),
            ))
        }
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &email.id,
        ))),
        PID_TAG_SEARCH_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &email.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_mailbox_role(&email.mailbox_id, &email.mailbox_role),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(
                mapi_mailstore::canonical_message_change_number(email),
            ),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => {
            Some(MapiValue::Binary(mapi_mailstore::predecessor_change_list(
                mapi_mailstore::canonical_message_change_number(email),
            )))
        }
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(
            mapi_mailstore::canonical_message_change_number(email),
        )),
        PID_TAG_INTERNET_MESSAGE_ID_W => email.internet_message_id.clone().map(MapiValue::String),
        PID_NAME_CONTENT_CLASS_W_TAG => {
            Some(MapiValue::String("urn:content-classes:message".to_string()))
        }
        PID_TAG_TRANSPORT_MESSAGE_HEADERS_W => Some(MapiValue::String(transport_headers(email))),
        _ => None,
    }
}

pub(in crate::mapi) fn email_sender_name(email: &JmapEmail) -> &str {
    email
        .sender_display
        .as_deref()
        .or(email.sender_address.as_deref())
        .or(email.from_display.as_deref())
        .unwrap_or(&email.from_address)
}

pub(in crate::mapi) fn email_sender_address(email: &JmapEmail) -> &str {
    email
        .sender_address
        .as_deref()
        .unwrap_or(&email.from_address)
}

pub(in crate::mapi) fn email_sent_representing_name(email: &JmapEmail) -> &str {
    email.from_display.as_deref().unwrap_or(&email.from_address)
}

pub(in crate::mapi) fn email_sent_representing_address(email: &JmapEmail) -> &str {
    &email.from_address
}

pub(in crate::mapi) fn conversation_index_for_uuid(conversation_id: Uuid) -> Vec<u8> {
    let mut value = Vec::with_capacity(22);
    value.extend_from_slice(&[0x01, 0, 0, 0, 0, 0]);
    value.extend_from_slice(conversation_id.as_bytes());
    value
}

pub(in crate::mapi) fn message_class_for_email(email: &JmapEmail) -> &'static str {
    if email.mailbox_role == "rss_feeds" {
        "IPM.Post.RSS"
    } else {
        "IPM.Note"
    }
}

pub(in crate::mapi) fn native_body_format(email: &JmapEmail) -> u32 {
    if email
        .body_html_sanitized
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
    {
        3
    } else if email.body_text.trim().is_empty() {
        0
    } else {
        1
    }
}

pub(in crate::mapi) fn html_body_from_plain_text(body_text: &str) -> Option<String> {
    if body_text.trim().is_empty() {
        return None;
    }
    let mut html = String::from("<html><body>");
    for ch in body_text.chars() {
        match ch {
            '&' => html.push_str("&amp;"),
            '<' => html.push_str("&lt;"),
            '>' => html.push_str("&gt;"),
            '"' => html.push_str("&quot;"),
            '\'' => html.push_str("&#39;"),
            '\r' => {}
            '\n' => html.push_str("<br>"),
            _ => html.push(ch),
        }
    }
    html.push_str("</body></html>");
    Some(html)
}

pub(in crate::mapi) fn uncompressed_rtf_body(body_text: &str) -> Vec<u8> {
    let mut rtf = String::from("{\\rtf1\\ansi\\deff0{\\fonttbl{\\f0\\fnil Segoe UI;}}\\f0\\fs20 ");
    append_rtf_escaped_text(&mut rtf, body_text);
    rtf.push('}');
    rtf_uncompressed_container(rtf.as_bytes())
}

fn append_rtf_escaped_text(output: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => output.push_str("\\\\"),
            '{' => output.push_str("\\{"),
            '}' => output.push_str("\\}"),
            '\r' => {}
            '\n' => output.push_str("\\par "),
            '\t' => output.push_str("\\tab "),
            ' '..='~' => output.push(ch),
            _ => {
                let mut units = [0; 2];
                for unit in ch.encode_utf16(&mut units) {
                    let signed = *unit as i16;
                    output.push_str(&format!("\\u{signed}?"));
                }
            }
        }
    }
}

fn rtf_uncompressed_container(raw: &[u8]) -> Vec<u8> {
    let raw_size = u32::try_from(raw.len()).expect("RTF body too large for MAPI");
    let compressed_size = raw_size
        .checked_add(12)
        .expect("RTF body too large for MAPI");
    let mut value = Vec::with_capacity(raw.len() + 16);
    value.extend_from_slice(&compressed_size.to_le_bytes());
    value.extend_from_slice(&raw_size.to_le_bytes());
    value.extend_from_slice(&0x414C_454D_u32.to_le_bytes());
    value.extend_from_slice(&0_u32.to_le_bytes());
    value.extend_from_slice(raw);
    value
}

fn transport_headers(email: &JmapEmail) -> String {
    let mut headers = Vec::new();
    if let Some(message_id) = email.internet_message_id.as_deref() {
        headers.push(format!("Message-ID: {message_id}"));
    }
    headers.push(format!(
        "From: {}",
        email.from_display.as_deref().unwrap_or(&email.from_address)
    ));
    let to = display_to(email);
    if !to.is_empty() {
        headers.push(format!("To: {to}"));
    }
    let cc = display_cc(email);
    if !cc.is_empty() {
        headers.push(format!("Cc: {cc}"));
    }
    headers.push(format!("Subject: {}", email.subject));
    headers.join("\r\n")
}

pub(in crate::mapi) fn conversation_id_from_index(value: &[u8]) -> Option<Uuid> {
    let bytes: [u8; 16] = value.get(6..22)?.try_into().ok()?;
    Some(Uuid::from_bytes(bytes))
}

pub(in crate::mapi) fn conversation_action_subject(
    action: &lpe_storage::ConversationAction,
) -> String {
    let subject = action.subject.trim();
    if subject.is_empty() {
        "Conv.Action".to_string()
    } else if subject.starts_with("Conv.Action") {
        subject.to_string()
    } else {
        format!("Conv.Action: {subject}")
    }
}

pub(in crate::mapi) fn conversation_action_size(action: &lpe_storage::ConversationAction) -> usize {
    conversation_action_subject(action)
        .len()
        .saturating_add(action.categories_json.len())
        .saturating_add(
            action
                .move_folder_entry_id
                .as_ref()
                .map(Vec::len)
                .unwrap_or_default(),
        )
        .saturating_add(
            action
                .move_store_entry_id
                .as_ref()
                .map(Vec::len)
                .unwrap_or_default(),
        )
}

fn rss_email_named_property_value(email: &JmapEmail, property_tag: u32) -> Option<MapiValue> {
    if email.mailbox_role != "rss_feeds" {
        return None;
    }
    match property_tag {
        PID_LID_POST_RSS_CHANNEL_LINK_W_TAG => Some(MapiValue::String(String::new())),
        PID_LID_POST_RSS_ITEM_LINK_W_TAG => Some(MapiValue::String(String::new())),
        PID_LID_POST_RSS_ITEM_HASH_TAG => Some(MapiValue::I32(
            (mapi_mailstore::canonical_message_change_number(email) & 0x7FFF_FFFF) as i32,
        )),
        PID_LID_POST_RSS_ITEM_GUID_W_TAG => Some(MapiValue::String(
            email
                .internet_message_id
                .clone()
                .unwrap_or_else(|| email.id.to_string()),
        )),
        PID_LID_POST_RSS_CHANNEL_W_TAG => Some(MapiValue::String(email.mailbox_name.clone())),
        PID_LID_POST_RSS_ITEM_XML_W_TAG => Some(MapiValue::String(email.body_text.clone())),
        PID_LID_POST_RSS_SUBSCRIPTION_W_TAG => Some(MapiValue::String(email.mailbox_name.clone())),
        _ => None,
    }
}

fn email_percent_complete(email: &JmapEmail) -> f64 {
    if email.followup_flag_status == "complete" {
        1.0
    } else {
        0.0
    }
}

pub(in crate::mapi) fn mapi_message_size_value(size_octets: i64) -> MapiValue {
    MapiValue::U32(size_octets.clamp(0, i64::from(u32::MAX)) as u32)
}

pub(in crate::mapi) fn mapi_message_size_extended_value(size_octets: i64) -> MapiValue {
    MapiValue::I64(size_octets.max(0))
}

pub(in crate::mapi) fn jmap_import_from_pending_message(
    principal: &AccountPrincipal,
    mailbox: &JmapMailbox,
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
    attachments: Vec<AttachmentUploadInput>,
) -> JmapImportedEmailInput {
    let subject = pending_text_property(
        properties,
        &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
    );
    let body_text = pending_body_text_property(properties);
    let from_address =
        optional_pending_text_property(properties, &[PID_TAG_SENDER_EMAIL_ADDRESS_W])
            .unwrap_or_else(|| principal.email.clone());
    let from_display = optional_pending_text_property(properties, &[PID_TAG_SENDER_NAME_W])
        .or_else(|| Some(principal.display_name.clone()));
    let internet_message_id =
        optional_pending_text_property(properties, &[PID_TAG_INTERNET_MESSAGE_ID_W]);
    let thread_id = match properties.get(&PID_TAG_CONVERSATION_INDEX) {
        Some(MapiValue::Binary(value)) => conversation_id_from_index(value),
        _ => None,
    };
    let size_octets = subject
        .len()
        .saturating_add(body_text.len())
        .min(i64::MAX as usize) as i64;
    let (to, cc, bcc) = pending_recipients_for_import(recipients);

    JmapImportedEmailInput {
        account_id: principal.account_id,
        submitted_by_account_id: principal.account_id,
        mailbox_id: mailbox.id,
        source: "mapi-save-message".to_string(),
        raw_message: None,
        from_display,
        from_address,
        sender_display: None,
        sender_address: None,
        to,
        cc,
        bcc,
        subject,
        body_text,
        body_html_sanitized: pending_html_property(properties),
        internet_message_id,
        mime_blob_ref: format!("mapi-save-message:{}", Uuid::new_v4()),
        size_octets,
        received_at: None,
        thread_id,
        attachments,
    }
}

pub(in crate::mapi) fn pending_recipients_for_import(
    recipients: &[PendingRecipient],
) -> (
    Vec<SubmittedRecipientInput>,
    Vec<SubmittedRecipientInput>,
    Vec<SubmittedRecipientInput>,
) {
    let mut to = Vec::new();
    let mut cc = Vec::new();
    let mut bcc = Vec::new();
    for recipient in recipients {
        let input = SubmittedRecipientInput {
            address: recipient.address.clone(),
            display_name: recipient.display_name.clone(),
        };
        match recipient.recipient_type {
            0x02 => cc.push(input),
            0x03 => bcc.push(input),
            _ => to.push(input),
        }
    }
    (to, cc, bcc)
}

pub(in crate::mapi) fn mapi_submit_from_pending_message(
    principal: &AccountPrincipal,
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
) -> SubmitMessageInput {
    let subject = pending_text_property(
        properties,
        &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
    );
    let body_text = pending_body_text_property(properties);
    let from_address =
        optional_pending_submit_address(properties, &[PID_TAG_SENDER_EMAIL_ADDRESS_W])
            .unwrap_or_else(|| principal.email.clone());
    let from_display = optional_pending_text_property(properties, &[PID_TAG_SENDER_NAME_W])
        .or_else(|| Some(principal.display_name.clone()));
    let internet_message_id =
        optional_pending_text_property(properties, &[PID_TAG_INTERNET_MESSAGE_ID_W]);
    let (to, cc, bcc) = pending_recipients_for_import(recipients);

    SubmitMessageInput {
        draft_message_id: None,
        account_id: principal.account_id,
        submitted_by_account_id: principal.account_id,
        source: "mapi-submit-message".to_string(),
        from_display,
        from_address,
        sender_display: None,
        sender_address: None,
        to,
        cc,
        bcc,
        subject,
        body_text,
        body_html_sanitized: pending_html_property(properties),
        internet_message_id,
        mime_blob_ref: Some(format!("mapi-submit-message:{}", Uuid::new_v4())),
        size_octets: pending_message_size(properties),
        unread: Some(false),
        flagged: Some(false),
        attachments: Vec::new(),
    }
}

fn optional_pending_submit_address(
    properties: &HashMap<u32, MapiValue>,
    tags: &[u32],
) -> Option<String> {
    optional_pending_text_property(properties, tags).and_then(normalize_mapi_submit_address)
}

pub(in crate::mapi) fn normalize_mapi_submit_address(value: String) -> Option<String> {
    let trimmed = value.trim();
    let address = trimmed
        .strip_prefix("SMTP:")
        .or_else(|| trimmed.strip_prefix("smtp:"))
        .unwrap_or(trimmed)
        .trim();
    let normalized = lpe_storage::normalize_mailbox_email(address);
    (!normalized.is_empty()).then_some(normalized)
}

pub(in crate::mapi) fn mapi_submit_from_email(
    principal: &AccountPrincipal,
    email: &JmapEmail,
    attachments: Vec<AttachmentUploadInput>,
) -> SubmitMessageInput {
    SubmitMessageInput {
        draft_message_id: Some(email.id),
        account_id: principal.account_id,
        submitted_by_account_id: principal.account_id,
        source: "mapi-submit-message".to_string(),
        from_display: email.from_display.clone(),
        from_address: email.from_address.clone(),
        sender_display: email.sender_display.clone(),
        sender_address: email.sender_address.clone(),
        to: submitted_recipients_from_addresses(&email.to),
        cc: submitted_recipients_from_addresses(&email.cc),
        bcc: submitted_recipients_from_addresses(&email.bcc),
        subject: email.subject.clone(),
        body_text: email.body_text.clone(),
        body_html_sanitized: email.body_html_sanitized.clone(),
        internet_message_id: email.internet_message_id.clone(),
        mime_blob_ref: email.mime_blob_ref.clone(),
        size_octets: i64::try_from(email.size_octets).unwrap_or(i64::MAX),
        unread: Some(email.unread),
        flagged: Some(email.flagged),
        attachments,
    }
}

pub(in crate::mapi) fn submitted_recipients_from_addresses(
    addresses: &[JmapEmailAddress],
) -> Vec<SubmittedRecipientInput> {
    addresses
        .iter()
        .map(|address| SubmittedRecipientInput {
            address: address.address.clone(),
            display_name: address.display_name.clone(),
        })
        .collect()
}

pub(in crate::mapi) fn submitted_mapi_folder_id(
    submitted: &SubmittedMessage,
    mailboxes: &[JmapMailbox],
) -> u64 {
    mailboxes
        .iter()
        .find(|mailbox| mailbox.id == submitted.sent_mailbox_id)
        .map(mapi_folder_id)
        .unwrap_or(SENT_FOLDER_ID)
}

pub(in crate::mapi) fn apply_pending_recipient_changes(
    recipients: &mut Vec<PendingRecipient>,
    changes: Vec<PendingRecipientChange>,
) {
    for change in changes {
        match change {
            PendingRecipientChange::Delete(row_id) => {
                recipients.retain(|recipient| recipient.row_id != row_id);
            }
            PendingRecipientChange::Upsert(recipient) => {
                if let Some(existing) = recipients
                    .iter_mut()
                    .find(|existing| existing.row_id == recipient.row_id)
                {
                    *existing = recipient;
                } else {
                    recipients.push(recipient);
                }
            }
        }
    }
    recipients.sort_by_key(|recipient| recipient.row_id);
}

pub(in crate::mapi) async fn apply_canonical_message_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    message_id: u64,
    values: Vec<(u32, MapiValue)>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Result<()>
where
    S: ExchangeStore,
{
    let email = message_for_id(folder_id, message_id, mailboxes, emails)
        .ok_or_else(|| anyhow!("canonical MAPI message was not found"))?;
    let mut subject = None;
    let mut body_text = None;
    let mut followup_values = Vec::new();
    for (tag, value) in values {
        match tag {
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                subject = Some(
                    value
                        .into_text()
                        .ok_or_else(|| anyhow!("invalid PidTagSubject value"))?,
                );
            }
            PID_TAG_BODY_W => {
                body_text = Some(
                    value
                        .into_text()
                        .ok_or_else(|| anyhow!("invalid PidTagBody value"))?,
                );
            }
            _ => followup_values.push((tag, value)),
        }
    }

    if subject.is_some() || body_text.is_some() {
        store
            .update_jmap_email_content(
                principal.account_id,
                email.id,
                subject,
                body_text,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-set-message-content".to_string(),
                    subject: format!("message:{}", email.id),
                },
            )
            .await?;
    }
    let update = message_followup_update_from_mapi_values(followup_values)?;
    if message_followup_update_is_empty(&update) {
        return Ok(());
    }

    store
        .update_jmap_email_followup_flags(
            principal.account_id,
            email.id,
            update,
            AuditEntryInput {
                actor: principal.email.clone(),
                action: "mapi-set-message-properties".to_string(),
                subject: format!("message:{}", email.id),
            },
        )
        .await?;
    Ok(())
}

pub(in crate::mapi) fn message_followup_update_from_mapi_values(
    values: Vec<(u32, MapiValue)>,
) -> Result<lpe_storage::JmapEmailFollowupUpdate> {
    let mut update = lpe_storage::JmapEmailFollowupUpdate::default();
    for (tag, value) in values {
        match tag {
            PID_TAG_MESSAGE_FLAGS => {
                let flags = value
                    .into_u32()
                    .ok_or_else(|| anyhow!("invalid PidTagMessageFlags value"))?;
                update.unread = Some(flags & MSGFLAG_READ == 0);
            }
            PID_TAG_FLAG_STATUS => {
                let status = match value
                    .as_i64()
                    .ok_or_else(|| anyhow!("invalid PidTagFlagStatus value"))?
                {
                    0 => "none",
                    1 => "complete",
                    2 => "flagged",
                    _ => return Err(anyhow!("invalid PidTagFlagStatus value")),
                };
                update.flagged = Some(status != "none");
                update.followup_flag_status = Some(status.to_string());
            }
            PID_TAG_FOLLOWUP_ICON => {
                update.followup_icon = Some(
                    value
                        .as_i64()
                        .and_then(|value| i32::try_from(value).ok())
                        .ok_or_else(|| anyhow!("invalid PidTagFollowupIcon value"))?,
                );
            }
            PID_TAG_TODO_ITEM_FLAGS => {
                update.todo_item_flags = Some(
                    value
                        .as_i64()
                        .and_then(|value| i32::try_from(value).ok())
                        .ok_or_else(|| anyhow!("invalid PidTagToDoItemFlags value"))?,
                );
            }
            PID_TAG_FLAG_COMPLETE_TIME => {
                update.followup_completed_at = Some(
                    value
                        .as_i64()
                        .and_then(filetime_to_rfc3339_utc)
                        .ok_or_else(|| anyhow!("invalid PidTagFlagCompleteTime value"))?,
                );
            }
            PID_LID_TASK_START_DATE_TAG => {
                update.followup_start_at = Some(
                    value
                        .as_i64()
                        .and_then(filetime_to_rfc3339_utc)
                        .ok_or_else(|| anyhow!("invalid PidLidTaskStartDate value"))?,
                );
            }
            PID_LID_TASK_DUE_DATE_TAG => {
                update.followup_due_at = Some(
                    value
                        .as_i64()
                        .and_then(filetime_to_rfc3339_utc)
                        .ok_or_else(|| anyhow!("invalid PidLidTaskDueDate value"))?,
                );
            }
            PID_LID_REMINDER_SET_TAG => {
                update.reminder_set = Some(
                    value
                        .as_bool()
                        .ok_or_else(|| anyhow!("invalid PidLidReminderSet value"))?,
                );
            }
            PID_LID_REMINDER_TIME_TAG | PID_LID_REMINDER_SIGNAL_TIME_TAG => {
                update.reminder_at = Some(
                    value
                        .as_i64()
                        .and_then(filetime_to_rfc3339_utc)
                        .ok_or_else(|| anyhow!("invalid reminder time value"))?,
                );
            }
            PID_LID_FLAG_REQUEST_W_TAG => {
                update.followup_request = Some(
                    value
                        .into_text()
                        .ok_or_else(|| anyhow!("invalid PidLidFlagRequest value"))?,
                );
            }
            PID_TAG_SWAPPED_TODO_STORE => {
                let MapiValue::Binary(bytes) = value else {
                    return Err(anyhow!("invalid PidTagSwappedToDoStore value"));
                };
                update.swapped_todo_store_id = Some(
                    Uuid::from_slice(&bytes)
                        .map_err(|_| anyhow!("invalid PidTagSwappedToDoStore value"))?,
                );
            }
            PID_TAG_SWAPPED_TODO_DATA => {
                let MapiValue::Binary(bytes) = value else {
                    return Err(anyhow!("invalid PidTagSwappedToDoData value"));
                };
                parse_swapped_todo_data(&bytes)
                    .map_err(|error| anyhow!("invalid PidTagSwappedToDoData value: {error}"))?;
                update.swapped_todo_data = Some(bytes);
            }
            PID_NAME_KEYWORDS_TAG => {
                update.categories = Some(categories_from_mapi_value(value)?);
            }
            PID_TAG_SOURCE_KEY | PID_TAG_CHANGE_KEY | PID_TAG_PREDECESSOR_CHANGE_LIST => {}
            _ => return Err(anyhow!("canonical MAPI message property is not mutable")),
        }
    }
    Ok(update)
}

pub(in crate::mapi) fn message_followup_update_is_empty(
    update: &lpe_storage::JmapEmailFollowupUpdate,
) -> bool {
    update.unread.is_none()
        && update.flagged.is_none()
        && update.followup_flag_status.is_none()
        && update.followup_icon.is_none()
        && update.todo_item_flags.is_none()
        && update.followup_request.is_none()
        && update.followup_start_at.is_none()
        && update.followup_due_at.is_none()
        && update.followup_completed_at.is_none()
        && update.reminder_set.is_none()
        && update.reminder_at.is_none()
        && update.reminder_dismissed_at.is_none()
        && update.swapped_todo_store_id.is_none()
        && update.swapped_todo_data.is_none()
        && update.categories.is_none()
}

pub(in crate::mapi) fn categories_from_mapi_value(value: MapiValue) -> Result<Vec<String>> {
    let mut categories = match value {
        MapiValue::MultiString(values) => values,
        MapiValue::String(value) => vec![value],
        _ => return Err(anyhow!("invalid PidNameKeywords value")),
    };
    for category in &mut categories {
        *category = category.trim().to_string();
    }
    categories.retain(|category| !category.is_empty());
    categories.sort();
    categories.dedup();
    Ok(categories)
}

pub(in crate::mapi) fn filetime_to_rfc3339_utc(filetime: i64) -> Option<String> {
    filetime_to_date_time(filetime).map(|(date, time)| format!("{date}T{time}:00Z"))
}

#[derive(Debug, PartialEq, Eq)]
pub(in crate::mapi) struct SwappedToDoData {
    pub(in crate::mapi) flags: u32,
    pub(in crate::mapi) todo_item_flags: Option<u32>,
    pub(in crate::mapi) flag_request: Option<String>,
    pub(in crate::mapi) start_minutes: Option<u32>,
    pub(in crate::mapi) due_minutes: Option<u32>,
    pub(in crate::mapi) reminder_minutes: Option<u32>,
    pub(in crate::mapi) reminder_set: Option<bool>,
}

pub(in crate::mapi) const SWAPPED_TODO_DATA_LEN: usize = 540;
pub(in crate::mapi) const SWAPPED_TODO_DATA_VERSION: u32 = 1;
const SWAPPED_TODO_NO_DATE: u32 = 0x5AE9_80E0;
pub(in crate::mapi) const SWAPPED_TODO_FLAG_TODO_ITEM: u32 = 0x0000_0001;
pub(in crate::mapi) const SWAPPED_TODO_FLAG_START_DATE: u32 = 0x0000_0008;
pub(in crate::mapi) const SWAPPED_TODO_FLAG_DUE_DATE: u32 = 0x0000_0010;
pub(in crate::mapi) const SWAPPED_TODO_FLAG_FLAG_TO: u32 = 0x0000_0020;
pub(in crate::mapi) const SWAPPED_TODO_FLAG_REMINDER_SET: u32 = 0x0000_0040;
pub(in crate::mapi) const SWAPPED_TODO_FLAG_REMINDER: u32 = 0x0000_0080;
const SWAPPED_TODO_KNOWN_FLAGS: u32 = SWAPPED_TODO_FLAG_TODO_ITEM
    | SWAPPED_TODO_FLAG_START_DATE
    | SWAPPED_TODO_FLAG_DUE_DATE
    | SWAPPED_TODO_FLAG_FLAG_TO
    | SWAPPED_TODO_FLAG_REMINDER_SET
    | SWAPPED_TODO_FLAG_REMINDER;

pub(in crate::mapi) fn parse_swapped_todo_data(bytes: &[u8]) -> Result<SwappedToDoData> {
    if bytes.len() != SWAPPED_TODO_DATA_LEN {
        return Err(anyhow!("expected {SWAPPED_TODO_DATA_LEN} bytes"));
    }
    let version = read_swapped_u32(bytes, 0)?;
    if version != SWAPPED_TODO_DATA_VERSION {
        return Err(anyhow!("unsupported version {version}"));
    }
    let flags = read_swapped_u32(bytes, 4)?;
    if flags & !SWAPPED_TODO_KNOWN_FLAGS != 0 {
        return Err(anyhow!(
            "unknown flags {:#010x}",
            flags & !SWAPPED_TODO_KNOWN_FLAGS
        ));
    }
    let todo_item_flags = (flags & SWAPPED_TODO_FLAG_TODO_ITEM != 0)
        .then(|| read_swapped_u32(bytes, 8))
        .transpose()?;
    let flag_request = if flags & SWAPPED_TODO_FLAG_FLAG_TO != 0 {
        Some(read_swapped_utf16z(
            bytes
                .get(12..524)
                .ok_or_else(|| anyhow!("truncated flag text"))?,
        )?)
    } else {
        None
    };
    let start_minutes =
        swapped_todo_minutes(bytes, 524, flags & SWAPPED_TODO_FLAG_START_DATE != 0)?;
    let due_minutes = swapped_todo_minutes(bytes, 528, flags & SWAPPED_TODO_FLAG_DUE_DATE != 0)?;
    let reminder_minutes =
        swapped_todo_minutes(bytes, 532, flags & SWAPPED_TODO_FLAG_REMINDER != 0)?;
    let reminder_set = if flags & SWAPPED_TODO_FLAG_REMINDER_SET != 0 {
        match read_swapped_u32(bytes, 536)? {
            0 => Some(false),
            1 => Some(true),
            value => return Err(anyhow!("invalid reminder boolean {value}")),
        }
    } else {
        None
    };
    Ok(SwappedToDoData {
        flags,
        todo_item_flags,
        flag_request,
        start_minutes,
        due_minutes,
        reminder_minutes,
        reminder_set,
    })
}

fn swapped_todo_minutes(bytes: &[u8], offset: usize, valid: bool) -> Result<Option<u32>> {
    if !valid {
        return Ok(None);
    }
    let value = read_swapped_u32(bytes, offset)?;
    Ok((value != SWAPPED_TODO_NO_DATE).then_some(value))
}

fn read_swapped_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| anyhow!("truncated u32"))?;
    Ok(u32::from_le_bytes(
        value.try_into().map_err(|_| anyhow!("invalid u32"))?,
    ))
}

fn read_swapped_utf16z(bytes: &[u8]) -> Result<String> {
    if bytes.len() % 2 != 0 {
        return Err(anyhow!("odd utf16 byte length"));
    }
    let units = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .take_while(|unit| *unit != 0)
        .collect::<Vec<_>>();
    String::from_utf16(&units).map_err(|_| anyhow!("invalid utf16 flag text"))
}
