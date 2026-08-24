use super::*;

const EMAIL_RSS_NAMED_PROPERTY_TAGS: &[u32] = &[
    PID_LID_POST_RSS_CHANNEL_LINK_W_TAG,
    PID_LID_POST_RSS_ITEM_LINK_W_TAG,
    PID_LID_POST_RSS_ITEM_HASH_TAG,
    PID_LID_POST_RSS_ITEM_GUID_W_TAG,
    PID_LID_POST_RSS_CHANNEL_W_TAG,
    PID_LID_POST_RSS_ITEM_XML_W_TAG,
    PID_LID_POST_RSS_SUBSCRIPTION_W_TAG,
];

pub(crate) fn email_generated_property_tags(email: &JmapEmail) -> Vec<u32> {
    let mut tags = email_meeting_property_tags(email);
    if !email.categories.is_empty() {
        tags.extend([PID_TAG_HAS_NAMED_PROPERTIES, PID_NAME_KEYWORDS_TAG]);
    }
    if email.mailbox_role == "rss_feeds" {
        tags.push(PID_TAG_HAS_NAMED_PROPERTIES);
        tags.extend_from_slice(EMAIL_RSS_NAMED_PROPERTY_TAGS);
    }
    tags.sort_unstable();
    tags.dedup();
    tags
}

pub(crate) fn email_has_named_properties(email: &JmapEmail) -> bool {
    email_generated_property_tags(email)
        .into_iter()
        .any(|property_tag| MapiPropertyTag::new(property_tag).property_id() >= 0x8000)
}

fn calendar_response_subject_prefix(email: &JmapEmail) -> Option<&'static str> {
    let response = email.calendar_meeting_response.as_ref()?;
    match response.method.as_str() {
        "COUNTER" => Some("New Time Proposed: "),
        "REPLY" => match response.partstat.as_str() {
            "accepted" => Some("Accepted: "),
            "tentative" => Some("Tentative: "),
            "declined" => Some("Declined: "),
            _ => None,
        },
        _ => None,
    }
}

pub(crate) fn email_subject_prefix(email: &JmapEmail) -> &'static str {
    calendar_response_subject_prefix(email).unwrap_or_default()
}

pub(crate) fn email_normalized_subject(email: &JmapEmail) -> &str {
    email
        .subject
        .strip_prefix(email_subject_prefix(email))
        .unwrap_or(&email.subject)
}

pub(super) fn calendar_response_icon_index(email: &JmapEmail) -> Option<u32> {
    let response = email.calendar_meeting_response.as_ref()?;
    match response.method.as_str() {
        "COUNTER" => Some(0x0000_0407),
        "REPLY" => match response.partstat.as_str() {
            "accepted" => Some(0x0000_0405),
            "declined" => Some(0x0000_0406),
            "tentative" => Some(0x0000_0407),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn calendar_meeting_owner_appointment_id(email: &JmapEmail) -> Option<u32> {
    let start = email
        .calendar_meeting_request
        .as_ref()
        .map(|request| request.meeting_start.as_str())
        .or_else(|| {
            email
                .calendar_meeting_response
                .as_ref()
                .and_then(|response| response.meeting_start.as_deref())
        })?;
    Some(super::calendar::owner_appointment_id_from_filetime(
        mapi_mailstore::filetime_from_rfc3339_utc(start),
    ))
}

pub(super) fn transport_headers(email: &JmapEmail) -> String {
    let mut headers = Vec::new();
    if let Some(message_id) = email.internet_message_id.as_deref() {
        headers.push(format!("Message-ID: {message_id}"));
    }
    headers.push(format!(
        "From: {}",
        transport_header_mailbox(email.from_display.as_deref(), &email.from_address)
    ));
    if let Some(sender_address) = email.sender_address.as_deref() {
        headers.push(format!(
            "Sender: {}",
            transport_header_mailbox(email.sender_display.as_deref(), sender_address)
        ));
    }
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

fn transport_header_mailbox(display_name: Option<&str>, address: &str) -> String {
    match display_name.map(str::trim) {
        Some(display_name) if !display_name.is_empty() && display_name != address => {
            format!("{display_name} <{address}>")
        }
        _ => address.to_string(),
    }
}
