use super::{AttachmentUploadInput, SubmitMessageInput};
use uuid::Uuid;

pub(super) fn render_submission_raw_message(
    from_address: &str,
    input: &SubmitMessageInput,
    body_text: &str,
    attachments: &[AttachmentUploadInput],
) -> String {
    let Some(calendar) = attachments.iter().find(|attachment| {
        attachment
            .media_type
            .trim()
            .to_ascii_lowercase()
            .starts_with("text/calendar;")
    }) else {
        return format!(
            "From: {from_address}\r\nSubject: {}\r\n\r\n{body_text}",
            input.subject
        );
    };
    let boundary = format!("lpe-calendar-{}", Uuid::new_v4());
    let mut headers = vec![format!("From: {from_address}")];
    if !input.to.is_empty() {
        headers.push(format!("To: {}", recipient_header(&input.to)));
    }
    if !input.cc.is_empty() {
        headers.push(format!("Cc: {}", recipient_header(&input.cc)));
    }
    headers.extend([
        format!("Subject: {}", input.subject),
        "MIME-Version: 1.0".to_string(),
        "Content-Class: urn:content-classes:calendarmessage".to_string(),
        format!("Content-Type: multipart/alternative; boundary=\"{boundary}\""),
        String::new(),
        format!("--{boundary}"),
        "Content-Type: text/plain; charset=UTF-8".to_string(),
        "Content-Transfer-Encoding: 8bit".to_string(),
        String::new(),
        body_text.to_string(),
        format!("--{boundary}"),
        format!("Content-Type: {}", calendar.media_type.trim()),
        "Content-Transfer-Encoding: 8bit".to_string(),
        "Content-Disposition: inline; filename=\"invite.ics\"".to_string(),
        String::new(),
        String::from_utf8_lossy(&calendar.blob_bytes).into_owned(),
        format!("--{boundary}--"),
        String::new(),
    ]);
    headers.join("\r\n")
}

fn recipient_header(recipients: &[super::SubmittedRecipientInput]) -> String {
    recipients
        .iter()
        .map(|recipient| match recipient.display_name.as_deref() {
            Some(name) if !name.trim().is_empty() => {
                format!("{} <{}>", name.trim(), recipient.address)
            }
            _ => recipient.address.clone(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn input() -> SubmitMessageInput {
        SubmitMessageInput {
            draft_message_id: None,
            account_id: Uuid::nil(),
            submitted_by_account_id: Uuid::nil(),
            source: "test".to_string(),
            from_display: None,
            from_address: "organizer@example.test".to_string(),
            sender_display: None,
            sender_address: None,
            to: vec![super::super::SubmittedRecipientInput {
                address: "attendee@example.test".to_string(),
                display_name: Some("Attendee".to_string()),
            }],
            cc: Vec::new(),
            bcc: vec![super::super::SubmittedRecipientInput {
                address: "secret@example.test".to_string(),
                display_name: None,
            }],
            subject: "Review".to_string(),
            body_text: "Agenda".to_string(),
            body_html_sanitized: None,
            internet_message_id: None,
            mime_blob_ref: None,
            size_octets: 6,
            unread: None,
            flagged: None,
            attachments: Vec::new(),
        }
    }

    #[test]
    fn calendar_invitation_renders_calendar_mime_without_bcc() {
        let input = input();
        let raw = render_submission_raw_message(
            "organizer@example.test",
            &input,
            "Agenda",
            &[AttachmentUploadInput {
                file_name: "invite.ics".to_string(),
                media_type: "text/calendar; method=REQUEST; charset=UTF-8".to_string(),
                disposition: Some("inline".to_string()),
                content_id: None,
                blob_bytes: b"BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nEND:VCALENDAR".to_vec(),
            }],
        );

        assert!(raw.contains("Content-Type: multipart/alternative"));
        assert!(raw.contains("Content-Class: urn:content-classes:calendarmessage"));
        assert!(raw.contains("Content-Type: text/calendar; method=REQUEST; charset=UTF-8"));
        assert!(raw.contains("METHOD:REQUEST"));
        assert!(raw.contains("To: Attendee <attendee@example.test>"));
        assert!(!raw.contains("secret@example.test"));
    }
}
