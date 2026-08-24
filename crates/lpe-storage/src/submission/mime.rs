use super::{AttachmentUploadInput, SubmitMessageInput};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use lpe_domain::mail_format::{
    format_mailbox_address, quote_header_parameter, sanitize_header_value, DisplayNamePolicy,
};
use uuid::Uuid;

pub(super) fn render_submission_raw_message(
    from_address: &str,
    input: &SubmitMessageInput,
    body_text: &str,
    attachments: &[AttachmentUploadInput],
) -> String {
    let mut headers = submission_headers(from_address, input);
    let Some((calendar_index, calendar)) = attachments
        .iter()
        .enumerate()
        .find(|(_, attachment)| is_scheduling_calendar_part(attachment))
    else {
        if attachments.is_empty() {
            headers.extend([String::new(), body_text.to_string()]);
            return headers.join("\r\n");
        }

        let boundary = format!("lpe-mixed-{}", Uuid::new_v4());
        headers.extend([
            "MIME-Version: 1.0".to_string(),
            format!("Content-Type: multipart/mixed; boundary=\"{boundary}\""),
            String::new(),
            format!("--{boundary}"),
            "Content-Type: text/plain; charset=UTF-8".to_string(),
            "Content-Transfer-Encoding: 8bit".to_string(),
            String::new(),
            body_text.to_string(),
        ]);
        for attachment in attachments {
            headers.push(format!("--{boundary}"));
            headers.extend(render_attachment_mime_part(attachment));
        }
        headers.extend([format!("--{boundary}--"), String::new()]);
        return headers.join("\r\n");
    };
    let alternative_boundary = format!("lpe-calendar-{}", Uuid::new_v4());
    let attachment_count = attachments.len().saturating_sub(1);
    let mixed_boundary =
        (attachment_count > 0).then(|| format!("lpe-calendar-mixed-{}", Uuid::new_v4()));
    headers.push("MIME-Version: 1.0".to_string());
    headers.push("Content-Class: urn:content-classes:calendarmessage".to_string());
    headers.push(match mixed_boundary.as_deref() {
        Some(boundary) => format!("Content-Type: multipart/mixed; boundary=\"{boundary}\""),
        None => {
            format!("Content-Type: multipart/alternative; boundary=\"{alternative_boundary}\"")
        }
    });
    headers.push(String::new());
    if let Some(boundary) = mixed_boundary.as_deref() {
        headers.extend([
            format!("--{boundary}"),
            format!("Content-Type: multipart/alternative; boundary=\"{alternative_boundary}\""),
            String::new(),
        ]);
    }
    headers.extend([
        format!("--{alternative_boundary}"),
        "Content-Type: text/plain; charset=UTF-8".to_string(),
        "Content-Transfer-Encoding: 8bit".to_string(),
        String::new(),
        body_text.to_string(),
        format!("--{alternative_boundary}"),
        format!(
            "Content-Type: {}",
            sanitize_header_value(calendar.media_type.trim())
        ),
        "Content-Transfer-Encoding: 8bit".to_string(),
        "Content-Disposition: inline; filename=\"invite.ics\"".to_string(),
        String::new(),
        String::from_utf8_lossy(&calendar.blob_bytes).into_owned(),
        format!("--{alternative_boundary}--"),
        String::new(),
    ]);
    if let Some(boundary) = mixed_boundary.as_deref() {
        for (index, attachment) in attachments.iter().enumerate() {
            if index == calendar_index {
                continue;
            }
            headers.push(format!("--{boundary}"));
            headers.extend(render_attachment_mime_part(attachment));
        }
        headers.extend([format!("--{boundary}--"), String::new()]);
    }
    headers.join("\r\n")
}

fn submission_headers(from_address: &str, input: &SubmitMessageInput) -> Vec<String> {
    let mut headers = vec![format!(
        "From: {}",
        header_mailbox(input.from_display.as_deref(), from_address)
    )];
    if let Some(sender_address) = input
        .sender_address
        .as_deref()
        .filter(|sender_address| !sender_address.eq_ignore_ascii_case(from_address))
    {
        headers.push(format!(
            "Sender: {}",
            header_mailbox(input.sender_display.as_deref(), sender_address)
        ));
    }
    if !input.to.is_empty() {
        headers.push(format!("To: {}", recipient_header(&input.to)));
    }
    if !input.cc.is_empty() {
        headers.push(format!("Cc: {}", recipient_header(&input.cc)));
    }
    headers.push(format!(
        "Subject: {}",
        sanitize_header_value(&input.subject)
    ));
    headers
}

fn render_attachment_mime_part(attachment: &AttachmentUploadInput) -> Vec<String> {
    let file_name = quote_header_parameter(&attachment.file_name);
    let media_type = sanitize_header_value(&attachment.media_type);
    let disposition = attachment
        .disposition
        .as_deref()
        .and_then(|value| value.split(';').next())
        .filter(|value| value.trim().eq_ignore_ascii_case("inline"))
        .map_or("attachment", |_| "inline");
    let mut part = vec![
        format!("Content-Type: {media_type}; name=\"{file_name}\""),
        "Content-Transfer-Encoding: base64".to_string(),
        format!("Content-Disposition: {disposition}; filename=\"{file_name}\""),
    ];
    if let Some(content_id) = attachment
        .content_id
        .as_deref()
        .map(sanitize_header_value)
        .map(|value| value.trim_matches(['<', '>']).to_string())
        .filter(|value| !value.is_empty())
    {
        part.push(format!("Content-ID: <{content_id}>"));
    }
    part.extend([
        String::new(),
        base64_mime_lines(&attachment.blob_bytes),
        String::new(),
    ]);
    part
}

fn base64_mime_lines(bytes: &[u8]) -> String {
    bytes
        .chunks(57)
        .map(|chunk| BASE64_STANDARD.encode(chunk))
        .collect::<Vec<_>>()
        .join("\r\n")
}

pub(super) fn is_scheduling_calendar_part(attachment: &AttachmentUploadInput) -> bool {
    attachment.is_scheduling_body
        && crate::mail::is_text_calendar_media_type(&attachment.media_type)
        && !attachment
            .disposition
            .as_deref()
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("attachment"))
}

fn recipient_header(recipients: &[super::SubmittedRecipientInput]) -> String {
    recipients
        .iter()
        .map(|recipient| {
            format_mailbox_address(
                &recipient.address,
                recipient.display_name.as_deref(),
                DisplayNamePolicy::OmitIfEqualsAddress,
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn header_mailbox(display_name: Option<&str>, address: &str) -> String {
    format_mailbox_address(
        address,
        display_name,
        DisplayNamePolicy::OmitIfEqualsAddress,
    )
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
            replace_attachments: false,
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
                is_scheduling_body: true,
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

    #[test]
    fn calendar_request_preserves_pdf_and_ordinary_calendar_attachments() {
        assert_scheduling_message_preserves_ordinary_attachments("REQUEST");
    }

    #[test]
    fn calendar_reply_preserves_pdf_and_ordinary_calendar_attachments() {
        assert_scheduling_message_preserves_ordinary_attachments("REPLY");
    }

    fn assert_scheduling_message_preserves_ordinary_attachments(method: &str) {
        let mut input = input();
        input.from_display = Some("Represented Attendee".to_string());
        input.sender_display = Some("Delegate".to_string());
        input.sender_address = Some("delegate@example.test".to_string());
        let pdf = b"%PDF-1.7\r\n\0binary-agenda";
        let ordinary_ics = b"BEGIN:VCALENDAR\r\nMETHOD:PUBLISH\r\nEND:VCALENDAR";
        let scheduling_ics =
            format!("BEGIN:VCALENDAR\r\nMETHOD:{method}\r\nEND:VCALENDAR").into_bytes();
        let raw = render_submission_raw_message(
            "attendee@example.test",
            &input,
            "Response",
            &[
                AttachmentUploadInput {
                    file_name: "agenda.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    disposition: Some("attachment".to_string()),
                    content_id: None,
                    is_scheduling_body: false,
                    blob_bytes: pdf.to_vec(),
                },
                AttachmentUploadInput {
                    file_name: "response.ics".to_string(),
                    media_type: format!("text/calendar; method={method}; charset=UTF-8"),
                    disposition: Some("inline".to_string()),
                    content_id: None,
                    is_scheduling_body: true,
                    blob_bytes: scheduling_ics,
                },
                AttachmentUploadInput {
                    file_name: "reference.ics".to_string(),
                    media_type: "text/calendar; method=PUBLISH".to_string(),
                    disposition: Some("attachment; filename=ignored.ics".to_string()),
                    content_id: None,
                    is_scheduling_body: false,
                    blob_bytes: ordinary_ics.to_vec(),
                },
            ],
        );

        assert!(raw.contains("Content-Type: multipart/mixed"));
        assert!(raw.contains("Content-Type: multipart/alternative"));
        assert_eq!(
            raw.matches("Content-Disposition: inline; filename=\"invite.ics\"")
                .count(),
            1
        );
        assert_eq!(raw.matches(&format!("METHOD:{method}")).count(), 1);
        assert!(raw.contains(&format!(
            "Content-Type: text/calendar; method={method}; charset=UTF-8"
        )));
        assert!(raw.contains("Content-Type: application/pdf; name=\"agenda.pdf\""));
        assert!(raw.contains("Content-Disposition: attachment; filename=\"agenda.pdf\""));
        assert!(raw.contains(&base64_mime_lines(pdf)));
        assert!(raw.contains("Content-Type: text/calendar; method=PUBLISH; name=\"reference.ics\""));
        assert!(raw.contains("Content-Disposition: attachment; filename=\"reference.ics\""));
        assert!(raw.contains(&base64_mime_lines(ordinary_ics)));
        assert!(!raw.contains("\r\nMETHOD:PUBLISH\r\n"));
        assert!(raw.contains("From: Represented Attendee <attendee@example.test>"));
        assert!(raw.contains("Sender: Delegate <delegate@example.test>"));
    }

    #[test]
    fn calendar_identity_headers_quote_display_names_with_commas() {
        let mut input = input();
        input.from_display = Some("Ducret, Denis".to_string());
        input.to[0].display_name = Some("Attendee, Alice".to_string());
        let raw = render_submission_raw_message(
            "denis.ducret@sdic.ch",
            &input,
            "Response",
            &[AttachmentUploadInput {
                file_name: "response.ics".to_string(),
                media_type: "text/calendar; method=REPLY".to_string(),
                disposition: Some("inline".to_string()),
                content_id: None,
                is_scheduling_body: true,
                blob_bytes: b"BEGIN:VCALENDAR\r\nMETHOD:REPLY\r\nEND:VCALENDAR".to_vec(),
            }],
        );

        assert!(raw.contains("From: \"Ducret, Denis\" <denis.ducret@sdic.ch>"));
        assert!(raw.contains("To: \"Attendee, Alice\" <attendee@example.test>"));
    }

    #[test]
    fn calendar_identity_headers_reject_line_break_injection() {
        let mut input = input();
        input.from_display = Some("Attendee\r\nBcc: injected@example.test".to_string());
        input.sender_display = Some("Delegate\nX-Injected: yes".to_string());
        input.sender_address = Some("delegate@example.test".to_string());
        input.subject = "Response\r\nX-Subject: yes".to_string();
        input.to[0].display_name = Some("Attendee\r\nX-Recipient: yes".to_string());
        input.to[0].address = "attendee@example.test\r\nX-Address: yes".to_string();
        let raw = render_submission_raw_message(
            "attendee@example.test",
            &input,
            "Response",
            &[AttachmentUploadInput {
                file_name: "response.ics".to_string(),
                media_type: "text/calendar; method=REPLY\r\nX-Media: yes".to_string(),
                disposition: Some("inline".to_string()),
                content_id: None,
                is_scheduling_body: true,
                blob_bytes: b"BEGIN:VCALENDAR\r\nMETHOD:REPLY\r\nEND:VCALENDAR".to_vec(),
            }],
        );

        assert!(!raw.contains("\r\nBcc: injected@example.test"));
        assert!(!raw.contains("\r\nX-Injected: yes"));
        assert!(!raw.contains("\r\nX-Subject: yes"));
        assert!(!raw.contains("\r\nX-Recipient: yes"));
        assert!(!raw.contains("\r\nX-Address: yes"));
        assert!(!raw.contains("\r\nX-Media: yes"));
        assert!(
            raw.contains("From: \"Attendee Bcc: injected@example.test\" <attendee@example.test>")
        );
        assert!(raw.contains("Sender: \"Delegate X-Injected: yes\" <delegate@example.test>"));
    }

    #[test]
    fn ordinary_message_headers_reject_line_break_injection() {
        let mut input = input();
        input.subject = "Review\r\nBcc: injected@example.test".to_string();
        let raw = render_submission_raw_message(
            "organizer@example.test\r\nX-From: yes",
            &input,
            "Agenda",
            &[],
        );

        assert!(!raw.contains("\r\nBcc: injected@example.test"));
        assert!(!raw.contains("\r\nX-From: yes"));
        assert!(raw.contains("Subject: Review Bcc: injected@example.test"));
    }

    #[test]
    fn ordinary_calendar_attachment_is_not_promoted_to_scheduling_body() {
        let input = input();
        let ordinary_calendar = b"BEGIN:VCALENDAR\r\nMETHOD:PUBLISH\r\nEND:VCALENDAR";
        let raw = render_submission_raw_message(
            "organizer@example.test",
            &input,
            "Agenda",
            &[AttachmentUploadInput {
                file_name: "reference.ics".to_string(),
                media_type: "text/calendar; method=PUBLISH".to_string(),
                disposition: Some("attachment; filename=reference.ics".to_string()),
                content_id: None,
                is_scheduling_body: false,
                blob_bytes: ordinary_calendar.to_vec(),
            }],
        );

        assert!(!raw.contains("Content-Class: urn:content-classes:calendarmessage"));
        assert!(!raw.contains("multipart/alternative"));
        assert!(raw.contains("Content-Type: multipart/mixed"));
        assert!(raw.contains("Content-Disposition: attachment; filename=\"reference.ics\""));
        assert!(raw.contains(&base64_mime_lines(ordinary_calendar)));
        assert!(raw.contains("To: Attendee <attendee@example.test>"));
        assert!(!raw.contains("secret@example.test"));
    }
}
