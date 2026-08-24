use lpe_storage::{
    mail::{parse_calendar_meeting_request, parse_rfc822_message},
    AttachmentUploadInput,
};

fn assert_probe1_request(request: lpe_storage::CalendarMeetingRequest) {
    assert_eq!(request.organizer.unwrap().email, "denis.ducret@sdic.ch");
    assert_eq!(request.attendees.len(), 1);
    assert_eq!(request.attendees[0].email, "test@l-p-e.ch");
    assert!(request.response_requested);
    assert_eq!(request.meeting_start, "2026-08-25T10:00:00Z");
    assert_eq!(request.meeting_end, "2026-08-25T10:30:00Z");
    assert_eq!(request.meeting_sequence, 1);
}

#[test]
fn exchange_probe1_request_payload_is_actionable() {
    let request = parse_calendar_meeting_request(&[AttachmentUploadInput {
        file_name: "calendar-1.ics".to_string(),
        media_type: "text/calendar; method=REQUEST; charset=utf-8".to_string(),
        disposition: None,
        content_id: None,
        is_scheduling_body: true,
        blob_bytes: include_bytes!("fixtures/probe1-request.ics").to_vec(),
    }])
    .expect("the exact Exchange Probe 1 payload must be an actionable request");

    assert_probe1_request(request);
}

#[test]
fn exchange_probe1_request_after_plain_mixed_body_is_actionable() {
    let message = format!(
        concat!(
            "From: Denis Ducret <denis.ducret@sdic.ch>\r\n",
            "To: test@l-p-e.ch\r\n",
            "Subject: Probe 1\r\n",
            "MIME-Version: 1.0\r\n",
            "Content-Type: multipart/mixed; boundary=probe1\r\n",
            "\r\n",
            "--probe1\r\n",
            "Content-Type: text/plain; charset=utf-8\r\n",
            "\r\n",
            "Probe 1\r\n",
            "--probe1\r\n",
            "Content-Type: text/calendar; charset=utf-8; method=REQUEST\r\n",
            "\r\n",
            "{}\r\n",
            "--probe1--\r\n"
        ),
        include_str!("fixtures/probe1-request.ics")
    );
    let parsed = parse_rfc822_message(message.as_bytes()).expect("parse Probe 1 MIME message");

    assert_eq!(parsed.attachments.len(), 1);
    assert!(parsed.attachments[0].is_scheduling_body);
    let request = parse_calendar_meeting_request(&parsed.attachments)
        .expect("the Probe 1 iMIP part must remain actionable after a plain mixed child");
    assert_probe1_request(request);
}
