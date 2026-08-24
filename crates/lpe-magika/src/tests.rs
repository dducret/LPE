use super::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

#[derive(Debug, Clone)]
struct FakeDetector {
    detection: MagikaDetection,
}

impl Detector for FakeDetector {
    fn detect(&self, _source: DetectionSource<'_>) -> Result<MagikaDetection> {
        Ok(self.detection.clone())
    }
}

fn detection(mime_type: &str, extension: &str, score: f32) -> MagikaDetection {
    MagikaDetection {
        label: extension.to_string(),
        mime_type: mime_type.to_string(),
        description: extension.to_string(),
        group: "document".to_string(),
        extensions: vec![extension.to_string()],
        score: Some(score),
    }
}

#[test]
fn supported_attachment_kind_is_accepted() {
    let validator = Validator::new(
        FakeDetector {
            detection: detection("application/pdf", "pdf", 0.99),
        },
        0.80,
    );
    let outcome = validator
        .validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::AttachmentParsing,
                declared_mime: Some("application/pdf".to_string()),
                filename: Some("report.pdf".to_string()),
                expected_kind: ExpectedKind::SupportedAttachmentText,
            },
            b"pdf",
        )
        .unwrap();
    assert_eq!(outcome.policy_decision, PolicyDecision::Accept);
}

#[test]
fn smtp_mismatch_is_rejected() {
    let validator = Validator::new(
        FakeDetector {
            detection: detection("application/x-msdownload", "exe", 0.99),
        },
        0.80,
    );
    let outcome = validator
        .validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::LpeCtInboundSmtp,
                declared_mime: Some("application/pdf".to_string()),
                filename: Some("invoice.pdf".to_string()),
                expected_kind: ExpectedKind::Any,
            },
            b"exe",
        )
        .unwrap();
    assert_eq!(outcome.policy_decision, PolicyDecision::Reject);
    assert!(outcome.mismatch);
}

#[test]
fn smtp_client_submission_unknown_file_is_restricted() {
    let validator = Validator::new(
        FakeDetector {
            detection: MagikaDetection {
                label: "unknown_binary".to_string(),
                mime_type: "application/octet-stream".to_string(),
                description: "unknown".to_string(),
                group: "unknown".to_string(),
                extensions: Vec::new(),
                score: Some(0.99),
            },
        },
        0.80,
    );
    let outcome = validator
        .validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::SmtpClientSubmission,
                declared_mime: None,
                filename: None,
                expected_kind: ExpectedKind::Any,
            },
            b"blob",
        )
        .unwrap();
    assert_eq!(outcome.policy_decision, PolicyDecision::Restrict);
}

const PROBE_1_CALENDAR_ICS: &str = concat!(
    "BEGIN:VCALENDAR\r\n",
    "METHOD:REQUEST\r\n",
    "PRODID:Microsoft Exchange Server 2010\r\n",
    "VERSION:2.0\r\n",
    "BEGIN:VTIMEZONE\r\n",
    "TZID:W. Europe Standard Time\r\n",
    "BEGIN:STANDARD\r\n",
    "DTSTART:16010101T030000\r\n",
    "TZOFFSETFROM:+0200\r\n",
    "TZOFFSETTO:+0100\r\n",
    "RRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=-1SU;BYMONTH=10\r\n",
    "END:STANDARD\r\n",
    "BEGIN:DAYLIGHT\r\n",
    "DTSTART:16010101T020000\r\n",
    "TZOFFSETFROM:+0100\r\n",
    "TZOFFSETTO:+0200\r\n",
    "RRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=-1SU;BYMONTH=3\r\n",
    "END:DAYLIGHT\r\n",
    "END:VTIMEZONE\r\n",
    "BEGIN:VEVENT\r\n",
    "ORGANIZER;CN=Denis Ducret:mailto:denis.ducret@sdic.ch\r\n",
    "ATTENDEE;ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=TRUE;CN=test:mailt\r\n",
    " o:test@l-p-e.ch\r\n",
    "DESCRIPTION;LANGUAGE=en-US:\\n\r\n",
    "UID:040000008200E00074C5B7101A82E0080000000042E49166E533DD01000000000000000\r\n",
    " 01000000051A32B1E5BC4B145B2F9D1991C9F8EA9\r\n",
    "SUMMARY;LANGUAGE=en-US:Probe 1\r\n",
    "DTSTART;TZID=W. Europe Standard Time:20260825T120000\r\n",
    "DTEND;TZID=W. Europe Standard Time:20260825T123000\r\n",
    "CLASS:PUBLIC\r\n",
    "PRIORITY:5\r\n",
    "DTSTAMP:20260824T162706Z\r\n",
    "TRANSP:OPAQUE\r\n",
    "STATUS:CONFIRMED\r\n",
    "SEQUENCE:1\r\n",
    "LOCATION;LANGUAGE=en-US:Planches 2\r\n",
    "X-MICROSOFT-CDO-APPT-SEQUENCE:1\r\n",
    "X-MICROSOFT-CDO-OWNERAPPTID:2124990247\r\n",
    "X-MICROSOFT-CDO-BUSYSTATUS:TENTATIVE\r\n",
    "X-MICROSOFT-CDO-INTENDEDSTATUS:BUSY\r\n",
    "X-MICROSOFT-CDO-ALLDAYEVENT:FALSE\r\n",
    "X-MICROSOFT-CDO-IMPORTANCE:1\r\n",
    "X-MICROSOFT-CDO-INSTTYPE:0\r\n",
    "X-MICROSOFT-DONOTFORWARDMEETING:FALSE\r\n",
    "X-MICROSOFT-DISALLOW-COUNTER:FALSE\r\n",
    "X-MICROSOFT-REQUESTEDATTENDANCEMODE:DEFAULT\r\n",
    "X-MICROSOFT-ISRESPONSEREQUESTED:TRUE\r\n",
    "X-MICROSOFT-LOCATIONDISPLAYNAME:Planches 2\r\n",
    "X-MICROSOFT-LOCATIONSOURCE:None\r\n",
    "X-MICROSOFT-LOCATIONS:[{\"DisplayName\":\"Planches 2\"\\,\"LocationAnnotation\":\"\"\r\n",
    " \\,\"LocationUri\":\"\"\\,\"LocationStreet\":\"\"\\,\"LocationCity\":\"\"\\,\"LocationState\r\n",
    " \":\"\"\\,\"LocationCountry\":\"\"\\,\"LocationPostalCode\":\"\"\\,\"LocationFullAddress\"\r\n",
    " :\"\"}]\r\n",
    "BEGIN:VALARM\r\n",
    "DESCRIPTION:REMINDER\r\n",
    "TRIGGER;RELATED=START:-PT15M\r\n",
    "ACTION:DISPLAY\r\n",
    "END:VALARM\r\n",
    "END:VEVENT\r\n",
    "END:VCALENDAR",
);

fn probe_1_multipart_alternative(disposition: &str) -> Vec<u8> {
    let encoded = BASE64.encode(PROBE_1_CALENDAR_ICS.as_bytes());
    let encoded = encoded
        .as_bytes()
        .chunks(76)
        .map(|chunk| std::str::from_utf8(chunk).expect("base64 is ASCII"))
        .collect::<Vec<_>>()
        .join("\r\n");
    format!(
        concat!(
            "MIME-Version: 1.0\r\n",
            "Content-Type: multipart/alternative; boundary=\"probe-1\"\r\n",
            "\r\n",
            "--probe-1\r\n",
            "Content-Type: text/plain; charset=utf-8\r\n",
            "\r\n",
            "Probe 1\r\n",
            "--probe-1\r\n",
            "Content-Type: text/html; charset=utf-8\r\n",
            "\r\n",
            "<html><body>Probe 1</body></html>\r\n",
            "--probe-1\r\n",
            "Content-Type: text/calendar; method=REQUEST; charset=UTF-8; name=\"calendar-1.ics\"\r\n",
            "Content-Disposition: {disposition}; filename=\"calendar-1.ics\"\r\n",
            "Content-Transfer-Encoding: base64\r\n",
            "\r\n",
            "{encoded}\r\n",
            "--probe-1--\r\n",
        ),
        disposition = disposition,
        encoded = encoded,
    )
    .into_bytes()
}

#[test]
fn collect_mime_attachment_parts_selects_exact_probe_1_calendar_alternative() {
    let message = probe_1_multipart_alternative("inline");

    let attachments = collect_mime_attachment_parts(&message).unwrap();

    assert_eq!(PROBE_1_CALENDAR_ICS.len(), 1_857);
    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].filename.as_deref(), Some("calendar-1.ics"));
    assert_eq!(
        attachments[0].declared_mime.as_deref(),
        Some("text/calendar; method=REQUEST; charset=UTF-8; name=\"calendar-1.ics\"")
    );
    assert_eq!(
        attachments[0].content_disposition.as_deref(),
        Some("inline; filename=\"calendar-1.ics\"")
    );
    assert!(attachments[0].is_scheduling_body);
    assert_eq!(attachments[0].bytes, PROBE_1_CALENDAR_ICS.as_bytes());
}

#[test]
fn collect_mime_attachment_parts_keeps_exact_probe_1_explicit_attachment_non_scheduling() {
    let message = probe_1_multipart_alternative("attachment");

    let attachments = collect_mime_attachment_parts(&message).unwrap();

    assert_eq!(attachments.len(), 1);
    assert_eq!(
        attachments[0].content_disposition.as_deref(),
        Some("attachment; filename=\"calendar-1.ics\"")
    );
    assert!(!attachments[0].is_scheduling_body);
    assert_eq!(attachments[0].bytes, PROBE_1_CALENDAR_ICS.as_bytes());
}

#[test]
fn collect_mime_attachment_parts_extracts_attachment_payloads() {
    let message = concat!(
        "Content-Type: multipart/mixed; boundary=\"abc\"\r\n",
        "\r\n",
        "--abc\r\n",
        "Content-Type: text/plain\r\n",
        "\r\n",
        "Body\r\n",
        "--abc\r\n",
        "Content-Type: application/pdf; name=\"report.pdf\"\r\n",
        "Content-Disposition: attachment; filename=\"report.pdf\"\r\n",
        "Content-Transfer-Encoding: base64\r\n",
        "\r\n",
        "UERG\r\n",
        "--abc--\r\n"
    );
    let attachments = collect_mime_attachment_parts(message.as_bytes()).unwrap();
    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].filename.as_deref(), Some("report.pdf"));
    assert_eq!(
        attachments[0].declared_mime.as_deref(),
        Some("application/pdf; name=\"report.pdf\"")
    );
    assert_eq!(attachments[0].bytes, b"PDF".to_vec());
}

#[test]
fn collect_mime_attachment_parts_marks_calendar_in_mixed_alternative_as_scheduling_body() {
    let message = concat!(
        "Content-Type: multipart/mixed; boundary=\"outer\"\r\n",
        "\r\n",
        "--outer\r\n",
        "Content-Type: multipart/alternative; boundary=\"invite\"\r\n",
        "\r\n",
        "--invite\r\n",
        "Content-Type: text/plain; charset=utf-8\r\n",
        "\r\n",
        "\r\n",
        "--invite\r\n",
        "Content-Type: text/calendar; charset=utf-8; method=COUNTER\r\n",
        "Content-Transfer-Encoding: base64\r\n",
        "\r\n",
        "QkVHSU46VkNBTEVOREFSDQpNRVRIT0Q6Q09VTlRFUg0KRU5EOlZDQUxFTkRBUg==\r\n",
        "--invite--\r\n",
        "--outer--\r\n"
    );

    let attachments = collect_mime_attachment_parts(message.as_bytes()).unwrap();

    assert_eq!(attachments.len(), 1);
    assert_eq!(
        attachments[0].declared_mime.as_deref(),
        Some("text/calendar; charset=utf-8; method=COUNTER")
    );
    assert_eq!(attachments[0].content_disposition, None);
    assert!(attachments[0].is_scheduling_body);
    assert_eq!(
        attachments[0].bytes,
        b"BEGIN:VCALENDAR\r\nMETHOD:COUNTER\r\nEND:VCALENDAR".to_vec()
    );
}

#[test]
fn collect_mime_attachment_parts_selects_last_calendar_alternative_for_scheduling() {
    let message = concat!(
        "Content-Type: multipart/alternative; boundary=\"invite\"\r\n",
        "\r\n",
        "--invite\r\n",
        "Content-Type: text/calendar; method=PUBLISH\r\n",
        "\r\n",
        "BEGIN:VCALENDAR\r\nMETHOD:PUBLISH\r\nEND:VCALENDAR\r\n",
        "--invite\r\n",
        "Content-Type: text/calendar; method=REQUEST\r\n",
        "\r\n",
        "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nEND:VCALENDAR\r\n",
        "--invite--\r\n"
    );

    let attachments = collect_mime_attachment_parts(message.as_bytes()).unwrap();

    assert_eq!(attachments.len(), 2);
    assert!(!attachments[0].is_scheduling_body);
    assert!(attachments[1].is_scheduling_body);
}

#[test]
fn collect_mime_attachment_parts_does_not_select_calendar_before_later_plain_alternative() {
    let message = concat!(
        "Content-Type: multipart/alternative; boundary=\"invite\"\r\n",
        "\r\n",
        "--invite\r\n",
        "Content-Type: text/calendar; method=REQUEST\r\n",
        "\r\n",
        "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nEND:VCALENDAR\r\n",
        "--invite\r\n",
        "Content-Type: text/plain\r\n",
        "\r\n",
        "Fallback body\r\n",
        "--invite--\r\n"
    );

    let attachments = collect_mime_attachment_parts(message.as_bytes()).unwrap();

    assert_eq!(attachments.len(), 1);
    assert!(!attachments[0].is_scheduling_body);
}

#[test]
fn collect_mime_attachment_parts_preserves_explicit_calendar_attachment_role() {
    let message = concat!(
        "Content-Type: multipart/mixed; boundary=\"mixed\"\r\n",
        "\r\n",
        "--mixed\r\n",
        "Content-Type: text/plain\r\n",
        "\r\n",
        "Body\r\n",
        "--mixed\r\n",
        "Content-Type: text/calendar; method=REQUEST; name=\"copy.ics\"\r\n",
        "Content-Disposition: attachment; filename=\"copy.ics\"\r\n",
        "\r\n",
        "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nEND:VCALENDAR\r\n",
        "--mixed--\r\n"
    );

    let attachments = collect_mime_attachment_parts(message.as_bytes()).unwrap();

    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].filename.as_deref(), Some("copy.ics"));
    assert!(attachments[0]
        .content_disposition
        .as_deref()
        .is_some_and(|value| value.starts_with("attachment")));
    assert!(!attachments[0].is_scheduling_body);
}

#[test]
fn collect_mime_attachment_parts_marks_direct_first_mixed_calendar_as_scheduling_body() {
    let message = concat!(
        "Content-Type: multipart/mixed; boundary=\"mixed\"\r\n",
        "\r\n",
        "--mixed\r\n",
        "Content-Type: text/calendar; method=REQUEST\r\n",
        "\r\n",
        "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n",
        "--mixed--\r\n"
    );

    let attachments = collect_mime_attachment_parts(message.as_bytes()).unwrap();

    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].content_disposition, None);
    assert!(attachments[0].is_scheduling_body);
}

#[test]
fn collect_mime_attachment_parts_marks_calendar_after_plain_mixed_body_for_outlook_imip() {
    let message = concat!(
        "Content-Type: multipart/mixed; boundary=\"mixed\"\r\n",
        "\r\n",
        "--mixed\r\n",
        "Content-Type: text/plain\r\n",
        "\r\n",
        "Body\r\n",
        "--mixed\r\n",
        "Content-Type: text/calendar; method=REQUEST\r\n",
        "\r\n",
        "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nEND:VCALENDAR\r\n",
        "--mixed--\r\n"
    );

    let attachments = collect_mime_attachment_parts(message.as_bytes()).unwrap();

    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].content_disposition, None);
    assert!(attachments[0].is_scheduling_body);
}

#[test]
fn collect_mime_attachment_parts_selects_only_first_eligible_mixed_imip_calendar() {
    let message = concat!(
        "Content-Type: multipart/mixed; boundary=\"mixed\"\r\n",
        "\r\n",
        "--mixed\r\n",
        "Content-Type: text/plain\r\n",
        "\r\n",
        "Body\r\n",
        "--mixed\r\n",
        "Content-Type: text/calendar; method=REQUEST; name=\"attached.ics\"\r\n",
        "Content-Disposition: attachment; filename=\"attached.ics\"\r\n",
        "\r\n",
        "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nEND:VCALENDAR\r\n",
        "--mixed\r\n",
        "Content-Type: text/calendar; method=REQUEST\r\n",
        "\r\n",
        "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nEND:VCALENDAR\r\n",
        "--mixed\r\n",
        "Content-Type: text/calendar; method=CANCEL\r\n",
        "\r\n",
        "BEGIN:VCALENDAR\r\nMETHOD:CANCEL\r\nEND:VCALENDAR\r\n",
        "--mixed--\r\n"
    );

    let attachments = collect_mime_attachment_parts(message.as_bytes()).unwrap();

    assert_eq!(attachments.len(), 3);
    assert!(!attachments[0].is_scheduling_body);
    assert!(attachments[1].is_scheduling_body);
    assert!(!attachments[2].is_scheduling_body);
}

#[test]
fn extract_visible_text_prefers_plaintext_from_multipart_alternative() {
    let message = concat!(
        "Subject: =?UTF-8?Q?Bonjour_=C3=A9quipe?=\r\n",
        "Content-Type: multipart/alternative; boundary=\"b1\"\r\n",
        "\r\n",
        "--b1\r\n",
        "Content-Type: text/plain; charset=utf-8\r\n",
        "Content-Transfer-Encoding: quoted-printable\r\n",
        "\r\n",
        "Ligne=20un=0ALigne=20deux\r\n",
        "--b1\r\n",
        "Content-Type: text/html; charset=utf-8\r\n",
        "\r\n",
        "<p>Ignored</p>\r\n",
        "--b1--\r\n"
    );

    assert_eq!(
        parse_rfc822_header_value(message.as_bytes(), "subject").as_deref(),
        Some("Bonjour équipe")
    );
    assert_eq!(
        extract_visible_text(message.as_bytes()).unwrap(),
        "Ligne un\nLigne deux"
    );
}

#[test]
fn extract_visible_text_decodes_quoted_printable_iso_8859_1() {
    let message = concat!(
        "Content-Type: multipart/alternative; boundary=\"b1\"\r\n",
        "\r\n",
        "--b1\r\n",
        "Content-Type: text/plain; charset=\"iso-8859-1\"\r\n",
        "Content-Transfer-Encoding: quoted-printable\r\n",
        "\r\n",
        "Test re=E7u ok, dans junk\r\n",
        "--b1--\r\n"
    );

    assert_eq!(
        extract_visible_text(message.as_bytes()).unwrap(),
        "Test reçu ok, dans junk"
    );
}

#[test]
fn extract_visible_text_uses_html_when_plaintext_is_missing() {
    let message = concat!(
        "Content-Type: text/html; charset=utf-8\r\n",
        "Content-Transfer-Encoding: base64\r\n",
        "\r\n",
        "PHA+SGVsbG88L3A+\r\n"
    );

    assert_eq!(extract_visible_text(message.as_bytes()).unwrap(), "Hello");
}

#[test]
fn collect_mime_attachment_parts_handles_non_utf8_body_bytes() {
    let mut message = b"Content-Type: multipart/mixed; boundary=\"b1\"\r\n\r\n--b1\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"blob.bin\"\r\n\r\n".to_vec();
    message.extend_from_slice(&[0xff, 0xfe, 0x00, 0x41]);
    message.extend_from_slice(b"\r\n--b1--\r\n");

    let attachments = collect_mime_attachment_parts(&message).unwrap();
    assert_eq!(attachments.len(), 1);
    assert_eq!(
        attachments[0].bytes,
        vec![0xff, 0xfe, 0x00, 0x41, b'\r', b'\n']
    );
}
