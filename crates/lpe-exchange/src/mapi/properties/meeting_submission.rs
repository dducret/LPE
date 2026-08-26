use super::*;

pub(in crate::mapi) fn meeting_scheduling_input_property_tag(property_tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(property_tag),
        PID_TAG_MESSAGE_CLASS_W
            | PID_TAG_SUBJECT_W
            | PID_TAG_SUBJECT_PREFIX_W
            | PID_TAG_NORMALIZED_SUBJECT_W
            | PID_TAG_BODY_W
            | PID_TAG_BODY_HTML_W
            | PID_TAG_HTML_BINARY
            | PID_TAG_RTF_COMPRESSED
            | PID_TAG_SENDER_NAME_W
            | PID_TAG_SENDER_EMAIL_ADDRESS_W
            | PID_TAG_SENDER_SMTP_ADDRESS_W
            | PID_TAG_SENT_REPRESENTING_NAME_W
            | PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W
            | PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W
            | PID_TAG_START_DATE
            | PID_TAG_END_DATE
            | PID_TAG_CLIENT_SUBMIT_TIME
            | PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_CREATION_TIME
            | PID_LID_COMMON_START_TAG
            | PID_LID_COMMON_END_TAG
            | PID_LID_APPOINTMENT_START_WHOLE_TAG
            | PID_LID_APPOINTMENT_END_WHOLE_TAG
            | PID_LID_LOCATION_W_TAG
            | PID_LID_APPOINTMENT_SEQUENCE_TAG
            | PID_LID_APPOINTMENT_STATE_FLAGS_TAG
            | PID_LID_RECURRING_TAG
            | PID_LID_IS_RECURRING_TAG
            | PID_LID_APPOINTMENT_RECUR_TAG
            | PID_LID_GLOBAL_OBJECT_ID_TAG
            | PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG
            | PID_LID_ATTENDEE_CRITICAL_CHANGE_TAG
            | PID_LID_APPOINTMENT_COUNTER_PROPOSAL_TAG
            | PID_LID_APPOINTMENT_PROPOSED_START_WHOLE_TAG
            | PID_LID_APPOINTMENT_PROPOSED_END_WHOLE_TAG
    )
}

pub(in crate::mapi) fn meeting_scheduling_attachments(
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
    from_address: &str,
    from_name: Option<&str>,
) -> Result<Vec<AttachmentUploadInput>> {
    let message_class = optional_pending_text_property(properties, &[PID_TAG_MESSAGE_CLASS_W]);
    if message_class.as_deref().is_some_and(|message_class| {
        message_class
            .trim()
            .to_ascii_lowercase()
            .starts_with("ipm.schedule.meeting.")
            && !message_class
                .trim()
                .eq_ignore_ascii_case("IPM.Schedule.Meeting.Request")
            && meeting_response_partstat(properties).is_none()
    }) {
        bail!("unsupported MAPI scheduling message class");
    }
    let scheduling_kind = if meeting_response_partstat(properties).is_some() {
        "response"
    } else if is_mapi_meeting_request(properties) {
        "request"
    } else {
        return Ok(Vec::new());
    };
    if scheduling_kind == "request" && has_recurring_meeting_properties(properties) {
        bail!("recurring MAPI meeting request submission is unsupported");
    }
    let attachments = if scheduling_kind == "response" {
        meeting_response_attachment(properties, recipients, from_address, from_name)
    } else {
        meeting_request_attachment(properties, recipients, from_address, from_name)
    };
    if attachments.is_empty() {
        bail!("MAPI meeting {scheduling_kind} is missing required scheduling fields");
    }
    Ok(attachments)
}

fn has_recurring_meeting_properties(properties: &HashMap<u32, MapiValue>) -> bool {
    [PID_LID_RECURRING_TAG, PID_LID_IS_RECURRING_TAG]
        .into_iter()
        .any(|tag| properties.get(&tag).and_then(MapiValue::as_bool) == Some(true))
        || properties
            .get(&PID_LID_APPOINTMENT_RECUR_TAG)
            .is_some_and(|value| matches!(value, MapiValue::Binary(blob) if !blob.is_empty()))
}

fn meeting_response_attachment(
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
    responder_address: &str,
    responder_name: Option<&str>,
) -> Vec<AttachmentUploadInput> {
    let Some(partstat) = meeting_response_partstat(properties) else {
        return Vec::new();
    };
    let Some((original_start, original_end)) = meeting_interval(
        properties,
        &[
            PID_LID_APPOINTMENT_START_WHOLE_TAG,
            PID_LID_COMMON_START_TAG,
            PID_TAG_START_DATE,
        ],
        &[
            PID_LID_APPOINTMENT_END_WHOLE_TAG,
            PID_LID_COMMON_END_TAG,
            PID_TAG_END_DATE,
        ],
    ) else {
        return Vec::new();
    };
    let Some(uid) = complete_nonrecurring_meeting_uid(properties) else {
        return Vec::new();
    };
    let Some(organizer) = response_organizer(recipients) else {
        return Vec::new();
    };
    let responder_address = responder_address.trim();
    if responder_address.is_empty() {
        return Vec::new();
    }
    let meeting_sequence = match properties
        .get(&PID_LID_APPOINTMENT_SEQUENCE_TAG)
        .and_then(MapiValue::as_i64)
    {
        Some(value) => {
            let Ok(value) = i32::try_from(value) else {
                return Vec::new();
            };
            if value < 0 {
                return Vec::new();
            }
            Some(value)
        }
        None => None,
    };

    let counter = properties
        .get(&PID_LID_APPOINTMENT_COUNTER_PROPOSAL_TAG)
        .and_then(MapiValue::as_bool)
        .unwrap_or(false);
    let (method, start, end) = if counter {
        let Some((proposed_start, proposed_end)) = meeting_interval(
            properties,
            &[PID_LID_APPOINTMENT_PROPOSED_START_WHOLE_TAG],
            &[PID_LID_APPOINTMENT_PROPOSED_END_WHOLE_TAG],
        ) else {
            return Vec::new();
        };
        ("COUNTER", proposed_start, proposed_end)
    } else {
        ("REPLY", original_start.clone(), original_end.clone())
    };

    let dtstamp = scheduling_dtstamp(
        properties,
        &[
            PID_LID_ATTENDEE_CRITICAL_CHANGE_TAG,
            PID_TAG_CLIENT_SUBMIT_TIME,
            PID_TAG_LAST_MODIFICATION_TIME,
            PID_TAG_CREATION_TIME,
        ],
    );
    let subject = pending_message_subject(properties);
    let body = pending_body_text_property(properties);
    let location = optional_pending_text_property(properties, &[PID_LID_LOCATION_W_TAG]);
    let Some(organizer_address) = normalize_mapi_submit_address(organizer.address.clone()) else {
        return Vec::new();
    };
    let organizer_name = organizer
        .display_name
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(&organizer_address);
    let responder_name = responder_name
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(responder_address);

    let mut lines = vec![
        "BEGIN:VCALENDAR".to_string(),
        "VERSION:2.0".to_string(),
        "PRODID:-//LPE//MAPI//EN".to_string(),
        format!("METHOD:{method}"),
        "BEGIN:VEVENT".to_string(),
        format!("UID:{}", ical_text_escape(&uid)),
        format!("DTSTAMP:{dtstamp}"),
        format!("DTSTART:{start}"),
        format!("DTEND:{end}"),
        format!("SUMMARY:{}", ical_text_escape(&subject)),
        format!(
            "ORGANIZER;CN={}:mailto:{}",
            ical_parameter_escape(organizer_name),
            organizer_address
        ),
        format!(
            "ATTENDEE;CN={};PARTSTAT={partstat}:mailto:{}",
            ical_parameter_escape(responder_name),
            responder_address
        ),
    ];
    if counter {
        lines.push(format!("X-MS-OLK-ORIGINALSTART:{original_start}"));
        lines.push(format!("X-MS-OLK-ORIGINALEND:{original_end}"));
    }
    if let Some(sequence) = meeting_sequence {
        lines.push(format!("SEQUENCE:{sequence}"));
    }
    if let Some(location) = location.filter(|value| !value.trim().is_empty()) {
        lines.push(format!("LOCATION:{}", ical_text_escape(&location)));
    }
    if !body.trim().is_empty() {
        // [MS-OXCICAL] section 2.1.3.1.1.20.5 maps a response body to COMMENT.
        lines.push(format!("COMMENT:{}", ical_text_escape(&body)));
    }
    lines.push("END:VEVENT".to_string());
    lines.push("END:VCALENDAR".to_string());

    vec![AttachmentUploadInput {
        file_name: "response.ics".to_string(),
        media_type: format!("text/calendar; method={method}; charset=UTF-8"),
        disposition: Some("inline".to_string()),
        content_id: None,
        is_scheduling_body: true,
        blob_bytes: fold_icalendar_content_lines(&lines).into_bytes(),
    }]
}

fn meeting_response_partstat(properties: &HashMap<u32, MapiValue>) -> Option<&'static str> {
    let message_class = optional_pending_text_property(properties, &[PID_TAG_MESSAGE_CLASS_W])?;
    let message_class = message_class.trim();
    if message_class.eq_ignore_ascii_case("IPM.Schedule.Meeting.Resp.Pos") {
        Some("ACCEPTED")
    } else if message_class.eq_ignore_ascii_case("IPM.Schedule.Meeting.Resp.Tent") {
        Some("TENTATIVE")
    } else if message_class.eq_ignore_ascii_case("IPM.Schedule.Meeting.Resp.Neg") {
        Some("DECLINED")
    } else {
        None
    }
}

fn complete_nonrecurring_meeting_uid(properties: &HashMap<u32, MapiValue>) -> Option<String> {
    let MapiValue::Binary(value) = properties
        .get(&PID_LID_GLOBAL_OBJECT_ID_TAG)
        .or_else(|| properties.get(&PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG))?
    else {
        return None;
    };
    if value
        .get(16..20)
        .is_none_or(|instance_date| instance_date.iter().any(|byte| *byte != 0))
    {
        return None;
    }
    lpe_storage::calendar_uid_from_global_object_id(value)
}

fn response_organizer(recipients: &[PendingRecipient]) -> Option<&PendingRecipient> {
    let delivery_recipients = recipients
        .iter()
        .filter(|recipient| !recipient.is_originator() && !recipient.address.trim().is_empty())
        .collect::<Vec<_>>();
    let marked_organizers = delivery_recipients
        .iter()
        .copied()
        .filter(|recipient| recipient.recipient_flags & 0x0000_0002 != 0)
        .collect::<Vec<_>>();
    match marked_organizers.as_slice() {
        [organizer] => return Some(*organizer),
        [] => {}
        _ => return None,
    }

    // [MS-OXOCAL] section 4.2.2.4 describes the organizer as the sole
    // delivery recipient on an outbound response; older clients do not
    // consistently preserve recipOrganizer on that response row.
    let [organizer] = delivery_recipients.as_slice() else {
        return None;
    };
    Some(*organizer)
}

fn meeting_interval(
    properties: &HashMap<u32, MapiValue>,
    start_tags: &[u32],
    end_tags: &[u32],
) -> Option<(String, String)> {
    let start = start_tags
        .iter()
        .find_map(|tag| properties.get(tag).and_then(MapiValue::as_i64))?;
    let end = end_tags
        .iter()
        .find_map(|tag| properties.get(tag).and_then(MapiValue::as_i64))?;
    if end <= start {
        return None;
    }
    Some((ical_utc_filetime(start)?, ical_utc_filetime(end)?))
}

fn meeting_request_attachment(
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
    organizer_address: &str,
    organizer_name: Option<&str>,
) -> Vec<AttachmentUploadInput> {
    if !is_mapi_meeting_request(properties) {
        return Vec::new();
    }

    let start = properties
        .get(&PID_LID_APPOINTMENT_START_WHOLE_TAG)
        .or_else(|| properties.get(&PID_LID_COMMON_START_TAG))
        .or_else(|| properties.get(&PID_TAG_START_DATE))
        .and_then(MapiValue::as_i64)
        .and_then(ical_utc_filetime);
    let end = properties
        .get(&PID_LID_APPOINTMENT_END_WHOLE_TAG)
        .or_else(|| properties.get(&PID_LID_COMMON_END_TAG))
        .or_else(|| properties.get(&PID_TAG_END_DATE))
        .and_then(MapiValue::as_i64)
        .and_then(ical_utc_filetime);
    let uid = complete_nonrecurring_meeting_uid(properties);
    let (Some(start), Some(end), Some(uid)) = (start, end, uid) else {
        return Vec::new();
    };
    let sequence = match properties
        .get(&PID_LID_APPOINTMENT_SEQUENCE_TAG)
        .and_then(MapiValue::as_i64)
    {
        Some(value) => {
            let Ok(value) = i32::try_from(value) else {
                return Vec::new();
            };
            if value < 0 {
                return Vec::new();
            }
            value
        }
        None => 0,
    };
    let dtstamp = scheduling_dtstamp(
        properties,
        &[
            PID_TAG_CLIENT_SUBMIT_TIME,
            PID_TAG_LAST_MODIFICATION_TIME,
            PID_TAG_CREATION_TIME,
        ],
    );

    let subject = pending_message_subject(properties);
    let body = pending_body_text_property(properties);
    let location = optional_pending_text_property(properties, &[PID_LID_LOCATION_W_TAG]);
    let mut lines = vec![
        "BEGIN:VCALENDAR".to_string(),
        "VERSION:2.0".to_string(),
        "PRODID:-//LPE//MAPI//EN".to_string(),
        "METHOD:REQUEST".to_string(),
        "BEGIN:VEVENT".to_string(),
        format!("UID:{}", ical_text_escape(&uid)),
        format!("DTSTAMP:{dtstamp}"),
        format!("DTSTART:{start}"),
        format!("DTEND:{end}"),
        format!("SEQUENCE:{sequence}"),
        format!("SUMMARY:{}", ical_text_escape(&subject)),
        format!(
            "ORGANIZER;CN={}:mailto:{}",
            ical_parameter_escape(organizer_name.unwrap_or(organizer_address)),
            organizer_address
        ),
    ];
    if let Some(location) = location.filter(|value| !value.trim().is_empty()) {
        lines.push(format!("LOCATION:{}", ical_text_escape(&location)));
    }
    if !body.trim().is_empty() {
        lines.push(format!("DESCRIPTION:{}", ical_text_escape(&body)));
    }
    for recipient in recipients {
        if recipient.is_calendar_organizer()
            || matches!(recipient.recipient_type & 0x0F, 0x03)
            || recipient.address.trim().is_empty()
        {
            continue;
        }
        let Some(address) = normalize_mapi_submit_address(recipient.address.clone()) else {
            continue;
        };
        let role = if recipient.recipient_type & 0x0F == 0x02 {
            "OPT-PARTICIPANT"
        } else {
            "REQ-PARTICIPANT"
        };
        let name = recipient.display_name.as_deref().unwrap_or(&address);
        lines.push(format!(
            "ATTENDEE;CN={};ROLE={role};PARTSTAT=NEEDS-ACTION;RSVP=TRUE:mailto:{}",
            ical_parameter_escape(name),
            address
        ));
    }
    lines.push("END:VEVENT".to_string());
    lines.push("END:VCALENDAR".to_string());
    vec![AttachmentUploadInput {
        file_name: "invite.ics".to_string(),
        media_type: "text/calendar; method=REQUEST; charset=UTF-8".to_string(),
        disposition: Some("inline".to_string()),
        content_id: None,
        is_scheduling_body: true,
        blob_bytes: fold_icalendar_content_lines(&lines).into_bytes(),
    }]
}

pub(in crate::mapi) fn is_mapi_meeting_request(properties: &HashMap<u32, MapiValue>) -> bool {
    if meeting_response_partstat(properties).is_some() {
        return false;
    }
    if let Some(message_class) =
        optional_pending_text_property(properties, &[PID_TAG_MESSAGE_CLASS_W])
    {
        return message_class
            .trim()
            .eq_ignore_ascii_case("IPM.Schedule.Meeting.Request");
    }
    properties
        .get(&PID_LID_APPOINTMENT_STATE_FLAGS_TAG)
        .and_then(MapiValue::as_i64)
        .is_some_and(|flags| flags & 0x0000_0001 != 0)
}

fn ical_utc_filetime(value: i64) -> Option<String> {
    filetime_to_rfc3339_utc(value).map(|value| value.replace('-', "").replace(':', ""))
}

fn scheduling_dtstamp(properties: &HashMap<u32, MapiValue>, tags: &[u32]) -> String {
    tags.iter()
        .find_map(|tag| {
            properties
                .get(tag)
                .and_then(MapiValue::as_i64)
                .and_then(ical_utc_filetime)
        })
        .unwrap_or_else(|| {
            let current_filetime = i64::try_from(lpe_domain::current_windows_filetime())
                .expect("current Windows FILETIME fits signed MAPI PtypTime");
            ical_utc_filetime(current_filetime)
                .expect("current Windows FILETIME has a UTC iCalendar representation")
        })
}

fn ical_text_escape(value: &str) -> String {
    value
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace(';', "\\;")
        .replace(',', "\\,")
}

fn ical_parameter_escape(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .replace('^', "^^")
        .replace('"', "'");
    format!("\"{sanitized}\"")
}

fn fold_icalendar_content_lines(lines: &[String]) -> String {
    lines
        .iter()
        .map(|line| fold_icalendar_content_line(line))
        .collect::<Vec<_>>()
        .join("\r\n")
}

fn fold_icalendar_content_line(line: &str) -> String {
    // RFC 5545 section 3.1 limits physical content lines to 75 octets. A
    // continuation begins with one space, leaving 74 octets for its content.
    let mut folded = String::with_capacity(line.len());
    let mut physical_octets = 0usize;
    for character in line.chars() {
        let character_octets = character.len_utf8();
        if physical_octets + character_octets > 75 {
            folded.push_str("\r\n ");
            physical_octets = 1;
        }
        folded.push(character);
        physical_octets += character_octets;
    }
    folded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filetime(value: &str) -> MapiValue {
        MapiValue::I64(mapi_mailstore::filetime_from_rfc3339_utc(value) as i64)
    }

    fn complete_goid() -> Vec<u8> {
        let value = "040000008200E00074C5B7101A82E00800000000C08470CD9E31DD01000000000000000010000000ECFF8AEC00CE584390F914BF6A87F955";
        (0..value.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&value[index..index + 2], 16).unwrap())
            .collect()
    }

    fn third_party_goid(uid: &str) -> Vec<u8> {
        let mut data = b"vCal-Uid\x01\0\0\0".to_vec();
        data.extend_from_slice(uid.as_bytes());
        let mut value = vec![
            0x04, 0x00, 0x00, 0x00, 0x82, 0x00, 0xE0, 0x00, 0x74, 0xC5, 0xB7, 0x10, 0x1A, 0x82,
            0xE0, 0x08,
        ];
        value.extend_from_slice(&[0; 20]);
        value.extend_from_slice(&(data.len() as u32).to_le_bytes());
        value.extend_from_slice(&data);
        value
    }

    fn response_properties(message_class: &str) -> HashMap<u32, MapiValue> {
        HashMap::from([
            (
                PID_TAG_MESSAGE_CLASS_W,
                MapiValue::String(message_class.to_string()),
            ),
            (PID_TAG_SUBJECT_W, MapiValue::String("Probe 6".to_string())),
            (
                PID_TAG_BODY_W,
                MapiValue::String("Could we move this?".to_string()),
            ),
            (
                PID_LID_GLOBAL_OBJECT_ID_TAG,
                MapiValue::Binary(complete_goid()),
            ),
            (
                PID_LID_APPOINTMENT_START_WHOLE_TAG,
                filetime("2026-08-21T18:00:00Z"),
            ),
            (
                PID_LID_APPOINTMENT_END_WHOLE_TAG,
                filetime("2026-08-21T18:30:00Z"),
            ),
            (PID_TAG_CLIENT_SUBMIT_TIME, filetime("2026-08-21T19:48:00Z")),
            (
                PID_LID_ATTENDEE_CRITICAL_CHANGE_TAG,
                filetime("2026-08-21T19:49:00Z"),
            ),
        ])
    }

    fn organizer() -> Vec<PendingRecipient> {
        vec![PendingRecipient {
            row_id: 1,
            address: "organizer@example.test".to_string(),
            display_name: Some("Organizer".to_string()),
            recipient_type: 0x01,
            recipient_flags: 0x0000_0003,
        }]
    }

    #[test]
    fn accepted_response_generates_selected_reply_for_sent_representing_identity() {
        let properties = response_properties("IPM.Schedule.Meeting.Resp.Pos");
        let attachments = meeting_scheduling_attachments(
            &properties,
            &organizer(),
            "attendee@example.test",
            Some("Attendee"),
        )
        .unwrap();

        assert_eq!(attachments.len(), 1);
        assert!(attachments[0].is_scheduling_body);
        assert_eq!(
            attachments[0].media_type,
            "text/calendar; method=REPLY; charset=UTF-8"
        );
        let calendar = String::from_utf8_lossy(&attachments[0].blob_bytes);
        assert!(calendar.contains("METHOD:REPLY"));
        assert!(calendar.contains("UID:040000008200E00074C5B7101A82E008"));
        assert!(!calendar.contains("UID:mapi-goid:"));
        assert!(calendar.contains("DTSTAMP:20260821T194900Z"));
        assert!(calendar.contains("DTSTART:20260821T180000Z"));
        assert!(calendar.contains("DTEND:20260821T183000Z"));
        assert!(calendar.contains("ORGANIZER;CN=\"Organizer\":mailto:organizer@example.test"));
        assert!(calendar
            .contains("ATTENDEE;CN=\"Attendee\";PARTSTAT=ACCEPTED:mailto:attendee@example.test"));
        assert!(calendar.contains("COMMENT:Could we move this?"));
        assert!(!calendar.contains("X-MS-OLK-ORIGINALSTART"));
    }

    #[test]
    fn probe_6_declined_counter_uses_proposal_and_preserves_original_interval() {
        let mut properties = response_properties("IPM.Schedule.Meeting.Resp.Neg");
        properties.insert(
            PID_LID_APPOINTMENT_COUNTER_PROPOSAL_TAG,
            MapiValue::Bool(true),
        );
        properties.insert(
            PID_LID_APPOINTMENT_PROPOSED_START_WHOLE_TAG,
            filetime("2026-08-21T19:00:00Z"),
        );
        properties.insert(
            PID_LID_APPOINTMENT_PROPOSED_END_WHOLE_TAG,
            filetime("2026-08-21T19:30:00Z"),
        );

        let attachments = meeting_scheduling_attachments(
            &properties,
            &organizer(),
            "denis.ducret@sdic.ch",
            Some("Denis Ducret"),
        )
        .unwrap();

        assert_eq!(attachments.len(), 1);
        assert_eq!(
            attachments[0].media_type,
            "text/calendar; method=COUNTER; charset=UTF-8"
        );
        let calendar = String::from_utf8_lossy(&attachments[0].blob_bytes);
        assert!(calendar.contains("METHOD:COUNTER"));
        assert!(calendar.contains("DTSTART:20260821T190000Z"));
        assert!(calendar.contains("DTEND:20260821T193000Z"));
        assert!(calendar.contains("X-MS-OLK-ORIGINALSTART:20260821T180000Z"));
        assert!(calendar.contains("X-MS-OLK-ORIGINALEND:20260821T183000Z"));
        assert!(calendar.contains(
            "ATTENDEE;CN=\"Denis Ducret\";PARTSTAT=DECLINED:mailto:denis.ducret@sdic.ch"
        ));
    }

    #[test]
    fn response_without_a_client_timestamp_uses_submit_time_not_appointment_start() {
        let mut properties = response_properties("IPM.Schedule.Meeting.Resp.Pos");
        for tag in [
            PID_LID_ATTENDEE_CRITICAL_CHANGE_TAG,
            PID_TAG_CLIENT_SUBMIT_TIME,
            PID_TAG_LAST_MODIFICATION_TIME,
            PID_TAG_CREATION_TIME,
        ] {
            properties.remove(&tag);
        }

        let attachments = meeting_scheduling_attachments(
            &properties,
            &organizer(),
            "attendee@example.test",
            Some("Attendee"),
        )
        .unwrap();
        let calendar = String::from_utf8_lossy(&attachments[0].blob_bytes);
        let dtstamp = calendar
            .lines()
            .find(|line| line.starts_with("DTSTAMP:"))
            .expect("response has DTSTAMP");

        assert_ne!(dtstamp, "DTSTAMP:20260821T180000Z");
    }

    #[test]
    fn response_generation_rejects_ambiguous_organizer_rows() {
        let properties = response_properties("IPM.Schedule.Meeting.Resp.Pos");
        let mut duplicate_marked = organizer();
        duplicate_marked.push(PendingRecipient {
            row_id: 2,
            address: "other-organizer@example.test".to_string(),
            display_name: Some("Other Organizer".to_string()),
            recipient_type: 0x01,
            recipient_flags: 0x0000_0003,
        });
        assert!(meeting_scheduling_attachments(
            &properties,
            &duplicate_marked,
            "attendee@example.test",
            Some("Attendee"),
        )
        .is_err());

        let unmarked = duplicate_marked
            .into_iter()
            .map(|mut recipient| {
                recipient.recipient_flags = 0x0000_0001;
                recipient
            })
            .collect::<Vec<_>>();
        assert!(meeting_scheduling_attachments(
            &properties,
            &unmarked,
            "attendee@example.test",
            Some("Attendee"),
        )
        .is_err());
    }

    #[test]
    fn response_calendar_quotes_rfc5545_display_name_parameters() {
        let properties = response_properties("IPM.Schedule.Meeting.Resp.Pos");
        let mut recipients = organizer();
        recipients[0].display_name = Some("Team: Europe".to_string());

        let attachments = meeting_scheduling_attachments(
            &properties,
            &recipients,
            "denis.ducret@sdic.ch",
            Some("Ducret, Denis"),
        )
        .unwrap();
        let calendar = String::from_utf8_lossy(&attachments[0].blob_bytes);

        assert!(calendar.contains("ORGANIZER;CN=\"Team: Europe\":mailto:organizer@example.test"));
        assert!(calendar.contains(
            "ATTENDEE;CN=\"Ducret, Denis\";PARTSTAT=ACCEPTED:mailto:denis.ducret@sdic.ch"
        ));
    }

    #[test]
    fn generated_calendar_folds_long_lines_without_splitting_utf8_or_uid() {
        let mut properties = response_properties("IPM.Schedule.Meeting.Resp.Pos");
        properties.insert(
            PID_TAG_SUBJECT_W,
            MapiValue::String(format!("{} équipe", "Long subject".repeat(12))),
        );

        let attachments = meeting_scheduling_attachments(
            &properties,
            &organizer(),
            "attendee@example.test",
            Some("Attendee"),
        )
        .unwrap();
        let calendar = std::str::from_utf8(&attachments[0].blob_bytes).unwrap();

        assert!(calendar
            .split("\r\n")
            .all(|physical_line| physical_line.len() <= 75));
        assert!(calendar.contains("\r\n "));
        let unfolded = calendar.replace("\r\n ", "");
        assert!(unfolded.contains(&format!(
            "UID:{}",
            lpe_storage::calendar_uid_from_global_object_id(&complete_goid()).unwrap()
        )));
        assert!(unfolded.contains("équipe"));
    }

    #[test]
    fn generated_calendar_escapes_third_party_uid_text() {
        let uid = "third,party;team\\slot@example.test";
        let mut properties = response_properties("IPM.Schedule.Meeting.Resp.Pos");
        properties.insert(
            PID_LID_GLOBAL_OBJECT_ID_TAG,
            MapiValue::Binary(third_party_goid(uid)),
        );

        let attachments = meeting_scheduling_attachments(
            &properties,
            &organizer(),
            "attendee@example.test",
            Some("Attendee"),
        )
        .unwrap();
        let calendar = std::str::from_utf8(&attachments[0].blob_bytes).unwrap();

        assert!(calendar.contains("UID:third\\,party\\;team\\\\slot@example.test"));
        assert_eq!(
            lpe_storage::mail::parse_calendar_meeting_response(&attachments)
                .unwrap()
                .uid,
            uid
        );

        properties.insert(
            PID_TAG_MESSAGE_CLASS_W,
            MapiValue::String("IPM.Schedule.Meeting.Request".to_string()),
        );
        let attendee = PendingRecipient {
            row_id: 1,
            address: "attendee@example.test".to_string(),
            display_name: Some("Attendee".to_string()),
            recipient_type: 0x01,
            recipient_flags: 0x0000_0001,
        };
        let request_attachments = meeting_scheduling_attachments(
            &properties,
            &[attendee],
            "organizer@example.test",
            Some("Organizer"),
        )
        .unwrap();
        assert!(std::str::from_utf8(&request_attachments[0].blob_bytes)
            .unwrap()
            .contains("UID:third\\,party\\;team\\\\slot@example.test"));
        assert_eq!(
            lpe_storage::mail::parse_calendar_meeting_request(&request_attachments)
                .unwrap()
                .uid,
            uid
        );
    }

    #[test]
    fn response_calendar_rejects_line_break_injection() {
        let mut properties = response_properties("IPM.Schedule.Meeting.Resp.Pos");
        properties.insert(
            PID_TAG_SUBJECT_W,
            MapiValue::String("Probe\r\nX-Summary: yes".to_string()),
        );
        properties.insert(
            PID_TAG_BODY_W,
            MapiValue::String("Reply\r\nX-Comment: yes".to_string()),
        );
        let mut recipients = organizer();
        recipients[0].display_name = Some("Organizer\r\nX-Cn: yes".to_string());

        let attachments = meeting_scheduling_attachments(
            &properties,
            &recipients,
            "attendee@example.test",
            Some("Attendee\r\nX-Attendee: yes"),
        )
        .unwrap();
        let calendar = String::from_utf8_lossy(&attachments[0].blob_bytes);

        assert!(!calendar.contains("\r\nX-Summary: yes"));
        assert!(!calendar.contains("\r\nX-Comment: yes"));
        assert!(!calendar.contains("\r\nX-Cn: yes"));
        assert!(!calendar.contains("\r\nX-Attendee: yes"));
        assert!(calendar.contains("SUMMARY:Probe\\nX-Summary: yes"));
        assert!(calendar.contains("COMMENT:Reply\\nX-Comment: yes"));

        recipients[0].address =
            "organizer@example.test\r\nATTENDEE:mailto:injected@example.test".to_string();
        assert!(meeting_scheduling_attachments(
            &properties,
            &recipients,
            "attendee@example.test",
            Some("Attendee"),
        )
        .is_err());
    }

    #[test]
    fn response_generation_fails_closed_on_incomplete_correlation_fields() {
        let mut cases = Vec::new();

        let mut missing_goid = response_properties("IPM.Schedule.Meeting.Resp.Tent");
        missing_goid.remove(&PID_LID_GLOBAL_OBJECT_ID_TAG);
        cases.push((missing_goid, organizer()));

        let mut malformed_goid = response_properties("IPM.Schedule.Meeting.Resp.Tent");
        malformed_goid.insert(
            PID_LID_GLOBAL_OBJECT_ID_TAG,
            MapiValue::Binary(vec![0x04, 0x00, 0x00, 0x00]),
        );
        cases.push((malformed_goid, organizer()));

        let mut occurrence_goid = response_properties("IPM.Schedule.Meeting.Resp.Tent");
        let mut value = complete_goid();
        value[16..20].copy_from_slice(&[0x07, 0xEA, 0x08, 0x18]);
        occurrence_goid.insert(PID_LID_GLOBAL_OBJECT_ID_TAG, MapiValue::Binary(value));
        cases.push((occurrence_goid, organizer()));

        let mut missing_original_end = response_properties("IPM.Schedule.Meeting.Resp.Tent");
        missing_original_end.remove(&PID_LID_APPOINTMENT_END_WHOLE_TAG);
        cases.push((missing_original_end, organizer()));

        let mut invalid_sequence = response_properties("IPM.Schedule.Meeting.Resp.Tent");
        invalid_sequence.insert(
            PID_LID_APPOINTMENT_SEQUENCE_TAG,
            MapiValue::I64(i64::from(i32::MAX) + 1),
        );
        cases.push((invalid_sequence, organizer()));

        cases.push((
            response_properties("IPM.Schedule.Meeting.Resp.Tent"),
            Vec::new(),
        ));

        let mut missing_proposed_end = response_properties("IPM.Schedule.Meeting.Resp.Neg");
        missing_proposed_end.insert(
            PID_LID_APPOINTMENT_COUNTER_PROPOSAL_TAG,
            MapiValue::Bool(true),
        );
        missing_proposed_end.insert(
            PID_LID_APPOINTMENT_PROPOSED_START_WHOLE_TAG,
            filetime("2026-08-21T19:00:00Z"),
        );
        cases.push((missing_proposed_end, organizer()));

        let mut unsupported_cancellation = response_properties("IPM.Schedule.Meeting.Canceled");
        unsupported_cancellation.insert(
            PID_LID_APPOINTMENT_STATE_FLAGS_TAG,
            MapiValue::I32(0x0000_0005),
        );
        cases.push((unsupported_cancellation, organizer()));

        for (properties, recipients) in cases {
            assert!(meeting_scheduling_attachments(
                &properties,
                &recipients,
                "attendee@example.test",
                Some("Attendee"),
            )
            .is_err());
        }
    }

    #[test]
    fn recurring_request_signals_fail_closed_before_one_off_icalendar_generation() {
        let mut base = response_properties("IPM.Schedule.Meeting.Request");
        let attendee = PendingRecipient {
            row_id: 1,
            address: "attendee@example.test".to_string(),
            display_name: Some("Attendee".to_string()),
            recipient_type: 0x01,
            recipient_flags: 0x0000_0001,
        };

        for (tag, value) in [
            (PID_LID_RECURRING_TAG, MapiValue::Bool(true)),
            (PID_LID_IS_RECURRING_TAG, MapiValue::Bool(true)),
            (PID_LID_APPOINTMENT_RECUR_TAG, MapiValue::Binary(vec![0x01])),
        ] {
            let mut properties = base.clone();
            properties.insert(tag, value);
            let error = meeting_scheduling_attachments(
                &properties,
                std::slice::from_ref(&attendee),
                "organizer@example.test",
                Some("Organizer"),
            )
            .unwrap_err();
            assert!(error
                .to_string()
                .contains("recurring MAPI meeting request submission is unsupported"));
            assert!(meeting_scheduling_input_property_tag(tag));
        }

        base.insert(PID_LID_RECURRING_TAG, MapiValue::Bool(false));
        base.insert(PID_LID_IS_RECURRING_TAG, MapiValue::Bool(false));
        base.insert(PID_LID_APPOINTMENT_RECUR_TAG, MapiValue::Binary(Vec::new()));
        assert!(!has_recurring_meeting_properties(&base));
    }

    #[test]
    fn explicit_ordinary_message_class_suppresses_stale_meeting_state_flags() {
        let properties = HashMap::from([
            (
                PID_TAG_MESSAGE_CLASS_W,
                MapiValue::String("IPM.Note".to_string()),
            ),
            (PID_LID_APPOINTMENT_STATE_FLAGS_TAG, MapiValue::I32(3)),
        ]);

        assert!(!is_mapi_meeting_request(&properties));
        assert!(meeting_scheduling_attachments(
            &properties,
            &[],
            "alice@example.test",
            Some("Alice"),
        )
        .unwrap()
        .is_empty());
    }
}
