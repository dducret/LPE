use super::*;

const PID_TAG_RECIPIENT_ROW_ID: u32 = 0x3000_0003;

#[derive(Clone, Debug)]
pub(crate) struct MapiRecipient {
    pub(crate) recipient_type: u8,
    pub(crate) recipient_flags: u32,
    pub(crate) track_status: u32,
    pub(crate) order: u32,
    pub(crate) address: JmapEmailAddress,
}

pub(crate) const MESSAGE_RECIPIENT_COLUMNS: [u32; 16] = [
    PID_TAG_RECIPIENT_ROW_ID,
    PID_TAG_RECIPIENT_TYPE,
    PID_TAG_RECIPIENT_FLAGS,
    PID_TAG_RECIPIENT_ORDER,
    PID_TAG_RECIPIENT_TRACK_STATUS,
    PID_TAG_OBJECT_TYPE,
    PID_TAG_DISPLAY_TYPE,
    PID_TAG_DISPLAY_TYPE_EX,
    PID_TAG_ADDRESS_TYPE_W,
    PID_TAG_EMAIL_ADDRESS_W,
    PID_TAG_SMTP_ADDRESS_W,
    PID_TAG_DISPLAY_NAME_W,
    PID_TAG_RECIPIENT_DISPLAY_NAME_W,
    PID_TAG_SEARCH_KEY,
    PID_TAG_ENTRY_ID,
    PID_TAG_RECIPIENT_ENTRY_ID,
];

pub(crate) fn message_recipients(email: &JmapEmail) -> Vec<MapiRecipient> {
    let recipients = if let Some(request) = email.calendar_meeting_request.as_ref() {
        let mut recipients = Vec::new();
        if let Some(organizer) = request.organizer.as_ref() {
            recipients.push(MapiRecipient {
                recipient_type: 0x01,
                recipient_flags: 0x0000_0003,
                track_status: 0,
                order: 0,
                address: JmapEmailAddress {
                    address: organizer.email.clone(),
                    display_name: (!organizer.display_name.is_empty())
                        .then(|| organizer.display_name.clone()),
                },
            });
        }
        recipients.extend(request.attendees.iter().map(|attendee| MapiRecipient {
            recipient_type: meeting_attendee_recipient_type(attendee),
            recipient_flags: 0x0000_0001,
            track_status: meeting_track_status(&attendee.partstat),
            order: 0,
            address: JmapEmailAddress {
                address: attendee.email.clone(),
                display_name:
                    (!attendee.display_name.is_empty()).then(|| attendee.display_name.clone()),
            },
        }));
        recipients
    } else if let Some(response) = email.calendar_meeting_response.as_ref() {
        let mut recipients = Vec::new();
        if let Some(organizer) = response.organizer.as_ref() {
            // [MS-OXCICAL] section 2.1.3.1.1.20.16: the organizer row
            // carries both the sendable and organizer bits.
            recipients.push(MapiRecipient {
                recipient_type: 0x01,
                recipient_flags: 0x0000_0003,
                track_status: 0,
                order: 0,
                address: JmapEmailAddress {
                    address: organizer.email.clone(),
                    display_name: (!organizer.display_name.is_empty())
                        .then(|| organizer.display_name.clone()),
                },
            });
        } else {
            recipients.extend(ordinary_message_recipients(email));
        }
        if !response.attendee_email.trim().is_empty() {
            recipients.push(MapiRecipient {
                recipient_type: 0x01,
                recipient_flags: 0x0000_0001,
                track_status: meeting_track_status(&response.partstat),
                order: 0,
                address: JmapEmailAddress {
                    address: response.attendee_email.clone(),
                    display_name: (!response.attendee_name.is_empty())
                        .then(|| response.attendee_name.clone()),
                },
            });
        }
        recipients
    } else {
        ordinary_message_recipients(email)
    };
    recipients
        .into_iter()
        .enumerate()
        .map(|(order, mut recipient)| {
            recipient.order = order.min(u32::MAX as usize) as u32;
            recipient
        })
        .collect()
}

fn ordinary_message_recipients(email: &JmapEmail) -> Vec<MapiRecipient> {
    email
        .to
        .iter()
        .map(|address| MapiRecipient {
            recipient_type: 0x01,
            recipient_flags: 0x0000_0001,
            track_status: 0,
            order: 0,
            address: address.clone(),
        })
        .chain(email.cc.iter().map(|address| MapiRecipient {
            recipient_type: 0x02,
            recipient_flags: 0x0000_0001,
            track_status: 0,
            order: 0,
            address: address.clone(),
        }))
        .chain(
            message_can_expose_bcc(email)
                .then_some(email.bcc.iter())
                .into_iter()
                .flatten()
                .map(|address| MapiRecipient {
                    recipient_type: 0x03,
                    recipient_flags: 0x0000_0001,
                    track_status: 0,
                    order: 0,
                    address: address.clone(),
                }),
        )
        .collect()
}

fn meeting_track_status(partstat: &str) -> u32 {
    match partstat.to_ascii_lowercase().as_str() {
        "accepted" => 0x0000_0003,
        "tentative" => 0x0000_0002,
        "declined" => 0x0000_0004,
        _ => 0,
    }
}

pub(in crate::mapi) fn message_can_expose_bcc(email: &JmapEmail) -> bool {
    email.mailbox_role == "sent"
}

pub(in crate::mapi) fn serialize_recipient_row(
    recipient: &MapiRecipient,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    let recipient_flags = 0x0200u16 | 0x0010 | 0x0008 | 0x0003;
    row.extend_from_slice(&recipient_flags.to_le_bytes());
    write_utf16z(&mut row, &recipient.address.address);
    write_utf16z(&mut row, recipient.display_name());
    row.extend_from_slice(&(columns.len().min(u16::MAX as usize) as u16).to_le_bytes());
    write_standard_property_row(&mut row, &recipient_property_row(recipient, columns));
    row
}

pub(in crate::mapi) fn serialize_pending_recipient_row(recipient: &PendingRecipient) -> Vec<u8> {
    let mut row = Vec::new();
    let recipient_flags = 0x0200u16 | 0x0010 | 0x0008 | 0x0003;
    row.extend_from_slice(&recipient_flags.to_le_bytes());
    write_utf16z(&mut row, &recipient.address);
    write_utf16z(
        &mut row,
        recipient
            .display_name
            .as_deref()
            .unwrap_or(&recipient.address),
    );
    row.extend_from_slice(&0u16.to_le_bytes());
    row
}

fn recipient_property_row(recipient: &MapiRecipient, columns: &[u32]) -> Vec<u8> {
    let mut row = Vec::new();
    for property_tag in columns {
        let value = recipient
            .property_value(*property_tag)
            .expect("the fixed recipient column set has a projected value");
        write_mapi_value(&mut row, *property_tag, &value);
    }
    row
}

impl MapiRecipient {
    pub(crate) fn display_name(&self) -> &str {
        self.address
            .display_name
            .as_deref()
            .unwrap_or(&self.address.address)
    }

    pub(crate) fn entry_id(&self) -> Vec<u8> {
        // [MS-OXCICAL] sections 2.1.3.1.1.20.2 and 2.1.3.1.1.20.16:
        // unresolved SMTP attendees and organizers use a One-Off EntryID.
        calendar_one_off_entry_id(self.display_name(), &self.address.address)
    }

    pub(crate) fn search_key(&self) -> Vec<u8> {
        smtp_search_key(&self.address.address)
    }

    pub(in crate::mapi) fn property_value(&self, property_tag: u32) -> Option<MapiValue> {
        match property_tag {
            PID_TAG_RECIPIENT_ROW_ID | PID_TAG_RECIPIENT_ORDER => Some(MapiValue::U32(self.order)),
            PID_TAG_RECIPIENT_TYPE => Some(MapiValue::U32(u32::from(self.recipient_type))),
            PID_TAG_RECIPIENT_FLAGS => Some(MapiValue::U32(self.recipient_flags)),
            PID_TAG_RECIPIENT_TRACK_STATUS => Some(MapiValue::U32(self.track_status)),
            PID_TAG_OBJECT_TYPE => Some(MapiValue::U32(6)),
            PID_TAG_DISPLAY_TYPE | PID_TAG_DISPLAY_TYPE_EX => Some(MapiValue::U32(0)),
            PID_TAG_ADDRESS_TYPE_W => Some(MapiValue::String("SMTP".to_string())),
            PID_TAG_EMAIL_ADDRESS_W | PID_TAG_SMTP_ADDRESS_W => {
                Some(MapiValue::String(self.address.address.clone()))
            }
            PID_TAG_DISPLAY_NAME_W
            | PID_TAG_RECIPIENT_DISPLAY_NAME_W
            | PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W => {
                Some(MapiValue::String(self.display_name().to_string()))
            }
            PID_TAG_SEARCH_KEY => Some(MapiValue::Binary(self.search_key())),
            PID_TAG_ENTRY_ID | PID_TAG_RECIPIENT_ENTRY_ID => {
                Some(MapiValue::Binary(self.entry_id()))
            }
            _ => None,
        }
    }
}

pub(in crate::mapi) fn display_to(email: &JmapEmail) -> String {
    if let Some(request) = email.calendar_meeting_request.as_ref() {
        return request
            .attendees
            .iter()
            .filter(|attendee| meeting_attendee_recipient_type(attendee) == 0x01)
            .map(meeting_attendee_label)
            .collect::<Vec<_>>()
            .join("; ");
    }
    email
        .to
        .iter()
        .map(|address| {
            address
                .display_name
                .as_deref()
                .unwrap_or(&address.address)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("; ")
}

pub(in crate::mapi) fn display_cc(email: &JmapEmail) -> String {
    if let Some(request) = email.calendar_meeting_request.as_ref() {
        return request
            .attendees
            .iter()
            .filter(|attendee| meeting_attendee_recipient_type(attendee) == 0x02)
            .map(meeting_attendee_label)
            .collect::<Vec<_>>()
            .join("; ");
    }
    email
        .cc
        .iter()
        .map(|address| {
            address
                .display_name
                .as_deref()
                .unwrap_or(&address.address)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn meeting_attendee_label(attendee: &lpe_storage::CalendarMeetingAttendee) -> String {
    if attendee.display_name.trim().is_empty() {
        attendee.email.clone()
    } else {
        attendee.display_name.clone()
    }
}

fn meeting_attendee_recipient_type(attendee: &lpe_storage::CalendarMeetingAttendee) -> u8 {
    // [MS-OXCICAL] section 2.1.3.1.1.20.2: the table is ordered, so
    // CHAIR/required and optional roles take precedence over CUTYPE.
    if matches!(attendee.role.as_str(), "CHAIR" | "REQ-PARTICIPANT") {
        0x01
    } else if attendee.role == "OPT-PARTICIPANT" {
        0x02
    } else if matches!(attendee.cutype.as_str(), "RESOURCE" | "ROOM")
        || attendee.role == "NON-PARTICIPANT"
    {
        0x03
    } else {
        0x01
    }
}

pub(in crate::mapi) fn display_bcc(email: &JmapEmail) -> String {
    message_can_expose_bcc(email)
        .then_some(email.bcc.iter())
        .into_iter()
        .flatten()
        .map(|address| {
            address
                .display_name
                .as_deref()
                .unwrap_or(&address.address)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("; ")
}
