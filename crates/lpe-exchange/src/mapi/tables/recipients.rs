use super::*;

const PID_TAG_RECIPIENT_ROW_ID: u32 = 0x3000_0003;
const PID_TAG_RECIPIENT_TRACK_STATUS_TIME: u32 = 0x5FFB_0040;
const PID_TAG_RECIPIENT_PROPOSED: u32 = 0x5FE1_000B;
const PID_TAG_RECIPIENT_PROPOSED_START_TIME: u32 = 0x5FE3_0040;
const PID_TAG_RECIPIENT_PROPOSED_END_TIME: u32 = 0x5FE4_0040;

#[derive(Clone, Debug)]
pub(crate) struct MapiRecipient {
    pub(crate) recipient_type: u8,
    pub(crate) recipient_flags: u32,
    pub(crate) track_status: u32,
    pub(crate) track_status_time: Option<u64>,
    pub(crate) order: u32,
    pub(crate) address: JmapEmailAddress,
    principal_identity: Option<PrincipalRecipientIdentity>,
    proposed: Option<bool>,
    proposed_start: Option<u64>,
    proposed_end: Option<u64>,
}

#[derive(Clone, Debug)]
struct PrincipalRecipientIdentity {
    legacy_dn: String,
    entry_id: Vec<u8>,
    search_key: Vec<u8>,
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

// [MS-OXOCAL] sections 2.2.4.10.4 through 2.2.4.10.6: the organizer's
// Meeting object carries an attendee's active counter proposal on its
// RecipientRow. Ordinary mail keeps its existing recipient column contract.
pub(crate) const CALENDAR_EVENT_RECIPIENT_COLUMNS: [u32; 20] = [
    PID_TAG_RECIPIENT_ROW_ID,
    PID_TAG_RECIPIENT_TYPE,
    PID_TAG_RECIPIENT_FLAGS,
    PID_TAG_RECIPIENT_ORDER,
    PID_TAG_RECIPIENT_TRACK_STATUS,
    PID_TAG_RECIPIENT_TRACK_STATUS_TIME,
    PID_TAG_RECIPIENT_PROPOSED,
    PID_TAG_RECIPIENT_PROPOSED_START_TIME,
    PID_TAG_RECIPIENT_PROPOSED_END_TIME,
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
                track_status_time: None,
                order: 0,
                address: JmapEmailAddress {
                    address: organizer.email.clone(),
                    display_name: (!organizer.display_name.is_empty())
                        .then(|| organizer.display_name.clone()),
                },
                principal_identity: None,
                proposed: None,
                proposed_start: None,
                proposed_end: None,
            });
        }
        recipients.extend(request.attendees.iter().map(|attendee| MapiRecipient {
            recipient_type: meeting_attendee_recipient_type(attendee),
            recipient_flags: 0x0000_0001,
            track_status: meeting_track_status(&attendee.partstat),
            track_status_time: None,
            order: 0,
            address: JmapEmailAddress {
                address: attendee.email.clone(),
                display_name:
                    (!attendee.display_name.is_empty()).then(|| attendee.display_name.clone()),
            },
            principal_identity: None,
            proposed: None,
            proposed_start: None,
            proposed_end: None,
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
                track_status_time: None,
                order: 0,
                address: JmapEmailAddress {
                    address: organizer.email.clone(),
                    display_name: (!organizer.display_name.is_empty())
                        .then(|| organizer.display_name.clone()),
                },
                principal_identity: None,
                proposed: None,
                proposed_start: None,
                proposed_end: None,
            });
        } else {
            recipients.extend(ordinary_message_recipients(email));
        }
        if !response.attendee_email.trim().is_empty() {
            recipients.push(MapiRecipient {
                recipient_type: 0x01,
                recipient_flags: 0x0000_0001,
                track_status: meeting_track_status(&response.partstat),
                track_status_time: None,
                order: 0,
                address: JmapEmailAddress {
                    address: response.attendee_email.clone(),
                    display_name: (!response.attendee_name.is_empty())
                        .then(|| response.attendee_name.clone()),
                },
                principal_identity: None,
                proposed: None,
                proposed_start: None,
                proposed_end: None,
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

pub(crate) fn message_recipients_for_principal(
    email: &JmapEmail,
    principal: &AccountPrincipal,
) -> Vec<MapiRecipient> {
    let mut recipients = message_recipients(email);
    resolve_principal_recipient_identities(&mut recipients, principal);
    recipients
}

pub(crate) fn calendar_event_recipients_for_principal(
    event: &crate::mapi_store::MapiEvent,
    principal: &AccountPrincipal,
) -> Vec<MapiRecipient> {
    let participant_metadata =
        lpe_storage::parse_calendar_participants_metadata(&event.event.attendees_json);
    if participant_metadata.attendees.is_empty() {
        return Vec::new();
    }
    let mut recipients = calendar_pending_recipients(&event.event)
        .into_iter()
        .map(|recipient| {
            let attendee = if recipient.is_calendar_organizer() {
                None
            } else {
                recipient
                    .row_id
                    .checked_sub(1)
                    .and_then(|index| participant_metadata.attendees.get(index as usize))
            };
            let proposed_interval = attendee
                .filter(|attendee| attendee.counter_proposal)
                .and_then(|attendee| {
                    let start = crate::mapi_mailstore::filetime_from_rfc3339_utc(
                        attendee.proposed_start.as_deref()?,
                    );
                    let end = crate::mapi_mailstore::filetime_from_rfc3339_utc(
                        attendee.proposed_end.as_deref()?,
                    );
                    (start != 0 && end > start).then_some((start, end))
                });
            let track_status = attendee
                .map(|attendee| meeting_track_status(&attendee.partstat))
                .unwrap_or_default();
            MapiRecipient {
                recipient_type: recipient.recipient_type,
                recipient_flags: recipient.recipient_flags,
                track_status,
                // [MS-OXOCAL] sections 2.2.4.10.3 and 3.1.4.8.5.2:
                // the attendee row records when its current response arrived.
                track_status_time: attendee
                    .filter(|_| track_status != 0)
                    .and_then(|attendee| event.recipient_response_time(&attendee.email)),
                order: recipient.row_id,
                address: JmapEmailAddress {
                    address: recipient.address,
                    display_name: recipient.display_name,
                },
                principal_identity: None,
                proposed: attendee.map(|_| proposed_interval.is_some()),
                proposed_start: proposed_interval.map(|(start, _)| start),
                proposed_end: proposed_interval.map(|(_, end)| end),
            }
        })
        .collect::<Vec<_>>();
    resolve_principal_recipient_identities(&mut recipients, principal);
    recipients
}

pub(crate) fn calendar_event_recipient_columns() -> &'static [u32] {
    // RopReadRecipients does not repeat the column array advertised by
    // RopOpenMessage. Keep one Event contract so a concurrent COUNTER update
    // cannot make Outlook decode later rows against a different shape.
    &CALENDAR_EVENT_RECIPIENT_COLUMNS
}

fn resolve_principal_recipient_identities(
    recipients: &mut [MapiRecipient],
    principal: &AccountPrincipal,
) {
    let principal_email = principal.email.trim();
    if principal_email.is_empty() {
        return;
    }

    let entry = crate::mapi::nspi::principal_address_book_entry(principal);
    let identity = PrincipalRecipientIdentity {
        legacy_dn: crate::mapi::nspi::nspi_entry_unprefixed_legacy_dn(&entry),
        entry_id: crate::mapi::nspi::nspi_entry_permanent_entry_id(&entry),
        search_key: crate::mapi::nspi::nspi_entry_search_key(&entry),
    };
    for recipient in recipients.iter_mut() {
        if recipient
            .address
            .address
            .trim()
            .eq_ignore_ascii_case(principal_email)
        {
            recipient.principal_identity = Some(identity.clone());
        }
    }
}

fn ordinary_message_recipients(email: &JmapEmail) -> Vec<MapiRecipient> {
    email
        .to
        .iter()
        .map(|address| MapiRecipient {
            recipient_type: 0x01,
            recipient_flags: 0x0000_0001,
            track_status: 0,
            track_status_time: None,
            order: 0,
            address: address.clone(),
            principal_identity: None,
            proposed: None,
            proposed_start: None,
            proposed_end: None,
        })
        .chain(email.cc.iter().map(|address| MapiRecipient {
            recipient_type: 0x02,
            recipient_flags: 0x0000_0001,
            track_status: 0,
            track_status_time: None,
            order: 0,
            address: address.clone(),
            principal_identity: None,
            proposed: None,
            proposed_start: None,
            proposed_end: None,
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
                    track_status_time: None,
                    order: 0,
                    address: address.clone(),
                    principal_identity: None,
                    proposed: None,
                    proposed_start: None,
                    proposed_end: None,
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
    let property_row = recipient_property_row(recipient, columns);
    let mut serialized_property_row = Vec::with_capacity(property_row.len().saturating_add(1));
    write_standard_property_row(&mut serialized_property_row, &property_row);
    serialize_recipient_row_with_property_row(recipient, columns.len(), &serialized_property_row)
}

pub(in crate::mapi) fn serialize_calendar_event_recipient_row(
    recipient: &MapiRecipient,
    columns: &[u32],
) -> Vec<u8> {
    let values = columns
        .iter()
        .map(|property_tag| recipient.property_value(*property_tag))
        .collect::<Vec<_>>();
    let flagged = values.iter().any(Option::is_none);
    let mut property_row = vec![u8::from(flagged)];
    for (property_tag, value) in columns.iter().zip(values) {
        if let Some(value) = value {
            if flagged {
                property_row.push(0);
            }
            write_mapi_value(&mut property_row, *property_tag, &value);
        } else {
            property_row.push(0x0A);
            write_u32(&mut property_row, 0x8004_010F);
        }
    }
    serialize_recipient_row_with_property_row(recipient, columns.len(), &property_row)
}

fn serialize_recipient_row_with_property_row(
    recipient: &MapiRecipient,
    column_count: usize,
    property_row: &[u8],
) -> Vec<u8> {
    let mut row = Vec::new();
    if let Some(identity) = recipient.principal_identity.as_ref() {
        // [MS-OXCDATA] sections 2.8.3.1 and 2.8.3.2: a resolved
        // Address Book recipient uses the compact X500DN framing.
        let recipient_flags = 0x0200u16 | 0x0010 | 0x0001;
        row.extend_from_slice(&recipient_flags.to_le_bytes());
        row.push(0); // No MetaTagDnPrefix bytes are reused.
        row.push(0); // Messaging user.
        row.extend_from_slice(identity.legacy_dn.as_bytes());
        row.push(0);
        write_utf16z(&mut row, recipient.display_name());
    } else {
        let recipient_flags = 0x0200u16 | 0x0010 | 0x0008 | 0x0003;
        row.extend_from_slice(&recipient_flags.to_le_bytes());
        write_utf16z(&mut row, &recipient.address.address);
        write_utf16z(&mut row, recipient.display_name());
    }
    row.extend_from_slice(&(column_count.min(u16::MAX as usize) as u16).to_le_bytes());
    row.extend_from_slice(property_row);
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
        if let Some(identity) = self.principal_identity.as_ref() {
            return identity.entry_id.clone();
        }
        // [MS-OXCICAL] sections 2.1.3.1.1.20.2 and 2.1.3.1.1.20.16:
        // unresolved SMTP attendees and organizers use a One-Off EntryID.
        calendar_one_off_entry_id(self.display_name(), &self.address.address)
    }

    pub(crate) fn search_key(&self) -> Vec<u8> {
        self.principal_identity
            .as_ref()
            .map(|identity| identity.search_key.clone())
            .unwrap_or_else(|| smtp_search_key(&self.address.address))
    }

    pub(crate) fn property_value(&self, property_tag: u32) -> Option<MapiValue> {
        match property_tag {
            PID_TAG_RECIPIENT_ROW_ID | PID_TAG_RECIPIENT_ORDER => Some(MapiValue::U32(self.order)),
            PID_TAG_RECIPIENT_TYPE => Some(MapiValue::U32(u32::from(self.recipient_type))),
            PID_TAG_RECIPIENT_FLAGS => Some(MapiValue::U32(self.recipient_flags)),
            PID_TAG_RECIPIENT_TRACK_STATUS => Some(MapiValue::U32(self.track_status)),
            PID_TAG_RECIPIENT_TRACK_STATUS_TIME => self.track_status_time.map(MapiValue::U64),
            PID_TAG_RECIPIENT_PROPOSED => self.proposed.map(MapiValue::Bool),
            PID_TAG_RECIPIENT_PROPOSED_START_TIME => self.proposed_start.map(MapiValue::U64),
            PID_TAG_RECIPIENT_PROPOSED_END_TIME => self.proposed_end.map(MapiValue::U64),
            PID_TAG_OBJECT_TYPE => Some(MapiValue::U32(6)),
            PID_TAG_DISPLAY_TYPE => Some(MapiValue::U32(0)),
            PID_TAG_DISPLAY_TYPE_EX => Some(MapiValue::U32(
                self.principal_identity
                    .as_ref()
                    .map(|_| 0x4000_0000)
                    .unwrap_or_default(),
            )),
            PID_TAG_ADDRESS_TYPE_W => Some(MapiValue::String(
                if self.principal_identity.is_some() {
                    "EX"
                } else {
                    "SMTP"
                }
                .to_string(),
            )),
            PID_TAG_EMAIL_ADDRESS_W => Some(MapiValue::String(
                self.principal_identity
                    .as_ref()
                    .map(|identity| identity.legacy_dn.clone())
                    .unwrap_or_else(|| self.address.address.clone()),
            )),
            PID_TAG_SMTP_ADDRESS_W => Some(MapiValue::String(self.address.address.clone())),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendar_event_recipient_columns_are_invariant() {
        assert_eq!(
            calendar_event_recipient_columns(),
            &CALENDAR_EVENT_RECIPIENT_COLUMNS
        );
    }
}
