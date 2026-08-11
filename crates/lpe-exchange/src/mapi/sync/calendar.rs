use super::*;
use lpe_storage::{parse_calendar_participants_metadata, AccessibleEvent};

fn calendar_one_off_entry_id(display_name: &str, email_address: &str) -> Vec<u8> {
    // [MS-OXCDATA] section 2.2.5.1 and [MS-OXCICAL] section
    // 2.1.3.1.1.20.2: unresolved SMTP attendees use a Unicode One-Off EntryID.
    let mut entry_id = Vec::new();
    entry_id.extend_from_slice(&0u32.to_le_bytes());
    entry_id.extend_from_slice(&[
        0x81, 0x2B, 0x1F, 0xA4, 0xBE, 0xA3, 0x10, 0x19, 0x9D, 0x6E, 0x00, 0xDD, 0x01, 0x0F, 0x54,
        0x02,
    ]);
    entry_id.extend_from_slice(&0u16.to_le_bytes());
    entry_id.extend_from_slice(&[0x01, 0x80]);
    for value in [display_name, "SMTP", email_address] {
        for unit in value.encode_utf16() {
            entry_id.extend_from_slice(&unit.to_le_bytes());
        }
        entry_id.extend_from_slice(&0u16.to_le_bytes());
    }
    entry_id
}

fn calendar_recipient_sync_fact(
    event: &AccessibleEvent,
    recipient: PendingRecipient,
    track_status: u32,
) -> mapi_mailstore::SpecialMessageRecipientSyncFact {
    let display_name = recipient
        .display_name
        .clone()
        .unwrap_or_else(|| recipient.address.clone());
    let is_owner = !recipient.address.trim().is_empty()
        && recipient
            .address
            .trim()
            .eq_ignore_ascii_case(event.owner_email.trim());
    let (address_type, email_address, entry_id, display_type_ex) = if is_owner {
        let address_book_entry = crate::store::ExchangeAddressBookEntry {
            id: event.owner_account_id,
            display_name: display_name.clone(),
            email: recipient.address.clone(),
            entry_kind: crate::store::ExchangeAddressBookEntryKind::Account,
            directory_kind: crate::store::ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
            details: crate::store::ExchangeAddressBookEntryDetails::default(),
        };
        (
            "EX".to_string(),
            crate::mapi::nspi::nspi_entry_unprefixed_legacy_dn(&address_book_entry),
            crate::mapi::nspi::nspi_entry_permanent_entry_id(&address_book_entry),
            0x4000_0000,
        )
    } else {
        (
            "SMTP".to_string(),
            recipient.address.clone(),
            calendar_one_off_entry_id(&display_name, &recipient.address),
            0,
        )
    };
    mapi_mailstore::SpecialMessageRecipientSyncFact {
        row_id: recipient.row_id,
        recipient_type: u32::from(recipient.recipient_type),
        recipient_flags: recipient.recipient_flags,
        track_status,
        display_type_ex,
        address_type,
        email_address,
        smtp_address: recipient.address,
        display_name,
        entry_id,
    }
}

pub(super) fn calendar_recipient_sync_facts(
    event: &AccessibleEvent,
) -> Vec<mapi_mailstore::SpecialMessageRecipientSyncFact> {
    let participant_metadata = parse_calendar_participants_metadata(&event.attendees_json);
    calendar_pending_recipients(event)
        .into_iter()
        .filter(|recipient| !recipient.address.trim().is_empty())
        .map(|recipient| {
            let track_status = if recipient.is_calendar_organizer() {
                0
            } else {
                recipient
                    .row_id
                    .checked_sub(1)
                    .and_then(|index| participant_metadata.attendees.get(index as usize))
                    .map(|attendee| match attendee.partstat.as_str() {
                        "tentative" => 2,
                        "accepted" => 3,
                        "declined" => 4,
                        _ => 0,
                    })
                    .unwrap_or(0)
            };
            calendar_recipient_sync_fact(event, recipient, track_status)
        })
        .collect()
}
