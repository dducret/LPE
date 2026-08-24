use super::*;
use crate::mapi::properties::MapiValue;
use crate::mapi::{message_recipients, message_recipients_for_principal, MapiRecipient};
use lpe_mail_auth::AccountPrincipal;

pub(super) fn write_visible_recipient_facts(buffer: &mut Vec<u8>, email: &JmapEmail) {
    let recipients = message_recipients(email);
    buffer.extend_from_slice(&(recipients.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for recipient in recipients.into_iter().take(u16::MAX as usize) {
        buffer.push(recipient.recipient_type);
        buffer.extend_from_slice(&recipient.recipient_flags.to_le_bytes());
        buffer.extend_from_slice(&recipient.track_status.to_le_bytes());
        buffer.extend_from_slice(&recipient.order.to_le_bytes());
        write_prefixed_bytes(buffer, recipient.address.address.as_bytes());
        write_prefixed_bytes(buffer, recipient.display_name().as_bytes());
    }
}

pub(super) fn write_fast_transfer_visible_recipients(buffer: &mut Vec<u8>, email: &JmapEmail) {
    for recipient in message_recipients(email) {
        write_fast_transfer_recipient(buffer, &recipient);
    }
}

pub(super) fn write_fast_transfer_visible_recipients_for_principal(
    buffer: &mut Vec<u8>,
    email: &JmapEmail,
    principal: &AccountPrincipal,
) {
    for recipient in message_recipients_for_principal(email, principal) {
        write_fast_transfer_recipient(buffer, &recipient);
    }
}

fn write_fast_transfer_recipient(buffer: &mut Vec<u8>, recipient: &MapiRecipient) {
    write_u32(buffer, START_RECIP);
    // [MS-OXCFXICS] section 2.2.4.3.23 fixes PidTagRowid in the first
    // position of each recipient element.
    for property_tag in [
        PID_TAG_ROWID,
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
    ] {
        match recipient
            .property_value(property_tag)
            .expect("the fixed FastTransfer recipient set has a projected value")
        {
            MapiValue::U32(value) => write_i32_property(buffer, property_tag, value as i32),
            MapiValue::String(value) => write_utf16_property(buffer, property_tag, &value),
            MapiValue::Binary(value) => write_binary_property(buffer, property_tag, &value),
            value => unreachable!("unexpected FastTransfer recipient value {value:?}"),
        }
    }
    write_u32(buffer, END_TO_RECIP);
}
