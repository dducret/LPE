use super::*;
use crate::mapi::{message_recipients, MapiRecipient};

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

fn write_fast_transfer_recipient(buffer: &mut Vec<u8>, recipient: &MapiRecipient) {
    let entry_id = recipient.entry_id();
    write_u32(buffer, START_RECIP);
    // [MS-OXCFXICS] section 2.2.4.3.23 fixes PidTagRowid in the first
    // position of each recipient element.
    write_i32_property(buffer, PID_TAG_ROWID, recipient.order as i32);
    write_i32_property(
        buffer,
        PID_TAG_RECIPIENT_TYPE,
        i32::from(recipient.recipient_type),
    );
    write_i32_property(
        buffer,
        PID_TAG_RECIPIENT_FLAGS,
        recipient.recipient_flags as i32,
    );
    write_i32_property(buffer, PID_TAG_RECIPIENT_ORDER, recipient.order as i32);
    write_i32_property(
        buffer,
        PID_TAG_RECIPIENT_TRACK_STATUS,
        recipient.track_status as i32,
    );
    write_i32_property(buffer, PID_TAG_OBJECT_TYPE, 6);
    write_i32_property(buffer, PID_TAG_DISPLAY_TYPE, 0);
    write_i32_property(buffer, PID_TAG_DISPLAY_TYPE_EX, 0);
    write_utf16_property(buffer, PID_TAG_ADDRESS_TYPE_W, "SMTP");
    write_utf16_property(buffer, PID_TAG_EMAIL_ADDRESS_W, &recipient.address.address);
    write_utf16_property(buffer, PID_TAG_SMTP_ADDRESS_W, &recipient.address.address);
    write_utf16_property(buffer, PID_TAG_DISPLAY_NAME_W, recipient.display_name());
    write_utf16_property(
        buffer,
        PID_TAG_RECIPIENT_DISPLAY_NAME_W,
        recipient.display_name(),
    );
    write_binary_property(buffer, PID_TAG_SEARCH_KEY, &recipient.search_key());
    write_binary_property(buffer, PID_TAG_ENTRY_ID, &entry_id);
    write_binary_property(buffer, PID_TAG_RECIPIENT_ENTRY_ID, &entry_id);
    write_u32(buffer, END_TO_RECIP);
}
