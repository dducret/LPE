use super::*;
use crate::mapi::properties::{
    email_property_value_for_principal, email_recipient_delivery_property_tags,
    email_sender_address, email_sender_name, email_sent_representing_address,
    email_sent_representing_name, sender_entry_id, sent_representing_entry_id, smtp_search_key,
    MapiValue,
};
use lpe_mail_auth::AccountPrincipal;

pub(super) fn write_fast_transfer_message_identity(
    buffer: &mut Vec<u8>,
    email: &JmapEmail,
    principal: Option<&AccountPrincipal>,
    mut includes: impl FnMut(u32) -> bool,
) {
    let sender_name = email_sender_name(email);
    let sender_address = email_sender_address(email);
    write_identity(
        buffer,
        &mut includes,
        sender_name,
        sender_address,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_ADDRESS_TYPE_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_SENDER_SMTP_ADDRESS_W,
        PID_TAG_SENDER_ENTRY_ID,
        PID_TAG_SENDER_SEARCH_KEY,
        &sender_entry_id(email),
    );

    let representing_name = email_sent_representing_name(email);
    let representing_address = email_sent_representing_address(email);
    write_identity(
        buffer,
        &mut includes,
        representing_name,
        representing_address,
        PID_TAG_SENT_REPRESENTING_NAME_W,
        PID_TAG_SENT_REPRESENTING_ADDRESS_TYPE_W,
        PID_TAG_SENT_REPRESENTING_EMAIL_ADDRESS_W,
        PID_TAG_SENT_REPRESENTING_SMTP_ADDRESS_W,
        PID_TAG_SENT_REPRESENTING_ENTRY_ID,
        PID_TAG_SENT_REPRESENTING_SEARCH_KEY,
        &sent_representing_entry_id(email),
    );

    let Some(principal) = principal else {
        return;
    };
    for property_tag in email_recipient_delivery_property_tags(email, principal) {
        if !includes(property_tag) {
            continue;
        }
        match email_property_value_for_principal(email, principal, property_tag)
            .expect("enumerated receiver property has a principal-aware value")
        {
            MapiValue::Bool(value) => write_bool_property(buffer, property_tag, value),
            MapiValue::String(value) => write_utf16_property(buffer, property_tag, &value),
            MapiValue::Binary(value) => write_binary_property(buffer, property_tag, &value),
            value => unreachable!("unexpected FastTransfer receiver value {value:?}"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn write_identity(
    buffer: &mut Vec<u8>,
    includes: &mut impl FnMut(u32) -> bool,
    name: &str,
    address: &str,
    name_tag: u32,
    address_type_tag: u32,
    email_address_tag: u32,
    smtp_address_tag: u32,
    entry_id_tag: u32,
    search_key_tag: u32,
    entry_id: &[u8],
) {
    for (property_tag, value) in [
        (name_tag, name),
        (address_type_tag, "SMTP"),
        (email_address_tag, address),
        (smtp_address_tag, address),
    ] {
        if includes(property_tag) {
            write_utf16_property(buffer, property_tag, value);
        }
    }
    if includes(entry_id_tag) {
        write_binary_property(buffer, entry_id_tag, entry_id);
    }
    if includes(search_key_tag) {
        write_binary_property(buffer, search_key_tag, &smtp_search_key(address));
    }
}
