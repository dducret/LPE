use super::*;

pub(in crate::mapi) fn message_flags(email: &JmapEmail) -> u32 {
    mapi_mailstore::canonical_message_flags(email)
}

pub(in crate::mapi) fn unread_from_read_flags(read_flags: Option<u8>) -> Option<bool> {
    match read_flags {
        Some(flags) if flags & 0x10 != 0 => None,
        Some(flags) if flags & 0x04 != 0 => Some(true),
        Some(_) => Some(false),
        None => Some(false),
    }
}

pub(in crate::mapi) fn read_flags_are_valid(read_flags: Option<u8>, allow_default: bool) -> bool {
    let Some(flags) = read_flags else {
        return false;
    };
    const RF_SUPPRESS_RECEIPT: u8 = 0x01;
    const RF_RESERVED: u8 = 0x0A;
    const RF_CLEAR_READ_FLAG: u8 = 0x04;
    const RF_GENERATE_RECEIPT_ONLY: u8 = 0x10;
    const RF_CLEAR_NOTIFY_READ: u8 = 0x20;
    const RF_CLEAR_NOTIFY_UNREAD: u8 = 0x40;
    const RF_KNOWN_MASK: u8 = RF_SUPPRESS_RECEIPT
        | RF_RESERVED
        | RF_CLEAR_READ_FLAG
        | RF_GENERATE_RECEIPT_ONLY
        | RF_CLEAR_NOTIFY_READ
        | RF_CLEAR_NOTIFY_UNREAD;

    if flags & !RF_KNOWN_MASK != 0 {
        return false;
    }
    let effective = flags & !RF_RESERVED;
    let valid = matches!(effective, 0x00 | 0x01 | 0x05 | 0x10 | 0x20 | 0x40);
    valid && (allow_default || effective != 0)
}
