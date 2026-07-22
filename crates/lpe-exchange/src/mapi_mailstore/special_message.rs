use super::*;
use crate::mapi::properties::{
    fast_transfer_named_property_for_message_tag, MapiNamedPropertyKind,
};

// [MS-OXCROPS] sections 2.2.12.7.1 and 2.2.12.8.1.
const ROP_FAST_TRANSFER_SOURCE_COPY_TO: u8 = 0x4D;
const ROP_FAST_TRANSFER_SOURCE_COPY_PROPERTIES: u8 = 0x69;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpecialMessageSyncFact {
    pub(crate) folder_id: u64,
    pub(crate) item_id: u64,
    pub(crate) canonical_id: Uuid,
    pub(crate) associated: bool,
    pub(crate) subject: String,
    pub(crate) body_text: Option<String>,
    pub(crate) message_class: String,
    pub(crate) last_modified_filetime: u64,
    pub(crate) message_size: i64,
    pub(crate) read_state: Option<bool>,
    pub(crate) named_properties: Vec<(u32, SpecialMessagePropertyValue)>,
    pub(crate) named_property_definitions: HashMap<u16, crate::mapi::properties::MapiNamedProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SpecialMessagePropertyValue {
    Binary(Vec<u8>),
    Bool(bool),
    Guid([u8; 16]),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    String(String),
    MultiString(Vec<String>),
    Time(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SpecialMessageFastTransferSelection {
    access: bool,
    access_level: bool,
    search_key: bool,
}

impl SpecialMessageFastTransferSelection {
    #[cfg(test)]
    pub(crate) const fn all() -> Self {
        Self {
            access: true,
            access_level: true,
            search_key: true,
        }
    }

    pub(crate) fn for_copy_rop(rop_id: u8, property_tags: &[u32]) -> Self {
        Self {
            access: fast_transfer_property_included(rop_id, property_tags, PID_TAG_ACCESS),
            access_level: fast_transfer_property_included(
                rop_id,
                property_tags,
                PID_TAG_ACCESS_LEVEL,
            ),
            search_key: fast_transfer_property_included(rop_id, property_tags, PID_TAG_SEARCH_KEY),
        }
    }
}

pub(crate) fn fast_transfer_property_included(
    rop_id: u8,
    property_tags: &[u32],
    property_tag: u32,
) -> bool {
    match rop_id {
        ROP_FAST_TRANSFER_SOURCE_COPY_TO => !property_tags.contains(&property_tag),
        ROP_FAST_TRANSFER_SOURCE_COPY_PROPERTIES => property_tags.contains(&property_tag),
        _ => true,
    }
}

fn special_message_binary_property(
    object: &SpecialMessageSyncFact,
    property_tag: u32,
) -> Option<&[u8]> {
    object
        .named_properties
        .iter()
        .find_map(|(tag, value)| match (*tag == property_tag, value) {
            (true, SpecialMessagePropertyValue::Binary(value)) => Some(value.as_slice()),
            _ => None,
        })
}

fn special_message_u32_property(object: &SpecialMessageSyncFact, property_tag: u32) -> Option<u32> {
    object
        .named_properties
        .iter()
        .find_map(|(tag, value)| match (*tag == property_tag, value) {
            (true, SpecialMessagePropertyValue::I32(value)) => u32::try_from(*value).ok(),
            (true, SpecialMessagePropertyValue::U32(value)) => Some(*value),
            _ => None,
        })
}

pub(crate) fn special_message_source_key(object: &SpecialMessageSyncFact) -> Vec<u8> {
    // [MS-OXCFXICS] section 3.2.5.5: output a persisted PidTagSourceKey and
    // generate one from the internal identifier only when it is missing.
    special_message_binary_property(object, PID_TAG_SOURCE_KEY)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| source_key_for_store_id(object.item_id))
}

pub(super) fn special_message_sync_source_key(
    object: &SpecialMessageSyncFact,
    sync_flags: u16,
) -> Vec<u8> {
    if sync_flags & SYNC_FLAG_NO_FOREIGN_IDENTIFIERS != 0 {
        source_key_for_store_id(object.item_id)
    } else {
        special_message_source_key(object)
    }
}

pub(super) fn special_message_parent_source_key(object: &SpecialMessageSyncFact) -> Vec<u8> {
    special_message_binary_property(object, PID_TAG_PARENT_SOURCE_KEY)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| source_key_for_store_id(object.folder_id))
}

pub(super) fn special_message_search_key(object: &SpecialMessageSyncFact) -> Vec<u8> {
    // [MS-OXCPRPT] section 2.2.1.9: SearchKey is a read-only search identity.
    special_message_binary_property(object, PID_TAG_SEARCH_KEY)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| source_key_for_store_id(object.item_id))
}

pub(super) fn special_message_change_key(object: &SpecialMessageSyncFact) -> Vec<u8> {
    special_message_binary_property(object, PID_TAG_CHANGE_KEY)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| change_key_for_change_number(change_number_for_store_id(object.item_id)))
}

pub(super) fn special_message_predecessor_change_list(object: &SpecialMessageSyncFact) -> Vec<u8> {
    special_message_binary_property(object, PID_TAG_PREDECESSOR_CHANGE_LIST)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| predecessor_change_list(change_number_for_store_id(object.item_id)))
}

pub(super) fn special_message_change_number(object: &SpecialMessageSyncFact) -> u64 {
    object
        .named_properties
        .iter()
        .find_map(|(tag, value)| match (*tag, value) {
            (PID_TAG_CHANGE_NUMBER, SpecialMessagePropertyValue::U64(value)) => Some(*value),
            _ => None,
        })
        .unwrap_or_else(|| change_number_for_store_id(object.item_id))
}

pub(super) fn special_message_flags(object: &SpecialMessageSyncFact) -> u32 {
    let flags = object
        .named_properties
        .iter()
        .find_map(|(tag, value)| match (*tag, value) {
            (PID_TAG_MESSAGE_FLAGS, SpecialMessagePropertyValue::I32(value)) => Some(*value as u32),
            (PID_TAG_MESSAGE_FLAGS, SpecialMessagePropertyValue::U32(value)) => Some(*value),
            _ => None,
        });
    match flags {
        Some(flags) if object.associated => flags | MSGFLAG_FAI,
        Some(flags) => flags,
        None => {
            if object.associated {
                MSGFLAG_FAI
            } else if object.read_state == Some(false) {
                0
            } else {
                MSGFLAG_READ
            }
        }
    }
}

pub(super) fn special_message_access(object: &SpecialMessageSyncFact) -> u32 {
    special_message_u32_property(object, PID_TAG_ACCESS).unwrap_or(MAPI_MESSAGE_ACCESS)
}

pub(super) fn special_message_access_level(object: &SpecialMessageSyncFact) -> u32 {
    // [MS-OXCPRPT] section 2.2.1.2 defines zero as read-only. For the traced
    // CopyTo this value is inferred from the ReadOnly OpenModeFlags=0x00 in
    // [MS-OXCMSG] section 2.2.3.1.1; [MS-OXCFXICS] section 4.5 also projects
    // zero in Microsoft's content-synchronization example. FastTransfer does
    // not itself specify this handle-to-value relationship.
    special_message_u32_property(object, PID_TAG_ACCESS_LEVEL).unwrap_or(0)
}

pub(super) fn special_message_property_is_server_access(property_tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(property_tag),
        PID_TAG_ACCESS | PID_TAG_ACCESS_LEVEL
    )
}

pub(super) fn special_message_property_is_ics_identity(property_tag: u32) -> bool {
    matches!(
        property_tag,
        PID_TAG_SOURCE_KEY
            | PID_TAG_PARENT_SOURCE_KEY
            | PID_TAG_RECORD_KEY
            | PID_TAG_SEARCH_KEY
            | PID_TAG_CHANGE_KEY
            | PID_TAG_PREDECESSOR_CHANGE_LIST
            | PID_TAG_CHANGE_NUMBER
    )
}

pub(super) fn special_message_property_is_copy_identity(property_tag: u32) -> bool {
    matches!(
        property_tag,
        PID_TAG_SOURCE_KEY
            | PID_TAG_PARENT_SOURCE_KEY
            | PID_TAG_RECORD_KEY
            | PID_TAG_SEARCH_KEY
            | PID_TAG_CHANGE_KEY
            | PID_TAG_PREDECESSOR_CHANGE_LIST
            | PID_TAG_CHANGE_NUMBER
    )
}

pub(crate) fn fast_transfer_message_content_buffer_with_special_object(
    folder_id: u64,
    object: &SpecialMessageSyncFact,
    send_options: u8,
    selection: SpecialMessageFastTransferSelection,
    message_children: FastTransferMessageChildren,
) -> Vec<u8> {
    let mut buffer = Vec::new();
    write_fast_transfer_special_message_content(
        &mut buffer,
        folder_id,
        object,
        send_options,
        selection,
        message_children,
    );
    buffer
}

fn write_fast_transfer_special_message_content(
    buffer: &mut Vec<u8>,
    folder_id: u64,
    object: &SpecialMessageSyncFact,
    send_options: u8,
    selection: SpecialMessageFastTransferSelection,
    message_children: FastTransferMessageChildren,
) {
    let source_key = special_message_source_key(object);
    let change_key = special_message_change_key(object);
    let predecessor_change_list = special_message_predecessor_change_list(object);
    write_binary_property(
        buffer,
        PID_TAG_PARENT_SOURCE_KEY,
        &source_key_for_store_id(folder_id),
    );
    write_binary_property(buffer, PID_TAG_SOURCE_KEY, &source_key);
    // [MS-OXCMSG] section 2.2.1.1 requires Access and AccessLevel on every
    // Message object. [MS-OXCPRPT] sections 2.2.1.1 and 2.2.1.2 define their
    // values, and [MS-OXCFXICS] sections 3.2.5.8.1.1 and 3.2.5.8.1.2 apply
    // the CopyTo exclusion and CopyProperties inclusion lists.
    if selection.access {
        write_i32_property(
            buffer,
            PID_TAG_ACCESS,
            special_message_access(object) as i32,
        );
    }
    if selection.access_level {
        write_i32_property(
            buffer,
            PID_TAG_ACCESS_LEVEL,
            special_message_access_level(object) as i32,
        );
    }
    // [MS-OXCMSG] sections 2.2.1.1 and 3.2.5.2 and [MS-OXCPRPT]
    // section 2.2.1.9: every Message has a server-generated, read-only
    // SearchKey. It remains transmittable in the direct messageContent root
    // under [MS-OXCFXICS] sections 3.2.5.8.1.1 and 3.2.5.12.
    if selection.search_key {
        write_binary_property(
            buffer,
            PID_TAG_SEARCH_KEY,
            &special_message_search_key(object),
        );
    }
    write_u32(buffer, PID_TAG_LAST_MODIFICATION_TIME);
    write_i64(buffer, object.last_modified_filetime as i64);
    write_binary_property(buffer, PID_TAG_CHANGE_KEY, &change_key);
    write_binary_property(
        buffer,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &predecessor_change_list,
    );
    // [MS-OXCFXICS] sections 2.2.4.3.16 and 3.2.5.12, with
    // [MS-OXPROPS] section 1.3.3: direct messageContent downloads exclude
    // provider-internal PidTagAssociated (0x67AA) and PidTagMid (0x674A).
    // [MS-OXCMSG] section 2.2.1.6: mfFAI remains the transmittable FAI
    // discriminator for CopyTo/CopyProperties.
    let message_flags = special_message_flags(object);
    write_i32_property(buffer, PID_TAG_MESSAGE_FLAGS, message_flags as i32);
    write_utf16_property(buffer, PID_TAG_SUBJECT_W, &object.subject);
    // [MS-OXCFXICS] sections 2.2.3.1.1.1.1 and 3.2.5.8.1.1:
    // canonical subjects are stored as Unicode, so Unicode/ForceUnicode
    // select PtypUnicode; without either flag use PtypString8.
    if send_options & (FAST_TRANSFER_SEND_OPTION_UNICODE | FAST_TRANSFER_SEND_OPTION_FORCE_UNICODE)
        != 0
    {
        write_utf16_property(buffer, PID_TAG_NORMALIZED_SUBJECT_W, &object.subject);
    } else {
        write_string8_property(buffer, PID_TAG_NORMALIZED_SUBJECT_A, &object.subject);
    }
    write_utf16_property(buffer, PID_TAG_MESSAGE_CLASS_W, &object.message_class);
    if let Some(body_text) = &object.body_text {
        write_utf16_property(buffer, PID_TAG_BODY_W, body_text);
    }
    write_i32_property(buffer, PID_TAG_MESSAGE_SIZE, object.message_size as i32);
    for (tag, value) in &object.named_properties {
        if !special_message_property_is_copy_identity(*tag)
            && !special_message_property_is_server_access(*tag)
            && *tag != PID_TAG_MESSAGE_FLAGS
            && !provider_defined_internal_property(*tag)
        {
            write_special_message_property(buffer, object, *tag, value);
        }
    }
    // [MS-OXCFXICS] sections 2.2.4.1.5.1, 2.2.4.3.12, and 3.2.5.10:
    // included recipient and attachment collections are each preceded by
    // MetaTagFXDelProp, including when the collection is empty.
    if message_children.recipients {
        write_i32_property(
            buffer,
            META_TAG_FX_DEL_PROP,
            PID_TAG_MESSAGE_RECIPIENTS as i32,
        );
    }
    if message_children.attachments {
        write_i32_property(
            buffer,
            META_TAG_FX_DEL_PROP,
            PID_TAG_MESSAGE_ATTACHMENTS as i32,
        );
    }
}

pub(super) fn write_special_message_property(
    buffer: &mut Vec<u8>,
    object: &SpecialMessageSyncFact,
    property_tag: u32,
    value: &SpecialMessagePropertyValue,
) {
    if !write_fast_transfer_property_info(buffer, object, property_tag) {
        return;
    }
    match value {
        SpecialMessagePropertyValue::Binary(value) => {
            write_u32(buffer, value.len().min(u32::MAX as usize) as u32);
            buffer.extend_from_slice(value);
        }
        SpecialMessagePropertyValue::Bool(value) => {
            buffer.extend_from_slice(&(*value as u16).to_le_bytes());
        }
        SpecialMessagePropertyValue::Guid(value) => buffer.extend_from_slice(value),
        SpecialMessagePropertyValue::I32(value) => write_i32(buffer, *value),
        SpecialMessagePropertyValue::I64(value) => write_i64(buffer, *value),
        SpecialMessagePropertyValue::U32(value) => write_u32(buffer, *value),
        SpecialMessagePropertyValue::U64(value) => write_i64(buffer, *value as i64),
        SpecialMessagePropertyValue::String(value) => {
            let mut bytes = value
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<_>>();
            bytes.extend_from_slice(&0u16.to_le_bytes());
            write_u32(buffer, bytes.len().min(u32::MAX as usize) as u32);
            buffer.extend_from_slice(&bytes);
        }
        SpecialMessagePropertyValue::MultiString(values) => {
            write_u32(buffer, values.len().min(u32::MAX as usize) as u32);
            for value in values.iter().take(u32::MAX as usize) {
                let mut bytes = value
                    .encode_utf16()
                    .flat_map(u16::to_le_bytes)
                    .collect::<Vec<_>>();
                bytes.extend_from_slice(&0u16.to_le_bytes());
                write_u32(buffer, bytes.len().min(u32::MAX as usize) as u32);
                buffer.extend_from_slice(&bytes);
            }
        }
        SpecialMessagePropertyValue::Time(value) => {
            write_i64(buffer, filetime_from_rfc3339_utc(value) as i64)
        }
    }
}

fn write_fast_transfer_property_info(
    buffer: &mut Vec<u8>,
    object: &SpecialMessageSyncFact,
    property_tag: u32,
) -> bool {
    let property_id = (property_tag >> 16) as u16;
    if property_id < 0x8000 {
        write_u32(buffer, property_tag);
        return true;
    }

    let property = object
        .named_property_definitions
        .get(&property_id)
        .cloned()
        .or_else(|| {
            fast_transfer_named_property_for_message_tag(&object.message_class, property_tag)
        });
    let Some(property) = property else {
        tracing::error!(
            adapter = "mapi",
            message_class = %object.message_class,
            property_tag = format_args!("0x{property_tag:08x}"),
            "cannot encode FastTransfer named property without its mailbox mapping"
        );
        return false;
    };

    // [MS-OXCFXICS] section 2.2.4.1: a named property is serialized as
    // the property tag, property-set GUID and its LID/name definition.
    write_u32(buffer, property_tag);
    buffer.extend_from_slice(&property.guid);
    match property.kind {
        MapiNamedPropertyKind::Lid(lid) => {
            buffer.push(0x00);
            write_u32(buffer, lid);
        }
        MapiNamedPropertyKind::Name(name) => {
            buffer.push(0x01);
            buffer.extend(name.encode_utf16().flat_map(u16::to_le_bytes));
            buffer.extend_from_slice(&0u16.to_le_bytes());
        }
    }
    true
}
