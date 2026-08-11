use super::*;
use crate::mapi::properties::{
    fast_transfer_named_property_for_message_tag, MapiNamedPropertyKind,
};

use super::FastTransferDirectPropertyFilter;

pub(super) const PID_TAG_MESSAGE_STATUS: u32 = 0x0E17_0003;
pub(super) const PID_TAG_HAS_ATTACHMENTS: u32 = 0x0E1B_000B;

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
    pub(crate) recipients: Vec<SpecialMessageRecipientSyncFact>,
    pub(crate) named_properties: Vec<(u32, SpecialMessagePropertyValue)>,
    pub(crate) named_property_definitions: HashMap<u16, crate::mapi::properties::MapiNamedProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpecialMessageRecipientSyncFact {
    pub(crate) row_id: u32,
    pub(crate) recipient_type: u32,
    pub(crate) recipient_flags: u32,
    pub(crate) track_status: u32,
    pub(crate) display_type_ex: u32,
    pub(crate) address_type: String,
    pub(crate) email_address: String,
    pub(crate) smtp_address: String,
    pub(crate) display_name: String,
    pub(crate) entry_id: Vec<u8>,
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

pub(super) fn special_message_delivery_sort_time(object: &SpecialMessageSyncFact) -> u64 {
    object
        .named_properties
        .iter()
        .find_map(|(tag, value)| {
            (canonical_property_storage_tag(*tag) == PID_TAG_MESSAGE_DELIVERY_TIME)
                .then(|| match value {
                    SpecialMessagePropertyValue::I64(value) => u64::try_from(*value).ok(),
                    SpecialMessagePropertyValue::U64(value) => Some(*value),
                    SpecialMessagePropertyValue::Time(value) => {
                        super::manifest::parse_rfc3339_utc_filetime(value)
                    }
                    _ => None,
                })
                .flatten()
        })
        .unwrap_or(object.last_modified_filetime)
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

fn special_message_bool_property(
    object: &SpecialMessageSyncFact,
    property_tag: u32,
) -> Option<bool> {
    object
        .named_properties
        .iter()
        .find_map(|(tag, value)| match (*tag == property_tag, value) {
            (true, SpecialMessagePropertyValue::Bool(value)) => Some(*value),
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

pub(crate) fn special_message_sync_source_key(
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

pub(crate) fn special_message_sync_parent_source_key(
    object: &SpecialMessageSyncFact,
    sync_flags: u16,
) -> Vec<u8> {
    if sync_flags & SYNC_FLAG_NO_FOREIGN_IDENTIFIERS != 0 {
        source_key_for_store_id(object.folder_id)
    } else {
        special_message_parent_source_key(object)
    }
}

pub(super) fn special_message_search_key(object: &SpecialMessageSyncFact) -> Vec<u8> {
    // [MS-OXCPRPT] section 2.2.1.9: SearchKey is a read-only search identity.
    special_message_binary_property(object, PID_TAG_SEARCH_KEY)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            crate::mapi::identity::generated_message_search_key(&object.canonical_id)
        })
}

pub(crate) fn special_message_change_key(object: &SpecialMessageSyncFact) -> Vec<u8> {
    special_message_binary_property(object, PID_TAG_CHANGE_KEY)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| change_key_for_change_number(change_number_for_store_id(object.item_id)))
}

pub(crate) fn special_message_predecessor_change_list(object: &SpecialMessageSyncFact) -> Vec<u8> {
    special_message_binary_property(object, PID_TAG_PREDECESSOR_CHANGE_LIST)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| predecessor_change_list(change_number_for_store_id(object.item_id)))
}

pub(crate) fn special_message_change_number(object: &SpecialMessageSyncFact) -> u64 {
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
    let mut flags = match flags {
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
    };
    // Calendar and the other item builders calculate canonical attachment
    // presence in SpecialMessageSyncFact. Use it to adjust mfHasAttach, then
    // derive PidTagHasAttachments from those flags as [MS-OXCMSG] sections
    // 2.2.1.2 and 2.2.1.6 require.
    if let Some(has_attachments) = special_message_bool_property(object, PID_TAG_HAS_ATTACHMENTS) {
        if has_attachments {
            flags |= MSGFLAG_HASATTACH;
        } else {
            flags &= !MSGFLAG_HASATTACH;
        }
    }
    flags
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

pub(super) fn special_message_has_attachments(object: &SpecialMessageSyncFact) -> bool {
    special_message_flags(object) & MSGFLAG_HASATTACH != 0
}

pub(super) fn special_message_status(object: &SpecialMessageSyncFact) -> u32 {
    if crate::mapi_store::is_outlook_configuration_message_class_name(
        &object.message_class,
        "IPM.Configuration.MessageListSettings",
    ) {
        return 0;
    }
    special_message_u32_property(object, PID_TAG_MESSAGE_STATUS).unwrap_or(0)
}

pub(super) fn special_message_property_is_server_projected(property_tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(property_tag),
        PID_TAG_ACCESS | PID_TAG_ACCESS_LEVEL | PID_TAG_HAS_ATTACHMENTS | PID_TAG_MESSAGE_STATUS
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
            | PID_TAG_ENTRY_ID
            | PID_TAG_PARENT_ENTRY_ID
            | PID_TAG_RECORD_KEY
            | PID_TAG_SEARCH_KEY
            | PID_TAG_CHANGE_KEY
            | PID_TAG_PREDECESSOR_CHANGE_LIST
            | PID_TAG_CHANGE_NUMBER
    )
}

pub(crate) fn fast_transfer_message_content_buffer_with_special_object(
    entry_id: Option<&[u8]>,
    parent_entry_id: Option<&[u8]>,
    object: &SpecialMessageSyncFact,
    send_options: u8,
    property_filter: FastTransferDirectPropertyFilter<'_>,
    message_children: FastTransferMessageChildren,
) -> Vec<u8> {
    let mut buffer = Vec::new();
    write_fast_transfer_special_message_content(
        &mut buffer,
        entry_id,
        parent_entry_id,
        object,
        send_options,
        property_filter,
        message_children,
    );
    buffer
}

fn write_fast_transfer_special_message_content(
    buffer: &mut Vec<u8>,
    entry_id: Option<&[u8]>,
    parent_entry_id: Option<&[u8]>,
    object: &SpecialMessageSyncFact,
    send_options: u8,
    property_filter: FastTransferDirectPropertyFilter<'_>,
    message_children: FastTransferMessageChildren,
) {
    let source_key = special_message_source_key(object);
    let change_key = special_message_change_key(object);
    let predecessor_change_list = special_message_predecessor_change_list(object);
    if property_filter.includes(PID_TAG_SOURCE_KEY) {
        write_binary_property(buffer, PID_TAG_SOURCE_KEY, &source_key);
    }
    // LPE exposes the containing-folder SourceKey on Message objects through
    // GetProps, tables, and ICS. [MS-OXCFXICS] sections 2.2.4.3.16,
    // 3.2.5.8.1.1, 3.2.5.10, and 3.2.5.12 require direct CopyTo to apply its
    // exclusion list to that same property bag.
    if object.associated
        && crate::mapi_store::is_outlook_configuration_message_class(&object.message_class)
        && property_filter.includes(PID_TAG_PARENT_SOURCE_KEY)
    {
        write_binary_property(
            buffer,
            PID_TAG_PARENT_SOURCE_KEY,
            &special_message_parent_source_key(object),
        );
    }
    // [MS-OXCFXICS] sections 2.2.3.1.1.1.1, 3.2.5.10, and 3.2.5.12 define
    // the CopyTo exclusion list and provider-internal download filtering.
    // PidTagEntryId (0x0FFF0102) is outside that range; when the object family
    // supplies one, keep its direct projection identical to GetProps and ICS.
    if property_filter.includes(PID_TAG_ENTRY_ID) {
        if let Some(entry_id) = entry_id {
            write_binary_property(buffer, PID_TAG_ENTRY_ID, entry_id);
        }
    }
    // [MS-OXPROPS] section 2.860 and [MS-OXCFOLD] section 2.2.2.2.1.7
    // define PidTagParentEntryId as the containing Folder EntryID.
    // [MS-OXCFXICS] sections 2.2.3.1.1.1.1, 2.2.4.3.16, 3.2.5.10, and
    // 3.2.5.12 keep this property eligible for a direct Message CopyTo.
    if property_filter.includes(PID_TAG_PARENT_ENTRY_ID) {
        if let Some(parent_entry_id) = parent_entry_id {
            write_binary_property(buffer, PID_TAG_PARENT_ENTRY_ID, parent_entry_id);
        }
    }
    // [MS-OXCMSG] section 2.2.1.1 requires Access and AccessLevel on every
    // Message object. [MS-OXCPRPT] sections 2.2.1.1 and 2.2.1.2 define their
    // values, and [MS-OXCFXICS] sections 3.2.5.8.1.1 and 3.2.5.8.1.2 apply
    // the CopyTo exclusion and CopyProperties inclusion lists.
    if property_filter.includes(PID_TAG_ACCESS) {
        write_i32_property(
            buffer,
            PID_TAG_ACCESS,
            special_message_access(object) as i32,
        );
    }
    if property_filter.includes(PID_TAG_ACCESS_LEVEL) {
        write_i32_property(
            buffer,
            PID_TAG_ACCESS_LEVEL,
            special_message_access_level(object) as i32,
        );
    }
    // [MS-OXCMSG] sections 2.2.1.2 and 2.2.1.6 define the coherent
    // HasAttachments/mfHasAttach projection. Section 2.2.1.8 defines
    // MessageStatus; its zero fallback matches LPE's effective default and
    // the content-synchronization example in [MS-OXCFXICS] section 4.5.
    // MessageListSettings also has an Exchange-observed zero server projection.
    // CopyTo/CopyProperties apply their exclusion/inclusion lists to both.
    if property_filter.includes(PID_TAG_HAS_ATTACHMENTS) {
        write_bool_property(
            buffer,
            PID_TAG_HAS_ATTACHMENTS,
            special_message_has_attachments(object),
        );
    }
    if property_filter.includes(PID_TAG_MESSAGE_STATUS) {
        write_i32_property(
            buffer,
            PID_TAG_MESSAGE_STATUS,
            special_message_status(object) as i32,
        );
    }
    // [MS-OXCMSG] sections 2.2.1.1 and 3.2.5.2 and [MS-OXCPRPT]
    // section 2.2.1.9: every Message has a server-generated, read-only
    // SearchKey. It remains transmittable in the direct messageContent root
    // under [MS-OXCFXICS] sections 3.2.5.8.1.1 and 3.2.5.12.
    if property_filter.includes(PID_TAG_SEARCH_KEY) {
        write_binary_property(
            buffer,
            PID_TAG_SEARCH_KEY,
            &special_message_search_key(object),
        );
    }
    if property_filter.includes(PID_TAG_LAST_MODIFICATION_TIME) {
        write_u32(buffer, PID_TAG_LAST_MODIFICATION_TIME);
        write_i64(buffer, object.last_modified_filetime as i64);
    }
    if property_filter.includes(PID_TAG_CHANGE_KEY) {
        write_binary_property(buffer, PID_TAG_CHANGE_KEY, &change_key);
    }
    if property_filter.includes(PID_TAG_PREDECESSOR_CHANGE_LIST) {
        write_binary_property(
            buffer,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            &predecessor_change_list,
        );
    }
    // [MS-OXCFXICS] sections 2.2.4.3.16 and 3.2.5.12, with
    // [MS-OXPROPS] section 1.3.3: direct messageContent downloads exclude
    // provider-internal PidTagAssociated (0x67AA) and PidTagMid (0x674A).
    // [MS-OXCMSG] section 2.2.1.6: mfFAI remains the transmittable FAI
    // discriminator for CopyTo/CopyProperties.
    let message_flags = special_message_flags(object);
    if property_filter.includes(PID_TAG_MESSAGE_FLAGS) {
        write_i32_property(buffer, PID_TAG_MESSAGE_FLAGS, message_flags as i32);
    }
    if property_filter.includes(PID_TAG_SUBJECT_W) {
        write_utf16_property(buffer, PID_TAG_SUBJECT_W, &object.subject);
    }
    // [MS-OXCFXICS] sections 2.2.3.1.1.1.1 and 3.2.5.8.1.1:
    // canonical subjects are stored as Unicode, so Unicode/ForceUnicode
    // select PtypUnicode; without either flag use PtypString8.
    if send_options & (FAST_TRANSFER_SEND_OPTION_UNICODE | FAST_TRANSFER_SEND_OPTION_FORCE_UNICODE)
        != 0
    {
        if property_filter.includes(PID_TAG_NORMALIZED_SUBJECT_W) {
            write_utf16_property(buffer, PID_TAG_NORMALIZED_SUBJECT_W, &object.subject);
        }
    } else if property_filter.includes(PID_TAG_NORMALIZED_SUBJECT_A) {
        write_string8_property(buffer, PID_TAG_NORMALIZED_SUBJECT_A, &object.subject);
    }
    if property_filter.includes(PID_TAG_MESSAGE_CLASS_W) {
        write_utf16_property(buffer, PID_TAG_MESSAGE_CLASS_W, &object.message_class);
    }
    if property_filter.includes(PID_TAG_BODY_W) {
        if let Some(body_text) = &object.body_text {
            write_utf16_property(buffer, PID_TAG_BODY_W, body_text);
        }
    }
    if property_filter.includes(PID_TAG_MESSAGE_SIZE) {
        write_i32_property(buffer, PID_TAG_MESSAGE_SIZE, object.message_size as i32);
    }
    for (tag, value) in &object.named_properties {
        if !special_message_property_is_copy_identity(*tag)
            && !special_message_property_is_server_projected(*tag)
            && *tag != PID_TAG_MESSAGE_FLAGS
            && !provider_defined_internal_property(*tag)
            && property_filter.includes(*tag)
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
        write_fast_transfer_special_recipients(buffer, &object.recipients);
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
) -> bool {
    if !write_fast_transfer_property_info(buffer, object, property_tag) {
        return false;
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
    true
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
