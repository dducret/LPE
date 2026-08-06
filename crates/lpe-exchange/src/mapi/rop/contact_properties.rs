use super::*;

const CONTACT_SUBJECT_ALIASES: &[u32] = &[
    PID_TAG_DISPLAY_NAME_W,
    PID_TAG_SUBJECT_W,
    PID_TAG_NORMALIZED_SUBJECT_W,
];

pub(in crate::mapi) fn contact_object_property_is_deleted(
    object: Option<&MapiObject>,
    property_tag: u32,
) -> bool {
    matches!(
        object,
        Some(MapiObject::Contact { transaction, .. })
            if transaction
                .deleted_properties
                .contains(&canonical_property_storage_tag(property_tag))
    )
}

pub(in crate::mapi) fn serialize_contact_object_property(
    object: &MapiObject,
    principal: &AccountPrincipal,
    snapshot: &MapiMailStoreSnapshot,
    property_tag: u32,
) -> Vec<u8> {
    let MapiObject::Contact {
        folder_id,
        contact_id,
        transaction,
    } = object
    else {
        unreachable!("contact property serializer requires a Contact object")
    };
    let storage_tag = canonical_property_storage_tag(property_tag);
    if transaction.deleted_properties.contains(&storage_tag) {
        let mut value = Vec::new();
        write_property_default(&mut value, property_tag);
        return value;
    }
    if let Some(value) = staged_contact_property_value(transaction, storage_tag) {
        let mut serialized = Vec::new();
        write_mapi_value(&mut serialized, property_tag, value);
        return serialized;
    }
    snapshot
        .contact_for_id(*folder_id, *contact_id)
        .map(|contact| {
            serialize_mapi_contact_row(
                contact,
                contact.folder_id,
                principal.account_id,
                &[property_tag],
            )
        })
        .unwrap_or_else(|| {
            let mut value = Vec::new();
            write_property_default(&mut value, property_tag);
            value
        })
}

fn staged_contact_property_value(
    transaction: &MapiContactTransaction,
    property_tag: u32,
) -> Option<&MapiValue> {
    transaction
        .pending_properties
        .get(&property_tag)
        .or_else(|| {
            CONTACT_SUBJECT_ALIASES
                .contains(&property_tag)
                .then(|| {
                    CONTACT_SUBJECT_ALIASES
                        .iter()
                        .find_map(|tag| transaction.pending_properties.get(tag))
                })
                .flatten()
        })
}
