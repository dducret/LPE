use super::*;
use anyhow::{bail, Result};

const CONTACT_SUBJECT_ALIASES: &[u32] = &[
    PID_TAG_DISPLAY_NAME_W,
    PID_TAG_SUBJECT_W,
    PID_TAG_NORMALIZED_SUBJECT_W,
];

pub(super) fn stage_contact_property_values(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    values: Vec<(u32, MapiValue)>,
) -> Result<()> {
    let Some(MapiObject::Contact {
        folder_id,
        contact_id,
        transaction,
    }) = input_object_mut(session, handle_slots, request)
    else {
        bail!("MAPI Contact handle was not found");
    };
    let contact = snapshot
        .contact_for_id(*folder_id, *contact_id)
        .ok_or_else(|| anyhow::anyhow!("canonical MAPI Contact was not found"))?;
    if !event_handle_is_writable(
        transaction.open_mode_flags,
        contact.contact.rights.may_write,
    ) {
        bail!("MAPI Contact handle is not writable");
    }

    let canonical_properties = values
        .iter()
        .filter(|(tag, _)| !is_custom_property_tag(canonical_property_storage_tag(*tag)))
        .map(|(tag, value)| (canonical_property_storage_tag(*tag), value.clone()))
        .collect::<HashMap<_, _>>();
    reject_unsupported_mapi_contact_properties(&canonical_properties)?;
    apply_contact_property_values(
        &mut transaction.pending_properties,
        &mut transaction.deleted_properties,
        &values,
    );
    Ok(())
}

pub(super) fn stage_contact_property_deletions(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<()> {
    let Some(MapiObject::Contact {
        folder_id,
        contact_id,
        transaction,
    }) = input_object_mut(session, handle_slots, request)
    else {
        bail!("MAPI Contact handle was not found");
    };
    let contact = snapshot
        .contact_for_id(*folder_id, *contact_id)
        .ok_or_else(|| anyhow::anyhow!("canonical MAPI Contact was not found"))?;
    if !event_handle_is_writable(
        transaction.open_mode_flags,
        contact.contact.rights.may_write,
    ) {
        bail!("MAPI Contact handle is not writable");
    }

    for property_tag in property_tags {
        let storage_tag = canonical_property_storage_tag(*property_tag);
        if is_custom_property_tag(storage_tag) {
            transaction.pending_properties.remove(&storage_tag);
            transaction.deleted_properties.insert(storage_tag);
            continue;
        }
        if !contact_property_is_clearable(storage_tag) {
            bail!("MAPI Contact property {property_tag:#010X} cannot be cleared");
        }
        transaction.pending_properties.remove(&storage_tag);
        transaction.deleted_properties.insert(storage_tag);
    }
    Ok(())
}

pub(super) fn staged_contact_commit_input(
    principal: &AccountPrincipal,
    contact: &crate::mapi_store::MapiContact,
    transaction: &MapiContactTransaction,
    force_save: bool,
) -> Result<lpe_storage::MapiContactCommitInput> {
    let (canonical_values, custom_values) =
        split_custom_property_values(transaction.pending_properties.clone().into_iter().collect());
    let canonical_values = canonical_values.into_iter().collect::<HashMap<_, _>>();
    let deleted_canonical_properties = transaction
        .deleted_properties
        .iter()
        .copied()
        .filter(|tag| !is_custom_property_tag(*tag))
        .collect::<HashSet<_>>();
    let contact_input = contact_input_from_mapi_with_deletions(
        principal.account_id,
        Some(contact.canonical_id),
        &contact.contact,
        &canonical_values,
        &deleted_canonical_properties,
    )?;
    let mut custom_property_upserts = custom_values
        .into_iter()
        .map(|(property_tag, value)| {
            let mut property_value = Vec::new();
            write_mapi_value(&mut property_value, property_tag, &value);
            MapiContactCustomPropertyValue {
                property_tag,
                property_type: MapiPropertyTag::new(property_tag).property_type_code(),
                property_value,
            }
        })
        .collect::<Vec<_>>();
    custom_property_upserts.sort_by_key(|value| value.property_tag);
    let mut custom_property_deletes = transaction
        .deleted_properties
        .iter()
        .copied()
        .filter(|tag| is_custom_property_tag(*tag))
        .collect::<Vec<_>>();
    custom_property_deletes.sort_unstable();
    Ok(lpe_storage::MapiContactCommitInput {
        principal_account_id: principal.account_id,
        contact_id: contact.canonical_id,
        expected_modseq: transaction.base_modseq,
        force_save,
        contact: contact_input,
        custom_property_upserts,
        custom_property_deletes,
    })
}

fn apply_contact_property_values(
    pending: &mut HashMap<u32, MapiValue>,
    deleted: &mut HashSet<u32>,
    values: &[(u32, MapiValue)],
) {
    if values
        .iter()
        .any(|(tag, _)| CONTACT_SUBJECT_ALIASES.contains(&canonical_property_storage_tag(*tag)))
    {
        for tag in CONTACT_SUBJECT_ALIASES {
            pending.remove(tag);
            deleted.remove(tag);
        }
    }
    for (tag, value) in values {
        let storage_tag = canonical_property_storage_tag(*tag);
        pending.insert(storage_tag, value.clone());
        deleted.remove(&storage_tag);
    }
    if let Some((_, value)) = values
        .iter()
        .rev()
        .find(|(tag, _)| CONTACT_SUBJECT_ALIASES.contains(&canonical_property_storage_tag(*tag)))
    {
        for tag in CONTACT_SUBJECT_ALIASES {
            pending.insert(*tag, value.clone());
        }
    }
}

fn contact_property_is_clearable(property_tag: u32) -> bool {
    matches!(
        property_tag,
        PID_TAG_DISPLAY_NAME_PREFIX_W
            | PID_TAG_GIVEN_NAME_W
            | PID_TAG_MIDDLE_NAME_W
            | PID_TAG_SURNAME_W
            | PID_TAG_GENERATION_W
            | PID_TAG_NICKNAME_W
            | PID_TAG_MOBILE_TELEPHONE_NUMBER_W
            | PID_TAG_BUSINESS_TELEPHONE_NUMBER_W
            | PID_TAG_HOME_TELEPHONE_NUMBER_W
            | PID_TAG_PRIMARY_TELEPHONE_NUMBER_W
            | PID_TAG_COMPANY_NAME_W
            | PID_TAG_DEPARTMENT_NAME_W
            | PID_TAG_TITLE_W
            | PID_TAG_PERSONAL_HOME_PAGE_W
            | PID_TAG_BUSINESS_HOME_PAGE_W
            | PID_TAG_BODY_W
            | PID_LID_EMAIL2_DISPLAY_NAME_W_TAG
            | PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG
            | PID_LID_EMAIL2_ORIGINAL_DISPLAY_NAME_W_TAG
            | PID_LID_EMAIL3_DISPLAY_NAME_W_TAG
            | PID_LID_EMAIL3_EMAIL_ADDRESS_W_TAG
            | PID_LID_EMAIL3_ORIGINAL_DISPLAY_NAME_W_TAG
    )
}
