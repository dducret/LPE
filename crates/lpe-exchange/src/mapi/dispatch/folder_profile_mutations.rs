use super::*;

pub(super) fn folder_profile_property_values(
    folder_id: u64,
    values: &[(u32, MapiValue)],
) -> Result<Vec<crate::store::MapiFolderProfilePropertyValue>> {
    let mut profile_values = Vec::new();
    for (tag, value) in values {
        let storage_tag = canonical_property_storage_tag(*tag);
        match (storage_tag, value) {
            (PID_TAG_EXTENDED_FOLDER_FLAGS, MapiValue::Binary(bytes)) => {
                profile_values.push(crate::store::MapiFolderProfilePropertyValue {
                    folder_id,
                    property_tag: storage_tag,
                    property_type: (PID_TAG_EXTENDED_FOLDER_FLAGS & 0xffff) as u16,
                    property_value: bytes.clone(),
                });
            }
            (PID_TAG_ADDITIONAL_REN_ENTRY_IDS, _)
                if matches!(folder_id, ROOT_FOLDER_ID | INBOX_FOLDER_ID) =>
            {
                let property_value = additional_ren_entry_ids_profile_bytes(value)
                    .ok_or_else(|| anyhow!("invalid PidTagAdditionalRenEntryIds value"))?;
                profile_values.push(crate::store::MapiFolderProfilePropertyValue {
                    folder_id: INBOX_FOLDER_ID,
                    property_tag: storage_tag,
                    property_type: (PID_TAG_ADDITIONAL_REN_ENTRY_IDS & 0xffff) as u16,
                    property_value,
                });
            }
            _ => {}
        }
    }
    Ok(profile_values)
}

pub(super) fn imported_folder_profile_property_values(
    principal: &AccountPrincipal,
    folder_id: u64,
    values: &[(u32, MapiValue)],
    existing_profile_values: &[crate::store::MapiFolderProfilePropertyValue],
) -> Result<(
    Vec<crate::store::MapiFolderProfilePropertyValue>,
    Vec<MapiSpecialFolderAlias>,
)> {
    let folder = MapiObject::Folder {
        folder_id,
        properties: HashMap::new(),
    };
    let aliases = default_folder_entry_id_aliases(Some(&folder), values);
    let existing_additional_ren_entry_ids = existing_profile_values
        .iter()
        .find(|value| {
            value.folder_id == INBOX_FOLDER_ID
                && value.property_tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS
                && value.property_type == (PID_TAG_ADDITIONAL_REN_ENTRY_IDS & 0xffff) as u16
        })
        .and_then(|value| additional_ren_entry_ids_from_profile_bytes(&value.property_value));
    let mut normalized = Vec::new();
    for (tag, value) in values {
        match (canonical_property_storage_tag(*tag), value) {
            (PID_TAG_EXTENDED_FOLDER_FLAGS, MapiValue::Binary(bytes))
                if !bytes.is_empty() && bytes.len() <= 4096 =>
            {
                normalized.push((PID_TAG_EXTENDED_FOLDER_FLAGS, value.clone()));
            }
            (PID_TAG_EXTENDED_FOLDER_FLAGS, _) => {
                return Err(anyhow!("invalid PidTagExtendedFolderFlags value"));
            }
            (PID_TAG_ADDITIONAL_REN_ENTRY_IDS, _) if folder_id == INBOX_FOLDER_ID => {
                let value = merge_additional_ren_entry_ids(
                    principal,
                    existing_additional_ren_entry_ids.as_ref(),
                    value.clone(),
                )
                .ok_or_else(|| anyhow!("invalid PidTagAdditionalRenEntryIds value"))?;
                if additional_ren_entry_ids_profile_bytes(&value).is_none() {
                    return Err(anyhow!("invalid PidTagAdditionalRenEntryIds value"));
                }
                normalized.push((PID_TAG_ADDITIONAL_REN_ENTRY_IDS, value));
            }
            _ => {}
        }
    }
    Ok((
        folder_profile_property_values(folder_id, &normalized)?,
        aliases,
    ))
}

pub(super) async fn persist_profile_folder_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    values: &[(u32, MapiValue)],
) -> Result<()>
where
    S: ExchangeStore,
{
    let folder_profile_values = folder_profile_property_values(folder_id, values)?;
    if folder_profile_values
        .iter()
        .any(|value| value.property_tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS)
    {
        return Err(anyhow!(
            "PidTagAdditionalRenEntryIds requires an atomic hierarchy commit"
        ));
    }
    if !folder_profile_values.is_empty() {
        store
            .upsert_mapi_folder_profile_property_values(
                principal.account_id,
                &folder_profile_values,
            )
            .await?;
    }
    if folder_id != IPM_SUBTREE_FOLDER_ID {
        return Ok(());
    }
    for (tag, value) in values {
        if canonical_property_storage_tag(*tag) == PID_TAG_OST_OSTID {
            if let MapiValue::Binary(ost_id) = value {
                store
                    .store_mapi_ipm_subtree_ost_id(principal.account_id, ost_id)
                    .await?;
            }
        }
    }
    Ok(())
}
