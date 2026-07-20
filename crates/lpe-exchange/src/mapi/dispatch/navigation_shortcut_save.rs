use super::*;

pub(super) fn stage_existing_navigation_shortcut_property_values(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    values: Vec<(u32, MapiValue)>,
) -> Result<()> {
    let Some(MapiObject::NavigationShortcut {
        folder_id,
        pending_properties,
        deleted_properties,
        ..
    }) = input_object_mut(session, handle_slots, request)
    else {
        return Err(anyhow!("MAPI navigation shortcut handle was not found"));
    };
    if !snapshot
        .folder_access_for_principal(*folder_id, principal.account_id)
        .map(|access| access.may_write)
        .unwrap_or(true)
    {
        return Err(anyhow!(
            "MAPI navigation shortcut mutation denied by canonical folder rights"
        ));
    }
    // [MS-OXOCFG] section 3.1.4.10 and [MS-OXCROPS] sections 2.2.8.6
    // and 2.2.6.3: RopSetProperties changes stay on the open Message handle
    // until RopSaveChangesMessage persists the WLink.
    for (tag, value) in values {
        let tag = canonical_property_storage_tag(tag);
        deleted_properties.remove(&tag);
        pending_properties.insert(tag, value);
    }
    Ok(())
}

pub(super) fn stage_existing_navigation_shortcut_property_deletions(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<Vec<(usize, u32, u32)>> {
    let Some(MapiObject::NavigationShortcut {
        folder_id,
        pending_properties,
        deleted_properties,
        ..
    }) = input_object_mut(session, handle_slots, request)
    else {
        return Err(anyhow!("MAPI navigation shortcut handle was not found"));
    };
    if !snapshot
        .folder_access_for_principal(*folder_id, principal.account_id)
        .map(|access| access.may_write)
        .unwrap_or(true)
    {
        return Err(anyhow!(
            "MAPI navigation shortcut mutation denied by canonical folder rights"
        ));
    }

    let mut problems = Vec::new();
    for (index, tag) in property_tags.iter().enumerate() {
        let storage_tag = canonical_property_storage_tag(*tag);
        if storage_tag != PID_TAG_WLINK_ENTRY_ID {
            problems.push((index, *tag, 0x8004_0102));
            continue;
        }
        // [MS-OXCROPS] sections 2.2.8.8 and 2.2.6.3: deletion mutates the
        // open Message and is published only by SaveChangesMessage.
        // [MS-OXOCFG] section 3.1.4.10 defines that save sequence for WLinks.
        pending_properties.remove(&storage_tag);
        deleted_properties.insert(storage_tag);
    }
    Ok(problems)
}

pub(super) fn fai_import_is_reflected_in_client_replica(
    disposition: MapiFaiImportDisposition,
) -> bool {
    // [MS-OXCFXICS] sections 2.2.1.1.3 and 3.2.5.3: CnsetSeenFAI
    // acknowledges only FAI changes already reflected in the client replica.
    // A server-winning conflict therefore has to be downloaded again.
    matches!(
        disposition,
        MapiFaiImportDisposition::Applied
            | MapiFaiImportDisposition::ConflictResolved {
                imported_wins: true
            }
    )
}

fn pending_common_views_message_is_navigation_shortcut(
    properties: &HashMap<u32, MapiValue>,
) -> bool {
    // [MS-OXOCFG] section 2.2.9 defines navigation shortcuts as associated
    // messages whose exact MessageClass is IPM.Microsoft.WunderBar.Link. The
    // class can arrive after ImportMessageChange, so classification happens
    // at SaveChangesMessage after all staged property writes are visible.
    optional_pending_text_property(
        properties,
        &[PID_TAG_MESSAGE_CLASS_W, PID_TAG_MESSAGE_CLASS_STRING8],
    )
    .is_some_and(|message_class| message_class.eq_ignore_ascii_case("IPM.Microsoft.WunderBar.Link"))
}

fn navigation_shortcut_property_by_id<'a>(
    properties: &'a HashMap<u32, MapiValue>,
    property_tag: u32,
) -> Option<&'a MapiValue> {
    properties.get(&property_tag).or_else(|| {
        [0x0003u16, 0x001Fu16, 0x0048u16, 0x0102u16]
            .into_iter()
            .map(|property_type| (property_tag & 0xFFFF_0000) | u32::from(property_type))
            .filter(|candidate| *candidate != property_tag)
            .find_map(|candidate| properties.get(&candidate))
    })
}

fn required_navigation_shortcut_u32(
    properties: &HashMap<u32, MapiValue>,
    property_tag: u32,
) -> Result<u32> {
    match navigation_shortcut_property_by_id(properties, property_tag) {
        Some(MapiValue::I32(value)) => Ok(*value as u32),
        Some(MapiValue::U32(value)) => Ok(*value),
        _ => Err(anyhow!(
            "required WLink PtypInteger32 property 0x{property_tag:08x} is missing or malformed"
        )),
    }
}

fn required_navigation_shortcut_binary_16(
    properties: &HashMap<u32, MapiValue>,
    property_tag: u32,
) -> Result<[u8; 16]> {
    match properties.get(&property_tag) {
        Some(MapiValue::Binary(value)) => <[u8; 16]>::try_from(value.as_slice()).map_err(|_| {
            anyhow!("required WLink PtypBinary property 0x{property_tag:08x} is not 16 bytes")
        }),
        _ => Err(anyhow!(
            "required WLink PtypBinary property 0x{property_tag:08x} is missing or malformed"
        )),
    }
}

fn validated_navigation_shortcut_from_mapi_properties(
    account_id: Uuid,
    id: Option<Uuid>,
    properties: &HashMap<u32, MapiValue>,
) -> Result<crate::mapi_store::MapiNavigationShortcutMessage> {
    // [MS-OXOCFG] sections 2.2.9.1 through 2.2.9.14 and 3.1.4.10:
    // a WLink is a complete FAI Message. Reject incomplete or malformed state
    // instead of inventing a shortcut type, section, ordinal, group, or target.
    let subject = properties
        .get(&PID_TAG_SUBJECT_W)
        .or_else(|| properties.get(&PID_TAG_NORMALIZED_SUBJECT_W))
        .and_then(MapiValue::as_text)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("required WLink display name is missing"))?;
    let shortcut_type = required_navigation_shortcut_u32(properties, PID_TAG_WLINK_TYPE)?;
    if !matches!(shortcut_type, 0 | 1 | 2 | 4) {
        return Err(anyhow!("WLink type {shortcut_type} is invalid"));
    }
    let _flags = required_navigation_shortcut_u32(properties, PID_TAG_WLINK_FLAGS)?;
    let _save_stamp = required_navigation_shortcut_u32(properties, PID_TAG_WLINK_SAVE_STAMP)?;
    let section = required_navigation_shortcut_u32(properties, PID_TAG_WLINK_SECTION)?;
    if !(1..=7).contains(&section) {
        return Err(anyhow!("WLink section {section} is invalid"));
    }
    let ordinal = match navigation_shortcut_property_by_id(properties, PID_TAG_WLINK_ORDINAL) {
        Some(MapiValue::Binary(value))
            if !value.is_empty()
                && value.len() <= u16::MAX as usize
                && value
                    .last()
                    .is_some_and(|last| *last != 0x00 && *last != 0xFF) =>
        {
            value
        }
        _ => return Err(anyhow!("required WLink ordinal is missing or malformed")),
    };
    let group_name = if shortcut_type == 4 {
        subject
    } else {
        navigation_shortcut_property_by_id(properties, PID_TAG_WLINK_GROUP_NAME_W)
            .and_then(MapiValue::as_text)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("required WLink group name is missing"))?
    };
    let group_id = if shortcut_type == 4 {
        if navigation_shortcut_property_by_id(properties, PID_TAG_WLINK_ENTRY_ID).is_some() {
            return Err(anyhow!("a WLink group header cannot target a folder"));
        }
        required_navigation_shortcut_binary_16(properties, PID_TAG_WLINK_GROUP_HEADER_ID)?
    } else {
        match navigation_shortcut_property_by_id(properties, PID_TAG_WLINK_ENTRY_ID) {
            Some(MapiValue::Binary(value)) if !value.is_empty() => {}
            _ => {
                return Err(anyhow!(
                    "required WLink folder EntryID is missing or malformed"
                ))
            }
        }
        required_navigation_shortcut_binary_16(properties, PID_TAG_WLINK_GROUP_CLSID)?
    };
    let folder_type =
        required_navigation_shortcut_binary_16(properties, PID_TAG_WLINK_FOLDER_TYPE)?;

    let shortcut = navigation_shortcut_from_mapi_properties(account_id, id, properties);
    if shortcut.subject != subject
        || shortcut.shortcut_type != shortcut_type
        || shortcut.section != section
        || shortcut.ordinal.as_slice() != ordinal.as_slice()
        || shortcut.group_name != group_name
        || shortcut.group_header_id != Some(Uuid::from_bytes(group_id))
        || wlink_folder_type_guid(&shortcut) != folder_type
        || (shortcut_type != 4 && shortcut.target_folder_id.is_none())
    {
        return Err(anyhow!(
            "WLink properties do not identify one canonical shortcut"
        ));
    }
    Ok(shortcut)
}

pub(super) async fn append_pending_navigation_shortcut_save_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    mapi_request_id: &str,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    snapshot: &mut MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    handle: u32,
    folder_id: u64,
    properties: HashMap<u32, MapiValue>,
    imported_message_id: Option<u64>,
    fail_on_conflict: bool,
) {
    if !pending_common_views_message_is_navigation_shortcut(&properties) {
        // [MS-OXCFXICS] sections 3.2.5.9.4.2 and 3.3.5.8.7: the imported
        // Message is not committed until SaveChangesMessage. A Common Views
        // FAI that proves not to be a WLink therefore follows the canonical
        // associated-configuration path with its imported properties intact.
        append_pending_associated_config_save_response(
            store,
            principal,
            mapi_request_id,
            session,
            handle_slots,
            request,
            responses,
            handle,
            folder_id,
            &properties,
            imported_message_id,
            fail_on_conflict,
        )
        .await;
        return;
    }
    let shortcut = match validated_navigation_shortcut_from_mapi_properties(
        principal.account_id,
        None,
        &properties,
    ) {
        Ok(shortcut) => shortcut,
        Err(error) => {
            tracing::warn!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                mapi_request_id = %mapi_request_id,
                request_rop_id = "0x0c",
                folder_id = format_args!("0x{:016x}", folder_id),
                validation_error = %error,
                "rejected incomplete Common Views navigation shortcut"
            );
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        }
    };
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = %mapi_request_id,
        request_rop_id = "0x0c",
        folder_id = format_args!("0x{:016x}", folder_id),
        decoded_shortcut =
            %common_views_saved_shortcut_summary(&shortcut, &properties),
        "rca debug mapi common views navigation shortcut save"
    );
    let input = UpsertMapiNavigationShortcutInput {
        // [MS-OXCMSG] sections 2.2.3.2 and 2.2.3.3: a message
        // created by RopCreateMessage receives a new identity when it
        // is first saved, independently of another WLink's properties.
        id: imported_message_id
            .and_then(|message_id| {
                snapshot
                    .navigation_shortcut_message_for_id(message_id)
                    .map(|message| message.canonical_id)
            })
            .or_else(|| Some(Uuid::new_v4())),
        account_id: principal.account_id,
        subject: shortcut.subject,
        target_folder_id: shortcut.target_folder_id,
        shortcut_type: shortcut.shortcut_type,
        flags: shortcut.flags,
        save_stamp: shortcut.save_stamp,
        section: shortcut.section,
        ordinal: shortcut.ordinal,
        group_header_id: shortcut.group_header_id,
        group_name: shortcut.group_name,
        client_properties: shortcut.client_properties,
    };
    let saved: Result<(
        crate::store::MapiNavigationShortcutRecord,
        crate::store::MapiIdentityRecord,
        Option<MapiFaiImportDisposition>,
    )> = async {
        if let Some(imported_message_id) = imported_message_id {
            let imported_identity = imported_fai_identity(&properties, imported_message_id)?;
            let committed = store
                .commit_mapi_navigation_shortcut_import(CommitMapiNavigationShortcutImportInput {
                    shortcut: input,
                    identity: imported_identity,
                    fail_on_conflict,
                })
                .await?;
            crate::mapi::identity::remember_mapi_identity_with_source_key(
                committed.shortcut.id,
                committed.identity.object_id,
                Some(committed.identity.source_key.clone()),
            );
            Ok((
                committed.shortcut,
                committed.identity,
                Some(committed.disposition),
            ))
        } else {
            let committed = store
                .commit_mapi_navigation_shortcut_create(CommitMapiNavigationShortcutCreateInput {
                    shortcut: input,
                })
                .await?;
            crate::mapi::identity::remember_mapi_identity_with_source_key(
                committed.shortcut.id,
                committed.identity.object_id,
                Some(committed.identity.source_key.clone()),
            );
            Ok((committed.shortcut, committed.identity, None))
        }
    }
    .await;
    match saved {
        Ok((saved, shortcut_identity, import_disposition)) => {
            let changed = import_disposition
                .map(MapiFaiImportDisposition::changes_server_replica)
                .unwrap_or(true);
            session.record_last_post_hierarchy_create_save_object_context(format!(
                "kind=navigation_shortcut;send_candidate=false;create_associated=true;class=IPM.Microsoft.WunderBar.Link;request_id={mapi_request_id};folder=0x{folder_id:016x};role={};subject={};target_folder={};shortcut_type={};section={};ordinal={};group_name={};canonical_id={}",
                debug_role_for_folder_id(folder_id),
                saved.subject,
                saved.target_folder_id
                    .map(|id| format!("0x{id:016x}"))
                    .unwrap_or_else(|| "none".to_string()),
                saved.shortcut_type,
                saved.section,
                bytes_to_hex(&saved.ordinal),
                saved.group_name,
                saved.id
            ));
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                mapi_request_id = %mapi_request_id,
                request_rop_id = "0x0c",
                folder_id = format_args!("0x{:016x}", folder_id),
                navigation_shortcut_id = %saved.id,
                subject = %saved.subject,
                target_folder_id = saved
                    .target_folder_id
                    .map(|id| format!("0x{id:016x}"))
                    .unwrap_or_else(|| "none".to_string()),
                shortcut_type = saved.shortcut_type,
                section = saved.section,
                ordinal = %bytes_to_hex(&saved.ordinal),
                group_name = %saved.group_name,
                "rca debug persisted navigation shortcut"
            );
            if snapshot
                .remember_navigation_shortcut(saved, shortcut_identity.clone())
                .is_err()
            {
                responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
            let shortcut_id = shortcut_identity.object_id;
            session.handles.insert(
                handle,
                MapiObject::NavigationShortcut {
                    folder_id,
                    shortcut_id,
                    pending_properties: HashMap::new(),
                    deleted_properties: HashSet::new(),
                },
            );
            if import_disposition.is_some_and(fai_import_is_reflected_in_client_replica) {
                record_sync_upload_content_change(
                    session,
                    folder_id,
                    shortcut_id,
                    shortcut_identity.change_number,
                    true,
                    false,
                );
            }
            if changed {
                session.record_notification(MapiNotificationEvent::content(
                    folder_id,
                    Some(shortcut_id),
                ));
            }
            append_save_changes_message_response(
                session,
                responses,
                handle_slots,
                request,
                handle,
                shortcut_id,
            );
        }
        Err(error) => {
            let return_value = if error.is::<crate::store::MapiFaiImportObjectDeleted>() {
                // [MS-OXCFXICS] section 3.3.4.3.3.2.2.1 permits this error
                // at SaveChangesMessage. [MS-OXCDATA] section 2.4 defines
                // ecObjectDeleted as 0x8004010A; [MS-OXCROPS] section
                // 2.2.6.3 requires it on the RopSaveChangesMessage (0x0C).
                0x8004_010A
            } else if error.is::<crate::store::MapiFaiImportConflict>() {
                0x8004_0109
            } else {
                0x8004_010F
            };
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                return_value,
            ));
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_existing_navigation_shortcut_save_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    snapshot: &mut MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    handle: u32,
    folder_id: u64,
    shortcut_id: u64,
    pending_properties: HashMap<u32, MapiValue>,
    deleted_properties: HashSet<u32>,
) {
    if pending_properties.is_empty() && deleted_properties.is_empty() {
        append_save_changes_message_response(
            session,
            responses,
            handle_slots,
            request,
            handle,
            shortcut_id,
        );
        return;
    }
    let Some(existing) = snapshot
        .navigation_shortcut_message_for_id(shortcut_id)
        .filter(|message| message.folder_id == folder_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x0C,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let merged_properties = navigation_shortcut_properties_with_pending(
        &existing,
        principal.account_id,
        &pending_properties,
        &deleted_properties,
    );
    let updated = match validated_navigation_shortcut_from_mapi_properties(
        principal.account_id,
        Some(existing.canonical_id),
        &merged_properties,
    ) {
        Ok(updated) => crate::mapi_store::MapiNavigationShortcutMessage {
            id: existing.id,
            folder_id: existing.folder_id,
            canonical_id: existing.canonical_id,
            durable_identity: existing.durable_identity.clone(),
            ..updated
        },
        Err(_) => {
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        }
    };
    let input = UpsertMapiNavigationShortcutInput {
        id: Some(existing.canonical_id),
        account_id: principal.account_id,
        subject: updated.subject,
        target_folder_id: updated.target_folder_id,
        shortcut_type: updated.shortcut_type,
        flags: updated.flags,
        save_stamp: updated.save_stamp,
        section: updated.section,
        ordinal: updated.ordinal,
        group_header_id: updated.group_header_id,
        group_name: updated.group_name,
        client_properties: updated.client_properties,
    };
    match store.commit_mapi_navigation_shortcut_update(input).await {
        Ok(committed) => {
            crate::mapi::identity::remember_mapi_identity_with_source_key(
                committed.shortcut.id,
                committed.identity.object_id,
                Some(committed.identity.source_key.clone()),
            );
            if snapshot
                .remember_navigation_shortcut(committed.shortcut, committed.identity.clone())
                .is_err()
            {
                responses.extend_from_slice(&rop_error_response(
                    0x0C,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
            session.handles.insert(
                handle,
                MapiObject::NavigationShortcut {
                    folder_id,
                    shortcut_id,
                    pending_properties: HashMap::new(),
                    deleted_properties: HashSet::new(),
                },
            );
            session
                .record_notification(MapiNotificationEvent::content(folder_id, Some(shortcut_id)));
            append_save_changes_message_response(
                session,
                responses,
                handle_slots,
                request,
                handle,
                shortcut_id,
            );
        }
        Err(_) => {
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8004_010F,
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wlink_group_identifiers_reject_obsolete_ptyp_guid_tags() {
        // [MS-OXOCFG] v20250520 sections 2.2.9.3, 2.2.9.11, and
        // 2.2.9.12 define these properties as PtypBinary. A value carried by
        // the obsolete PtypGuid tag must not satisfy strict Save validation.
        let properties = HashMap::from([(0x6842_0048, MapiValue::Guid([0x33; 16]))]);

        assert!(
            required_navigation_shortcut_binary_16(&properties, PID_TAG_WLINK_GROUP_HEADER_ID,)
                .is_err()
        );
    }

    #[test]
    fn server_winning_wlink_conflict_is_not_acknowledged_as_seen_fai() {
        assert!(fai_import_is_reflected_in_client_replica(
            MapiFaiImportDisposition::Applied,
        ));
        assert!(!fai_import_is_reflected_in_client_replica(
            MapiFaiImportDisposition::IgnoredOlderOrSame,
        ));
        assert!(fai_import_is_reflected_in_client_replica(
            MapiFaiImportDisposition::ConflictResolved {
                imported_wins: true,
            },
        ));
        let server_winning_conflict = MapiFaiImportDisposition::ConflictResolved {
            imported_wins: false,
        };
        assert!(server_winning_conflict.changes_server_replica());
        assert!(!fai_import_is_reflected_in_client_replica(
            server_winning_conflict,
        ));
    }
}
