use super::*;

pub(super) fn first_fast_transfer_marker(request: &RopRequest) -> Option<u32> {
    let size = u16::from_le_bytes(request.payload.get(..2)?.try_into().ok()?) as usize;
    let bytes = request.payload.get(2..2 + size)?;
    let marker = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?);
    (marker & 0x4000_0000 != 0).then_some(marker)
}

pub(super) fn fast_transfer_destination_target_folder_id(object: &MapiObject) -> Option<u64> {
    match object {
        MapiObject::PendingMessage { folder_id, .. }
        | MapiObject::PendingAssociatedMessage { folder_id, .. }
        | MapiObject::PendingContact { folder_id, .. }
        | MapiObject::PendingEvent { folder_id, .. }
        | MapiObject::PendingTask { folder_id, .. }
        | MapiObject::PendingNote { folder_id, .. }
        | MapiObject::PendingJournalEntry { folder_id, .. }
        | MapiObject::PendingConversationAction { folder_id, .. }
        | MapiObject::PendingNavigationShortcut { folder_id, .. } => Some(*folder_id),
        _ => None,
    }
}

pub(super) fn staged_fast_transfer_destination_buffer(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
) -> Option<(u32, Vec<u8>)> {
    match input_object(session, handle_slots, request)? {
        MapiObject::FastTransferDestination {
            target_handle,
            buffer,
            ..
        } => {
            let mut full_buffer = buffer.clone();
            full_buffer.extend_from_slice(request.fast_transfer_upload_data());
            Some((*target_handle, full_buffer))
        }
        _ => None,
    }
}

pub(super) fn commit_fast_transfer_destination_buffer(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    full_buffer: Vec<u8>,
) {
    if let Some(MapiObject::FastTransferDestination { buffer, .. }) =
        input_object_mut(session, handle_slots, request)
    {
        *buffer = full_buffer;
    }
}

pub(super) fn apply_fast_transfer_destination_properties(
    session: &mut MapiSession,
    target_handle: u32,
    property_values: Vec<(u32, MapiValue)>,
) -> Option<()> {
    let properties = match session.handles.get_mut(&target_handle)? {
        MapiObject::PendingMessage { properties, .. }
        | MapiObject::PendingAssociatedMessage { properties, .. }
        | MapiObject::PendingContact { properties, .. }
        | MapiObject::PendingEvent { properties, .. }
        | MapiObject::PendingTask { properties, .. }
        | MapiObject::PendingNote { properties, .. }
        | MapiObject::PendingJournalEntry { properties, .. }
        | MapiObject::PendingConversationAction { properties, .. }
        | MapiObject::PendingNavigationShortcut { properties, .. } => properties,
        _ => return None,
    };
    for (property_tag, value) in property_values {
        properties.insert(canonical_property_storage_tag(property_tag), value);
    }
    Some(())
}

pub(super) fn fast_transfer_property_values(bytes: &[u8]) -> Result<Vec<(u32, MapiValue)>> {
    let mut cursor = Cursor::new(bytes);
    let mut values = Vec::new();
    while cursor.remaining() > 0 {
        let property_tag = cursor.read_u32()?;
        if property_tag & 0x4000_0000 != 0 {
            return Err(anyhow::anyhow!("unsupported FastTransfer marker"));
        }
        values.push((
            property_tag,
            read_fast_transfer_property_value(&mut cursor, property_tag)?,
        ));
    }
    Ok(values)
}

fn read_fast_transfer_property_value(
    cursor: &mut Cursor<'_>,
    property_tag: u32,
) -> Result<MapiValue> {
    match MapiPropertyType::from_code((property_tag & 0xFFFF) as u16) {
        Some(MapiPropertyType::Integer16) => Ok(MapiValue::I16(cursor.read_u16()? as i16)),
        Some(MapiPropertyType::Integer32) => Ok(MapiValue::I32(cursor.read_i32()?)),
        Some(MapiPropertyType::Floating32 | MapiPropertyType::Floating64) => Err(anyhow::anyhow!(
            "unsupported FastTransfer floating-point property type"
        )),
        Some(MapiPropertyType::Boolean) => Ok(MapiValue::Bool(cursor.read_u16()? != 0)),
        Some(MapiPropertyType::Integer64) | Some(MapiPropertyType::Time) => {
            Ok(MapiValue::I64(cursor.read_i64()?))
        }
        Some(MapiPropertyType::String8) => {
            let bytes = read_fast_transfer_variable_bytes(cursor)?;
            Ok(MapiValue::String(decode_fast_transfer_string8(&bytes)))
        }
        Some(MapiPropertyType::String) => {
            let bytes = read_fast_transfer_variable_bytes(cursor)?;
            Ok(MapiValue::String(decode_fast_transfer_utf16(&bytes)?))
        }
        Some(MapiPropertyType::Binary) => Ok(MapiValue::Binary(read_fast_transfer_variable_bytes(
            cursor,
        )?)),
        Some(MapiPropertyType::Guid) => {
            let bytes = cursor.read_bytes(16)?;
            Ok(MapiValue::Guid(bytes.try_into().unwrap_or([0; 16])))
        }
        _ => Err(anyhow::anyhow!("unsupported FastTransfer property type")),
    }
}

fn read_fast_transfer_variable_bytes(cursor: &mut Cursor<'_>) -> Result<Vec<u8>> {
    let len = cursor.read_u32()? as usize;
    Ok(cursor.read_bytes(len)?.to_vec())
}

fn decode_fast_transfer_string8(bytes: &[u8]) -> String {
    let trimmed = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    String::from_utf8_lossy(trimmed).into_owned()
}

fn decode_fast_transfer_utf16(bytes: &[u8]) -> Result<String> {
    if bytes.len() % 2 != 0 {
        return Err(anyhow::anyhow!("odd UTF-16 FastTransfer string length"));
    }
    let mut units = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();
    if units.last() == Some(&0) {
        units.pop();
    }
    Ok(String::from_utf16(&units)?)
}

pub(super) fn imported_property_source_key_global_counter(
    properties: &[(u32, MapiValue)],
) -> Option<u64> {
    properties
        .iter()
        .find_map(|(tag, value)| match (*tag, value) {
            (PID_TAG_SOURCE_KEY, MapiValue::Binary(bytes)) => {
                source_key_global_counter(bytes.as_slice())
            }
            _ => None,
        })
}

pub(super) fn import_message_change_conflicts_with_current_pcl(
    properties: &[(u32, MapiValue)],
    current_change_number: u64,
) -> bool {
    let Some(client_pcl) = properties
        .iter()
        .find_map(|(tag, value)| match (*tag, value) {
            (PID_TAG_PREDECESSOR_CHANGE_LIST, MapiValue::Binary(bytes)) => Some(bytes.as_slice()),
            _ => None,
        })
    else {
        return false;
    };
    let Ok(client_entries) = parse_predecessor_change_list_entries(client_pcl) else {
        return true;
    };
    let current_change_key = mapi_mailstore::change_key_for_change_number(current_change_number);
    client_entries.iter().all(|entry| {
        entry.guid != current_change_key[..16]
            || entry.counter
                < crate::mapi::identity::global_counter_from_globcnt(&current_change_key[16..22])
                    .unwrap_or(1)
    })
}

struct PredecessorChangeListEntry {
    guid: [u8; 16],
    counter: u64,
}

fn parse_predecessor_change_list_entries(
    bytes: &[u8],
) -> Result<Vec<PredecessorChangeListEntry>, ()> {
    let mut entries = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        let size = usize::from(*bytes.get(offset).ok_or(())?);
        offset += 1;
        if size != 22 {
            return Err(());
        }
        let change_key = bytes.get(offset..offset + size).ok_or(())?;
        offset += size;
        let guid = change_key[0..16].try_into().map_err(|_| ())?;
        let counter =
            crate::mapi::identity::global_counter_from_globcnt(&change_key[16..22]).ok_or(())?;
        entries.push(PredecessorChangeListEntry { guid, counter });
    }
    Ok(entries)
}

pub(super) fn persistable_import_source_key_global_counter(source_key: &[u8]) -> Option<u64> {
    let counter = source_key_global_counter(source_key)?;
    (import_source_key_identity_scope(counter) == "persistable_dynamic").then_some(counter)
}

pub(super) fn source_key_global_counter(source_key: &[u8]) -> Option<u64> {
    if source_key.len() != 22 || source_key[..16] != crate::mapi::identity::STORE_REPLICA_GUID {
        return None;
    }
    crate::mapi::identity::global_counter_from_globcnt(source_key.get(16..22)?)
}

pub(super) fn import_source_key_identity_scope(counter: u64) -> &'static str {
    if counter < crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER {
        "system_reserved"
    } else if counter > crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER {
        "out_of_lpe_persisted_range"
    } else {
        "persistable_dynamic"
    }
}

pub(super) fn upload_state_property_name(tag: u32) -> &'static str {
    match tag {
        0x4017_0003 | 0x4017_0102 => "MetaTagIdsetGiven",
        0x4018_0102 => "MetaTagIdsetDeleted",
        0x402D_0102 => "MetaTagIdsetRead",
        0x402E_0102 => "MetaTagIdsetUnread",
        0x6796_0102 => "MetaTagCnsetSeen",
        0x67DA_0102 => "MetaTagCnsetSeenFAI",
        0x67D2_0102 => "MetaTagCnsetRead",
        _ => "unknown",
    }
}

pub(super) fn upload_state_marker_bit(tag: u32) -> u8 {
    match tag {
        0x4017_0003 | 0x4017_0102 => 0x01,
        0x6796_0102 => 0x02,
        0x67DA_0102 => 0x04,
        0x67D2_0102 => 0x08,
        _ => 0,
    }
}

pub(super) fn uploaded_state_has_delta_anchor(marker_mask: u8) -> bool {
    marker_mask & 0x03 == 0x03
}

pub(super) fn mark_uploaded_state_stream(marker_mask: &mut u8, property_tag: u32) {
    *marker_mask |= upload_state_marker_bit(property_tag);
}

pub(super) fn record_sync_upload_content_change(
    session: &mut MapiSession,
    folder_id: u64,
    object_id: u64,
    change_number: u64,
    associated: bool,
    read_state_changed: bool,
) {
    for object in session.handles.values_mut() {
        let MapiObject::SynchronizationCollector {
            folder_id: collector_folder_id,
            sync_type,
            state,
            uploaded_object_ids,
            uploaded_normal_change_numbers,
            uploaded_fai_change_numbers,
            uploaded_read_change_numbers,
            ..
        } = object
        else {
            continue;
        };
        if *collector_folder_id != folder_id || *sync_type != 0x01 {
            continue;
        }
        if !uploaded_object_ids.contains(&object_id) {
            uploaded_object_ids.push(object_id);
        }
        if associated {
            if !uploaded_fai_change_numbers.contains(&change_number) {
                uploaded_fai_change_numbers.push(change_number);
            }
        } else if !uploaded_normal_change_numbers.contains(&change_number) {
            uploaded_normal_change_numbers.push(change_number);
        }
        if read_state_changed && !uploaded_read_change_numbers.contains(&change_number) {
            uploaded_read_change_numbers.push(change_number);
        }
        *state = mapi_mailstore::content_sync_state_stream_from_sets(
            uploaded_object_ids,
            uploaded_normal_change_numbers,
            uploaded_fai_change_numbers,
            uploaded_read_change_numbers,
        );
    }
}

pub(super) fn record_sync_upload_content_checkpoint(session: &mut MapiSession, folder_id: u64) {
    for object in session.handles.values_mut() {
        let MapiObject::SynchronizationCollector {
            folder_id: collector_folder_id,
            sync_type,
            state,
            uploaded_object_ids,
            uploaded_normal_change_numbers,
            uploaded_fai_change_numbers,
            uploaded_read_change_numbers,
            ..
        } = object
        else {
            continue;
        };
        if *collector_folder_id != folder_id || *sync_type != 0x01 {
            continue;
        }
        *state = mapi_mailstore::content_sync_state_stream_from_sets(
            uploaded_object_ids,
            uploaded_normal_change_numbers,
            uploaded_fai_change_numbers,
            uploaded_read_change_numbers,
        );
    }
}

pub(super) fn record_sync_upload_hierarchy_change(
    session: &mut MapiSession,
    folder_id: u64,
    object_id: u64,
) {
    for object in session.handles.values_mut() {
        let MapiObject::SynchronizationCollector {
            folder_id: collector_folder_id,
            sync_type,
            state,
            uploaded_object_ids,
            uploaded_normal_change_numbers,
            ..
        } = object
        else {
            continue;
        };
        if *collector_folder_id != folder_id || *sync_type != 0x02 {
            continue;
        }
        if !uploaded_object_ids.contains(&object_id) {
            uploaded_object_ids.push(object_id);
        }
        let change_number = mapi_mailstore::change_number_for_store_id(object_id);
        if !uploaded_normal_change_numbers.contains(&change_number) {
            uploaded_normal_change_numbers.push(change_number);
        }
        *state = mapi_mailstore::final_sync_state_stream(
            0x02,
            uploaded_object_ids,
            uploaded_normal_change_numbers,
        );
    }
}

pub(super) fn sync_mailboxes_with_collaboration_counts(
    mut mailboxes: Vec<JmapMailbox>,
    snapshot: &MapiMailStoreSnapshot,
    sync_root_folder_id: u64,
    sync_type: u8,
) -> Vec<JmapMailbox> {
    for mailbox in &mut mailboxes {
        let Some(folder_id) = try_mapi_folder_id(mailbox) else {
            continue;
        };
        if let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) {
            mailbox.total_emails = folder.item_count;
            mailbox.unread_emails = 0;
        }
    }
    if sync_type == MapiSyncType::Hierarchy.as_u8() {
        let mut folder_ids = mailboxes
            .iter()
            .filter_map(try_mapi_folder_id)
            .collect::<HashSet<_>>();
        for folder in snapshot.collaboration_folders() {
            if folder.kind != crate::mapi_store::MapiCollaborationFolderKind::Calendar {
                continue;
            }
            if !collaboration_folder_in_hierarchy_sync_scope(folder.id, sync_root_folder_id) {
                continue;
            }
            if !folder_ids.insert(folder.id) {
                continue;
            }
            let Some(canonical_id) = crate::mapi_store::collaboration_folder_identity_canonical_id(
                folder.kind,
                &folder.collection,
            ) else {
                continue;
            };
            crate::mapi::identity::remember_mapi_identity(canonical_id, folder.id);
            mailboxes.push(JmapMailbox {
                id: canonical_id,
                parent_id: Some(folder.collection.owner_account_id),
                role: "__mapi_collaboration_calendar".to_string(),
                name: folder.collection.display_name.clone(),
                sort_order: 57,
                modseq: mapi_mailstore::change_number_for_store_id(folder.id),
                total_emails: folder.item_count,
                unread_emails: 0,
                size_octets: 0,
                is_subscribed: true,
            });
        }
    }
    mailboxes
}

fn collaboration_folder_in_hierarchy_sync_scope(folder_id: u64, sync_root_folder_id: u64) -> bool {
    matches!(sync_root_folder_id, ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID)
        || folder_id == sync_root_folder_id
}

pub(super) async fn mapi_message_ids_for_deleted_changes<S>(
    store: &S,
    principal: &AccountPrincipal,
    message_ids: &[Uuid],
) -> Result<Vec<u64>>
where
    S: ExchangeStore,
{
    mapi_object_ids_for_deleted_changes(
        store,
        principal,
        MapiIdentityObjectKind::Message,
        message_ids,
    )
    .await
}

pub(super) async fn mapi_object_ids_for_deleted_changes<S>(
    store: &S,
    principal: &AccountPrincipal,
    object_kind: MapiIdentityObjectKind,
    object_ids: &[Uuid],
) -> Result<Vec<u64>>
where
    S: ExchangeStore,
{
    let requests = object_ids
        .iter()
        .map(|object_id| MapiIdentityRequest {
            object_kind,
            canonical_id: *object_id,
            reserved_global_counter: None,
            source_key: None,
        })
        .collect::<Vec<_>>();
    let identities = store
        .fetch_or_allocate_mapi_identities(principal.account_id, &requests)
        .await?;
    for identity in &identities {
        crate::mapi::identity::remember_mapi_identity_with_source_key(
            identity.canonical_id,
            identity.object_id,
            Some(identity.source_key.clone()),
        );
    }
    Ok(identities
        .into_iter()
        .map(|identity| identity.object_id)
        .collect())
}

pub(super) fn changed_special_ids_for_folder(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    changes: &MapiSyncChangeSet,
) -> Vec<Uuid> {
    let mut changed_ids = changes
        .changed_associated_config_ids
        .iter()
        .filter(|change| change.folder_id == folder_id)
        .map(|change| change.config_id)
        .collect::<Vec<_>>();
    if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Contacts)
        || matches!(
            folder_id,
            CONTACTS_SEARCH_FOLDER_ID
                | SUGGESTED_CONTACTS_FOLDER_ID
                | QUICK_CONTACTS_FOLDER_ID
                | IM_CONTACT_LIST_FOLDER_ID
        )
    {
        changed_ids.extend(changes.changed_contact_ids.iter().copied());
        return changed_ids;
    }
    if folder_id == CALENDAR_FOLDER_ID
        || snapshot
            .collaboration_folder_for_id(folder_id)
            .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
    {
        changed_ids.extend(changes.changed_calendar_event_ids.iter().copied());
        return changed_ids;
    }
    if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Task)
        || matches!(folder_id, TODO_SEARCH_FOLDER_ID | REMINDERS_FOLDER_ID)
    {
        changed_ids.extend(changes.changed_task_ids.iter().copied());
        return changed_ids;
    }
    match folder_id {
        NOTES_FOLDER_ID => changed_ids.extend(changes.changed_note_ids.iter().copied()),
        JOURNAL_FOLDER_ID => changed_ids.extend(changes.changed_journal_entry_ids.iter().copied()),
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => {
            changed_ids.extend(changes.changed_conversation_action_ids.iter().copied())
        }
        COMMON_VIEWS_FOLDER_ID => {
            changed_ids.extend(changes.changed_navigation_shortcut_ids.iter().copied())
        }
        _ => {}
    }
    changed_ids
}

pub(super) fn mapi_calendar_content_items_suppressed(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> bool {
    folder_id == CALENDAR_FOLDER_ID
        || snapshot
            .collaboration_folder_for_id(folder_id)
            .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
}

pub(super) async fn deleted_special_object_ids_for_folder<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    changes: &MapiSyncChangeSet,
) -> Vec<u64>
where
    S: ExchangeStore,
{
    let kind_and_ids = if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Contacts)
        || matches!(
            folder_id,
            CONTACTS_SEARCH_FOLDER_ID
                | SUGGESTED_CONTACTS_FOLDER_ID
                | QUICK_CONTACTS_FOLDER_ID
                | IM_CONTACT_LIST_FOLDER_ID
        ) {
        Some((
            MapiIdentityObjectKind::Contact,
            changes.deleted_contact_ids.clone(),
        ))
    } else if folder_id == CALENDAR_FOLDER_ID
        || snapshot
            .collaboration_folder_for_id(folder_id)
            .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
    {
        Some((
            MapiIdentityObjectKind::CalendarEvent,
            changes.deleted_calendar_event_ids.clone(),
        ))
    } else if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Task)
        || matches!(folder_id, TODO_SEARCH_FOLDER_ID | REMINDERS_FOLDER_ID)
    {
        Some((
            MapiIdentityObjectKind::Task,
            changes.deleted_task_ids.clone(),
        ))
    } else if folder_id == COMMON_VIEWS_FOLDER_ID {
        return store
            .fetch_mapi_object_ids_for_deleted_changes(
                principal.account_id,
                MapiIdentityObjectKind::NavigationShortcut,
                &changes.deleted_navigation_shortcut_ids,
            )
            .await
            .unwrap_or_default();
    } else {
        None
    };
    let mut deleted_object_ids = if let Some((object_kind, object_ids)) = kind_and_ids {
        mapi_object_ids_for_deleted_changes(store, principal, object_kind, &object_ids)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let associated_config_ids = changes
        .deleted_associated_config_ids
        .iter()
        .filter(|change| change.folder_id == folder_id)
        .map(|change| change.config_id)
        .collect::<Vec<_>>();
    if !associated_config_ids.is_empty() {
        deleted_object_ids.extend(
            store
                .fetch_mapi_object_ids_for_deleted_changes(
                    principal.account_id,
                    MapiIdentityObjectKind::AssociatedConfig,
                    &associated_config_ids,
                )
                .await
                .unwrap_or_default(),
        );
    }
    deleted_object_ids
}

pub(super) async fn remember_created_mapi_identity<S>(
    store: &S,
    principal: &AccountPrincipal,
    object_kind: MapiIdentityObjectKind,
    canonical_id: Uuid,
    reserved_global_counter: Option<u64>,
    source_key: Option<Vec<u8>>,
) -> Result<u64>
where
    S: ExchangeStore,
{
    let requests = [MapiIdentityRequest {
        object_kind,
        canonical_id,
        reserved_global_counter,
        source_key,
    }];
    let records = store
        .fetch_or_allocate_mapi_identities(principal.account_id, &requests)
        .await?;
    let object_id = records
        .first()
        .map(|record| record.object_id)
        .ok_or_else(|| anyhow::anyhow!("MAPI identity allocator returned no record"))?;
    let source_key = records
        .first()
        .map(|record| record.source_key.clone())
        .unwrap_or_default();
    crate::mapi::identity::remember_mapi_identity_with_source_key(
        canonical_id,
        object_id,
        Some(source_key),
    );
    Ok(object_id)
}

pub(super) async fn remember_created_message_mapi_identity<S>(
    store: &S,
    principal: &AccountPrincipal,
    canonical_id: Uuid,
    source_key: Option<Vec<u8>>,
) -> Result<(u64, bool, String)>
where
    S: ExchangeStore,
{
    let reserved_global_counter = source_key
        .as_deref()
        .and_then(persistable_import_source_key_global_counter);
    if reserved_global_counter.is_none() {
        let identity_fallback_reason = source_key
            .as_deref()
            .and_then(source_key_global_counter)
            .map(import_source_key_identity_scope)
            .filter(|scope| *scope != "persistable_dynamic")
            .unwrap_or("");
        let object_id = remember_created_mapi_identity(
            store,
            principal,
            MapiIdentityObjectKind::Message,
            canonical_id,
            None,
            None,
        )
        .await?;
        return Ok((object_id, false, identity_fallback_reason.to_string()));
    }

    match remember_created_mapi_identity(
        store,
        principal,
        MapiIdentityObjectKind::Message,
        canonical_id,
        reserved_global_counter,
        source_key.clone(),
    )
    .await
    {
        Ok(object_id) => Ok((object_id, true, String::new())),
        Err(error) => {
            let object_id = remember_created_mapi_identity(
                store,
                principal,
                MapiIdentityObjectKind::Message,
                canonical_id,
                None,
                None,
            )
            .await?;
            Ok((object_id, false, error.to_string()))
        }
    }
}
