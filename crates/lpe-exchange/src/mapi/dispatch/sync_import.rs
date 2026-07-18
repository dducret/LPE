use super::*;
use crate::mapi::wire::MapiError;
use crate::store::MAX_MAPI_LOCAL_REPLICA_ID_COUNT;

// Version 3 adds the owner Inbox special-folder identification properties to
// folderChange, so version 2 checkpoints must replay a full hierarchy once.
pub(super) const HIERARCHY_SYNC_CURSOR_VERSION: u64 = 3;

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

pub(super) fn append_tell_version_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    match input_object(session, handle_slots, request) {
        Some(MapiObject::SynchronizationSource { .. })
        | Some(MapiObject::SynchronizationCollector { .. })
        | Some(MapiObject::FastTransferDestination { .. }) => {
            responses.extend_from_slice(&rop_simple_success_response(request));
        }
        _ => responses.extend_from_slice(&rop_error_response(
            0x86,
            request.response_handle_index(),
            0x8004_0102,
        )),
    }
}

pub(super) fn append_set_local_replica_midset_deleted_response(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    match input_object_mut(session, handle_slots, request) {
        Some(MapiObject::SynchronizationSource {
            initial_state,
            state,
            ..
        }) => {
            initial_state.extend_from_slice(request.local_replica_midset_deleted());
            state.extend_from_slice(request.local_replica_midset_deleted());
            responses.extend_from_slice(&rop_simple_success_response(request));
        }
        Some(MapiObject::SynchronizationCollector { state, .. }) => {
            state.extend_from_slice(request.local_replica_midset_deleted());
            responses.extend_from_slice(&rop_simple_success_response(request));
        }
        _ => responses.extend_from_slice(&rop_error_response(
            0x93,
            request.response_handle_index(),
            0x8004_0102,
        )),
    }
}

pub(super) async fn append_get_local_replica_ids_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    // [MS-OXCROPS] section 3.2.5.4 requires ecNullObject for an unassigned
    // handle. [MS-OXCFXICS] section 2.2.3.2.4.7.1 then requires the assigned
    // InputServerObject to be a Logon object.
    let Some(input_handle) = input_handle(handle_slots, request) else {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            MapiError::NullObject.as_u32(),
        ));
        return;
    };
    let Some(input_object) = session.handles.get(&input_handle) else {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            MapiError::NullObject.as_u32(),
        ));
        return;
    };
    if !matches!(input_object, MapiObject::Logon) {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            ROP_ERROR_NOT_SUPPORTED,
        ));
        return;
    }

    let id_count = request.local_replica_id_count();
    if !(1..=MAX_MAPI_LOCAL_REPLICA_ID_COUNT).contains(&id_count) {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            MapiError::InvalidParameter.as_u32(),
        ));
        return;
    }

    match store
        .reserve_mapi_local_replica_ids(principal.account_id, id_count)
        .await
    {
        Ok(first_global_counter) => responses.extend_from_slice(
            &rop_get_local_replica_ids_response(request, first_global_counter),
        ),
        Err(error) => {
            tracing::error!(
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                account_id = %principal.account_id,
                id_count,
                error = %error,
                "failed to reserve MAPI local replica ID range"
            );
            responses.extend_from_slice(&rop_error_response(
                request.rop_id,
                request.response_handle_index(),
                MapiError::GeneralFailure.as_u32(),
            ));
        }
    }
}

pub(super) async fn append_local_replica_dispatch_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) -> bool
where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::SetLocalReplicaMidsetDeleted) => {
            append_set_local_replica_midset_deleted_response(
                session,
                handle_slots,
                request,
                responses,
            );
            false
        }
        Some(RopId::GetLocalReplicaIds) => {
            append_get_local_replica_ids_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                responses,
            )
            .await;
            true
        }
        _ => false,
    }
}

pub(super) fn is_sync_import_rop(rop_id: RopId) -> bool {
    matches!(
        rop_id,
        RopId::SynchronizationImportMessageChange
            | RopId::SynchronizationImportHierarchyChange
            | RopId::SynchronizationImportDeletes
            | RopId::SynchronizationImportMessageMove
            | RopId::SynchronizationImportReadStateChanges
            | RopId::SetLocalReplicaMidsetDeleted
            | RopId::GetLocalReplicaIds
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_sync_import_dispatch_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) -> bool
where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::SynchronizationImportMessageChange) => {
            append_synchronization_import_message_change_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
                output_handles,
            )
            .await;
            false
        }
        Some(RopId::SynchronizationImportHierarchyChange) => {
            append_synchronization_import_hierarchy_change_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                responses,
            )
            .await;
            false
        }
        Some(RopId::SynchronizationImportDeletes) => {
            append_synchronization_import_deletes_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
            )
            .await;
            false
        }
        Some(RopId::SynchronizationImportMessageMove) => {
            append_synchronization_import_message_move_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
            )
            .await;
            false
        }
        Some(RopId::SynchronizationImportReadStateChanges) => {
            append_synchronization_import_read_state_changes_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                responses,
            )
            .await;
            false
        }
        Some(RopId::SetLocalReplicaMidsetDeleted | RopId::GetLocalReplicaIds) => {
            append_local_replica_dispatch_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                responses,
            )
            .await
        }
        _ => false,
    }
}

pub(super) fn append_fast_transfer_source_copy_messages_response(
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let Some(folder_id) =
        input_object(session, handle_slots, request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x4B,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let requested_ids = request.fast_transfer_message_ids();
    let mut selected = emails_for_folder(folder_id, mailboxes, emails)
        .into_iter()
        .filter(|email| requested_ids.contains(&mapi_message_id(email)))
        .cloned()
        .collect::<Vec<_>>();
    selected.sort_by(|left, right| left.id.cmp(&right.id));
    let sync_attachment_facts = sync_attachment_facts_for(folder_id, &selected, snapshot);
    let transfer_buffer = mapi_mailstore::fast_transfer_message_list_buffer_with_attachments(
        &selected,
        &sync_attachment_facts,
    );
    let handle = session.allocate_output_handle(
        request.output_handle_index,
        MapiObject::SynchronizationSource {
            folder_id,
            mailbox_id: None,
            checkpoint_kind: MapiCheckpointKind::Content,
            checkpoint_change_sequence: 0,
            checkpoint_modseq: 1,
            checkpoint_store_allowed: true,
            checkpoint_skip_reason: "",
            checkpoint_zero_delta: false,
            sync_type: 0,
            initial_state: Vec::new(),
            state: Vec::new(),
            state_upload_property_tag: None,
            state_upload_buffer: Vec::new(),
            client_state_uploaded_bytes: 0,
            client_state_uploaded_marker_mask: 0,
            incremental_transfer_buffer: None,
            transfer_buffer,
            transfer_position: 0,
        },
    );
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    responses.extend_from_slice(&rop_fast_transfer_source_copy_response(request));
    output_handles.push(handle);
}

pub(super) fn append_fast_transfer_source_copy_response(
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let Some(object) = input_object(session, handle_slots, request).cloned() else {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let Some((folder_id, transfer_buffer)) = fast_transfer_manifest_for_object(
        request.rop_id,
        &object,
        principal,
        mailboxes,
        emails,
        snapshot,
    ) else {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    };
    let handle = session.allocate_output_handle(
        request.output_handle_index,
        MapiObject::SynchronizationSource {
            folder_id,
            mailbox_id: None,
            checkpoint_kind: MapiCheckpointKind::Content,
            checkpoint_change_sequence: 0,
            checkpoint_modseq: 1,
            checkpoint_store_allowed: true,
            checkpoint_skip_reason: "",
            checkpoint_zero_delta: false,
            sync_type: 0,
            initial_state: Vec::new(),
            state: Vec::new(),
            state_upload_property_tag: None,
            state_upload_buffer: Vec::new(),
            client_state_uploaded_bytes: 0,
            client_state_uploaded_marker_mask: 0,
            incremental_transfer_buffer: None,
            transfer_buffer,
            transfer_position: 0,
        },
    );
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    responses.extend_from_slice(&rop_fast_transfer_source_copy_response(request));
    output_handles.push(handle);
}

pub(super) fn append_synchronization_get_transfer_state_response(
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let source_object = input_object(session, handle_slots, request);
    let Some((
        folder_id,
        mailbox_id,
        checkpoint_kind,
        checkpoint_change_sequence,
        checkpoint_modseq,
        checkpoint_store_allowed,
        checkpoint_skip_reason,
        sync_type,
        state,
    )) = synchronization_context_state(source_object)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x82,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    };
    let transfer_buffer = if state.is_empty()
        && matches!(
            source_object,
            Some(MapiObject::SynchronizationCollector { .. })
        ) {
        mapi_mailstore::upload_sync_state_stream_from_sets(sync_type, &[], &[], &[])
    } else if state.is_empty() && matches!(sync_type, 0x01 | 0x02) {
        let sync_mailboxes = sync_mailboxes_for_excluding_deleted(
            folder_id,
            sync_type,
            mailboxes,
            &session.deleted_advertised_special_folders,
        );
        let sync_emails = sync_emails_for(folder_id, sync_type, mailboxes, emails);
        let sync_attachment_facts = sync_attachment_facts_for(folder_id, &sync_emails, snapshot);
        mapi_mailstore::sync_state_token_with_attachments(
            sync_type,
            folder_id,
            &sync_mailboxes,
            &sync_emails,
            &sync_attachment_facts,
        )
    } else {
        state
    };
    let (
        checkpoint_store_allowed,
        checkpoint_skip_reason,
        client_state_uploaded_bytes,
        client_state_uploaded_marker_mask,
    ) = match source_object {
        Some(MapiObject::SynchronizationCollector {
            client_state_uploaded_bytes,
            client_state_uploaded_marker_mask,
            ..
        }) => (
            checkpoint_store_allowed,
            checkpoint_skip_reason,
            *client_state_uploaded_bytes,
            *client_state_uploaded_marker_mask,
        ),
        _ => (checkpoint_store_allowed, checkpoint_skip_reason, 0, 0),
    };
    let handle = session.allocate_output_handle(
        request.output_handle_index,
        MapiObject::SynchronizationSource {
            folder_id,
            mailbox_id,
            checkpoint_kind,
            checkpoint_change_sequence,
            checkpoint_modseq,
            checkpoint_store_allowed,
            checkpoint_skip_reason,
            checkpoint_zero_delta: false,
            sync_type,
            initial_state: transfer_buffer.clone(),
            state: transfer_buffer.clone(),
            state_upload_property_tag: None,
            state_upload_buffer: Vec::new(),
            client_state_uploaded_bytes,
            client_state_uploaded_marker_mask,
            incremental_transfer_buffer: None,
            transfer_buffer,
            transfer_position: 0,
        },
    );
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    responses.extend_from_slice(&rop_synchronization_get_transfer_state_response(request));
    output_handles.push(handle);
}

pub(super) fn append_fast_transfer_destination_configure_response(
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let Some(target_handle) = input_handle(handle_slots, request) else {
        responses.extend_from_slice(&rop_error_response(
            0x53,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let Some(folder_id) = session
        .handles
        .get(&target_handle)
        .and_then(fast_transfer_destination_target_folder_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x53,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    };
    let handle = session.allocate_output_handle(
        request.output_handle_index,
        MapiObject::FastTransferDestination {
            folder_id,
            target_handle,
            buffer: Vec::new(),
        },
    );
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    responses.extend_from_slice(&rop_simple_success_response(request));
    output_handles.push(handle);
}

pub(super) fn append_synchronization_open_collector_response(
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let Some(folder_id) =
        input_object(session, handle_slots, request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x7E,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let sync_type = request.collector_sync_type();
    let handle = session.allocate_output_handle(
        request.output_handle_index,
        MapiObject::SynchronizationCollector {
            folder_id,
            mailbox_id: sync_checkpoint_mailbox_id(folder_id, sync_type, mailboxes),
            checkpoint_kind: sync_checkpoint_kind(sync_type),
            sync_type,
            state: Vec::new(),
            state_upload_property_tag: None,
            state_upload_buffer: Vec::new(),
            client_state_uploaded_bytes: 0,
            client_state_uploaded_marker_mask: 0,
            uploaded_object_ids: Vec::new(),
            uploaded_normal_change_numbers: Vec::new(),
            uploaded_fai_change_numbers: Vec::new(),
            uploaded_read_change_numbers: Vec::new(),
        },
    );
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    responses.extend_from_slice(&rop_simple_success_response(request));
    output_handles.push(handle);
}

pub(super) fn append_fast_transfer_destination_put_buffer_response(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) -> bool {
    if first_fast_transfer_marker(request).is_some() {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return true;
    }
    let upload_data = request.fast_transfer_upload_data().to_vec();
    let Some((target_handle, full_buffer)) =
        staged_fast_transfer_destination_buffer(session, handle_slots, request)
    else {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return false;
    };
    let property_values = match fast_transfer_property_values(&full_buffer) {
        Ok(values) => values,
        Err(_) => {
            responses.extend_from_slice(&rop_error_response(
                request.rop_id,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return true;
        }
    };
    if !property_values.is_empty()
        && apply_fast_transfer_destination_properties(session, target_handle, property_values)
            .is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return false;
    }
    commit_fast_transfer_destination_buffer(session, handle_slots, request, full_buffer);
    responses.extend_from_slice(&rop_fast_transfer_put_buffer_response(
        request,
        upload_data.len(),
    ));
    false
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

pub(super) fn imported_message_source_key(properties: &HashMap<u32, MapiValue>) -> Option<Vec<u8>> {
    let source_key = match properties.get(&PID_TAG_SOURCE_KEY)? {
        MapiValue::Binary(bytes) => bytes,
        _ => return None,
    };
    (source_key.len() == 22 && source_key[..16] == crate::mapi::identity::STORE_REPLICA_GUID)
        .then(|| source_key.clone())
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

pub(super) fn pending_message_is_sync_metadata_only(
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
) -> bool {
    !properties.is_empty()
        && recipients.is_empty()
        && properties.keys().all(|tag| {
            matches!(
                *tag,
                PID_TAG_SOURCE_KEY
                    | PID_TAG_LAST_MODIFICATION_TIME
                    | PID_TAG_CHANGE_KEY
                    | PID_TAG_PREDECESSOR_CHANGE_LIST
            )
        })
}

pub(super) fn pending_message_is_trash_sync_artifact(
    folder_id: u64,
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
) -> bool {
    folder_id == TRASH_FOLDER_ID
        && !properties.is_empty()
        && recipients.is_empty()
        && properties.keys().any(|tag| {
            matches!(
                *tag,
                PID_TAG_LAST_MODIFICATION_TIME
                    | PID_TAG_CHANGE_KEY
                    | PID_TAG_PREDECESSOR_CHANGE_LIST
            )
        })
        && imported_message_source_key(properties)
            .as_deref()
            .and_then(source_key_global_counter)
            .is_some_and(|counter| {
                import_source_key_identity_scope(counter) == "out_of_lpe_persisted_range"
            })
}

pub(super) fn imported_hierarchy_parent_mailbox_id(
    hierarchy_values: &[(u32, MapiValue)],
    collector_folder_id: u64,
    mailboxes: &[JmapMailbox],
) -> Option<Uuid> {
    hierarchy_values
        .iter()
        .find_map(|(tag, value)| match (tag, value) {
            (tag, MapiValue::Binary(bytes)) if *tag == PID_TAG_PARENT_SOURCE_KEY => {
                Some(bytes.as_slice())
            }
            _ => None,
        })
        .and_then(|parent_source_key| {
            mailboxes
                .iter()
                .find(|mailbox| {
                    mapi_mailstore::source_key_for_mailbox_folder(mailbox) == parent_source_key
                })
                .map(|mailbox| mailbox.id)
        })
        .or_else(|| {
            mailboxes
                .iter()
                .find(|mailbox| mapi_folder_id(mailbox) == collector_folder_id)
                .map(|mailbox| mailbox.id)
        })
}

pub(super) fn hierarchy_checkpoint_status(
    checkpoint_kind: MapiCheckpointKind,
    folder_id: u64,
    checkpoint: &MapiSyncCheckpoint,
) -> &'static str {
    if checkpoint_kind != MapiCheckpointKind::Hierarchy {
        return "usable";
    }
    if checkpoint
        .cursor_json
        .get("source")
        .and_then(serde_json::Value::as_str)
        != Some("emsmdb-ics-download")
    {
        return "stale-source";
    }
    if checkpoint
        .cursor_json
        .get("hierarchySyncVersion")
        .and_then(serde_json::Value::as_u64)
        != Some(HIERARCHY_SYNC_CURSOR_VERSION)
    {
        return "stale-version";
    }
    if checkpoint
        .cursor_json
        .get("syncRootFolderId")
        .and_then(serde_json::Value::as_u64)
        != Some(folder_id)
    {
        return "stale-root";
    }
    "usable"
}

pub(super) fn sync_property_filter_mode(
    sync_flags: u16,
    requested_property_tags: &[u32],
) -> &'static str {
    if requested_property_tags.is_empty() {
        "none"
    } else if sync_flags & 0x0080 == 0 {
        "exclude"
    } else {
        "only-specified"
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
        *state = mapi_mailstore::upload_sync_state_stream_from_sets(
            0x01,
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
        *state = mapi_mailstore::upload_sync_state_stream_from_sets(
            0x01,
            uploaded_normal_change_numbers,
            uploaded_fai_change_numbers,
            uploaded_read_change_numbers,
        );
    }
}

pub(super) fn record_sync_upload_hierarchy_change_with_change_number(
    session: &mut MapiSession,
    folder_id: u64,
    object_id: u64,
    change_number: u64,
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
        if !uploaded_normal_change_numbers.contains(&change_number) {
            uploaded_normal_change_numbers.push(change_number);
        }
        *state = mapi_mailstore::upload_sync_state_stream_from_sets(
            0x02,
            uploaded_normal_change_numbers,
            &[],
            &[],
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
        if folder_id == TRASH_FOLDER_ID {
            let deleted_event_count =
                u32::try_from(snapshot.events_for_folder(TRASH_FOLDER_ID).len())
                    .unwrap_or(u32::MAX);
            mailbox.total_emails = mailbox.total_emails.saturating_add(deleted_event_count);
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
    let mut deleted_object_ids = Vec::with_capacity(object_ids.len());
    for canonical_id in object_ids {
        let existing = store
            .fetch_mapi_object_ids_for_deleted_changes(
                principal.account_id,
                object_kind,
                std::slice::from_ref(canonical_id),
            )
            .await?;
        if let Some(object_id) = existing.first().copied() {
            deleted_object_ids.push(object_id);
            continue;
        }

        // A legacy tombstone can predate durable identity allocation. Keep the
        // deterministic fallback for that case, but never replace an existing
        // retired MID: [MS-OXCFXICS] section 2.2.4.4 identifies a deletion with
        // the source object's original IDSET member.
        let request = [MapiIdentityRequest {
            object_kind,
            canonical_id: *canonical_id,
            reserved_global_counter: None,
            source_key: None,
        }];
        let identity = store
            .fetch_or_allocate_mapi_identities(principal.account_id, &request)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("MAPI identity allocator returned no record"))?;
        crate::mapi::identity::remember_mapi_identity_with_source_key(
            identity.canonical_id,
            identity.object_id,
            Some(identity.source_key.clone()),
        );
        deleted_object_ids.push(identity.object_id);
    }
    Ok(deleted_object_ids)
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
    if folder_id == TRASH_FOLDER_ID {
        changed_ids.extend(changes.changed_deleted_calendar_event_ids.iter().copied());
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
    } else if folder_id == TRASH_FOLDER_ID {
        Some((
            MapiIdentityObjectKind::DeletedCalendarEvent,
            changes.deleted_deleted_calendar_event_ids.clone(),
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
    Ok(remember_created_mapi_identity_record(
        store,
        principal,
        object_kind,
        canonical_id,
        reserved_global_counter,
        source_key,
    )
    .await?
    .object_id)
}

pub(super) async fn remember_created_mapi_identity_record<S>(
    store: &S,
    principal: &AccountPrincipal,
    object_kind: MapiIdentityObjectKind,
    canonical_id: Uuid,
    reserved_global_counter: Option<u64>,
    source_key: Option<Vec<u8>>,
) -> Result<crate::store::MapiIdentityRecord>
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
    let record = records
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("MAPI identity allocator returned no record"))?;
    crate::mapi::identity::remember_mapi_identity_with_source_key(
        canonical_id,
        record.object_id,
        Some(record.source_key.clone()),
    );
    Ok(record)
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
