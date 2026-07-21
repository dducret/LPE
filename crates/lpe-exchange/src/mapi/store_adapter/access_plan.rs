use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::mapi) struct MapiAccessPlan {
    pub(in crate::mapi) requires_full_snapshot: bool,
    pub(in crate::mapi) requires_associated_contents: bool,
    pub(in crate::mapi) object_ids: Vec<u64>,
    pub(in crate::mapi) content_queries: Vec<MapiContentAccessQuery>,
}

impl MapiAccessPlan {
    fn full() -> Self {
        Self {
            requires_full_snapshot: true,
            requires_associated_contents: false,
            object_ids: Vec::new(),
            content_queries: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::mapi) struct MapiContentAccessQuery {
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) view_signature: u64,
    pub(in crate::mapi) offset: usize,
    pub(in crate::mapi) limit: usize,
    pub(in crate::mapi) sort_orders: Vec<MapiContentTableSort>,
}

pub(in crate::mapi) fn plan_mapi_store_access(
    session: &MapiSession,
    rop_buffer: &[u8],
) -> MapiAccessPlan {
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return MapiAccessPlan::full();
    };
    let Ok(mut handle_slots) = read_handle_table(handle_table) else {
        return MapiAccessPlan::full();
    };
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };
    let mut simulated_handles = session.handles.clone();
    let mut simulated_next_handle = session.next_handle;

    let mut cursor = Cursor::new(requests);
    while cursor.remaining() > 0 {
        let Ok(request) = read_rop_request(&mut cursor) else {
            return MapiAccessPlan::full();
        };
        if rop_requires_full_snapshot(request.rop_id)
            && !rop_uses_session_state_only(&simulated_handles, &handle_slots, &request)
        {
            return MapiAccessPlan::full();
        }
        if let Some(folder_id) = request.folder_id() {
            push_unique(
                &mut plan.object_ids,
                session.resolve_special_folder_alias(folder_id),
            );
        }
        if let Some(message_id) = request.message_id() {
            push_unique(&mut plan.object_ids, message_id);
        }
        if let Some(message_id) = request.status_message_id() {
            push_unique(
                &mut plan.object_ids,
                session.resolve_special_folder_alias(message_id),
            );
        }
        if let Some(object_id) = request.long_term_source_object_id() {
            push_unique(&mut plan.object_ids, object_id);
        }
        for message_id in request
            .message_ids()
            .into_iter()
            .chain(request.move_copy_message_ids())
            .chain(request.fast_transfer_message_ids())
            .chain(request.import_delete_message_ids())
            .chain(
                request
                    .import_read_state_changes()
                    .into_iter()
                    .map(|(message_id, _)| message_id),
            )
        {
            push_unique(&mut plan.object_ids, message_id);
        }
        if let Some(message_id) = request.import_message_id() {
            push_unique(&mut plan.object_ids, message_id);
        }
        if let Some(import_move) = request.import_move() {
            push_unique(&mut plan.object_ids, import_move.source_folder_id);
            push_unique(&mut plan.object_ids, import_move.source_message_id);
        }
        if let Some(folder_id) = request.delete_folder_id() {
            push_unique(&mut plan.object_ids, folder_id);
        }
        if let Some(target_handle) = request.move_copy_target_handle(&handle_slots) {
            if let Some(object) = session.handles.get(&target_handle) {
                add_object_ids_for_handle(&mut plan, object);
            }
        }
        if let Some(handle) = input_handle(&handle_slots, &request) {
            if let Some(object) = simulated_handles.get(&handle) {
                add_object_ids_for_handle(&mut plan, object);
            }
        }
        simulate_table_access(
            &mut plan,
            session,
            &mut simulated_handles,
            &mut simulated_next_handle,
            &mut handle_slots,
            &request,
        );
        if plan.requires_full_snapshot {
            return plan;
        }
    }
    plan
}

pub(in crate::mapi) fn hierarchy_sync_selective_fallback_plan(
    rop_buffer: &[u8],
) -> Option<MapiAccessPlan> {
    let (requests, _) = split_rop_buffer(rop_buffer)?;
    let mut saw_hierarchy_configure = false;
    let mut cursor = Cursor::new(requests);
    while cursor.remaining() > 0 {
        let request = read_rop_request(&mut cursor).ok()?;
        match request.rop_id {
            0x70 if request.sync_type() == 0x02 => saw_hierarchy_configure = true,
            0x4E if saw_hierarchy_configure => {}
            rop_id if rop_requires_full_snapshot(rop_id) => return None,
            _ => {}
        }
    }

    saw_hierarchy_configure.then_some(MapiAccessPlan {
        requires_full_snapshot: false,
        requires_associated_contents: false,
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    })
}

pub(in crate::mapi) fn requires_snapshot_backed_contents(
    plan: &MapiAccessPlan,
    mailboxes: &[JmapMailbox],
) -> bool {
    plan.requires_associated_contents
        || plan
            .object_ids
            .contains(&crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
        || plan.object_ids.contains(&COMMON_VIEWS_FOLDER_ID)
        || plan
            .object_ids
            .iter()
            .any(|folder_id| is_snapshot_backed_collaboration_folder(*folder_id))
        || plan
            .content_queries
            .iter()
            .any(|query| mailbox_id_for_mapi_folder_id(mailboxes, query.folder_id).is_none())
        || plan.object_ids.contains(&CALENDAR_FOLDER_ID)
}

fn is_snapshot_backed_collaboration_folder(folder_id: u64) -> bool {
    matches!(
        folder_id,
        CONTACTS_FOLDER_ID
            | SUGGESTED_CONTACTS_FOLDER_ID
            | QUICK_CONTACTS_FOLDER_ID
            | IM_CONTACT_LIST_FOLDER_ID
            | CONTACTS_SEARCH_FOLDER_ID
            | CALENDAR_FOLDER_ID
            | TASKS_FOLDER_ID
            | TODO_SEARCH_FOLDER_ID
            | REMINDERS_FOLDER_ID
            | NOTES_FOLDER_ID
            | JOURNAL_FOLDER_ID
            | TRACKED_MAIL_PROCESSING_FOLDER_ID
            | crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID
            | crate::mapi::identity::RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID
            | crate::mapi::identity::RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID
            | crate::mapi::identity::RECOVERABLE_ITEMS_PURGES_FOLDER_ID
    )
}

pub(in crate::mapi) fn rop_requires_full_snapshot(rop_id: u8) -> bool {
    matches!(
        rop_id,
        0x19 | 0x1A
            | 0x1B
            | 0x4B
            | 0x4C
            | 0x4D
            | 0x4E
            | 0x58
            | 0x3F
            | 0x70
            | 0x72
            | 0x73
            | 0x74
            | 0x78
            | 0x92
    )
}

fn rop_uses_session_state_only(
    handles: &HashMap<u32, MapiObject>,
    handle_slots: &[u32],
    request: &RopRequest,
) -> bool {
    match request.rop_id {
        0x4E => input_handle(handle_slots, request)
            .and_then(|handle| handles.get(&handle))
            .is_some_and(|object| matches!(object, MapiObject::SynchronizationSource { .. })),
        _ => false,
    }
}

pub(in crate::mapi) fn simulate_table_access(
    plan: &mut MapiAccessPlan,
    session: &MapiSession,
    handles: &mut HashMap<u32, MapiObject>,
    next_handle: &mut u32,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
) {
    match request.rop_id {
        0x02 => {
            let folder_id =
                session.resolve_special_folder_alias(request.folder_id().unwrap_or(ROOT_FOLDER_ID));
            let handle = simulate_allocate_handle(
                handles,
                next_handle,
                request.output_handle_index,
                MapiObject::Folder {
                    folder_id,
                    properties: HashMap::new(),
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
        }
        0x04 => {
            let folder_id = input_handle(handle_slots, request)
                .and_then(|handle| handles.get(&handle))
                .and_then(MapiObject::folder_id)
                .unwrap_or(ROOT_FOLDER_ID);
            let handle = simulate_allocate_handle(
                handles,
                next_handle,
                request.output_handle_index,
                MapiObject::HierarchyTable {
                    folder_id,
                    columns: default_hierarchy_columns(),
                    columns_set: false,
                    sort_orders: Vec::new(),
                    category_count: 0,
                    expanded_count: 0,
                    collapsed_categories: HashSet::new(),
                    deleted_advertised_special_folders: HashSet::new(),
                    restriction: None,
                    bookmarks: HashMap::new(),
                    next_bookmark: 1,
                    position: 0,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
        }
        0x05 => {
            let folder_id = input_handle(handle_slots, request)
                .and_then(|handle| handles.get(&handle))
                .and_then(MapiObject::folder_id)
                .unwrap_or(INBOX_FOLDER_ID);
            let associated = request
                .payload
                .first()
                .is_some_and(|flags| flags & 0x02 != 0);
            if associated && folder_id == COMMON_VIEWS_FOLDER_ID {
                plan.requires_associated_contents = true;
            }
            let sort_orders = simulated_default_view_content_sort(folder_id, associated);
            let handle = simulate_allocate_handle(
                handles,
                next_handle,
                request.output_handle_index,
                MapiObject::ContentsTable {
                    folder_id,
                    associated,
                    columns: Vec::new(),
                    columns_set: false,
                    sort_orders,
                    category_count: 0,
                    expanded_count: 0,
                    collapsed_categories: HashSet::new(),
                    restriction: None,
                    bookmarks: HashMap::new(),
                    next_bookmark: 1,
                    position: 0,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
        }
        0x06 => {
            let folder_id =
                session.resolve_special_folder_alias(request.folder_id().unwrap_or_else(|| {
                    input_handle(handle_slots, request)
                        .and_then(|handle| handles.get(&handle))
                        .and_then(MapiObject::folder_id)
                        .unwrap_or(INBOX_FOLDER_ID)
                }));
            let object =
                if folder_id == crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                    MapiObject::PendingConversationAction {
                        folder_id,
                        properties: HashMap::new(),
                    }
                } else {
                    MapiObject::PendingMessage {
                        folder_id,
                        properties: HashMap::new(),
                        recipients: Vec::new(),
                    }
                };
            let handle =
                simulate_allocate_handle(handles, next_handle, request.output_handle_index, object);
            set_handle_slot(handle_slots, request.output_handle_index, handle);
        }
        0x0C => {
            if input_handle(handle_slots, request)
                .and_then(|handle| handles.get(&handle))
                .is_some_and(|object| {
                    matches!(object, MapiObject::PendingConversationAction { .. })
                })
            {
                plan.requires_full_snapshot = true;
                return;
            }
        }
        0x12 => {
            if let Some(MapiObject::ContentsTable {
                folder_id,
                associated,
                columns,
                columns_set,
                sort_orders,
                category_count,
                restriction,
                position,
                ..
            }) = input_handle(handle_slots, request).and_then(|handle| handles.get_mut(&handle))
            {
                *columns = request.property_tags();
                *columns_set = true;
                if !*associated
                    && restriction.is_none()
                    && *category_count == 0
                    && is_windowable_mail_contents_folder(*folder_id)
                {
                    let Some(sql_sort_orders) = mapi_content_table_sort_orders(sort_orders) else {
                        plan.requires_full_snapshot = true;
                        return;
                    };
                    add_content_query(
                        plan,
                        *folder_id,
                        table_view_signature(sort_orders, restriction.as_ref()),
                        *position,
                        1,
                        sql_sort_orders,
                    );
                }
            }
        }
        0x13 => {
            if let Some(MapiObject::ContentsTable {
                sort_orders,
                position,
                bookmarks,
                ..
            }) = input_handle(handle_slots, request).and_then(|handle| handles.get_mut(&handle))
            {
                let parsed = request.sort_orders();
                if mapi_content_table_sort_orders(&parsed).is_none() {
                    plan.requires_full_snapshot = true;
                    return;
                }
                *sort_orders = parsed;
                *position = 0;
                bookmarks.clear();
            }
        }
        0x14 => {
            if let Some(MapiObject::ContentsTable {
                restriction,
                position,
                bookmarks,
                ..
            }) = input_handle(handle_slots, request).and_then(|handle| handles.get_mut(&handle))
            {
                match request.restriction() {
                    Ok(None) => *restriction = None,
                    Ok(Some(_)) | Err(_) => {
                        plan.requires_full_snapshot = true;
                        return;
                    }
                }
                *position = 0;
                bookmarks.clear();
            }
        }
        0x15 => {
            let Some(MapiObject::ContentsTable {
                folder_id,
                associated,
                sort_orders,
                category_count,
                restriction,
                position,
                ..
            }) = input_handle(handle_slots, request).and_then(|handle| handles.get_mut(&handle))
            else {
                return;
            };
            if *associated {
                return;
            }
            if restriction.is_some() {
                plan.requires_full_snapshot = true;
                return;
            }
            if *category_count > 0 || !is_windowable_mail_contents_folder(*folder_id) {
                plan.requires_full_snapshot = true;
                return;
            }
            let Some(sql_sort_orders) = mapi_content_table_sort_orders(sort_orders) else {
                plan.requires_full_snapshot = true;
                return;
            };
            let row_count = request.query_row_count().unwrap_or(0);
            let offset = if request.query_forward_read() {
                *position
            } else {
                (*position).saturating_sub(row_count)
            };
            add_content_query(
                plan,
                *folder_id,
                table_view_signature(sort_orders, restriction.as_ref()),
                offset,
                row_count,
                sql_sort_orders,
            );
            if !request.query_no_advance() {
                if request.query_forward_read() {
                    *position = (*position).saturating_add(row_count);
                } else {
                    *position = offset;
                }
            }
        }
        0x17 => {
            let Some(MapiObject::ContentsTable {
                folder_id,
                associated,
                sort_orders,
                category_count,
                restriction,
                position,
                ..
            }) = input_handle(handle_slots, request).and_then(|handle| handles.get(&handle))
            else {
                return;
            };
            if *associated {
                return;
            }
            if restriction.is_some() {
                plan.requires_full_snapshot = true;
                return;
            }
            if *category_count > 0 || !is_windowable_mail_contents_folder(*folder_id) {
                plan.requires_full_snapshot = true;
                return;
            }
            let Some(sql_sort_orders) = mapi_content_table_sort_orders(sort_orders) else {
                plan.requires_full_snapshot = true;
                return;
            };
            add_content_query(
                plan,
                *folder_id,
                table_view_signature(sort_orders, restriction.as_ref()),
                *position,
                0,
                sql_sort_orders,
            );
        }
        0x18 => {
            if let Some(MapiObject::ContentsTable {
                folder_id,
                associated,
                sort_orders,
                category_count,
                restriction,
                position,
                ..
            }) = input_handle(handle_slots, request).and_then(|handle| handles.get_mut(&handle))
            {
                if request.seek_origin().unwrap_or(1) == 2 {
                    plan.requires_full_snapshot = true;
                    return;
                }
                let base_position = if request.seek_origin().unwrap_or(1) == 0 {
                    0isize
                } else {
                    *position as isize
                };
                let requested_position =
                    base_position.saturating_add(request.seek_row_count().unwrap_or(0) as isize);
                *position = requested_position.max(0) as usize;
                if *associated {
                    return;
                }
                if *category_count > 0
                    || restriction.is_some()
                    || !is_windowable_mail_contents_folder(*folder_id)
                {
                    plan.requires_full_snapshot = true;
                    return;
                }
                let Some(sql_sort_orders) = mapi_content_table_sort_orders(sort_orders) else {
                    plan.requires_full_snapshot = true;
                    return;
                };
                add_content_query(
                    plan,
                    *folder_id,
                    table_view_signature(sort_orders, restriction.as_ref()),
                    *position,
                    0,
                    sql_sort_orders,
                );
            }
        }
        0x82 => {
            let Some(source_object) =
                input_handle(handle_slots, request).and_then(|handle| handles.get(&handle))
            else {
                return;
            };
            let client_state_selection_invalidated = matches!(
                source_object,
                MapiObject::SynchronizationSource {
                    client_state_selection_invalidated: true,
                    ..
                }
            );
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
            )) = synchronization_context_state(Some(source_object))
            else {
                return;
            };
            let handle = simulate_allocate_handle(
                handles,
                next_handle,
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
                    sync_flags: 0,
                    initial_state: state.clone(),
                    state: state.clone(),
                    state_upload_property_tag: None,
                    state_upload_buffer: Vec::new(),
                    client_state_uploaded_bytes: 0,
                    client_state_uploaded_marker_mask: 0,
                    client_state_selection_enabled: false,
                    client_state_selection_invalidated,
                    client_state_selection_applied: false,
                    download_change_facts: Vec::new(),
                    incremental_transfer_buffer: None,
                    transfer_buffer: state,
                    transfer_position: 0,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
        }
        0x4F => {
            let Some(object) =
                input_handle(handle_slots, request).and_then(|handle| handles.get(&handle))
            else {
                return;
            };
            match object {
                MapiObject::HierarchyTable { .. } => {}
                MapiObject::ContentsTable {
                    associated: true, ..
                } => {}
                _ => plan.requires_full_snapshot = true,
            }
        }
        _ => {}
    }
}

fn simulated_default_view_content_sort(folder_id: u64, associated: bool) -> Vec<MapiSortOrder> {
    if associated || !is_windowable_mail_contents_folder(folder_id) {
        return Vec::new();
    }
    let view_name = if folder_id == SENT_FOLDER_ID {
        "Sent To"
    } else {
        crate::mapi_store::outlook_default_folder_named_view_name(folder_id)
    };
    outlook_folder_view_sort_orders(folder_id, view_name)
}

fn simulate_allocate_handle(
    handles: &mut HashMap<u32, MapiObject>,
    next_handle: &mut u32,
    output_handle_index: Option<u8>,
    object: MapiObject,
) -> u32 {
    let preferred = output_handle_index.map(|index| index as u32 + 1);
    let handle = preferred
        .filter(|handle| !handles.contains_key(handle))
        .unwrap_or(*next_handle);
    *next_handle = next_handle.saturating_add(1).max(1);
    if handle >= *next_handle {
        *next_handle = handle.saturating_add(1).max(1);
    }
    handles.insert(handle, object);
    handle
}

pub(in crate::mapi) fn add_content_query(
    plan: &mut MapiAccessPlan,
    folder_id: u64,
    view_signature: u64,
    offset: usize,
    limit: usize,
    sort_orders: Vec<MapiContentTableSort>,
) {
    let mut merged_offset = offset;
    let mut merged_limit = limit;
    let mut index = 0;
    while index < plan.content_queries.len() {
        let query = &plan.content_queries[index];
        if query.folder_id == folder_id
            && query.view_signature == view_signature
            && query.sort_orders == sort_orders
            && content_query_ranges_can_merge(
                query.offset,
                query.limit,
                merged_offset,
                merged_limit,
            )
        {
            let query = plan.content_queries.remove(index);
            if query.limit == 0 {
                if merged_limit == 0 {
                    merged_offset = query.offset;
                }
            } else if merged_limit == 0 {
                merged_offset = query.offset;
                merged_limit = query.limit;
            } else {
                let merged_start = query.offset.min(merged_offset);
                let merged_end = query
                    .offset
                    .saturating_add(query.limit)
                    .max(merged_offset.saturating_add(merged_limit));
                merged_offset = merged_start;
                merged_limit = merged_end.saturating_sub(merged_start);
            }
        } else {
            index += 1;
        }
    }
    plan.content_queries.push(MapiContentAccessQuery {
        folder_id,
        view_signature,
        offset: merged_offset,
        limit: merged_limit,
        sort_orders,
    });
}

fn content_query_ranges_can_merge(
    left_offset: usize,
    left_limit: usize,
    right_offset: usize,
    right_limit: usize,
) -> bool {
    if left_limit == 0 && right_limit == 0 {
        return true;
    }
    if left_limit == 0 || right_limit == 0 {
        return true;
    }
    let left_end = left_offset.saturating_add(left_limit);
    let right_end = right_offset.saturating_add(right_limit);
    left_offset <= right_end && right_offset <= left_end
}

fn is_windowable_mail_contents_folder(folder_id: u64) -> bool {
    if is_snapshot_backed_collaboration_folder(folder_id) {
        return false;
    }
    match role_for_folder_id(folder_id) {
        Some(
            "inbox"
            | "drafts"
            | "sent"
            | "trash"
            | "outbox"
            | "junk"
            | "rss_feeds"
            | "archive"
            | "conversation_history",
        )
        | None => true,
        Some(_) => false,
    }
}

fn mapi_content_table_sort_orders(
    sort_orders: &[MapiSortOrder],
) -> Option<Vec<MapiContentTableSort>> {
    sort_orders
        .iter()
        .map(|sort| {
            let field = match sort.property_tag {
                PID_TAG_MESSAGE_DELIVERY_TIME | PID_TAG_LAST_MODIFICATION_TIME => {
                    MapiContentTableSortField::ReceivedAt
                }
                PID_TAG_CLIENT_SUBMIT_TIME => MapiContentTableSortField::ClientSubmitTime,
                PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                    MapiContentTableSortField::Subject
                }
                PID_TAG_SENDER_NAME_W => MapiContentTableSortField::SenderName,
                PID_TAG_SENDER_EMAIL_ADDRESS_W => MapiContentTableSortField::SenderEmail,
                PID_TAG_DISPLAY_TO_W => MapiContentTableSortField::DisplayTo,
                PID_TAG_MESSAGE_SIZE => MapiContentTableSortField::MessageSize,
                PID_TAG_HAS_ATTACHMENTS => MapiContentTableSortField::HasAttachments,
                PID_TAG_MESSAGE_FLAGS => MapiContentTableSortField::MessageFlags,
                _ => return None,
            };
            Some(MapiContentTableSort {
                field,
                descending: sort.order == 0x01,
            })
        })
        .collect()
}

fn add_object_ids_for_handle(plan: &mut MapiAccessPlan, object: &MapiObject) {
    match object {
        MapiObject::Folder { folder_id, .. }
        | MapiObject::HierarchyTable { folder_id, .. }
        | MapiObject::ContentsTable { folder_id, .. }
        | MapiObject::PendingMessage { folder_id, .. }
        | MapiObject::PendingAssociatedMessage { folder_id, .. }
        | MapiObject::PendingContact { folder_id, .. }
        | MapiObject::PendingEvent { folder_id, .. }
        | MapiObject::PendingTask { folder_id, .. }
        | MapiObject::PendingNote { folder_id, .. }
        | MapiObject::PendingJournalEntry { folder_id, .. }
        | MapiObject::PendingConversationAction { folder_id, .. }
        | MapiObject::PendingNavigationShortcut { folder_id, .. }
        | MapiObject::SynchronizationSource { folder_id, .. }
        | MapiObject::SynchronizationCollector { folder_id, .. }
        | MapiObject::FastTransferDestination { folder_id, .. }
        | MapiObject::RuleTable { folder_id, .. }
        | MapiObject::PermissionTable { folder_id, .. } => {
            push_unique(&mut plan.object_ids, *folder_id);
        }
        MapiObject::Message {
            folder_id,
            message_id,
            ..
        }
        | MapiObject::AttachmentTable {
            folder_id,
            message_id,
            ..
        }
        | MapiObject::Attachment {
            folder_id,
            message_id,
            ..
        }
        | MapiObject::PendingAttachment {
            folder_id,
            message_id,
            ..
        }
        | MapiObject::SavedAttachment {
            folder_id,
            message_id,
            ..
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *message_id);
        }
        MapiObject::Contact {
            folder_id,
            contact_id,
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *contact_id);
        }
        MapiObject::Event {
            folder_id,
            event_id,
            ..
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *event_id);
        }
        MapiObject::Task { folder_id, task_id } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *task_id);
        }
        MapiObject::Note { folder_id, note_id } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *note_id);
        }
        MapiObject::JournalEntry {
            folder_id,
            journal_entry_id,
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *journal_entry_id);
        }
        MapiObject::ConversationAction {
            folder_id,
            conversation_action_id,
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            if !mapi_store::is_outlook_default_conversation_action_id(*conversation_action_id) {
                push_unique(&mut plan.object_ids, *conversation_action_id);
            }
        }
        MapiObject::NavigationShortcut {
            folder_id,
            shortcut_id,
            ..
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            if !mapi_store::is_outlook_common_views_default_navigation_shortcut_id(*shortcut_id) {
                push_unique(&mut plan.object_ids, *shortcut_id);
            }
        }
        MapiObject::CommonViewNamedView { folder_id, view_id } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *view_id);
        }
        MapiObject::SearchFolderDefinitionMessage {
            folder_id,
            message_id,
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *message_id);
        }
        MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            ..
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            if !mapi_store::is_outlook_inbox_default_associated_config_id(*config_id) {
                push_unique(&mut plan.object_ids, *config_id);
            }
        }
        MapiObject::DelegateFreeBusyMessage {
            folder_id,
            message_id,
            ..
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *message_id);
        }
        MapiObject::RecoverableItem { folder_id, item_id } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *item_id);
        }
        MapiObject::PublicFolderItem {
            folder_id, item_id, ..
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *item_id);
        }
        MapiObject::PublicFolderLogon => {
            plan.requires_full_snapshot = true;
        }
        MapiObject::AttachmentStream { .. }
        | MapiObject::NotificationSubscription { .. }
        | MapiObject::Logon => {}
    }
}

fn push_unique(values: &mut Vec<u64>, value: u64) {
    if value != 0 && !values.contains(&value) {
        values.push(value);
    }
}

pub(in crate::mapi) fn mapi_identity_requests_for_mailboxes(
    mailboxes: &[JmapMailbox],
) -> Vec<MapiIdentityRequest> {
    mapi_store::mapi_folder_identity_requests(mailboxes)
}

pub(in crate::mapi) fn merge_requested_mailboxes(
    mailboxes: &mut Vec<JmapMailbox>,
    all_mailboxes: &[JmapMailbox],
    requested_mailbox_ids: &[Uuid],
) {
    for requested_id in requested_mailbox_ids {
        if mailboxes.iter().any(|mailbox| mailbox.id == *requested_id) {
            continue;
        }
        if let Some(mailbox) = all_mailboxes
            .iter()
            .find(|mailbox| mailbox.id == *requested_id)
        {
            mailboxes.push(mailbox.clone());
        }
    }
}

pub(in crate::mapi) fn mailbox_id_for_mapi_folder_id(
    mailboxes: &[JmapMailbox],
    folder_id: u64,
) -> Option<Uuid> {
    mailboxes
        .iter()
        .find(|mailbox| mapi_folder_id(mailbox) == folder_id)
        .map(|mailbox| mailbox.id)
}
