use super::properties::*;
use super::rop::*;
use super::session::*;
use super::sync::{INBOX_FOLDER_ID, ROOT_FOLDER_ID};
use super::tables::*;
use super::*;
use crate::mapi_store;
use crate::store::{MapiContentTableQuery, MapiContentTableSort, MapiContentTableSortField};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::mapi) struct MapiAccessPlan {
    pub(in crate::mapi) requires_full_snapshot: bool,
    pub(in crate::mapi) object_ids: Vec<u64>,
    pub(in crate::mapi) content_queries: Vec<MapiContentAccessQuery>,
}

impl MapiAccessPlan {
    fn full() -> Self {
        Self {
            requires_full_snapshot: true,
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
        object_ids: Vec::new(),
        content_queries: Vec::new(),
    };
    for object in session.handles.values() {
        add_object_ids_for_handle(&mut plan, object);
    }
    let mut simulated_handles = session.handles.clone();
    let mut simulated_next_handle = session.next_handle;

    let mut cursor = Cursor::new(requests);
    while cursor.remaining() > 0 {
        let Ok(request) = read_rop_request(&mut cursor) else {
            return MapiAccessPlan::full();
        };
        if rop_requires_full_snapshot(request.rop_id) {
            return MapiAccessPlan::full();
        }
        if let Some(folder_id) = request.folder_id() {
            push_unique(&mut plan.object_ids, folder_id);
        }
        if let Some(message_id) = request.message_id() {
            push_unique(&mut plan.object_ids, message_id);
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
        if let Some((message_id, target_folder_id)) = request.import_move() {
            push_unique(&mut plan.object_ids, message_id);
            push_unique(&mut plan.object_ids, target_folder_id);
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

pub(in crate::mapi) async fn load_mapi_store_for_access_plan<S>(
    store: &S,
    account_id: Uuid,
    plan: &MapiAccessPlan,
    full_message_limit: u64,
) -> Result<MapiMailStoreSnapshot>
where
    S: ExchangeStore,
{
    if plan.requires_full_snapshot {
        return store
            .load_mapi_mail_store(account_id, full_message_limit)
            .await;
    }

    let mailboxes = store.ensure_jmap_system_mailboxes(account_id).await?;
    let mailbox_requests = mapi_identity_requests_for_mailboxes(&mailboxes);
    for identity in store
        .fetch_or_allocate_mapi_identities(account_id, &mailbox_requests)
        .await?
    {
        crate::mapi::identity::remember_mapi_identity(identity.canonical_id, identity.object_id);
    }

    let identities = store
        .fetch_mapi_identities_by_object_ids(account_id, &plan.object_ids)
        .await?;
    for identity in &identities {
        crate::mapi::identity::remember_mapi_identity(identity.canonical_id, identity.object_id);
    }

    let mut content_windows = Vec::new();
    let mut content_message_ids = Vec::new();
    for query in &plan.content_queries {
        let Some(mailbox_id) = mailbox_id_for_mapi_folder_id(&mailboxes, query.folder_id) else {
            continue;
        };
        let result = store
            .query_mapi_content_table_ids(
                account_id,
                MapiContentTableQuery {
                    mailbox_id,
                    position: query.offset as u64,
                    limit: query.limit as u64,
                    sort_orders: query.sort_orders.clone(),
                },
            )
            .await?;
        content_message_ids.extend(result.ids.iter().copied());
        content_windows.push(mapi_store::MapiContentTableWindow {
            folder_id: query.folder_id,
            view_signature: query.view_signature,
            offset: query.offset,
            total: result.total.min(usize::MAX as u64) as usize,
            message_ids: result.ids,
        });
    }

    let mut message_ids = identities
        .iter()
        .filter(|identity| identity.object_kind == MapiIdentityObjectKind::Message)
        .map(|identity| identity.canonical_id)
        .collect::<Vec<_>>();
    for message_id in content_message_ids {
        if !message_ids.contains(&message_id) {
            message_ids.push(message_id);
        }
    }
    let message_identity_requests = message_ids
        .iter()
        .map(|message_id| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Message,
            canonical_id: *message_id,
            reserved_global_counter: None,
        })
        .collect::<Vec<_>>();
    for identity in store
        .fetch_or_allocate_mapi_identities(account_id, &message_identity_requests)
        .await?
    {
        crate::mapi::identity::remember_mapi_identity(identity.canonical_id, identity.object_id);
    }
    let emails = store.fetch_jmap_emails(account_id, &message_ids).await?;
    let mut attachments = Vec::with_capacity(emails.len());
    for email in &emails {
        attachments.push((
            email.id,
            store
                .fetch_message_attachments(account_id, email.id)
                .await?,
        ));
    }

    let contact_collections = store
        .fetch_accessible_contact_collections(account_id)
        .await?;
    let calendar_collections = store
        .fetch_accessible_calendar_collections(account_id)
        .await?;
    let contact_ids = identities
        .iter()
        .filter(|identity| identity.object_kind == MapiIdentityObjectKind::Contact)
        .map(|identity| identity.canonical_id)
        .collect::<Vec<_>>();
    let event_ids = identities
        .iter()
        .filter(|identity| identity.object_kind == MapiIdentityObjectKind::CalendarEvent)
        .map(|identity| identity.canonical_id)
        .collect::<Vec<_>>();
    let contacts = store
        .fetch_accessible_contacts_by_ids(account_id, &contact_ids)
        .await?;
    let events = store
        .fetch_accessible_events_by_ids(account_id, &event_ids)
        .await?;
    let mailbox_ids = mailboxes
        .iter()
        .map(|mailbox| mailbox.id)
        .collect::<Vec<_>>();
    let folder_permissions = store
        .fetch_mapi_folder_permissions(account_id, &mailbox_ids)
        .await?;

    Ok(MapiMailStoreSnapshot::new(
        mailboxes,
        emails,
        attachments,
        contact_collections,
        calendar_collections,
        contacts,
        events,
        folder_permissions,
    )
    .with_content_windows(content_windows))
}

fn rop_requires_full_snapshot(rop_id: u8) -> bool {
    matches!(
        rop_id,
        0x18 | 0x19
            | 0x1A
            | 0x1B
            | 0x4B
            | 0x4C
            | 0x4D
            | 0x4E
            | 0x4F
            | 0x70
            | 0x72
            | 0x73
            | 0x74
            | 0x78
            | 0x80
    )
}

fn simulate_table_access(
    plan: &mut MapiAccessPlan,
    handles: &mut HashMap<u32, MapiObject>,
    next_handle: &mut u32,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
) {
    match request.rop_id {
        0x02 => {
            let folder_id = request.folder_id().unwrap_or(ROOT_FOLDER_ID);
            let handle = simulate_allocate_handle(
                handles,
                next_handle,
                request.output_handle_index,
                MapiObject::Folder { folder_id },
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
                    sort_orders: Vec::new(),
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
            let handle = simulate_allocate_handle(
                handles,
                next_handle,
                request.output_handle_index,
                MapiObject::ContentsTable {
                    folder_id,
                    columns: Vec::new(),
                    sort_orders: Vec::new(),
                    restriction: None,
                    bookmarks: HashMap::new(),
                    next_bookmark: 1,
                    position: 0,
                },
            );
            set_handle_slot(handle_slots, request.output_handle_index, handle);
        }
        0x12 => {
            if let Some(MapiObject::ContentsTable { columns, .. }) =
                input_handle(handle_slots, request).and_then(|handle| handles.get_mut(&handle))
            {
                *columns = request.property_tags();
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
                sort_orders,
                restriction,
                position,
                ..
            }) = input_handle(handle_slots, request).and_then(|handle| handles.get_mut(&handle))
            else {
                return;
            };
            if restriction.is_some() {
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
                sort_orders,
                restriction,
                position,
                ..
            }) = input_handle(handle_slots, request).and_then(|handle| handles.get(&handle))
            else {
                return;
            };
            if restriction.is_some() {
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
        _ => {}
    }
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

fn add_content_query(
    plan: &mut MapiAccessPlan,
    folder_id: u64,
    view_signature: u64,
    offset: usize,
    limit: usize,
    sort_orders: Vec<MapiContentTableSort>,
) {
    if plan.content_queries.iter().any(|query| {
        query.folder_id == folder_id
            && query.view_signature == view_signature
            && query.offset == offset
            && query.limit == limit
    }) {
        return;
    }
    plan.content_queries.push(MapiContentAccessQuery {
        folder_id,
        view_signature,
        offset,
        limit,
        sort_orders,
    });
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
        MapiObject::Folder { folder_id }
        | MapiObject::HierarchyTable { folder_id, .. }
        | MapiObject::ContentsTable { folder_id, .. }
        | MapiObject::PendingMessage { folder_id, .. }
        | MapiObject::PendingContact { folder_id, .. }
        | MapiObject::PendingEvent { folder_id, .. }
        | MapiObject::SynchronizationSource { folder_id, .. }
        | MapiObject::SynchronizationCollector { folder_id, .. }
        | MapiObject::PermissionTable { folder_id, .. } => {
            push_unique(&mut plan.object_ids, *folder_id);
        }
        MapiObject::Message {
            folder_id,
            message_id,
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
        } => {
            push_unique(&mut plan.object_ids, *folder_id);
            push_unique(&mut plan.object_ids, *event_id);
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

fn mapi_identity_requests_for_mailboxes(mailboxes: &[JmapMailbox]) -> Vec<MapiIdentityRequest> {
    mailboxes
        .iter()
        .map(|mailbox| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Mailbox,
            canonical_id: mailbox.id,
            reserved_global_counter: mapi_store::reserved_folder_counter_for_role(&mailbox.role),
        })
        .collect()
}

fn mailbox_id_for_mapi_folder_id(mailboxes: &[JmapMailbox], folder_id: u64) -> Option<Uuid> {
    mailboxes
        .iter()
        .find(|mailbox| mapi_folder_id(mailbox) == folder_id)
        .map(|mailbox| mailbox.id)
}
