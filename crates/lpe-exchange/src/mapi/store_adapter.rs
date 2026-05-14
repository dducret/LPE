use super::rop::*;
use super::session::*;
use super::*;
use crate::mapi_store;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::mapi) struct MapiAccessPlan {
    pub(in crate::mapi) requires_full_snapshot: bool,
    pub(in crate::mapi) object_ids: Vec<u64>,
}

impl MapiAccessPlan {
    fn full() -> Self {
        Self {
            requires_full_snapshot: true,
            object_ids: Vec::new(),
        }
    }
}

pub(in crate::mapi) fn plan_mapi_store_access(
    session: &MapiSession,
    rop_buffer: &[u8],
) -> MapiAccessPlan {
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return MapiAccessPlan::full();
    };
    let handle_slots = read_handle_table(handle_table);
    let mut plan = MapiAccessPlan {
        requires_full_snapshot: false,
        object_ids: Vec::new(),
    };
    for object in session.handles.values() {
        add_object_ids_for_handle(&mut plan, object);
    }

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
            if let Some(object) = session.handles.get(&handle) {
                add_object_ids_for_handle(&mut plan, object);
            }
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

    let mailboxes = store.fetch_jmap_mailboxes(account_id).await?;
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

    let message_ids = identities
        .iter()
        .filter(|identity| identity.object_kind == MapiIdentityObjectKind::Message)
        .map(|identity| identity.canonical_id)
        .collect::<Vec<_>>();
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

    Ok(MapiMailStoreSnapshot::new(
        mailboxes,
        emails,
        attachments,
        contact_collections,
        calendar_collections,
        contacts,
        events,
    ))
}

fn rop_requires_full_snapshot(rop_id: u8) -> bool {
    matches!(
        rop_id,
        0x04 | 0x05
            | 0x12
            | 0x13
            | 0x14
            | 0x15
            | 0x16
            | 0x17
            | 0x18
            | 0x19
            | 0x1A
            | 0x1B
            | 0x4B
            | 0x4C
            | 0x4D
            | 0x4E
            | 0x70
            | 0x72
            | 0x73
            | 0x74
            | 0x78
            | 0x80
            | 0x81
            | 0x89
    )
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
        | MapiObject::SynchronizationCollector { folder_id, .. } => {
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
        MapiObject::AttachmentStream { .. } | MapiObject::Logon => {}
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
