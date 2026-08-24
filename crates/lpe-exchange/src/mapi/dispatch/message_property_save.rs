use super::*;

pub(super) fn reject_non_atomic_processed_save(
    session: &mut MapiSession,
    request: &RopRequest,
    handle: u32,
    folder_id: u64,
    message_id: u64,
    saved_email: &Option<MapiSavedEmail>,
    pending_properties: &HashMap<u32, MapiValue>,
    has_other_staged_mutations: bool,
    responses: &mut Vec<u8>,
) -> bool {
    let has_processed = pending_properties
        .keys()
        .any(|tag| canonical_property_storage_tag(*tag) == PID_TAG_PROCESSED);
    if !has_processed || (!has_other_staged_mutations && pending_properties.len() == 1) {
        return false;
    }
    responses.extend_from_slice(&rop_error_response(
        0x0C,
        request.response_handle_index(),
        0x8004_0102,
    ));
    session.handles.insert(
        handle,
        MapiObject::Message {
            folder_id,
            message_id,
            saved_email: saved_email.clone(),
            pending_properties: pending_properties.clone(),
        },
    );
    true
}

pub(super) async fn save_existing_message_property_values<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    request: &RopRequest,
    handle: u32,
    folder_id: u64,
    message_id: u64,
    saved_email: &mut Option<MapiSavedEmail>,
    pending_properties: &HashMap<u32, MapiValue>,
    has_other_staged_mutations: bool,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) -> Option<bool> {
    if reject_non_atomic_processed_save(
        session,
        request,
        handle,
        folder_id,
        message_id,
        saved_email,
        pending_properties,
        has_other_staged_mutations,
        responses,
    ) {
        return None;
    }
    if pending_properties.is_empty() {
        return Some(false);
    }
    match apply_staged_message_property_values(
        store,
        principal,
        folder_id,
        message_id,
        saved_email.clone(),
        pending_properties.clone(),
        mailboxes,
        emails,
        snapshot,
    )
    .await
    {
        Ok((Some(processed_request), changed)) => {
            *saved_email = Some(processed_request);
            Some(changed)
        }
        Ok((None, changed)) => Some(changed),
        Err(_) => {
            responses.extend_from_slice(&rop_error_response(
                0x0C,
                request.response_handle_index(),
                0x8004_0102,
            ));
            session.handles.insert(
                handle,
                MapiObject::Message {
                    folder_id,
                    message_id,
                    saved_email: saved_email.clone(),
                    pending_properties: pending_properties.clone(),
                },
            );
            None
        }
    }
}
