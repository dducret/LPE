use super::*;

pub(super) async fn append_get_properties_all_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    let property_tags = match input_object(session, handle_slots, request) {
        Some(MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID | INBOX_FOLDER_ID,
            ..
        }) => {
            let mut property_tags = default_folder_property_tags();
            property_tags.extend(default_folder_identity_property_tags());
            property_tags
        }
        Some(MapiObject::Folder { .. }) => default_folder_property_tags(),
        _ => Vec::new(),
    };
    hydrate_folder_handle_properties_for_request(
        store,
        principal,
        session,
        handle_slots,
        request,
        &property_tags,
    )
    .await;
    let object = attachment_overlay_object(session, handle_slots, request, snapshot);
    let custom_values =
        effective_local_freebusy_custom_property_values(object.as_ref(), snapshot, None)
            .unwrap_or_default();
    responses.extend_from_slice(&rop_get_properties_all_response_with_custom(
        request,
        session,
        object.as_ref(),
        principal,
        mailboxes,
        emails,
        snapshot,
        &custom_values,
    ));
}

pub(super) fn append_get_properties_list_response(
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    let object = input_object(session, handle_slots, request);
    let custom_values =
        effective_local_freebusy_custom_property_values(object, snapshot, None).unwrap_or_default();
    let custom_property_tags = custom_values.keys().copied().collect::<Vec<_>>();
    responses.extend_from_slice(&rop_get_properties_list_response_with_custom_tags(
        request,
        session,
        object,
        snapshot,
        Some(principal),
        &custom_property_tags,
    ));
}
