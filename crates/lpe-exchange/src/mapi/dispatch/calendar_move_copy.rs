use super::*;

pub(super) fn calendar_same_folder_move_partial_completion(
    request: &RopRequest,
    source_folder_id: u64,
    target_folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<bool> {
    if request.move_copy_want_copy() || source_folder_id != target_folder_id {
        return None;
    }
    if !snapshot
        .collaboration_folder_for_id(source_folder_id)
        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
    {
        return None;
    }

    // A move to the same Folder object has no canonical collection mutation.
    // [MS-OXCFOLD] section 3.2.5.6 requires PartialCompletion only when at
    // least one requested message fails; [MS-OXCROPS] section 2.2.4.6.1
    // defines the source/destination handles and message identifiers.
    Some(
        request
            .move_copy_message_ids()
            .into_iter()
            .any(|message_id| {
                snapshot
                    .event_for_id(source_folder_id, message_id)
                    .is_none()
            }),
    )
}

pub(super) async fn calendar_move_to_deleted_items_partial_completion<S>(
    store: &S,
    principal: &AccountPrincipal,
    request: &RopRequest,
    source_folder_id: u64,
    target_folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<bool>
where
    S: ExchangeStore,
{
    if request.move_copy_want_copy()
        || target_folder_id != TRASH_FOLDER_ID
        || !snapshot
            .collaboration_folder_for_id(source_folder_id)
            .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
    {
        return None;
    }

    let mut partial_completion = false;
    for message_id in request.move_copy_message_ids() {
        let Some(event) = snapshot.event_for_id(source_folder_id, message_id) else {
            partial_completion = true;
            continue;
        };
        let Ok(moved) = store
            .move_accessible_event_to_deleted_items(principal.account_id, event.canonical_id, None)
            .await
        else {
            partial_completion = true;
            continue;
        };
        let Some(identity) = moved.principal_identity else {
            partial_completion = true;
            continue;
        };
        if identity.old_mapi_object_id != message_id {
            partial_completion = true;
        }
        crate::mapi::identity::remember_mapi_identity_with_source_key(
            moved.event.id,
            identity.new_mapi_object_id,
            Some(identity.new_source_key.clone()),
        );
    }

    // Outlook implements a normal Calendar delete as an inter-folder move.
    // The canonical deleted Calendar lifecycle owns the destination object and
    // its new principal-scoped MID/SourceKey; no synthetic mail row is created.
    // [MS-OXCROPS] sections 2.2.4.6.1 and 2.2.4.6.2;
    // [MS-OXCFOLD] sections 2.2.1.6 and 3.2.5.6.
    Some(partial_completion)
}
