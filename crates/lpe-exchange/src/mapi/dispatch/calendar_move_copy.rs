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
