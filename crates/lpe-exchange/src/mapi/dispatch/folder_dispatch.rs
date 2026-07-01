use super::*;

pub(super) fn is_folder_dispatch_rop(rop_id: RopId) -> bool {
    matches!(
        rop_id,
        RopId::CreateFolder
            | RopId::DeleteFolder
            | RopId::MoveFolder
            | RopId::CopyFolder
            | RopId::EmptyFolder
            | RopId::HardDeleteMessagesAndSubfolders
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_folder_dispatch_response<S>(
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
) where
    S: ExchangeStore,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::CreateFolder) => {
            append_create_folder_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                snapshot,
                responses,
                output_handles,
            )
            .await;
        }
        Some(RopId::DeleteFolder) => {
            append_delete_folder_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                snapshot,
                responses,
            )
            .await;
        }
        Some(RopId::MoveFolder | RopId::CopyFolder) => {
            append_folder_move_copy_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                snapshot,
                responses,
            )
            .await;
        }
        Some(RopId::EmptyFolder | RopId::HardDeleteMessagesAndSubfolders) => {
            append_empty_folder_response(
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
        }
        _ => {}
    }
}
