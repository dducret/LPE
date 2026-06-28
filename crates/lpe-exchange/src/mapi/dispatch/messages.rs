use super::*;

pub(super) fn canonical_message_folder_id(email: &JmapEmail, mailboxes: &[JmapMailbox]) -> u64 {
    email
        .mailbox_states
        .iter()
        .find_map(|state| {
            mailboxes
                .iter()
                .find(|mailbox| mailbox.id == state.mailbox_id)
                .map(mapi_folder_id)
        })
        .or_else(|| {
            mailboxes
                .iter()
                .find(|mailbox| mailbox.id == email.mailbox_id)
                .map(mapi_folder_id)
        })
        .unwrap_or_else(|| {
            mailboxes
                .iter()
                .find(|mailbox| mailbox.role == email.mailbox_role)
                .map(mapi_folder_id)
                .unwrap_or(INBOX_FOLDER_ID)
        })
}

pub(super) fn fallback_open_message_folder_id(
    requested_folder_id: u64,
    email: &JmapEmail,
    mailboxes: &[JmapMailbox],
) -> u64 {
    if email_matches_folder(email, requested_folder_id, mailboxes) {
        requested_folder_id
    } else {
        canonical_message_folder_id(email, mailboxes)
    }
}

pub(super) fn open_message_folder_id(request: &RopRequest, message_id: u64) -> u64 {
    request.folder_id().unwrap_or_else(|| {
        if crate::mapi_store::is_outlook_local_freebusy_message_id(message_id) {
            FREEBUSY_DATA_FOLDER_ID
        } else {
            INBOX_FOLDER_ID
        }
    })
}

pub(super) fn unique_message_for_id(message_id: u64, emails: &[JmapEmail]) -> Option<&JmapEmail> {
    let mut matches = emails
        .iter()
        .filter(|email| mapi_item_id_matches(&email.id, message_id));
    let email = matches.next()?;
    matches.next().is_none().then_some(email)
}

pub(super) fn persisted_message_delete_is_best_effort(object: Option<&MapiObject>) -> bool {
    matches!(object, Some(MapiObject::Message { .. }))
}

pub(super) fn append_save_changes_message_response(
    responses: &mut Vec<u8>,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    handle: u32,
    message_id: u64,
) {
    set_handle_slot(handle_slots, Some(request.response_handle_index()), handle);
    responses.extend_from_slice(&rop_save_changes_message_response(request, message_id));
}
