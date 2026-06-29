use super::*;

pub(super) fn seek_row_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    rop_seek_row_bookmark_response(request, object, mailboxes, emails, snapshot, mailbox_guid)
}

pub(super) fn create_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    rop_create_bookmark_response(request, object, mailboxes, emails, snapshot, mailbox_guid)
}

pub(super) fn free_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
) -> Vec<u8> {
    rop_free_bookmark_response(request, object)
}
