use super::*;

pub(crate) fn fast_transfer_folder_content_without_subobjects(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    property_filter: FastTransferDirectPropertyFilter<'_>,
) -> Option<Vec<u8>> {
    let mailbox = mailboxes.iter().find(|mailbox| {
        let fallback = crate::mapi::identity::mapped_mapi_object_id(&mailbox.id).unwrap_or(0);
        mapi_folder_id_for_mailbox(mailbox, fallback) == folder_id
    })?;
    let mut buffer = Vec::new();
    write_u32(&mut buffer, START_TOP_FLD);
    write_fast_transfer_folder_properties(
        &mut buffer,
        folder_id,
        mailbox,
        mailboxes,
        emails,
        true,
        property_filter,
    );
    write_u32(&mut buffer, END_FOLDER);
    Some(buffer)
}
