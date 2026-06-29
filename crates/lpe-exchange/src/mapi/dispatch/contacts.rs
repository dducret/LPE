use super::*;

pub(super) fn mapi_folder_is_outlook_contacts_surface(folder_id: u64) -> bool {
    matches!(
        folder_id,
        CONTACTS_FOLDER_ID
            | SUGGESTED_CONTACTS_FOLDER_ID
            | QUICK_CONTACTS_FOLDER_ID
            | IM_CONTACT_LIST_FOLDER_ID
    )
}

pub(super) fn is_contact_link_timestamp_config(folder_id: u64, message_class: &str) -> bool {
    mapi_folder_is_outlook_contacts_surface(folder_id)
        && message_class == "IPM.Microsoft.ContactLink.TimeStamp"
}
