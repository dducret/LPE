use super::{write_object_id, write_u32, RopRequest};
use crate::mapi::identity::{CALENDAR_FOLDER_ID, INBOX_FOLDER_ID};
use crate::mapi::properties::{
    write_mapi_value, MapiValue, PID_TAG_FOLDER_ID, PID_TAG_LAST_MODIFICATION_TIME,
    PID_TAG_MESSAGE_CLASS_STRING8,
};
use crate::mapi::tables::write_standard_property_row;

pub(in crate::mapi) fn rop_get_receive_folder_response(
    request: &RopRequest,
    folder_id: u64,
    response_message_class: &str,
) -> Vec<u8> {
    let mut response = vec![0x27, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_object_id(&mut response, folder_id);
    response.extend_from_slice(response_message_class.as_bytes());
    response.push(0);
    response
}

pub(in crate::mapi) fn valid_receive_folder_message_class(message_class: &str) -> bool {
    let len = message_class.len() + 1;
    len <= 255
        && !message_class.starts_with('.')
        && !message_class.ends_with('.')
        && !message_class.contains("..")
        && message_class
            .bytes()
            .all(|byte| (0x20..=0x7E).contains(&byte))
}

#[derive(Clone, Copy)]
struct ReceiveFolderEntry {
    message_class: &'static str,
    folder_id: u64,
}

const RECEIVE_FOLDER_ENTRIES: &[ReceiveFolderEntry] = &[
    ReceiveFolderEntry {
        message_class: "",
        folder_id: INBOX_FOLDER_ID,
    },
    ReceiveFolderEntry {
        message_class: "IPM.Appointment",
        folder_id: CALENDAR_FOLDER_ID,
    },
    ReceiveFolderEntry {
        message_class: "IPM.Note",
        folder_id: INBOX_FOLDER_ID,
    },
    ReceiveFolderEntry {
        message_class: "IPM",
        folder_id: INBOX_FOLDER_ID,
    },
];

fn receive_folder_entry_matches(entry: ReceiveFolderEntry, message_class: &str) -> bool {
    if entry.message_class.is_empty() {
        return true;
    }
    if message_class.len() < entry.message_class.len()
        || !message_class.as_bytes()[..entry.message_class.len()]
            .eq_ignore_ascii_case(entry.message_class.as_bytes())
    {
        return false;
    }
    message_class.len() == entry.message_class.len()
        || message_class.as_bytes().get(entry.message_class.len()) == Some(&b'.')
}

fn receive_folder_entry_for_message_class(message_class: &str) -> ReceiveFolderEntry {
    RECEIVE_FOLDER_ENTRIES
        .iter()
        .copied()
        .filter(|entry| receive_folder_entry_matches(*entry, message_class))
        .max_by_key(|entry| entry.message_class.len())
        .unwrap_or(ReceiveFolderEntry {
            message_class: "",
            folder_id: INBOX_FOLDER_ID,
        })
}

pub(in crate::mapi) fn explicit_receive_folder_message_class(message_class: &str) -> &'static str {
    receive_folder_entry_for_message_class(message_class).message_class
}

pub(in crate::mapi) fn receive_folder_id_for_message_class(message_class: &str) -> u64 {
    receive_folder_entry_for_message_class(message_class).folder_id
}

pub(in crate::mapi) fn rop_get_receive_folder_table_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x68, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, RECEIVE_FOLDER_ENTRIES.len() as u32);
    for entry in RECEIVE_FOLDER_ENTRIES {
        let mut row = Vec::new();
        write_mapi_value(
            &mut row,
            PID_TAG_FOLDER_ID,
            &MapiValue::U64(entry.folder_id),
        );
        write_mapi_value(
            &mut row,
            PID_TAG_MESSAGE_CLASS_STRING8,
            &MapiValue::String(entry.message_class.to_string()),
        );
        write_mapi_value(
            &mut row,
            PID_TAG_LAST_MODIFICATION_TIME,
            &MapiValue::U64(crate::mapi_mailstore::filetime_from_change_number(
                crate::mapi_mailstore::change_number_for_store_id(entry.folder_id),
            )),
        );
        write_standard_property_row(&mut response, &row);
    }
    response
}
