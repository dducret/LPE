use super::*;

pub(in crate::mapi) fn default_view_supported_container_class(container_class: &str) -> bool {
    container_class == "IPF.Note"
        || container_class.starts_with("IPF.Note.")
        || container_class == "IPF.Contact"
        || container_class.starts_with("IPF.Contact.")
        || container_class == "IPF.Appointment"
        || container_class.starts_with("IPF.Appointment.")
        || container_class == "IPF.Task"
        || container_class.starts_with("IPF.Task.")
        || container_class == "IPF.StickyNote"
        || container_class.starts_with("IPF.StickyNote.")
        || container_class == "IPF.Journal"
        || container_class.starts_with("IPF.Journal.")
}

pub(in crate::mapi) fn default_view_supported_folder(
    folder_id: u64,
    container_class: &str,
) -> bool {
    if !default_view_supported_container_class(container_class) {
        return false;
    }
    if container_class == "IPF.Contact" || container_class.starts_with("IPF.Contact.") {
        return matches!(folder_id, CONTACTS_FOLDER_ID | CONTACTS_SEARCH_FOLDER_ID);
    }
    if container_class == "IPF.Appointment" || container_class.starts_with("IPF.Appointment.") {
        return folder_id == CALENDAR_FOLDER_ID;
    }
    if container_class == "IPF.Task" || container_class.starts_with("IPF.Task.") {
        return folder_id == TASKS_FOLDER_ID;
    }
    if container_class == "IPF.StickyNote" || container_class.starts_with("IPF.StickyNote.") {
        return folder_id == NOTES_FOLDER_ID;
    }
    if container_class == "IPF.Journal" || container_class.starts_with("IPF.Journal.") {
        return folder_id == JOURNAL_FOLDER_ID;
    }
    if matches!(
        folder_id,
        INBOX_FOLDER_ID
            | OUTBOX_FOLDER_ID
            | SENT_FOLDER_ID
            | TRASH_FOLDER_ID
            | DRAFTS_FOLDER_ID
            | JUNK_FOLDER_ID
            | ARCHIVE_FOLDER_ID
            | CONVERSATION_HISTORY_FOLDER_ID
    ) {
        return true;
    }
    !matches!(
        folder_id,
        ROOT_FOLDER_ID
            | DEFERRED_ACTION_FOLDER_ID
            | SPOOLER_QUEUE_FOLDER_ID
            | IPM_SUBTREE_FOLDER_ID
            | COMMON_VIEWS_FOLDER_ID
            | SCHEDULE_FOLDER_ID
            | SEARCH_FOLDER_ID
            | VIEWS_FOLDER_ID
            | SHORTCUTS_FOLDER_ID
            | FREEBUSY_DATA_FOLDER_ID
            | SYNC_ISSUES_FOLDER_ID
            | CONFLICTS_FOLDER_ID
            | LOCAL_FAILURES_FOLDER_ID
            | SERVER_FAILURES_FOLDER_ID
            | RSS_FEEDS_FOLDER_ID
            | TRACKED_MAIL_PROCESSING_FOLDER_ID
            | TODO_SEARCH_FOLDER_ID
            | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
            | RECOVERABLE_ITEMS_ROOT_FOLDER_ID
            | RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID
            | RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID
            | RECOVERABLE_ITEMS_PURGES_FOLDER_ID
            | QUICK_STEP_SETTINGS_FOLDER_ID
            | PUBLIC_FOLDERS_ROOT_FOLDER_ID
    )
}

pub(in crate::mapi) fn default_folder_view_entry_id(
    mailbox_guid: Uuid,
    folder_id: u64,
    container_class: &str,
) -> Option<MapiValue> {
    let (view_folder_id, view_id) =
        if let Some(view_id) = default_common_views_named_view_id(container_class, folder_id) {
            (COMMON_VIEWS_FOLDER_ID, view_id)
        } else {
            (
                folder_id,
                crate::mapi_store::outlook_default_folder_named_view_id(folder_id),
            )
        };
    crate::mapi::identity::message_entry_id_from_object_ids(mailbox_guid, view_folder_id, view_id)
        .map(MapiValue::Binary)
}

pub(in crate::mapi) fn default_view_uses_common_views(
    container_class: &str,
    folder_id: u64,
) -> bool {
    default_common_views_named_view_id(container_class, folder_id).is_some()
}

pub(in crate::mapi) fn default_common_views_named_view_id(
    container_class: &str,
    folder_id: u64,
) -> Option<u64> {
    (container_class == "IPF.Note" || container_class.starts_with("IPF.Note."))
        .then_some(folder_id)
        .and_then(|folder_id| match folder_id {
            SENT_FOLDER_ID => Some(crate::mapi_store::OUTLOOK_COMMON_VIEWS_SENT_TO_NAMED_VIEW_ID),
            _ => None,
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::mapi) enum ViewDefinitionKind {
    CalendarCompact,
    ContactList,
    JournalList,
    MailCompact,
    MailSentTo,
    NoteList,
    TaskList,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::mapi) struct ViewColumn {
    pub(in crate::mapi) property_tag: u32,
    pub(in crate::mapi) width: u32,
    pub(in crate::mapi) flags: u32,
    pub(in crate::mapi) kind: ViewColumnKind,
    pub(in crate::mapi) header: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::mapi) enum ViewColumnKind {
    Id,
    NamedId { guid: [u8; 16], id: u32 },
    NamedString { guid: [u8; 16], name: &'static str },
}

#[derive(Debug, Clone)]
pub(in crate::mapi) struct ViewDefinition {
    pub(in crate::mapi) kind: ViewDefinitionKind,
    pub(in crate::mapi) columns: Vec<ViewColumn>,
    pub(in crate::mapi) sort_column: usize,
    pub(in crate::mapi) sort_descending: bool,
}

const PID_TAG_SENT_REPRESENTING_NAME_STRING8: u32 =
    (PID_TAG_SENT_REPRESENTING_NAME_W & 0xFFFF_0000) | 0x001E;
const PID_TAG_DISPLAY_TO_STRING8: u32 = (PID_TAG_DISPLAY_TO_W & 0xFFFF_0000) | 0x001E;
const PID_TAG_SUBJECT_STRING8: u32 = (PID_TAG_SUBJECT_W & 0xFFFF_0000) | 0x001E;
const PID_NAME_KEYWORDS_STRING8_MULTI: u32 = (PID_NAME_KEYWORDS_TAG & 0xFFFF_0000) | 0x101E;

pub(in crate::mapi) fn outlook_mail_view_definition(view_name: &str) -> ViewDefinition {
    if view_name.eq_ignore_ascii_case("Sent To") {
        return ViewDefinition {
            kind: ViewDefinitionKind::MailSentTo,
            columns: vec![
                view_column(PID_TAG_IMPORTANCE, 0x12, 0x0000_2F4A, "Importance"),
                view_named_id_column(
                    PID_LID_REMINDER_SET_TAG,
                    0x12,
                    0x0000_3F40,
                    PSETID_COMMON_GUID,
                    PID_LID_REMINDER_SET,
                    "Reminder",
                ),
                view_column(PID_TAG_MESSAGE_CLASS_STRING8, 0x12, 0x0000_270A, "Icon"),
                view_column(PID_TAG_FLAG_STATUS, 0x12, 0x0000_2F4A, "Flag Status"),
                view_column(PID_TAG_HAS_ATTACHMENTS, 0x12, 0x0000_2F4A, "Attachment"),
                view_column(PID_TAG_DISPLAY_TO_STRING8, 0x0C, 0x0000_2F00, "To"),
                view_column(PID_TAG_SUBJECT_STRING8, 0x11, 0x0000_2F00, "Subject"),
                view_column(PID_TAG_CLIENT_SUBMIT_TIME, 0x10, 0x0000_2F40, "Sent"),
                view_column(PID_TAG_MESSAGE_SIZE, 0x0C, 0x0000_2740, "Size"),
                view_named_string_column(
                    PID_NAME_KEYWORDS_STRING8_MULTI,
                    0x12,
                    0x0000_7B20,
                    PS_PUBLIC_STRINGS_GUID,
                    "Keywords",
                    "Categories",
                ),
            ],
            sort_column: 7,
            sort_descending: true,
        };
    }
    if view_name.eq_ignore_ascii_case("Messages") {
        return ViewDefinition {
            kind: ViewDefinitionKind::MailCompact,
            columns: vec![
                view_column(PID_TAG_IMPORTANCE, 0x12, 0x0000_2F4A, "Importance"),
                view_named_id_column(
                    PID_LID_REMINDER_SET_TAG,
                    0x12,
                    0x0000_3F40,
                    PSETID_COMMON_GUID,
                    PID_LID_REMINDER_SET,
                    "Reminder",
                ),
                view_column(PID_TAG_MESSAGE_CLASS_STRING8, 0x12, 0x0000_270A, "Icon"),
                view_column(PID_TAG_FLAG_STATUS, 0x12, 0x0000_2F4A, "Flag Status"),
                view_column(PID_TAG_HAS_ATTACHMENTS, 0x12, 0x0000_2F4A, "Attachment"),
                view_column(
                    PID_TAG_SENT_REPRESENTING_NAME_STRING8,
                    0x0C,
                    0x0000_2F00,
                    "From",
                ),
                view_column(PID_TAG_SUBJECT_STRING8, 0x11, 0x0000_2F00, "Subject"),
                view_column(PID_TAG_MESSAGE_DELIVERY_TIME, 0x10, 0x0000_2F40, "Received"),
                view_column(PID_TAG_MESSAGE_SIZE, 0x0C, 0x0000_2740, "Size"),
                view_named_string_column(
                    PID_NAME_KEYWORDS_STRING8_MULTI,
                    0x12,
                    0x0000_7B20,
                    PS_PUBLIC_STRINGS_GUID,
                    "Keywords",
                    "Categories",
                ),
            ],
            sort_column: 7,
            sort_descending: true,
        };
    }

    ViewDefinition {
        kind: ViewDefinitionKind::MailCompact,
        columns: vec![
            view_column(PID_TAG_IMPORTANCE, 0x12, 0x0000_2F4A, "Importance"),
            view_named_id_column(
                PID_LID_REMINDER_SET_TAG,
                0x12,
                0x0000_3F40,
                PSETID_COMMON_GUID,
                PID_LID_REMINDER_SET,
                "Reminder",
            ),
            view_column(PID_TAG_MESSAGE_CLASS_STRING8, 0x12, 0x0000_270A, "Icon"),
            view_column(PID_TAG_FLAG_STATUS, 0x12, 0x0000_2F4A, "Flag Status"),
            view_column(PID_TAG_HAS_ATTACHMENTS, 0x12, 0x0000_2F4A, "Attachment"),
            view_column(
                PID_TAG_SENT_REPRESENTING_NAME_STRING8,
                0x0C,
                0x0000_2F00,
                "From",
            ),
            view_column(PID_TAG_SUBJECT_STRING8, 0x11, 0x0000_2F00, "Subject"),
            view_column(PID_TAG_MESSAGE_DELIVERY_TIME, 0x10, 0x0000_2F40, "Received"),
            view_column(PID_TAG_MESSAGE_SIZE, 0x0C, 0x0000_2740, "Size"),
            view_named_string_column(
                PID_NAME_KEYWORDS_STRING8_MULTI,
                0x12,
                0x0000_7B20,
                PS_PUBLIC_STRINGS_GUID,
                "Keywords",
                "Categories",
            ),
        ],
        sort_column: 7,
        sort_descending: true,
    }
}

pub(in crate::mapi) fn outlook_folder_view_definition(
    folder_id: u64,
    view_name: &str,
) -> ViewDefinition {
    match folder_id {
        INBOX_FOLDER_ID if view_name.eq_ignore_ascii_case("Compact") => {
            outlook_inbox_compact_view_definition()
        }
        CALENDAR_FOLDER_ID => outlook_calendar_view_definition(view_name),
        CONTACTS_FOLDER_ID
        | SUGGESTED_CONTACTS_FOLDER_ID
        | QUICK_CONTACTS_FOLDER_ID
        | IM_CONTACT_LIST_FOLDER_ID
        | CONTACTS_SEARCH_FOLDER_ID => outlook_contact_view_definition(view_name),
        JOURNAL_FOLDER_ID => outlook_journal_view_definition(view_name),
        NOTES_FOLDER_ID => outlook_note_view_definition(view_name),
        TASKS_FOLDER_ID | TODO_SEARCH_FOLDER_ID => outlook_task_view_definition(view_name),
        _ => outlook_mail_view_definition(view_name),
    }
}

pub(in crate::mapi) fn outlook_folder_view_sort_orders(
    folder_id: u64,
    view_name: &str,
) -> Vec<MapiSortOrder> {
    let definition = outlook_folder_view_definition(folder_id, view_name);
    definition
        .columns
        .get(definition.sort_column)
        .map(|column| MapiSortOrder {
            property_tag: column.property_tag,
            order: if definition.sort_descending {
                0x01
            } else {
                0x00
            },
        })
        .into_iter()
        .collect()
}

fn outlook_inbox_compact_view_definition() -> ViewDefinition {
    // MS-OXOCFG 2.2.6.1 and 4.2.1 define mail view descriptors as UI
    // columns; Outlook can still request row identity columns on the table.
    ViewDefinition {
        kind: ViewDefinitionKind::MailCompact,
        columns: vec![
            view_column(PID_TAG_IMPORTANCE, 0x12, 0x0000_2F4A, "Importance"),
            view_named_id_column(
                PID_LID_REMINDER_SET_TAG,
                0x12,
                0x0000_3F40,
                PSETID_COMMON_GUID,
                PID_LID_REMINDER_SET,
                "Reminder",
            ),
            view_column(PID_TAG_MESSAGE_CLASS_STRING8, 0x12, 0x0000_270A, "Icon"),
            view_column(PID_TAG_FLAG_STATUS, 0x12, 0x0000_2F4A, "Flag Status"),
            view_column(PID_TAG_HAS_ATTACHMENTS, 0x12, 0x0000_2F4A, "Attachment"),
            view_column(
                PID_TAG_SENT_REPRESENTING_NAME_STRING8,
                0x0C,
                0x0000_2F00,
                "From",
            ),
            view_column(PID_TAG_SUBJECT_STRING8, 0x11, 0x0000_2F00, "Subject"),
            view_column(PID_TAG_MESSAGE_DELIVERY_TIME, 0x10, 0x0000_2F40, "Received"),
            view_column(PID_TAG_MESSAGE_SIZE, 0x0C, 0x0000_2740, "Size"),
            view_named_string_column(
                PID_NAME_KEYWORDS_STRING8_MULTI,
                0x12,
                0x0000_7B20,
                PS_PUBLIC_STRINGS_GUID,
                "Keywords",
                "Categories",
            ),
        ],
        sort_column: 7,
        sort_descending: true,
    }
}

fn outlook_calendar_view_definition(_view_name: &str) -> ViewDefinition {
    // MS-OXOCFG 2.2.6.1 stores visible view columns. Row identity columns are
    // served by live table projections when Outlook requests them.
    ViewDefinition {
        kind: ViewDefinitionKind::CalendarCompact,
        columns: vec![
            view_column(PID_TAG_MESSAGE_CLASS_W, 0x12, 0x0000_270A, "Icon"),
            view_column(PID_TAG_SUBJECT_W, 0x18, 0x0000_2F00, "Subject"),
            view_column(PID_TAG_MESSAGE_FLAGS, 0x0C, 0x0000_2740, "Message Flags"),
            view_column(PID_TAG_MESSAGE_STATUS, 0x0C, 0x0000_2740, "Message Status"),
            view_named_id_column(
                PID_LID_COMMON_START_TAG,
                0x10,
                0x0000_3F40,
                PSETID_COMMON_GUID,
                PID_LID_COMMON_START,
                "Start",
            ),
            view_named_id_column(
                PID_LID_COMMON_END_TAG,
                0x10,
                0x0000_3F40,
                PSETID_COMMON_GUID,
                PID_LID_COMMON_END,
                "End",
            ),
            view_named_id_column(
                PID_LID_LOCATION_W_TAG,
                0x14,
                0x0000_3F00,
                PSETID_APPOINTMENT_GUID,
                PID_LID_LOCATION,
                "Location",
            ),
            view_named_id_column(
                PID_LID_BUSY_STATUS_TAG,
                0x0C,
                0x0000_3F40,
                PSETID_APPOINTMENT_GUID,
                PID_LID_BUSY_STATUS,
                "Busy",
            ),
        ],
        sort_column: 4,
        sort_descending: false,
    }
}

fn outlook_contact_view_definition(_view_name: &str) -> ViewDefinition {
    // MS-OXOCFG 2.2.6.1 stores visible view columns. Row identity columns are
    // served by live table projections when Outlook requests them.
    ViewDefinition {
        kind: ViewDefinitionKind::ContactList,
        columns: vec![
            view_column(PID_TAG_MESSAGE_FLAGS, 0x0C, 0x0000_2740, "Message Flags"),
            view_column(PID_TAG_MESSAGE_STATUS, 0x0C, 0x0000_2740, "Message Status"),
            view_column(PID_TAG_MESSAGE_CLASS_W, 0x12, 0x0000_270A, "Icon"),
            view_column(PID_TAG_DISPLAY_NAME_W, 0x18, 0x0000_2F00, "Full Name"),
            view_named_id_column(
                PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG,
                0x18,
                0x0000_3F00,
                PSETID_ADDRESS_GUID,
                PID_LID_EMAIL1_EMAIL_ADDRESS,
                "Email",
            ),
            view_column(
                PID_TAG_MOBILE_TELEPHONE_NUMBER_W,
                0x12,
                0x0000_2F00,
                "Mobile",
            ),
            view_column(PID_TAG_COMPANY_NAME_W, 0x14, 0x0000_2F00, "Company"),
            view_column(PID_TAG_TITLE_W, 0x14, 0x0000_2F00, "Job Title"),
        ],
        sort_column: 3,
        sort_descending: false,
    }
}

fn outlook_task_view_definition(_view_name: &str) -> ViewDefinition {
    // MS-OXOCFG 2.2.6.1 stores visible view columns. Row identity columns are
    // served by live table projections when Outlook requests them.
    ViewDefinition {
        kind: ViewDefinitionKind::TaskList,
        columns: vec![
            view_column(PID_TAG_MESSAGE_FLAGS, 0x0C, 0x0000_2740, "Message Flags"),
            view_column(PID_TAG_MESSAGE_STATUS, 0x0C, 0x0000_2740, "Message Status"),
            view_column(PID_TAG_MESSAGE_CLASS_W, 0x12, 0x0000_270A, "Icon"),
            view_column(PID_TAG_SUBJECT_W, 0x18, 0x0000_2F00, "Subject"),
            view_column(PID_TAG_FLAG_STATUS, 0x0C, 0x0000_2F4A, "Status"),
            view_named_id_column(
                PID_LID_TASK_DUE_DATE_TAG,
                0x10,
                0x0000_3F40,
                PSETID_TASK_GUID,
                PID_LID_TASK_DUE_DATE,
                "Due Date",
            ),
            view_named_id_column(
                PID_LID_TASK_START_DATE_TAG,
                0x10,
                0x0000_3F40,
                PSETID_TASK_GUID,
                PID_LID_TASK_START_DATE,
                "Start Date",
            ),
            view_named_id_column(
                PID_LID_PERCENT_COMPLETE_TAG,
                0x0C,
                0x0000_3F40,
                PSETID_TASK_GUID,
                PID_LID_PERCENT_COMPLETE,
                "% Complete",
            ),
        ],
        sort_column: 5,
        sort_descending: false,
    }
}

fn outlook_note_view_definition(_view_name: &str) -> ViewDefinition {
    // MS-OXOCFG 2.2.6.1 stores visible view columns. Row identity columns are
    // served by live table projections when Outlook requests them.
    ViewDefinition {
        kind: ViewDefinitionKind::NoteList,
        columns: vec![
            view_column(PID_TAG_MESSAGE_FLAGS, 0x0C, 0x0000_2740, "Message Flags"),
            view_column(PID_TAG_MESSAGE_STATUS, 0x0C, 0x0000_2740, "Message Status"),
            view_column(PID_TAG_MESSAGE_CLASS_W, 0x12, 0x0000_270A, "Icon"),
            view_column(PID_TAG_SUBJECT_W, 0x18, 0x0000_2F00, "Subject"),
            view_column(
                PID_TAG_LAST_MODIFICATION_TIME,
                0x10,
                0x0000_2F40,
                "Modified",
            ),
            view_named_id_column(
                PID_LID_NOTE_COLOR_TAG,
                0x0C,
                0x0000_3F40,
                PSETID_NOTE_GUID,
                PID_LID_NOTE_COLOR,
                "Color",
            ),
        ],
        sort_column: 4,
        sort_descending: true,
    }
}

fn outlook_journal_view_definition(_view_name: &str) -> ViewDefinition {
    // MS-OXOCFG 2.2.6.1 stores visible view columns. Row identity columns are
    // served by live table projections when Outlook requests them.
    ViewDefinition {
        kind: ViewDefinitionKind::JournalList,
        columns: vec![
            view_column(PID_TAG_MESSAGE_FLAGS, 0x0C, 0x0000_2740, "Message Flags"),
            view_column(PID_TAG_MESSAGE_STATUS, 0x0C, 0x0000_2740, "Message Status"),
            view_column(PID_TAG_MESSAGE_CLASS_W, 0x12, 0x0000_270A, "Icon"),
            view_column(PID_TAG_SUBJECT_W, 0x18, 0x0000_2F00, "Subject"),
            view_named_id_column(
                PID_LID_LOG_START_TAG,
                0x10,
                0x0000_3F40,
                PSETID_LOG_GUID,
                PID_LID_LOG_START,
                "Start",
            ),
            view_named_id_column(
                PID_LID_LOG_DURATION_TAG,
                0x0C,
                0x0000_3F40,
                PSETID_LOG_GUID,
                PID_LID_LOG_DURATION,
                "Duration",
            ),
            view_named_id_column(
                PID_LID_LOG_TYPE_W_TAG,
                0x12,
                0x0000_3F00,
                PSETID_LOG_GUID,
                PID_LID_LOG_TYPE,
                "Type",
            ),
        ],
        sort_column: 4,
        sort_descending: true,
    }
}

fn view_column(property_tag: u32, width: u32, flags: u32, header: &'static str) -> ViewColumn {
    ViewColumn {
        property_tag,
        width,
        flags,
        kind: ViewColumnKind::Id,
        header,
    }
}

fn view_named_id_column(
    property_tag: u32,
    width: u32,
    flags: u32,
    guid: [u8; 16],
    id: u32,
    header: &'static str,
) -> ViewColumn {
    ViewColumn {
        property_tag,
        width,
        flags,
        kind: ViewColumnKind::NamedId { guid, id },
        header,
    }
}

fn view_named_string_column(
    property_tag: u32,
    width: u32,
    flags: u32,
    guid: [u8; 16],
    name: &'static str,
    header: &'static str,
) -> ViewColumn {
    ViewColumn {
        property_tag,
        width,
        flags,
        kind: ViewColumnKind::NamedString { guid, name },
        header,
    }
}

pub(in crate::mapi) fn view_descriptor_binary(definition: &ViewDefinition) -> Vec<u8> {
    let column_count = definition.columns.len() + 1;
    let mut value = Vec::with_capacity(60 + column_count * 36);
    value.extend_from_slice(&[0; 8]);
    value.extend_from_slice(&8u32.to_le_bytes());
    value.extend_from_slice(
        &(if definition.sort_descending {
            0x0000_0002u32
        } else {
            0
        })
        .to_le_bytes(),
    );
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&(column_count as u32).to_le_bytes());
    value.extend_from_slice(&((definition.sort_column + 1) as u32).to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&[0; 24]);

    write_view_column_packet(&mut value, 0x0004_0001, 7, 0x0000_0028, ViewColumnKind::Id);
    for column in &definition.columns {
        write_view_column_packet(
            &mut value,
            column.property_tag,
            column.width,
            column.flags,
            column.kind,
        );
    }

    value
}

fn write_view_column_packet(
    value: &mut Vec<u8>,
    property_tag: u32,
    width: u32,
    flags: u32,
    kind: ViewColumnKind,
) {
    let property_id = match kind {
        ViewColumnKind::NamedString { .. } => property_tag >> 16,
        ViewColumnKind::NamedId { id, .. } => id,
        ViewColumnKind::Id => property_tag >> 16,
    };
    value.extend_from_slice(&(property_tag_type(property_tag) as u16).to_le_bytes());
    value.extend_from_slice(&(property_id as u16).to_le_bytes());
    value.extend_from_slice(&width.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&flags.to_le_bytes());
    match kind {
        ViewColumnKind::Id => value.extend_from_slice(&[0; 12]),
        ViewColumnKind::NamedId { .. } | ViewColumnKind::NamedString { .. } => {
            value.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0x34, 0x01, 0x9A, 0x11]);
        }
    }
    match kind {
        ViewColumnKind::Id => {
            value.extend_from_slice(&0u32.to_le_bytes());
            value.extend_from_slice(&property_id.to_le_bytes());
        }
        ViewColumnKind::NamedId { guid, id } => {
            value.extend_from_slice(&0u32.to_le_bytes());
            value.extend_from_slice(&id.to_le_bytes());
            value.extend_from_slice(&guid);
        }
        ViewColumnKind::NamedString { guid, name } => {
            value.extend_from_slice(&1u32.to_le_bytes());
            value.extend_from_slice(&0x0022_A764u32.to_le_bytes());
            value.extend_from_slice(&guid);
            let mut buffer = Vec::new();
            for unit in name.encode_utf16() {
                buffer.extend_from_slice(&unit.to_le_bytes());
            }
            buffer.extend_from_slice(&0u16.to_le_bytes());
            value.extend_from_slice(&(buffer.len() as u32).to_le_bytes());
            value.extend_from_slice(&buffer);
        }
    }
}

pub(in crate::mapi) fn view_descriptor_strings(definition: &ViewDefinition) -> String {
    let mut strings = String::new();
    strings.push('\n');
    for column in &definition.columns {
        strings.push_str(column.header);
        strings.push('\n');
    }
    strings
}

pub(in crate::mapi) fn view_descriptor_strings_binary(definition: &ViewDefinition) -> Vec<u8> {
    let strings = view_descriptor_strings(definition);
    let mut bytes = Vec::with_capacity(strings.encode_utf16().count() * 2);
    for unit in strings.encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    bytes
}

pub(in crate::mapi) fn log_view_definition_diagnostics(
    folder_id: u64,
    view_id: u64,
    view_name: &str,
    definition: &ViewDefinition,
) {
    let descriptor_len = view_descriptor_binary(definition).len();
    let descriptor_strings_len = view_descriptor_strings(definition).encode_utf16().count() * 2;
    tracing::debug!(
        folder_id = format_args!("0x{folder_id:016x}"),
        view_message_id = format_args!("0x{view_id:016x}"),
        view_name,
        canonical_version = 8u32,
        descriptor_binary_len = descriptor_len,
        descriptor_strings_len,
        column_count = definition.columns.len(),
        sort_count = 1usize,
        static_default = true,
        persisted = false,
        view_kind = ?definition.kind,
        "mapi named view descriptor"
    );
}

pub(in crate::mapi) fn view_descriptor_property_tags(descriptor: &[u8]) -> Vec<u32> {
    view_descriptor_all_property_tags(descriptor)
        .into_iter()
        .skip(1)
        .collect()
}

pub(in crate::mapi) fn view_descriptor_all_property_tags(descriptor: &[u8]) -> Vec<u32> {
    let Some(column_count) = descriptor
        .get(20..24)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .and_then(|count| usize::try_from(count).ok())
    else {
        return Vec::new();
    };

    let mut offset = 60usize;
    let mut tags = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let Some(packet) = descriptor.get(offset..offset + 36) else {
            break;
        };
        let property_type = u16::from_le_bytes([packet[0], packet[1]]) as u32;
        let property_id = u16::from_le_bytes([packet[2], packet[3]]) as u32;
        let flags = u32::from_le_bytes([packet[12], packet[13], packet[14], packet[15]]);
        let kind = u32::from_le_bytes([packet[28], packet[29], packet[30], packet[31]]);
        tags.push((property_id << 16) | property_type);
        offset += 36;

        if flags & 0x0000_1000 == 0 {
            continue;
        }
        offset = offset.saturating_add(16);
        if kind == 1 {
            let Some(length_bytes) = descriptor.get(offset..offset + 4) else {
                break;
            };
            let buffer_length = u32::from_le_bytes(
                length_bytes
                    .try_into()
                    .expect("slice length checked for view descriptor buffer length"),
            ) as usize;
            offset = offset.saturating_add(4).saturating_add(buffer_length);
        }
    }

    tags
}
