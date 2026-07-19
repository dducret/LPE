use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MapiNamedProperty {
    pub(crate) guid: [u8; 16],
    pub(crate) kind: MapiNamedPropertyKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MapiNamedPropertyKind {
    Lid(u32),
    Name(String),
}

pub(crate) fn well_known_named_property_id(property: &MapiNamedProperty) -> Option<u16> {
    if let Some(property_id) = well_known_named_properties()
        .into_iter()
        .find_map(|(property_id, candidate)| (candidate == *property).then_some(property_id))
    {
        return Some(property_id);
    }

    let property_id = well_known_lid_family_property_id(property)?;
    (well_known_named_property_for_id(property_id).as_ref() == Some(property))
        .then_some(property_id)
}

pub(in crate::mapi) fn well_known_named_property_for_id(
    property_id: u16,
) -> Option<MapiNamedProperty> {
    explicit_well_known_named_property_for_id(property_id)
        .or_else(|| well_known_lid_family_property_for_id(property_id))
}

fn explicit_well_known_named_property_for_id(property_id: u16) -> Option<MapiNamedProperty> {
    well_known_named_properties()
        .into_iter()
        .find_map(|(candidate_id, property)| (candidate_id == property_id).then_some(property))
}

pub(crate) fn fast_transfer_named_property_for_message_tag(
    _message_class: &str,
    property_tag: u32,
) -> Option<MapiNamedProperty> {
    let property_id = MapiPropertyTag::new(property_tag).property_id();
    if property_id < 0x8000 {
        return None;
    }

    // [MS-OXCFXICS] section 2.2.4.1 requires the same mailbox-level named
    // property definition used by RopGetNamesFromPropertyIds.
    well_known_named_property_for_id(property_id)
}

pub(crate) fn is_reserved_named_property_id(property_id: u16) -> bool {
    well_known_named_property_for_id(property_id).is_some()
}

pub(in crate::mapi) fn is_calendar_named_property(property: &MapiNamedProperty) -> bool {
    matches!(
        (&property.guid, &property.kind),
        (guid, MapiNamedPropertyKind::Lid(lid))
            if (*guid == PSETID_APPOINTMENT_GUID && (0x8200..=0x82ff).contains(lid))
                || (*guid == PSETID_COMMON_GUID && (0x8500..=0x85ff).contains(lid))
    )
}

fn well_known_lid_family_property_id(property: &MapiNamedProperty) -> Option<u16> {
    let MapiNamedPropertyKind::Lid(lid) = property.kind else {
        return None;
    };
    let property_id = u16::try_from(lid).ok()?;

    match property.guid {
        PSETID_ADDRESS_GUID if (0x8000..=0x80ff).contains(&lid) => Some(property_id),
        PSETID_APPOINTMENT_GUID if (0x8200..=0x82ff).contains(&lid) => Some(property_id),
        PSETID_COMMON_GUID
            if (0x8500..=0x85ff).contains(&lid) || matches!(lid, 0x8219 | 0x822c | 0x822d) =>
        {
            Some(property_id)
        }
        PSETID_LOG_GUID if (0x8700..=0x87ff).contains(&lid) => Some(property_id),
        PSETID_SHARING_GUID if (0x8a00..=0x8aff).contains(&lid) => Some(property_id),
        PSETID_NOTE_GUID if (0x8b00..=0x8bff).contains(&lid) => Some(property_id),
        _ => None,
    }
}

fn well_known_lid_family_property_for_id(property_id: u16) -> Option<MapiNamedProperty> {
    match property_id {
        0x8000..=0x80ff => Some(MapiNamedProperty {
            guid: PSETID_ADDRESS_GUID,
            kind: MapiNamedPropertyKind::Lid(u32::from(property_id)),
        }),
        0x8219 | 0x822c | 0x822d => Some(MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Lid(u32::from(property_id)),
        }),
        0x8200..=0x82ff => Some(MapiNamedProperty {
            guid: PSETID_APPOINTMENT_GUID,
            kind: MapiNamedPropertyKind::Lid(u32::from(property_id)),
        }),
        0x8500..=0x85ff => Some(MapiNamedProperty {
            guid: PSETID_COMMON_GUID,
            kind: MapiNamedPropertyKind::Lid(u32::from(property_id)),
        }),
        0x8700..=0x87ff => Some(MapiNamedProperty {
            guid: PSETID_LOG_GUID,
            kind: MapiNamedPropertyKind::Lid(u32::from(property_id)),
        }),
        0x8a00..=0x8aff => Some(MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Lid(u32::from(property_id)),
        }),
        0x8b00..=0x8bff => Some(MapiNamedProperty {
            guid: PSETID_NOTE_GUID,
            kind: MapiNamedPropertyKind::Lid(u32::from(property_id)),
        }),
        _ => None,
    }
}

pub(super) fn well_known_named_properties() -> Vec<(u16, MapiNamedProperty)> {
    [
        (
            PID_LID_GLOBAL_OBJECT_ID_NAMED_ID,
            PID_LID_GLOBAL_OBJECT_ID,
            PSETID_MEETING_GUID,
        ),
        (
            PID_LID_CLEAN_GLOBAL_OBJECT_ID_NAMED_ID,
            PID_LID_CLEAN_GLOBAL_OBJECT_ID,
            PSETID_MEETING_GUID,
        ),
        (
            0x8017,
            PID_LID_OUTLOOK_APPOINTMENT_8F07,
            OUTLOOK_VIEW_8F07_GUID,
        ),
    ]
    .into_iter()
    .chain(
        [
            (PID_LID_COMMON_START, PSETID_COMMON_GUID),
            (PID_LID_COMMON_END, PSETID_COMMON_GUID),
            (PID_LID_SIDE_EFFECTS, PSETID_COMMON_GUID),
            (PID_LID_OUTLOOK_COMMON_8514, PSETID_COMMON_GUID),
            (PID_LID_OUTLOOK_COMMON_8578, PSETID_COMMON_GUID),
            (PID_LID_OUTLOOK_COMMON_85B1, PSETID_COMMON_GUID),
            (PID_LID_OUTLOOK_COMMON_85EF, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_TIME, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_SET, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_DELTA, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_OVERRIDE, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_PLAY_SOUND, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_FILE_PARAMETER, PSETID_COMMON_GUID),
            (PID_LID_FLAG_REQUEST, PSETID_COMMON_GUID),
            (PID_LID_REMINDER_SIGNAL_TIME, PSETID_COMMON_GUID),
            (PID_LID_PERCENT_COMPLETE, PSETID_TASK_GUID),
            (PID_LID_TASK_START_DATE, PSETID_TASK_GUID),
            (PID_LID_TASK_DUE_DATE, PSETID_TASK_GUID),
            (PID_LID_BUSY_STATUS, PSETID_APPOINTMENT_GUID),
            (PID_LID_LOCATION, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_START_WHOLE, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_END_WHOLE, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_DURATION, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_COLOR, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_SUB_TYPE, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_RECUR, PSETID_APPOINTMENT_GUID),
            (PID_LID_APPOINTMENT_STATE_FLAGS, PSETID_APPOINTMENT_GUID),
            (PID_LID_RECURRING, PSETID_APPOINTMENT_GUID),
            (PID_LID_ALL_ATTENDEES_STRING, PSETID_APPOINTMENT_GUID),
            (PID_LID_TO_ATTENDEES_STRING, PSETID_APPOINTMENT_GUID),
            (PID_LID_CC_ATTENDEES_STRING, PSETID_APPOINTMENT_GUID),
            (PID_LID_TIME_ZONE_STRUCT, PSETID_APPOINTMENT_GUID),
            (PID_LID_TIME_ZONE_DESCRIPTION, PSETID_APPOINTMENT_GUID),
            (
                PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY,
                PSETID_APPOINTMENT_GUID,
            ),
            (
                PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY,
                PSETID_APPOINTMENT_GUID,
            ),
            (PID_LID_OUTLOOK_APPOINTMENT_8F07, PSETID_COMMON_GUID),
            (PID_LID_EMAIL1_DISPLAY_NAME, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL1_ADDRESS_TYPE, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL1_EMAIL_ADDRESS, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL1_ORIGINAL_DISPLAY_NAME, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL1_ORIGINAL_ENTRY_ID, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL2_DISPLAY_NAME, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL2_ADDRESS_TYPE, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL2_EMAIL_ADDRESS, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL2_ORIGINAL_DISPLAY_NAME, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL2_ORIGINAL_ENTRY_ID, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL3_DISPLAY_NAME, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL3_ADDRESS_TYPE, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL3_EMAIL_ADDRESS, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL3_ORIGINAL_DISPLAY_NAME, PSETID_ADDRESS_GUID),
            (PID_LID_EMAIL3_ORIGINAL_ENTRY_ID, PSETID_ADDRESS_GUID),
            (PID_LID_OUTLOOK_CONTACT_SOURCE_80E0, PSETID_ADDRESS_GUID),
            (PID_LID_OUTLOOK_CONTACT_SOURCE_80E2, PSETID_ADDRESS_GUID),
            (PID_LID_OUTLOOK_CONTACT_SOURCE_80E3, PSETID_ADDRESS_GUID),
            (PID_LID_OUTLOOK_CONTACT_SOURCE_80E5, PSETID_ADDRESS_GUID),
            (PID_LID_OUTLOOK_CONTACT_SOURCE_80E6, PSETID_ADDRESS_GUID),
            (PID_LID_OUTLOOK_CONTACT_SOURCE_80E8, PSETID_ADDRESS_GUID),
            (
                PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80E1,
                PS_PUBLIC_STRINGS_GUID,
            ),
            (
                PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EA,
                PS_PUBLIC_STRINGS_GUID,
            ),
            (
                PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EC,
                PS_PUBLIC_STRINGS_GUID,
            ),
            (
                PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80ED,
                PS_PUBLIC_STRINGS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_ADDRESS_TYPE,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_DISPLAY_NAME,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_ADDRESS_TYPE,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_DISPLAY_NAME,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_EMAIL_ADDRESS,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_ADDRESS_TYPE,
                PSETID_ADDRESS_GUID,
            ),
            (
                PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_EMAIL_ADDRESS,
                PSETID_ADDRESS_GUID,
            ),
            (PID_LID_COMPANIES, PSETID_COMMON_GUID),
            (PID_LID_CONTACTS, PSETID_COMMON_GUID),
            (PID_LID_CONTACT_LINK_SEARCH_KEY, PSETID_COMMON_GUID),
            (PID_LID_CONTACT_LINK_ENTRY, PSETID_COMMON_GUID),
            (PID_LID_CONTACT_LINK_NAME, PSETID_COMMON_GUID),
            (
                PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID,
                PSETID_COMMON_GUID,
            ),
            (
                PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID,
                PSETID_COMMON_GUID,
            ),
            (
                PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME,
                PSETID_COMMON_GUID,
            ),
            (PID_LID_CONVERSATION_PROCESSED, PSETID_COMMON_GUID),
            (
                PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME,
                PSETID_COMMON_GUID,
            ),
            (PID_LID_CONVERSATION_ACTION_VERSION, PSETID_COMMON_GUID),
            (PID_LID_LOG_TYPE, PSETID_LOG_GUID),
            (PID_LID_LOG_START, PSETID_LOG_GUID),
            (PID_LID_LOG_DURATION, PSETID_LOG_GUID),
            (PID_LID_LOG_END, PSETID_LOG_GUID),
            (PID_LID_LOG_FLAGS, PSETID_LOG_GUID),
            (PID_LID_LOG_TYPE_DESC, PSETID_LOG_GUID),
            (PID_LID_NOTE_COLOR, PSETID_NOTE_GUID),
            (PID_LID_NOTE_HEIGHT, PSETID_NOTE_GUID),
            (PID_LID_NOTE_WIDTH, PSETID_NOTE_GUID),
            (PID_LID_NOTE_X, PSETID_NOTE_GUID),
            (PID_LID_NOTE_Y, PSETID_NOTE_GUID),
            (PID_LID_POST_RSS_CHANNEL_LINK, PSETID_POST_RSS_GUID),
            (PID_LID_POST_RSS_ITEM_LINK, PSETID_POST_RSS_GUID),
            (PID_LID_POST_RSS_ITEM_HASH, PSETID_POST_RSS_GUID),
            (PID_LID_POST_RSS_ITEM_GUID, PSETID_POST_RSS_GUID),
            (PID_LID_POST_RSS_CHANNEL, PSETID_POST_RSS_GUID),
            (PID_LID_POST_RSS_ITEM_XML, PSETID_POST_RSS_GUID),
            (PID_LID_POST_RSS_SUBSCRIPTION, PSETID_POST_RSS_GUID),
            (PID_LID_OUTLOOK_SHARING_PROVIDER_GUID, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_REMOTE_NAME, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_REMOTE_UID, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_LOCAL_TYPE, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_CAPABILITIES, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8AA6, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A70, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A71, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A72, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A73, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A74, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A75, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A76, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A77, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A78, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A7E, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A80, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A88, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A8B, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A8D, PSETID_SHARING_GUID),
            (PID_LID_OUTLOOK_SHARING_8A8E, PSETID_SHARING_GUID),
        ]
        .into_iter()
        .map(|(lid, guid)| (lid as u16, lid, guid)),
    )
    .map(|(property_id, lid, guid)| {
        (
            property_id,
            MapiNamedProperty {
                guid,
                kind: MapiNamedPropertyKind::Lid(lid),
            },
        )
    })
    .collect::<Vec<_>>()
    .into_iter()
    .chain(std::iter::once((
        MapiPropertyTag::new(PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG)
            .property_id(),
        MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Name(
                "SharingCalendarGroupEntryAssociatedLocalFolderId".to_string(),
            ),
        },
    )))
    .chain(std::iter::once((
        MapiPropertyTag::new(PID_NAME_SHARING_SEND_AS_STATE_TAG).property_id(),
        MapiNamedProperty {
            guid: PSETID_SHARING_GUID,
            kind: MapiNamedPropertyKind::Name("SharingSendAsState".to_string()),
        },
    )))
    .chain(std::iter::once((
        0x9000,
        MapiNamedProperty {
            guid: PS_PUBLIC_STRINGS_GUID,
            kind: MapiNamedPropertyKind::Name("Keywords".to_string()),
        },
    )))
    .chain(std::iter::once((
        MapiPropertyTag::new(PID_NAME_OSC_CONTACT_SOURCES_TAG).property_id(),
        MapiNamedProperty {
            guid: PS_PUBLIC_STRINGS_GUID,
            kind: MapiNamedPropertyKind::Name("OscContactSources".to_string()),
        },
    )))
    .chain([
        (
            MapiPropertyTag::new(PID_NAME_CONTENT_CLASS_W_TAG).property_id(),
            MapiNamedProperty {
                guid: PS_INTERNET_HEADERS_GUID,
                kind: MapiNamedPropertyKind::Name("content-class".to_string()),
            },
        ),
        (
            MapiPropertyTag::new(PID_NAME_CONTENT_TYPE_W_TAG).property_id(),
            MapiNamedProperty {
                guid: PS_INTERNET_HEADERS_GUID,
                kind: MapiNamedPropertyKind::Name("content-type".to_string()),
            },
        ),
    ])
    .collect()
}
