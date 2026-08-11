use super::*;

#[test]
fn scoped_codec_accepts_only_advertised_legacy_folder_entry_ids() {
    let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let durable_calendar_id = mapi_store_id(FIRST_DYNAMIC_GLOBAL_COUNTER + 1);
    let codec = MapiIdentityCodec {
        replica_guid: STORE_REPLICA_GUID,
        logical_to_actual: HashMap::from([(CALENDAR_FOLDER_ID, durable_calendar_id)]),
        actual_to_logical: HashMap::from([(durable_calendar_id, CALENDAR_FOLDER_ID)]),
        special_canonical_ids: HashSet::new(),
    };

    let legacy_calendar =
        raw_folder_entry_id_from_object_id(mailbox_guid, CALENDAR_FOLDER_ID).unwrap();
    assert_eq!(
        codec.object_id_from_folder_entry_id(&legacy_calendar),
        Some(CALENDAR_FOLDER_ID)
    );

    let unadvertised =
        raw_folder_entry_id_from_object_id(mailbox_guid, CONVERSATION_HISTORY_FOLDER_ID).unwrap();
    assert_eq!(codec.object_id_from_folder_entry_id(&unadvertised), None);
}
