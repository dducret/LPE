use super::*;

const PID_TAG_WLINK_SAVE_STAMP: u32 = 0x6847_0003;
const PID_TAG_WLINK_FLAGS: u32 = 0x684A_0003;
const PID_TAG_WLINK_FOLDER_TYPE: u32 = 0x684F_0102;
const PID_TAG_WLINK_SECTION: u32 = 0x6852_0003;
const PID_TAG_WLINK_CALENDAR_COLOR: u32 = 0x6853_0003;
const PID_TAG_WLINK_ADDRESS_BOOK_EID: u32 = 0x6854_0102;
const PID_TAG_WLINK_CLIENT_ID: u32 = 0x6890_0102;
const PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID: u32 = 0x6891_0102;
const PID_TAG_WLINK_RO_GROUP_TYPE: u32 = 0x6892_0003;

#[tokio::test]
async fn mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload(
) -> anyhow::Result<()> {
    // [MS-OXOCFG] section 2.2.9.7 defines PidTagWlinkOrdinal as a
    // variable-length PtypBinary value sorted lexicographically. Sections
    // 2.2.9.15 through 2.2.9.19 and 3.1.4.10.2 define optional client-owned
    // Calendar shortcut properties that have to survive the FAI save/reload.
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let service = ExchangeService::new(storage.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await?;
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect))?,
    );
    let logon = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&mapi_private_logon_rops("alice"), &[u32::MAX])),
        )
        .await?;
    assert_eq!(logon.status(), StatusCode::OK);
    renew_mapi_request_id(&mut execute_headers);

    let calendar_ordinal = vec![0x80, 0x10, 0x20, 0x30, 0x40];
    let contacts_ordinal = vec![0x80, 0x11];
    let principal = AccountPrincipal {
        tenant_id: Uuid::parse_str("10000000-0000-0000-0000-000000000001")?,
        account_id: fixture.account_id,
        email: "alice@example.test".to_string(),
        display_name: "Alice Calendar".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    // Use the real MS-OXOABK PermanentEntryID and MS-OXCDATA Store Object
    // EntryID builders; these values remain opaque client-written bytes after save.
    let address_book_eid = crate::mapi::properties::mailbox_owner_entry_id(&principal);
    let address_book_store_eid =
        crate::mapi::identity::principal_mailbox_store_entry_id(&principal);
    let client_id = vec![
        0xC3, 0x10, 0x21, 0x32, 0x43, 0x54, 0x65, 0x76, 0x87, 0x98, 0xA9, 0xBA, 0xCB, 0xDC, 0xED,
        0xF3,
    ];
    let group_id = Uuid::from_bytes([0x5A; 16]);
    let calendar_folder_type = [
        0x02, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let contacts_folder_type = [
        0x01, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        fixture.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .ok_or_else(|| anyhow::anyhow!("Calendar EntryID could not be encoded"))?;
    let contacts_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        fixture.account_id,
        crate::mapi::identity::CONTACTS_FOLDER_ID,
    )
    .ok_or_else(|| anyhow::anyhow!("Contacts EntryID could not be encoded"))?;

    let mut calendar_values = Vec::new();
    append_mapi_utf16_property(
        &mut calendar_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.WunderBar.Link",
    );
    append_mapi_utf16_property(
        &mut calendar_values,
        PID_TAG_SUBJECT_W,
        "A variable ordinal Calendar",
    );
    append_mapi_binary_property(
        &mut calendar_values,
        PID_TAG_WLINK_ENTRY_ID,
        &calendar_entry_id,
    );
    append_mapi_i32_property(&mut calendar_values, PID_TAG_WLINK_TYPE, 0);
    append_mapi_i32_property(&mut calendar_values, PID_TAG_WLINK_FLAGS, 0x0000_1000);
    append_mapi_i32_property(&mut calendar_values, PID_TAG_WLINK_SAVE_STAMP, 0x1234_5678);
    append_mapi_i32_property(&mut calendar_values, PID_TAG_WLINK_SECTION, 3);
    append_mapi_binary_property(
        &mut calendar_values,
        PID_TAG_WLINK_ORDINAL,
        &calendar_ordinal,
    );
    append_mapi_binary_property(
        &mut calendar_values,
        PID_TAG_WLINK_GROUP_CLSID,
        group_id.as_bytes(),
    );
    append_mapi_utf16_property(
        &mut calendar_values,
        PID_TAG_WLINK_GROUP_NAME_W,
        "My Calendars",
    );
    append_mapi_i32_property(&mut calendar_values, PID_TAG_WLINK_CALENDAR_COLOR, 7);
    append_mapi_binary_property(
        &mut calendar_values,
        PID_TAG_WLINK_ADDRESS_BOOK_EID,
        &address_book_eid,
    );
    append_mapi_binary_property(
        &mut calendar_values,
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
        &address_book_store_eid,
    );
    append_mapi_binary_property(&mut calendar_values, PID_TAG_WLINK_CLIENT_ID, &client_id);
    append_mapi_i32_property(&mut calendar_values, PID_TAG_WLINK_RO_GROUP_TYPE, 3);
    append_mapi_binary_property(
        &mut calendar_values,
        PID_TAG_WLINK_FOLDER_TYPE,
        &calendar_folder_type,
    );

    let mut contacts_values = Vec::new();
    append_mapi_utf16_property(
        &mut contacts_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Microsoft.WunderBar.Link",
    );
    append_mapi_utf16_property(
        &mut contacts_values,
        PID_TAG_SUBJECT_W,
        "B Contacts ordinal",
    );
    append_mapi_binary_property(
        &mut contacts_values,
        PID_TAG_WLINK_ENTRY_ID,
        &contacts_entry_id,
    );
    append_mapi_i32_property(&mut contacts_values, PID_TAG_WLINK_TYPE, 0);
    append_mapi_i32_property(&mut contacts_values, PID_TAG_WLINK_FLAGS, 0);
    append_mapi_i32_property(&mut contacts_values, PID_TAG_WLINK_SAVE_STAMP, 0x1234_5678);
    append_mapi_i32_property(&mut contacts_values, PID_TAG_WLINK_SECTION, 4);
    append_mapi_binary_property(
        &mut contacts_values,
        PID_TAG_WLINK_ORDINAL,
        &contacts_ordinal,
    );
    append_mapi_binary_property(
        &mut contacts_values,
        PID_TAG_WLINK_GROUP_CLSID,
        group_id.as_bytes(),
    );
    append_mapi_utf16_property(
        &mut contacts_values,
        PID_TAG_WLINK_GROUP_NAME_W,
        "My Contacts",
    );
    append_mapi_binary_property(
        &mut contacts_values,
        PID_TAG_WLINK_FOLDER_TYPE,
        &contacts_folder_type,
    );

    let mut create_rops = Vec::new();
    append_rop_create_associated_message(
        &mut create_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_set_properties(&mut create_rops, 1, 16, &calendar_values);
    append_rop_save_changes_message_with_flags(&mut create_rops, 0, 1, 0x01);
    append_rop_create_associated_message(
        &mut create_rops,
        0,
        2,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    append_rop_set_properties(&mut create_rops, 2, 11, &contacts_values);
    append_rop_save_changes_message_with_flags(&mut create_rops, 0, 2, 0x01);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&create_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await?;
    let create_response = response_rops_from_execute_response(response).await;
    assert_eq!(
        create_response
            .windows(6)
            .filter(|window| *window == [0x0C, 0x00, 0, 0, 0, 0])
            .count(),
        2,
        "both WLink SaveChangesMessage ROPs must succeed: {create_response:02x?}"
    );

    let persisted = sqlx::query(
        r#"
        SELECT ordinal, calendar_color, address_book_entry_id,
               address_book_store_entry_id, client_id, ro_group_type
        FROM mapi_navigation_shortcuts
        WHERE account_id = $1 AND subject = 'A variable ordinal Calendar'
        "#,
    )
    .bind(fixture.account_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(persisted.get::<Vec<u8>, _>("ordinal"), calendar_ordinal);
    assert_eq!(persisted.get::<Option<i32>, _>("calendar_color"), Some(7));
    assert_eq!(
        persisted.get::<Option<Vec<u8>>, _>("address_book_entry_id"),
        Some(address_book_eid.clone())
    );
    assert_eq!(
        persisted.get::<Option<Vec<u8>>, _>("address_book_store_entry_id"),
        Some(address_book_store_eid.clone())
    );
    assert_eq!(
        persisted.get::<Option<Vec<u8>>, _>("client_id"),
        Some(client_id.clone())
    );
    assert_eq!(persisted.get::<Option<i32>, _>("ro_group_type"), Some(3));

    drop(service);
    let reloaded_service = ExchangeService::new(storage.clone());
    let reconnect = reloaded_service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await?;
    let mut reload_headers = mapi_headers("Execute");
    reload_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&reconnect))?,
    );
    let logon = reloaded_service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &reload_headers,
            &execute_body(&rop_buffer(&mapi_private_logon_rops("alice"), &[u32::MAX])),
        )
        .await?;
    assert_eq!(logon.status(), StatusCode::OK);
    renew_mapi_request_id(&mut reload_headers);

    let columns = [
        PID_TAG_SUBJECT_W,
        PID_TAG_WLINK_ORDINAL,
        PID_TAG_WLINK_CALENDAR_COLOR,
        PID_TAG_WLINK_ADDRESS_BOOK_EID,
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
        PID_TAG_WLINK_CLIENT_ID,
        PID_TAG_WLINK_RO_GROUP_TYPE,
    ];
    let mut table_rops = Vec::new();
    append_rop_open_folder(
        &mut table_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    table_rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x02, // RopGetContentsTable, Associated.
        0x12, 0x00, 0x02, 0x00, // RopSetColumns.
    ]);
    table_rops.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for tag in columns {
        table_rops.extend_from_slice(&tag.to_le_bytes());
    }
    table_rops.extend_from_slice(&[0x13, 0x00, 0x02, 0x00]); // RopSortTable.
    table_rops.extend_from_slice(&1u16.to_le_bytes());
    table_rops.extend_from_slice(&0u16.to_le_bytes());
    table_rops.extend_from_slice(&0u16.to_le_bytes());
    table_rops.extend_from_slice(&PID_TAG_WLINK_ORDINAL.to_le_bytes());
    table_rops.push(0); // TABLE_SORT_ASCEND.
    table_rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]); // RopQueryRows.
    table_rops.extend_from_slice(&50u16.to_le_bytes());
    let response = reloaded_service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &reload_headers,
            &execute_body(&rop_buffer(&table_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await?;
    let table_response = response_rops_from_execute_response(response).await;
    let calendar_subject = utf16z("A variable ordinal Calendar");
    let contacts_subject = utf16z("B Contacts ordinal");
    let calendar_position = table_response
        .windows(calendar_subject.len())
        .position(|window| window == calendar_subject)
        .ok_or_else(|| anyhow::anyhow!("reloaded Common Views table omitted Calendar WLink"))?;
    let contacts_position = table_response
        .windows(contacts_subject.len())
        .position(|window| window == contacts_subject)
        .ok_or_else(|| anyhow::anyhow!("reloaded Common Views table omitted Contacts WLink"))?;
    assert!(
        calendar_position < contacts_position,
        "PidTagWlinkOrdinal must sort as the complete lexicographic byte string"
    );
    for binary in [
        calendar_ordinal.as_slice(),
        address_book_eid.as_slice(),
        address_book_store_eid.as_slice(),
        client_id.as_slice(),
    ] {
        assert!(
            contains_bytes(&table_response, binary),
            "reloaded Common Views table omitted persisted WLink bytes {binary:02x?}"
        );
    }
    assert!(contains_bytes(&table_response, &7i32.to_le_bytes()));
    assert!(contains_bytes(&table_response, &3i32.to_le_bytes()));

    let sync_response = content_sync_response_rops_for_store_with_flags(
        storage.clone(),
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        &[],
        0x0010,
    )
    .await;
    let transfer =
        strict_content_sync_transfer_from_response(&sync_response).map_err(anyhow::Error::msg)?;
    let calendar_change = transfer
        .message_changes
        .iter()
        .find(|message| message.subject == "A variable ordinal Calendar")
        .ok_or_else(|| anyhow::anyhow!("reloaded ICS omitted Calendar WLink"))?;
    for tag in [
        PID_TAG_WLINK_ORDINAL,
        PID_TAG_WLINK_CALENDAR_COLOR,
        PID_TAG_WLINK_ADDRESS_BOOK_EID,
        PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
        PID_TAG_WLINK_CLIENT_ID,
        PID_TAG_WLINK_RO_GROUP_TYPE,
    ] {
        assert!(
            calendar_change.body_tags.contains(&tag),
            "reloaded ICS omitted WLink property 0x{tag:08x}"
        );
    }
    let chunks = mapi_fast_transfer_chunks(&sync_response);
    assert_eq!(chunks.len(), 1);
    let fast_transfer = &chunks[0].1;
    for expected in [
        mapi_binary_property(PID_TAG_WLINK_ORDINAL, &calendar_ordinal),
        mapi_binary_property(PID_TAG_WLINK_ADDRESS_BOOK_EID, &address_book_eid),
        mapi_binary_property(
            PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
            &address_book_store_eid,
        ),
        mapi_binary_property(PID_TAG_WLINK_CLIENT_ID, &client_id),
    ] {
        assert!(
            contains_bytes(fast_transfer, &expected),
            "reloaded ICS omitted exact WLink binary property {expected:02x?}"
        );
    }
    for (tag, value) in [
        (PID_TAG_WLINK_CALENDAR_COLOR, 7i32),
        (PID_TAG_WLINK_RO_GROUP_TYPE, 3i32),
    ] {
        let mut expected = tag.to_le_bytes().to_vec();
        expected.extend_from_slice(&value.to_le_bytes());
        assert!(contains_bytes(fast_transfer, &expected));
    }

    fixture.cleanup().await?;
    Ok(())
}
