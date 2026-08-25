use super::*;

async fn submit_new_mapi_message_with_identities(
    subject: &str,
    sender: (&str, &str),
    sent_representing: (&str, &str),
) -> (SubmitMessageInput, JmapEmail, lpe_storage::AuditEntryInput) {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox("22222222-2222-2222-2222-222222222222", "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let submitted_message_audits = store.submitted_message_audits.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, subject);
    append_mapi_utf16_property(&mut property_values, 0x0C1A_001F, sender.0);
    append_mapi_utf16_property(&mut property_values, 0x0C1F_001F, sender.1);
    append_mapi_utf16_property(&mut property_values, 0x0042_001F, sent_representing.0);
    append_mapi_utf16_property(&mut property_values, 0x0065_001F, sent_representing.1);
    let recipient = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 5, &property_values);
    append_rop_modify_recipients(&mut rops, 2, &[(1, 0x01, recipient.as_slice())]);
    append_rop_submit_message(&mut rops, 2);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x32, 0x02, 0, 0, 0, 0]));

    let input = submitted_messages.lock().unwrap().pop().unwrap();
    let audit = submitted_message_audits.lock().unwrap().pop().unwrap();
    let sent = emails
        .lock()
        .unwrap()
        .iter()
        .find(|email| email.mailbox_role == "sent" && email.subject == subject)
        .cloned()
        .unwrap();
    (input, sent, audit)
}

#[tokio::test]
async fn mapi_over_http_submit_new_message_maps_normal_sender_identity() {
    let (input, sent, audit) = submit_new_mapi_message_with_identities(
        "Normal MAPI identity",
        ("Alice Sender", "alice@example.test"),
        ("Alice From", "alice@example.test"),
    )
    .await;

    assert_eq!(input.account_id, FakeStore::account().account_id);
    assert_eq!(
        input.submitted_by_account_id,
        FakeStore::account().account_id
    );
    assert_eq!(input.from_address, "alice@example.test");
    assert_eq!(input.from_display.as_deref(), Some("Alice From"));
    assert_eq!(input.sender_address, None);
    assert_eq!(input.sender_display, None);
    assert_eq!(sent.from_address, input.from_address);
    assert_eq!(sent.sender_address, None);
    assert_eq!(sent.submitted_by_account_id, input.submitted_by_account_id);
    assert_eq!(audit.actor, "alice@example.test");
}

#[tokio::test]
async fn mapi_over_http_submit_new_message_maps_send_as_identity() {
    let (input, sent, audit) = submit_new_mapi_message_with_identities(
        "Send as MAPI identity",
        ("Shared Sender", "shared@example.test"),
        ("Shared From", "shared@example.test"),
    )
    .await;

    assert_eq!(input.from_address, "shared@example.test");
    assert_eq!(input.from_display.as_deref(), Some("Shared From"));
    assert_eq!(input.sender_address, None);
    assert_eq!(input.sender_display, None);
    assert_eq!(sent.from_address, input.from_address);
    assert_eq!(sent.sender_address, None);
    assert_eq!(sent.submitted_by_account_id, input.submitted_by_account_id);
    assert_eq!(audit.actor, "alice@example.test");
}

#[tokio::test]
async fn mapi_over_http_submit_new_message_maps_send_on_behalf_identity() {
    let (input, sent, audit) = submit_new_mapi_message_with_identities(
        "Send on behalf MAPI identity",
        ("Alice Sender", "alice@example.test"),
        ("Shared From", "shared@example.test"),
    )
    .await;

    assert_eq!(input.from_address, "shared@example.test");
    assert_eq!(input.from_display.as_deref(), Some("Shared From"));
    assert_eq!(input.sender_address.as_deref(), Some("alice@example.test"));
    assert_eq!(input.sender_display.as_deref(), Some("Alice Sender"));
    assert_eq!(sent.from_address, input.from_address);
    assert_eq!(sent.sender_address, input.sender_address);
    assert_eq!(sent.sender_display, input.sender_display);
    assert_eq!(sent.submitted_by_account_id, input.submitted_by_account_id);
    assert_eq!(audit.actor, "alice@example.test");
}

#[tokio::test]
async fn mapi_over_http_microsoft_subrestriction_matches_message_recipients() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 3;
    let mut first = FakeStore::email(
        "96969696-9696-9696-9696-969696969696",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Subrestriction first",
    );
    first.to = vec![JmapEmailAddress {
        address: "bob@example.test".to_string(),
        display_name: Some("Bob".to_string()),
    }];
    let mut target = FakeStore::email(
        "97979797-9797-9797-9797-979797979797",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Needle recipient target",
    );
    target.to = vec![JmapEmailAddress {
        address: "alice@example.test".to_string(),
        display_name: Some("Alice".to_string()),
    }];
    target.cc = vec![JmapEmailAddress {
        address: "bob@example.test".to_string(),
        display_name: Some("Bob".to_string()),
    }];
    let mut last = FakeStore::email(
        "98989898-9898-9898-9898-989898989898",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Subrestriction last",
    );
    last.to = vec![JmapEmailAddress {
        address: "charlie@example.test".to_string(),
        display_name: Some("Charlie".to_string()),
    }];
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![first, target, last])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut smtp_match = vec![0x04, 0x04]; // RES_PROPERTY, RELOP_EQ.
    smtp_match.extend_from_slice(&0x39FE_001Fu32.to_le_bytes()); // PidTagSmtpAddress.
    append_mapi_utf16_property(&mut smtp_match, 0x39FE_001F, "bob@example.test");
    let mut order_match = vec![0x04, 0x04]; // RES_PROPERTY, RELOP_EQ.
    order_match.extend_from_slice(&0x5FDF_0003u32.to_le_bytes()); // PidTagRecipientOrder.
    append_mapi_i32_property(&mut order_match, 0x5FDF_0003, 1);
    let mut flags_match = vec![0x04, 0x04]; // RES_PROPERTY, RELOP_EQ.
    flags_match.extend_from_slice(&0x5FFD_0003u32.to_le_bytes()); // PidTagRecipientFlags.
    append_mapi_i32_property(&mut flags_match, 0x5FFD_0003, 1);
    let mut track_status_match = vec![0x04, 0x04]; // RES_PROPERTY, RELOP_EQ.
    track_status_match.extend_from_slice(&0x5FFF_0003u32.to_le_bytes()); // PidTagRecipientTrackStatus.
    append_mapi_i32_property(&mut track_status_match, 0x5FFF_0003, 0);
    let mut recipient_match = vec![0x00]; // RES_AND.
    recipient_match.extend_from_slice(&4u16.to_le_bytes());
    recipient_match.extend_from_slice(&smtp_match);
    recipient_match.extend_from_slice(&order_match);
    recipient_match.extend_from_slice(&flags_match);
    recipient_match.extend_from_slice(&track_status_match);

    let mut restriction = vec![0x09]; // RES_SUBRESTRICTION.
    restriction.extend_from_slice(&0x0E12_000Du32.to_le_bytes()); // PidTagMessageRecipients.
    restriction.extend_from_slice(&recipient_match);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x4F, 0x00, 0x02, 0x00]); // RopFindRow
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.push(0);
    rops.extend_from_slice(&0u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Subrestriction first")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Needle recipient target")
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Subrestriction last")
    ));
}

#[tokio::test]
async fn mapi_over_http_microsoft_modify_recipients_accepts_type_flags_and_rejects_invalid_type() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let (mut execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let flagged_cc_row = mapi_recipient_row("Flagged Cc", "flagged-cc@example.test", 0x12);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x0E, 0x00, 0x02, // RopModifyRecipients
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&1u32.to_le_bytes());
    rops.push(0x12);
    rops.extend_from_slice(&(flagged_cc_row.len() as u16).to_le_bytes());
    rops.extend_from_slice(&flagged_cc_row);
    append_rop_save_changes_message(&mut rops, 1, 2);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    {
        let recorded = imported_emails.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert!(recorded[0].to.is_empty());
        assert_eq!(recorded[0].cc.len(), 1);
        assert_eq!(recorded[0].cc[0].address, "flagged-cc@example.test");
    }

    renew_mapi_request_id(&mut execute_headers);
    let invalid_row = mapi_recipient_row("Invalid", "invalid@example.test", 0x04);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x0E, 0x00, 0x02, // RopModifyRecipients
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&1u32.to_le_bytes());
    rops.push(0x04);
    rops.extend_from_slice(&(invalid_row.len() as u16).to_le_bytes());
    rops.extend_from_slice(&invalid_row);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0E, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert_eq!(imported_emails.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn mapi_over_http_modify_recipients_string8_rows_save_canonically() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut to_row = Vec::new();
    to_row.extend_from_slice(b"Bob\0");
    to_row.extend_from_slice(b"bob@example.test\0");
    to_row.extend_from_slice(&1i32.to_le_bytes());
    let mut bcc_row = Vec::new();
    bcc_row.extend_from_slice(b"Hidden\0");
    bcc_row.extend_from_slice(b"hidden@example.test\0");
    bcc_row.extend_from_slice(&3i32.to_le_bytes());

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "String8 recipients");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x06, 0x00, 0x01, 0x02, // RopCreateMessage
    ]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x02, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x0E, 0x00, 0x02, // RopModifyRecipients
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Eu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Eu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    for (row_id, recipient_type, row) in [
        (1u32, 0x01u8, to_row.as_slice()),
        (2u32, 0x03u8, bcc_row.as_slice()),
    ] {
        rops.extend_from_slice(&row_id.to_le_bytes());
        rops.push(recipient_type);
        rops.extend_from_slice(&(row.len() as u16).to_le_bytes());
        rops.extend_from_slice(row);
    }
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x01, 0x02, 0x00, // RopSaveChangesMessage
    ]);

    let request = execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0E, 0x02, 0, 0, 0, 0]));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].to.len(), 1);
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    assert_eq!(recorded[0].to[0].display_name.as_deref(), Some("Bob"));
    assert_eq!(recorded[0].bcc.len(), 1);
    assert_eq!(recorded[0].bcc[0].address, "hidden@example.test");
    assert_eq!(recorded[0].bcc[0].display_name.as_deref(), Some("Hidden"));
}

#[tokio::test]
async fn mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Wrapped recipients");
    let to_row = mapi_wrapped_recipient_row("Bob", "bob@example.test", 0x01);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 1, &property_values);
    append_rop_modify_recipients(&mut rops, 2, &[(1, 0x01, to_row.as_slice())]);
    append_rop_save_changes_message(&mut rops, 2, 2);

    let request = execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0E, 0x02, 0, 0, 0, 0]));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].to.len(), 1);
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    assert_eq!(recorded[0].to[0].display_name.as_deref(), Some("Bob"));
}

#[tokio::test]
async fn mapi_over_http_modify_recipients_x500_rows_save_canonically() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "X500 recipients");
    let legacy_dn = "O=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice-example-test";
    let to_row = mapi_wrapped_x500_recipient_row("Bob", legacy_dn);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 1, &property_values);
    append_rop_modify_recipients_with_columns(
        &mut rops,
        2,
        &[0x0FFE_0003, 0x3900_0003, 0x39FF_001F],
        &[(1, 0x01, to_row.as_slice())],
    );
    append_rop_save_changes_message(&mut rops, 2, 2);

    let request = execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0E, 0x02, 0, 0, 0, 0]));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].to.len(), 1);
    assert_eq!(recorded[0].to[0].address, "alice@example.test");
    assert_eq!(recorded[0].to[0].display_name.as_deref(), Some("Bob"));
}

#[tokio::test]
async fn mapi_over_http_microsoft_modify_recipients_example_saves_canonically() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let columns = [
        0x0FFE_0003u32, // PidTagObjectType
        0x3900_0003u32, // PidTagDisplayType
        0x39FF_001Fu32, // PidTagAddressBookDisplayNamePrintable
        0x39FE_001Fu32, // PidTagSmtpAddress
        0x3A71_0003u32, // PidTagSendInternetEncoding
        0x3905_0003u32, // PidTagDisplayTypeEx
        0x5FF6_001Fu32, // PidTagRecipientDisplayName
        0x5FFD_0003u32, // PidTagRecipientFlags
        0x5FFF_0003u32, // PidTagRecipientTrackStatus
        0x5FDE_0003u32, // Outlook recipient integer from the MS-OXCMSG example.
        0x5FDF_0003u32, // PidTagRecipientOrder
        0x5FF7_0102u32, // PidTagRecipientEntryId
    ];
    let mut row = Vec::new();
    row.extend_from_slice(&0x0651u16.to_le_bytes());
    row.push(b'Z');
    row.push(0);
    row.extend_from_slice(b"User2\0");
    row.extend_from_slice(&utf16z("User2"));
    row.extend_from_slice(&utf16z("user2"));
    row.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    row.push(0);
    row.extend_from_slice(&6i32.to_le_bytes());
    row.extend_from_slice(&0i32.to_le_bytes());
    row.extend_from_slice(&utf16z("user2"));
    row.extend_from_slice(&utf16z("user2@szfkuk-dom.extest.microsoft.com"));
    row.extend_from_slice(&0i32.to_le_bytes());
    row.extend_from_slice(&0x4000_0000i32.to_le_bytes());
    row.extend_from_slice(&utf16z("user2"));
    row.extend_from_slice(&1i32.to_le_bytes());
    row.extend_from_slice(&0i32.to_le_bytes());
    row.extend_from_slice(&0i32.to_le_bytes());
    row.extend_from_slice(&0i32.to_le_bytes());
    row.extend_from_slice(&124u16.to_le_bytes());
    row.extend_from_slice(&[0; 124]);

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x0037_001F,
        "MS-OXCMSG recipient example",
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 1, &property_values);
    append_rop_modify_recipients_with_columns(&mut rops, 2, &columns, &[(0, 0x01, row.as_slice())]);
    append_rop_save_changes_message(&mut rops, 2, 2);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x0E, 0x02, 0, 0, 0, 0]));
    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].to.len(), 1);
    assert_eq!(
        recorded[0].to[0].address,
        "user2@szfkuk-dom.extest.microsoft.com"
    );
    assert_eq!(recorded[0].to[0].display_name.as_deref(), Some("user2"));
}

#[tokio::test]
async fn mapi_over_http_remove_all_recipients_clears_pending_message_recipients() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let row = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x06, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x0E, 0x00, 0x02]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&1u32.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&(row.len() as u16).to_le_bytes());
    rops.extend_from_slice(&row);
    rops.extend_from_slice(&[
        0x0D, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, // RopRemoveAllRecipients
        0x0C, 0x00, 0x01, 0x02, 0x00, // RopSaveChangesMessage
    ]);

    let request = execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x0D, 0x02, 0, 0, 0, 0]));
    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert!(recorded[0].to.is_empty());
    assert!(recorded[0].cc.is_empty());
    assert!(recorded[0].bcc.is_empty());
}

#[tokio::test]
async fn mapi_over_http_microsoft_remove_all_recipients_stages_on_open_message_until_save() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let message_id = Uuid::parse_str("34343434-3434-3434-3434-343434343434").unwrap();
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Existing recipients",
    );
    email.to = vec![JmapEmailAddress {
        address: "bob@example.test".to_string(),
        display_name: Some("Bob".to_string()),
    }];
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(&message_id.to_string()));
    rops.extend_from_slice(&[
        0x0D, 0x00, 0x02, 0, 0, 0, 0, // RopRemoveAllRecipients, reserved ignored.
        0x0F, 0x00, 0x02, // RopReadRecipients on same handle.
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x03, // RopOpenMessage on a separate handle.
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(&message_id.to_string()));
    rops.extend_from_slice(&[
        0x0F, 0x00, 0x03, // RopReadRecipients on separate handle still sees canonical rows.
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x02, 0x02, 0x00, // RopSaveChangesMessage on first message handle.
    ]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[logon_handle, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0D, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x0F, 0x02, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(&response_rops, &utf16z("bob@example.test")));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x02, 0, 0, 0, 0]));
    let canonical = emails.lock().unwrap();
    assert!(canonical[0].to.is_empty());
    assert!(canonical[0].cc.is_empty());
    assert!(canonical[0].bcc.is_empty());
}

#[tokio::test]
async fn mapi_over_http_microsoft_modify_recipients_stages_on_open_message_until_save() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let message_id = Uuid::parse_str("45454545-4545-4545-4545-454545454545").unwrap();
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Existing recipients",
    );
    email.to = vec![JmapEmailAddress {
        address: "bob@example.test".to_string(),
        display_name: Some("Bob".to_string()),
    }];
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let replacement_row = mapi_recipient_row("Alice", "alice@example.test", 0x01);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(&message_id.to_string()));
    rops.extend_from_slice(&[
        0x0E, 0x00, 0x02, // RopModifyRecipients on first message handle.
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&(replacement_row.len() as u16).to_le_bytes());
    rops.extend_from_slice(&replacement_row);
    rops.extend_from_slice(&[
        0x0F, 0x00, 0x02, // RopReadRecipients on same handle sees staged Alice.
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x03, // RopOpenMessage on a separate handle.
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(&message_id.to_string()));
    rops.extend_from_slice(&[
        0x0F, 0x00, 0x03, // RopReadRecipients on separate handle still sees Bob.
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x02, 0x02, 0x00, // RopSaveChangesMessage on first message handle.
    ]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[logon_handle, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("alice@example.test")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("bob@example.test")));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x02, 0, 0, 0, 0]));
    let canonical = emails.lock().unwrap();
    assert_eq!(canonical[0].to.len(), 1);
    assert_eq!(canonical[0].to[0].address, "alice@example.test");
    assert_eq!(canonical[0].to[0].display_name.as_deref(), Some("Alice"));
    assert!(canonical[0].cc.is_empty());
    assert!(canonical[0].bcc.is_empty());
}

#[tokio::test]
async fn mapi_over_http_submit_pending_message_uses_canonical_submission() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox("22222222-2222-2222-2222-222222222222", "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let projection_store = store.clone();
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Submit from MAPI");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Canonical submit body");
    append_mapi_utf16_property(&mut property_values, 0x0C1F_001F, "SMTP:alice@example.test");
    append_mapi_utf16_property(
        &mut property_values,
        0x1035_001F,
        "<mapi-submit@example.test>",
    );

    let to_row = mapi_recipient_row("Bob", "SMTP:bob@example.test", 0x01);
    let bcc_row = mapi_recipient_row("Hidden", "hidden@example.test", 0x03);
    let attachment_bytes = b"%PDF-direct-pending-submit";
    let mut attachment_properties = Vec::new();
    append_mapi_utf16_property(
        &mut attachment_properties,
        0x3707_001F,
        "direct-pending.pdf",
    );
    append_mapi_utf16_property(&mut attachment_properties, 0x370E_001F, "application/pdf");
    append_mapi_binary_property(&mut attachment_properties, 0x3701_0102, attachment_bytes);
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x06, 0x00, 0x01, 0x02, // RopCreateMessage
    ]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x02, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x0E, 0x00, 0x02, // RopModifyRecipients
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    for (row_id, recipient_type, row) in [
        (1u32, 0x01u8, to_row.as_slice()),
        (2u32, 0x03u8, bcc_row.as_slice()),
    ] {
        rops.extend_from_slice(&row_id.to_le_bytes());
        rops.push(recipient_type);
        rops.extend_from_slice(&(row.len() as u16).to_le_bytes());
        rops.extend_from_slice(row);
    }
    rops.extend_from_slice(&[0x23, 0x00, 0x02, 0x03]); // RopCreateAttachment
    append_rop_set_properties(&mut rops, 3, 3, &attachment_properties);
    rops.extend_from_slice(&[0x25, 0x00, 0x02, 0x03, 0x00]); // RopSaveChangesAttachment
    rops.extend_from_slice(&[
        0x32, 0x00, 0x02, 0x00, // RopSubmitMessage
    ]);
    append_rop_get_properties_specific(&mut rops, 2, &[0x0037_001F]);

    let request = execute_body(&rop_buffer(
        &rops,
        &[logon_handle, u32::MAX, u32::MAX, u32::MAX],
    ));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x32, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &[0x25, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &[0x07, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &utf16z("Submit from MAPI")));
    {
        let recorded = submitted_messages.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].source, "mapi-submit-message");
        assert_eq!(recorded[0].draft_message_id, None);
        assert_eq!(recorded[0].subject, "Submit from MAPI");
        assert_eq!(recorded[0].body_text, "Canonical submit body");
        assert_eq!(recorded[0].from_address, "alice@example.test");
        assert_eq!(recorded[0].to.len(), 1);
        assert_eq!(recorded[0].to[0].address, "bob@example.test");
        assert_eq!(recorded[0].bcc.len(), 1);
        assert_eq!(recorded[0].bcc[0].address, "hidden@example.test");
        assert_eq!(recorded[0].attachments.len(), 1);
        assert_eq!(recorded[0].attachments[0].file_name, "direct-pending.pdf");
        assert_eq!(recorded[0].attachments[0].media_type, "application/pdf");
        assert_eq!(recorded[0].attachments[0].blob_bytes, attachment_bytes);
        assert_eq!(
            recorded[0].internet_message_id.as_deref(),
            Some("<mapi-submit@example.test>")
        );
    }

    let sent = {
        let canonical = emails.lock().unwrap();
        let sent = canonical
            .iter()
            .filter(|email| email.mailbox_role == "sent" && email.subject == "Submit from MAPI")
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(sent.len(), 1);
        assert!(canonical.iter().all(|email| email.mailbox_role != "outbox"));
        sent[0].clone()
    };
    assert_eq!(
        sent.mailbox_id,
        Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap()
    );
    assert_eq!(sent.mailbox_ids, vec![sent.mailbox_id]);
    assert_eq!(sent.mailbox_states.len(), 1);
    assert_eq!(sent.mailbox_states[0].mailbox_id, sent.mailbox_id);
    assert_eq!(sent.mailbox_states[0].modseq, sent.modseq);
    assert_eq!(sent.delivery_status, "queued");

    let visible = projection_store
        .fetch_jmap_emails(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].mailbox_role, "sent");
    assert!(visible[0].bcc.is_empty());
    let protected = projection_store
        .fetch_jmap_emails_with_protected_bcc(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(protected[0].bcc[0].address, "hidden@example.test");
    let hidden_search = projection_store
        .query_jmap_email_ids(
            FakeStore::account().account_id,
            Some(sent.mailbox_id),
            Some("hidden@example.test"),
            0,
            10,
        )
        .await
        .unwrap();
    assert!(hidden_search.ids.is_empty());
    let subject_search = projection_store
        .query_jmap_email_ids(
            FakeStore::account().account_id,
            Some(sent.mailbox_id),
            Some("Submit from MAPI"),
            0,
            10,
        )
        .await
        .unwrap();
    assert_eq!(subject_search.ids, vec![sent.id]);
}

#[tokio::test]
async fn mapi_over_http_transport_send_uses_canonical_submission() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox("22222222-2222-2222-2222-222222222222", "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let projection_store = store.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x0037_001F,
        "Transport send from MAPI",
    );
    append_mapi_utf16_property(
        &mut property_values,
        0x1000_001F,
        "Canonical transport body",
    );
    append_mapi_utf16_property(&mut property_values, 0x0C1F_001F, "SMTP:");
    let to_row = mapi_recipient_row("Bob", "bob@example.test", 0x01);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 2, &property_values);
    append_rop_modify_recipients(&mut rops, 2, &[(1, 0x01, to_row.as_slice())]);
    append_rop_transport_send(&mut rops, 2);

    let request = execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x4A, 0x02, 0, 0, 0, 0, 1]));
    {
        let recorded = submitted_messages.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].source, "mapi-submit-message");
        assert_eq!(recorded[0].draft_message_id, None);
        assert_eq!(recorded[0].subject, "Transport send from MAPI");
        assert_eq!(recorded[0].body_text, "Canonical transport body");
        assert_eq!(recorded[0].from_address, "alice@example.test");
        assert_eq!(recorded[0].to.len(), 1);
        assert_eq!(recorded[0].to[0].address, "bob@example.test");
    }

    let sent = {
        let canonical = emails.lock().unwrap();
        let sent = canonical
            .iter()
            .filter(|email| {
                email.mailbox_role == "sent" && email.subject == "Transport send from MAPI"
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(sent.len(), 1);
        assert!(canonical.iter().all(|email| email.mailbox_role != "outbox"));
        sent[0].clone()
    };
    assert_eq!(sent.delivery_status, "queued");
    assert_eq!(sent.mailbox_states[0].modseq, sent.modseq);
    let visible = projection_store
        .fetch_jmap_emails(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(visible[0].mailbox_role, "sent");
    assert!(visible[0].bcc.is_empty());
}

#[tokio::test]
async fn mapi_transport_send_accepts_initial_request_before_cached_calendar_upload(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let target_calendar = storage
        .create_accessible_calendar_collection(account_id, "Probe 4 target")
        .await?;
    let uid_guard_event = storage
        .create_accessible_event(
            account_id,
            Some(&target_calendar.id),
            UpsertClientEventInput {
                id: None,
                account_id,
                uid: "probe-4-visible-uid-guard".to_string(),
                date: "2026-08-27".to_string(),
                time: "10:00".to_string(),
                time_zone: "UTC".to_string(),
                duration_minutes: 30,
                all_day: false,
                status: "confirmed".to_string(),
                sequence: 0,
                recurrence_rule: String::new(),
                recurrence_json: "{}".to_string(),
                recurrence_exceptions_json: "[]".to_string(),
                title: "UID guard".to_string(),
                location: String::new(),
                organizer_json: "{}".to_string(),
                attendees: String::new(),
                attendees_json: "{}".to_string(),
                notes: String::new(),
                body_html: String::new(),
            },
        )
        .await?;
    let target_calendar_folder_id = storage
        .load_mapi_mail_store(account_id, 500)
        .await?
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == target_calendar.id)
        .expect("Probe 4 target Calendar is projected to MAPI")
        .id;
    let calendar_modseq_before = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COALESCE((
            SELECT current_modseq
            FROM account_sync_state
            WHERE account_id = $1 AND category = 'calendar'
        ), 0)
        "#,
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await?;
    let event_log_count_before = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM mail_change_log WHERE account_id = $1 AND object_kind = 'calendar_event'",
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await?;
    let outbox_folder_id = durable_special_folder_id_for_test(
        &storage,
        account_id,
        crate::mapi::identity::OUTBOX_FOLDER_ID,
    )
    .await;
    let service = ExchangeService::new(storage.clone());
    let (mut execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;
    let global_object_id = [
        0x04, 0x00, 0x00, 0x00, 0x82, 0x00, 0xE0, 0x00, 0x74, 0xC5, 0xB7, 0x10, 0x1A, 0x82, 0xE0,
        0x08, 0x00, 0x00, 0x00, 0x00, 0x30, 0xD9, 0xF1, 0xC4, 0x13, 0x34, 0xDD, 0x01, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x5D, 0x74, 0x43, 0xF7, 0xC1,
        0x02, 0x9F, 0x4D, 0x95, 0xDA, 0x59, 0x28, 0x2B, 0xB8, 0x59, 0x3D,
    ];
    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x001A_001F,
        "IPM.Schedule.Meeting.Request",
    );
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Probe 4 ordering");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Meeting body");
    append_mapi_utf16_property(&mut property_values, 0x0042_001F, "Alice Calendar");
    append_mapi_utf16_property(&mut property_values, 0x0065_001F, "alice@example.test");
    append_mapi_i32_property(&mut property_values, 0x8217_0003, 0x0000_0001);
    append_mapi_i64_property(
        &mut property_values,
        0x820D_0040,
        test_filetime("2026-08-26", "09:00"),
    );
    append_mapi_i64_property(
        &mut property_values,
        0x820E_0040,
        test_filetime("2026-08-26", "09:30"),
    );
    append_mapi_binary_property(&mut property_values, 0x8001_0102, &global_object_id);
    append_mapi_binary_property(&mut property_values, 0x8002_0102, &global_object_id);
    append_mapi_i32_property(&mut property_values, 0x8201_0003, 0);
    append_mapi_utf16_property(&mut property_values, 0x8208_001F, "Planches 2");
    append_mapi_i64_property(
        &mut property_values,
        0x0039_0040,
        test_filetime("2026-08-24", "19:59"),
    );
    let mut create_rops = Vec::new();
    append_rop_create_message(&mut create_rops, 0, 1, outbox_folder_id);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&create_rops, &[logon_handle, u32::MAX])),
        )
        .await?;
    let body = response_bytes(response).await;
    let (create_response, create_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(&create_response, &[0x06, 0x01, 0, 0, 0, 0]));
    let pending_request_handle = create_handles[1];
    assert_ne!(pending_request_handle, u32::MAX);

    let mut organizer = Vec::new();
    organizer.extend_from_slice(&utf16z("Alice Calendar"));
    organizer.extend_from_slice(&utf16z("alice@example.test"));
    organizer.extend_from_slice(&1i32.to_le_bytes());
    organizer.extend_from_slice(&3u32.to_le_bytes());
    let mut attendee = Vec::new();
    attendee.extend_from_slice(&utf16z("Denis"));
    attendee.extend_from_slice(&utf16z("denis.ducret@sdic.ch"));
    attendee.extend_from_slice(&1i32.to_le_bytes());
    attendee.extend_from_slice(&1u32.to_le_bytes());
    let mut repeated_location = Vec::new();
    append_mapi_utf16_property(&mut repeated_location, 0x8208_001F, "Planches 2");
    let mut repeated_sequence = Vec::new();
    append_mapi_i32_property(&mut repeated_sequence, 0x8201_0003, 0);
    let mut send_rops = Vec::new();
    append_rop_set_properties(&mut send_rops, 0, 13, &property_values);
    append_rop_set_properties(&mut send_rops, 0, 1, &repeated_location);
    append_rop_modify_recipients_with_columns(
        &mut send_rops,
        0,
        &[0x3001_001F, 0x3003_001F, 0x0C15_0003, 0x5FFD_0003],
        &[(0, 0x01, &organizer), (1, 0x01, &attendee)],
    );
    append_rop_set_properties(&mut send_rops, 0, 1, &repeated_sequence);
    append_rop_transport_send(&mut send_rops, 0);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&send_rops, &[pending_request_handle])),
        )
        .await?;
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(
        contains_bytes(&response_rops, &[0x4A, 0x00, 0, 0, 0, 0, 1]),
        "TransportSend did not succeed: {response_rops:02x?}"
    );

    let placeholder = sqlx::query(
        r#"
        SELECT id, projection_state, attendees_json::text AS attendees_json
        FROM calendar_events
        WHERE owner_account_id = $1
          AND projection_state = 'mapi_submission_placeholder'
        "#,
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await?;
    let placeholder_id = placeholder.get::<Uuid, _>("id");
    assert_eq!(
        placeholder.get::<String, _>("projection_state"),
        "mapi_submission_placeholder"
    );
    assert!(placeholder
        .get::<String, _>("attendees_json")
        .contains("denis.ducret@sdic.ch"));
    let visible_events = storage.fetch_accessible_events(account_id).await?;
    assert_eq!(visible_events.len(), 1);
    assert_eq!(visible_events[0].id, uid_guard_event.id);
    let identity_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM mapi_object_identities WHERE canonical_id = $1",
    )
    .bind(placeholder_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(identity_count, 0);
    let calendar_modseq_after_send = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COALESCE((
            SELECT current_modseq
            FROM account_sync_state
            WHERE account_id = $1 AND category = 'calendar'
        ), 0)
        "#,
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(calendar_modseq_after_send, calendar_modseq_before);
    let event_log_count_after_send = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM mail_change_log WHERE account_id = $1 AND object_kind = 'calendar_event'",
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(event_log_count_after_send, event_log_count_before);

    let guard_version = storage
        .fetch_mapi_event_versions(account_id, &[uid_guard_event.id])
        .await?
        .into_iter()
        .next()
        .expect("the visible UID-guard Event has a MAPI version");
    let uid_collision = storage
        .commit_mapi_event_update(MapiEventCommitInput {
            principal_account_id: account_id,
            event_id: uid_guard_event.id,
            expected_modseq: guard_version.canonical_modseq,
            force_save: false,
            imported_identity: None,
            event: Some(UpsertClientEventInput {
                id: Some(uid_guard_event.id),
                account_id,
                uid: format!(
                    "mapi-goid:{}",
                    lpe_domain::crypto::hex_lower(global_object_id)
                ),
                date: uid_guard_event.date.clone(),
                time: uid_guard_event.time.clone(),
                time_zone: uid_guard_event.time_zone.clone(),
                duration_minutes: uid_guard_event.duration_minutes,
                all_day: uid_guard_event.all_day,
                status: uid_guard_event.status.clone(),
                sequence: uid_guard_event.sequence,
                recurrence_rule: uid_guard_event.recurrence_rule.clone(),
                recurrence_json: uid_guard_event.recurrence_json.clone(),
                recurrence_exceptions_json: uid_guard_event.recurrence_exceptions_json.clone(),
                title: uid_guard_event.title.clone(),
                location: uid_guard_event.location.clone(),
                organizer_json: uid_guard_event.organizer_json.clone(),
                attendees: uid_guard_event.attendees.clone(),
                attendees_json: uid_guard_event.attendees_json.clone(),
                notes: uid_guard_event.notes.clone(),
                body_html: uid_guard_event.body_html.clone(),
            }),
            reminder: MapiEventReminderPatch::default(),
            custom_property_upserts: Vec::new(),
            custom_property_deletes: Vec::new(),
            attachment_changes: MapiEventAttachmentChanges::default(),
        })
        .await
        .expect_err("a visible MAPI Event cannot take a hidden placeholder UID");
    assert!(uid_collision
        .to_string()
        .contains("reserved by a pending MAPI meeting upload"));

    let queued = sqlx::query(
        r#"
        SELECT queue.status,
               classification.classification,
               classification.metadata_json
        FROM submission_queue queue
        JOIN mailbox_messages mailbox_message
          ON mailbox_message.tenant_id = queue.tenant_id
         AND mailbox_message.account_id = queue.account_id
         AND mailbox_message.id = queue.sent_mailbox_message_id
        JOIN messages message
          ON message.tenant_id = mailbox_message.tenant_id
         AND message.id = mailbox_message.message_id
        JOIN calendar_mail_classifications classification
          ON classification.tenant_id = message.tenant_id
         AND classification.message_id = message.id
        WHERE queue.account_id = $1
          AND message.normalized_subject = 'Probe 4 ordering'
        "#,
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(queued.get::<String, _>("status"), "queued");
    assert_eq!(queued.get::<String, _>("classification"), "request");
    let metadata = queued.get::<serde_json::Value, _>("metadata_json");
    let request = &metadata["request"];
    assert_eq!(
        request["uid"],
        serde_json::Value::String(format!(
            "mapi-goid:{}",
            lpe_domain::crypto::hex_lower(global_object_id)
        ))
    );
    assert_eq!(request["meeting_sequence"], 0);
    assert_eq!(request["meeting_start"], "2026-08-26T09:00:00Z");
    assert_eq!(request["meeting_end"], "2026-08-26T09:30:00Z");
    assert_eq!(request["organizer"]["email"], "alice@example.test");
    assert_eq!(request["attendees"][0]["email"], "denis.ducret@sdic.ch");

    let response_uid = request["uid"]
        .as_str()
        .expect("queued request classification contains a UID");
    let inbound_response = format!(
        concat!(
            "From: Denis <denis.ducret@sdic.ch>\r\n",
            "To: Alice Calendar <alice@example.test>\r\n",
            "Subject: Accepted: Probe 4 ordering\r\n",
            "MIME-Version: 1.0\r\n",
            "Content-Type: text/calendar; method=REPLY; charset=UTF-8\r\n",
            "\r\n",
            "BEGIN:VCALENDAR\r\n",
            "VERSION:2.0\r\n",
            "METHOD:REPLY\r\n",
            "BEGIN:VEVENT\r\n",
            "UID:{}\r\n",
            "DTSTAMP:20260824T200000Z\r\n",
            "DTSTART:20260826T090000Z\r\n",
            "DTEND:20260826T093000Z\r\n",
            "SEQUENCE:0\r\n",
            "ORGANIZER;CN=Alice Calendar:mailto:alice@example.test\r\n",
            "ATTENDEE;CN=Denis;PARTSTAT=ACCEPTED:mailto:denis.ducret@sdic.ch\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n"
        ),
        response_uid
    )
    .into_bytes();
    let delivery = storage
        .deliver_inbound_message(lpe_domain::InboundDeliveryRequest {
            trace_id: format!("probe-4-early-response-{}", Uuid::new_v4()),
            peer: "192.0.2.10:25".to_string(),
            helo: "mx.sdic.ch".to_string(),
            mail_from: "denis.ducret@sdic.ch".to_string(),
            rcpt_to: vec!["alice@example.test".to_string()],
            subject: "Accepted: Probe 4 ordering".to_string(),
            body_text: String::new(),
            internet_message_id: None,
            raw_message: inbound_response,
        })
        .await?;
    assert!(delivery.accepted);
    let early_response = sqlx::query(
        r#"
        SELECT attendees_json #>> '{attendees,0,partstat}' AS partstat,
               meeting_response_state_json ? 'denis.ducret@sdic.ch' AS has_watermark
        FROM calendar_events
        WHERE id = $1
          AND projection_state = 'mapi_submission_placeholder'
        "#,
    )
    .bind(placeholder_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(early_response.get::<String, _>("partstat"), "accepted");
    assert!(early_response.get::<bool, _>("has_watermark"));
    let calendar_modseq_after_response = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COALESCE((
            SELECT current_modseq
            FROM account_sync_state
            WHERE account_id = $1 AND category = 'calendar'
        ), 0)
        "#,
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(calendar_modseq_after_response, calendar_modseq_before);
    let event_log_count_after_response = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM mail_change_log WHERE account_id = $1 AND object_kind = 'calendar_event'",
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(event_log_count_after_response, event_log_count_before);

    let imported_counter = storage
        .reserve_mapi_local_replica_ids(account_id, 1)
        .await?;
    let imported_message_id = crate::mapi::identity::mapi_store_id(imported_counter);
    let source_key = crate::mapi::identity::source_key_for_object_id(imported_message_id);
    let change_key = vec![
        0x2e, 0xbb, 0x24, 0x39, 0x44, 0x3e, 0x0a, 0x43, 0x8d, 0x4a, 0x69, 0x81, 0x3f, 0x51, 0xf4,
        0xe0, 0x00, 0x00, 0x04, 0x71,
    ];
    let mut predecessor_change_list = vec![change_key.len() as u8];
    predecessor_change_list.extend_from_slice(&change_key);
    let imported_last_modification_time = test_filetime("2026-08-24", "19:59");

    // Outlook uploads the separate Calendar item in a fresh MAPI context.
    // Keep ImportMessageChange and SaveChangesMessage in separate Execute
    // requests so the imported pending Event handle must survive the boundary.
    let (mut upload_headers, upload_logon_handle) = mapi_connect_with_private_logon(&service).await;
    let mut collector_rops = Vec::new();
    append_rop_open_folder(&mut collector_rops, 0, 1, target_calendar_folder_id);
    collector_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &upload_headers,
            &execute_body(&rop_buffer(
                &collector_rops,
                &[upload_logon_handle, u32::MAX, u32::MAX],
            )),
        )
        .await?;
    let body = response_bytes(response).await;
    let (collector_response, collector_handles) =
        response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(
        &collector_response,
        &[0x7E, 0x02, 0, 0, 0, 0]
    ));
    let collector_handle = collector_handles[2];
    assert_ne!(collector_handle, u32::MAX);

    let mut identity_values = Vec::new();
    append_mapi_binary_property(&mut identity_values, PID_TAG_SOURCE_KEY, &source_key);
    append_mapi_i64_property(
        &mut identity_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        imported_last_modification_time,
    );
    append_mapi_binary_property(&mut identity_values, PID_TAG_CHANGE_KEY, &change_key);
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &predecessor_change_list,
    );
    let mut import_rops = vec![
        0x72, 0x00, 0x00, 0x01, 0x01, // RopSynchronizationImportMessageChange.
    ];
    import_rops.extend_from_slice(&4u16.to_le_bytes());
    import_rops.extend_from_slice(&identity_values);
    renew_mapi_request_id(&mut upload_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &upload_headers,
            &execute_body(&rop_buffer(&import_rops, &[collector_handle, u32::MAX])),
        )
        .await?;
    let body = response_bytes(response).await;
    let (import_response, import_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(
        contains_bytes(&import_response, &[0x72, 0x01, 0, 0, 0, 0]),
        "ImportMessageChange failed: {import_response:02x?}"
    );
    let pending_event_handle = import_handles[1];
    assert_ne!(pending_event_handle, u32::MAX);

    let mut appointment_values = Vec::new();
    append_mapi_utf16_property(
        &mut appointment_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Appointment",
    );
    append_mapi_utf16_property(
        &mut appointment_values,
        PID_TAG_SUBJECT_W,
        "Probe 4 ordering",
    );
    append_mapi_utf16_property(
        &mut appointment_values,
        0x1000_001F,
        "Imported rich meeting body",
    );
    append_mapi_utf16_property(
        &mut appointment_values,
        0x1013_001F,
        "<p>Imported rich meeting body</p>",
    );
    append_mapi_i64_property(
        &mut appointment_values,
        0x820D_0040,
        test_filetime("2026-08-26", "09:00"),
    );
    append_mapi_i64_property(
        &mut appointment_values,
        0x820E_0040,
        test_filetime("2026-08-26", "09:30"),
    );
    append_mapi_i32_property(&mut appointment_values, 0x8217_0003, 0x0000_0001);
    append_mapi_binary_property(&mut appointment_values, 0x8001_0102, &global_object_id);
    append_mapi_binary_property(&mut appointment_values, 0x8002_0102, &global_object_id);
    append_mapi_i32_property(&mut appointment_values, 0x8201_0003, 0);
    append_mapi_utf16_property(&mut appointment_values, 0x8208_001F, "Planches 2");
    append_mapi_binary_property(
        &mut appointment_values,
        0x825E_0102,
        &test_calendar_time_zone_definition("W. Europe Standard Time"),
    );
    append_mapi_bool_property(&mut appointment_values, 0x8503_000B, true);
    append_mapi_i64_property(
        &mut appointment_values,
        0x8560_0040,
        test_filetime("2026-08-26", "08:45"),
    );
    append_mapi_i32_property(&mut appointment_values, PID_TAG_IMPORTANCE, 2);
    append_mapi_i64_property(
        &mut appointment_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        imported_last_modification_time,
    );
    let mut final_sequence = Vec::new();
    append_mapi_i32_property(&mut final_sequence, 0x8201_0003, 0);
    let mut save_rops = Vec::new();
    append_rop_set_properties(&mut save_rops, 0, 16, &appointment_values);
    append_rop_modify_recipients_with_columns(
        &mut save_rops,
        0,
        &[0x3001_001F, 0x3003_001F, 0x0C15_0003, 0x5FFD_0003],
        &[(0, 0x01, &organizer), (1, 0x01, &attendee)],
    );
    append_rop_set_properties(&mut save_rops, 0, 1, &final_sequence);
    append_rop_save_changes_message_with_flags(&mut save_rops, 0, 0, 0x08);
    append_rop_get_properties_specific(
        &mut save_rops,
        0,
        &[
            PID_TAG_SOURCE_KEY,
            PID_TAG_CHANGE_KEY,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            PID_TAG_LAST_MODIFICATION_TIME,
        ],
    );
    renew_mapi_request_id(&mut upload_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &upload_headers,
            &execute_body(&rop_buffer(&save_rops, &[pending_event_handle])),
        )
        .await?;
    let body = response_bytes(response).await;
    let (save_response, save_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(
        contains_bytes(&save_response, &[0x0C, 0x00, 0, 0, 0, 0]),
        "SaveChangesMessage failed: {save_response:02x?}"
    );
    let mut expected_save = vec![0x0C, 0x00, 0, 0, 0, 0, 0x01];
    expected_save.extend_from_slice(&mapi_wire_id_bytes(imported_message_id));
    assert!(
        contains_bytes(&save_response, &expected_save),
        "SaveChangesMessage did not retain the imported MID: {save_response:02x?}"
    );
    assert!(
        contains_bytes(&save_response, &[0x07, 0x00, 0, 0, 0, 0, 0]),
        "GetPropertiesSpecific failed after the imported Event Save: {save_response:02x?}"
    );
    assert!(
        contains_bytes(&save_response, &change_key),
        "post-Save GetPropertiesSpecific omitted the imported ChangeKey"
    );
    assert_eq!(save_handles[0], collector_handles[1]);

    let imported = storage
        .fetch_accessible_events_in_collection(account_id, &target_calendar.id)
        .await?
        .into_iter()
        .find(|event| event.id == placeholder_id)
        .expect("the imported Calendar item adopted the hidden placeholder");
    assert_eq!(imported.time_zone, "Europe/Berlin");
    assert_eq!(imported.time, "11:00");
    assert_eq!(imported.notes, "Imported rich meeting body");
    assert_eq!(imported.body_html, "<p>Imported rich meeting body</p>");
    assert!(imported.attendees_json.contains("accepted"));

    let adopted = sqlx::query(
        r#"
        SELECT event.projection_state,
               event.calendar_id,
               event.reminder_set,
               identity.source_key,
               identity.mapi_change_number,
               identity.change_key,
               identity.predecessor_change_list,
               (EXTRACT(EPOCH FROM (
                    identity.updated_at - TIMESTAMPTZ '1601-01-01 00:00:00+00'
                )) * 10000000)::bigint AS last_modification_time,
               property_value.property_value
        FROM calendar_events event
        JOIN mapi_object_identities identity
          ON identity.tenant_id = event.tenant_id
         AND identity.account_id = event.owner_account_id
         AND identity.object_kind = 'calendar_event'
         AND identity.canonical_id = event.id
         AND identity.deleted_at IS NULL
        JOIN mapi_custom_property_values property_value
          ON property_value.tenant_id = event.tenant_id
         AND property_value.account_id = event.owner_account_id
         AND property_value.object_kind = 'calendar_event'
         AND property_value.canonical_id = event.id
         AND property_value.property_tag = $2
        WHERE event.id = $1
        "#,
    )
    .bind(placeholder_id)
    .bind(PID_TAG_IMPORTANCE as i64)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(adopted.get::<String, _>("projection_state"), "visible");
    assert_eq!(
        adopted.get::<Uuid, _>("calendar_id").to_string(),
        target_calendar.id
    );
    assert!(adopted.get::<bool, _>("reminder_set"));
    assert_eq!(adopted.get::<Vec<u8>, _>("source_key"), source_key);
    assert_eq!(adopted.get::<Vec<u8>, _>("change_key"), change_key);
    assert_eq!(
        adopted.get::<Vec<u8>, _>("predecessor_change_list"),
        predecessor_change_list
    );
    assert_eq!(
        adopted.get::<i64, _>("last_modification_time"),
        imported_last_modification_time
    );
    assert_ne!(
        adopted.get::<i64, _>("mapi_change_number") as u64,
        imported_counter
    );
    assert_eq!(
        adopted.get::<Vec<u8>, _>("property_value"),
        2i32.to_le_bytes()
    );
    let owner_uid_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM calendar_events WHERE owner_account_id = $1 AND uid = $2",
    )
    .bind(account_id)
    .bind(request["uid"].as_str().unwrap())
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(owner_uid_count, 1);
    let event_log_count_after_adoption = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM mail_change_log WHERE account_id = $1 AND object_kind = 'calendar_event'",
    )
    .bind(account_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(event_log_count_after_adoption, event_log_count_before + 1);

    fixture.cleanup().await?;
    Ok(())
}

struct OptimizedPostCommitFixture {
    store: FakeStore,
    service: ExchangeService<FakeStore>,
    rops: Vec<u8>,
    target_message_id: u64,
    outbox_id: Uuid,
}

async fn optimized_post_commit_fixture(
    fail_identity: bool,
    identity_object_id_override: Option<u64>,
    fail_mirror: bool,
    invalid_target: bool,
) -> OptimizedPostCommitFixture {
    const PID_TAG_TARGET_ENTRY_ID: u32 = 0x3010_0102;

    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let outbox_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let sent_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&outbox_id.to_string(), "outbox", "Outbox"),
            FakeStore::mailbox(&sent_id.to_string(), "sent", "Sent"),
        ])),
        fail_mapi_message_identity_allocation: fail_identity,
        mapi_message_identity_object_id_override: identity_object_id_override,
        fail_mapi_outbox_mirror: fail_mirror,
        ..Default::default()
    };
    let mailboxes = store
        .ensure_jmap_system_mailboxes(account.account_id)
        .await
        .unwrap();
    let folder_identities = store
        .fetch_or_allocate_mapi_identities(
            account.account_id,
            &crate::mapi_store::mapi_folder_identity_requests(&mailboxes),
        )
        .await
        .unwrap();
    let durable_inbox_id = folder_identities
        .iter()
        .find(|identity| identity.canonical_id == inbox_id)
        .expect("durable Inbox identity")
        .object_id;
    let durable_outbox_id = folder_identities
        .iter()
        .find(|identity| identity.canonical_id == outbox_id)
        .expect("durable Outbox identity")
        .object_id;
    let reservation_start = store
        .reserve_mapi_local_replica_ids(account.account_id, 0x100)
        .await
        .unwrap();
    let target_message_id = crate::mapi::identity::mapi_store_id(reservation_start + 7);
    let target_entry_id = if invalid_target {
        vec![0xFF; 16]
    } else {
        crate::mapi::identity::message_entry_id_from_object_ids(
            account.account_id,
            durable_outbox_id,
            target_message_id,
        )
        .expect("optimized-send TargetEntryId")
    };

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x0037_001F,
        "Post-commit optimized send",
    );
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Post-commit body");
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_TARGET_ENTRY_ID,
        &target_entry_id,
    );
    let recipient = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, durable_inbox_id);
    append_rop_create_message(&mut rops, 1, 2, durable_inbox_id);
    append_rop_set_properties(&mut rops, 2, 3, &property_values);
    append_rop_modify_recipients(&mut rops, 2, &[(1, 0x01, recipient.as_slice())]);
    append_rop_transport_send(&mut rops, 2);

    let service = ExchangeService::new(store.clone());
    OptimizedPostCommitFixture {
        store,
        service,
        rops,
        target_message_id,
        outbox_id,
    }
}

async fn execute_optimized_post_commit_fixture(
    fixture: &OptimizedPostCommitFixture,
    probe_submitted_handle: bool,
) -> Vec<u8> {
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&fixture.service).await;
    let mut rops = fixture.rops.clone();
    if probe_submitted_handle {
        append_rop_get_properties_specific(&mut rops, 2, &[0x0037_001F]);
    }
    let response = fixture
        .service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    response_rops_from_execute_response(response).await
}

#[tokio::test]
async fn mapi_submit_identity_failure_is_success_and_retires_the_issued_handle() {
    let fixture = optimized_post_commit_fixture(true, None, false, false).await;
    let response = execute_optimized_post_commit_fixture(&fixture, true).await;

    assert!(contains_bytes(&response, &[0x4A, 0x02, 0, 0, 0, 0, 1]));
    assert!(contains_bytes(
        &response,
        &[0x07, 0x02, 0x08, 0x01, 0x04, 0x80]
    ));
    assert_eq!(fixture.store.submitted_messages.lock().unwrap().len(), 1);
    assert_eq!(fixture.store.emails.lock().unwrap().len(), 1);
    assert!(!fixture
        .store
        .mapi_identities
        .lock()
        .unwrap()
        .contains_key(&Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap()));
}

#[tokio::test]
async fn mapi_optimized_send_mirror_failure_replays_from_durable_sent_without_duplicate() {
    let fixture = optimized_post_commit_fixture(false, None, true, false).await;
    let response = execute_optimized_post_commit_fixture(&fixture, true).await;
    assert!(contains_bytes(&response, &[0x4A, 0x02, 0, 0, 0, 0, 1]));
    assert!(contains_bytes(&response, &[0x07, 0x02, 0, 0, 0, 0]));
    assert_eq!(fixture.store.submitted_messages.lock().unwrap().len(), 1);
    {
        let emails = fixture.store.emails.lock().unwrap();
        assert_eq!(emails.len(), 1);
        assert!(!emails[0].mailbox_ids.contains(&fixture.outbox_id));
    }

    let retry = execute_optimized_post_commit_fixture(&fixture, false).await;
    assert!(contains_bytes(&retry, &[0x4A, 0x02, 0, 0, 0, 0, 1]));
    assert_eq!(fixture.store.submitted_messages.lock().unwrap().len(), 1);
    assert_eq!(fixture.store.emails.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn mapi_optimized_send_identity_mismatch_uses_actual_identity_and_replays() {
    let actual_message_id = crate::mapi::identity::mapi_store_id(0x0000_0000_00F0_0001);
    let fixture = optimized_post_commit_fixture(false, Some(actual_message_id), false, false).await;
    let response = execute_optimized_post_commit_fixture(&fixture, true).await;
    assert!(contains_bytes(&response, &[0x4A, 0x02, 0, 0, 0, 0, 1]));
    assert!(contains_bytes(&response, &[0x07, 0x02, 0, 0, 0, 0]));
    assert_ne!(fixture.target_message_id, actual_message_id);
    assert_eq!(
        fixture.store.mapi_identities.lock().unwrap()
            [&Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap()],
        actual_message_id
    );
    assert!(!fixture.store.emails.lock().unwrap()[0]
        .mailbox_ids
        .contains(&fixture.outbox_id));

    let retry = execute_optimized_post_commit_fixture(&fixture, false).await;
    assert!(contains_bytes(&retry, &[0x4A, 0x02, 0, 0, 0, 0, 1]));
    assert_eq!(fixture.store.submitted_messages.lock().unwrap().len(), 1);
    assert_eq!(fixture.store.emails.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn mapi_optimized_send_invalid_target_fails_before_canonical_submission() {
    let fixture = optimized_post_commit_fixture(false, None, false, true).await;
    let response = execute_optimized_post_commit_fixture(&fixture, false).await;

    assert!(contains_bytes(
        &response,
        &[0x4A, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));
    assert!(fixture.store.submitted_messages.lock().unwrap().is_empty());
    assert!(fixture.store.emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move() {
    const PID_TAG_TARGET_ENTRY_ID: u32 = 0x3010_0102;
    const PID_TAG_SOURCE_KEY: u32 = 0x65E0_0102;
    const PID_TAG_LAST_MODIFICATION_TIME: u32 = 0x3008_0040;
    const PID_TAG_CHANGE_KEY: u32 = 0x65E2_0102;
    const PID_TAG_PREDECESSOR_CHANGE_LIST: u32 = 0x65E3_0102;

    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let outbox_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let sent_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&outbox_id.to_string(), "outbox", "Outbox"),
            FakeStore::mailbox(&sent_id.to_string(), "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let mailboxes = store
        .ensure_jmap_system_mailboxes(account.account_id)
        .await
        .unwrap();
    let folder_identities = store
        .fetch_or_allocate_mapi_identities(
            account.account_id,
            &crate::mapi_store::mapi_folder_identity_requests(&mailboxes),
        )
        .await
        .unwrap();
    let durable_folder_id = |mailbox_id| {
        folder_identities
            .iter()
            .find(|identity| identity.canonical_id == mailbox_id)
            .expect("durable MAPI mailbox identity")
            .object_id
    };
    let durable_inbox_id = durable_folder_id(inbox_id);
    let durable_outbox_id = durable_folder_id(outbox_id);
    let durable_sent_id = durable_folder_id(sent_id);
    assert_ne!(
        durable_outbox_id,
        crate::mapi::identity::OUTBOX_FOLDER_ID,
        "the wire TargetEntryId must use the durable physical Outbox FID"
    );
    assert!(
        crate::mapi::identity::global_counter_from_store_id(durable_outbox_id).unwrap()
            >= crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER
    );

    let reservation_count = 0x0001_0000;
    let reservation_start = store
        .reserve_mapi_local_replica_ids(account.account_id, reservation_count)
        .await
        .unwrap();
    let reservation_end = reservation_start + u64::from(reservation_count);
    let source_global_counter = reservation_start + 0x0A5A;
    let destination_global_counter = reservation_start + 0x0C5A;
    assert!(destination_global_counter < reservation_end);
    let source_message_id = crate::mapi::identity::mapi_store_id(source_global_counter);
    let destination_message_id = crate::mapi::identity::mapi_store_id(destination_global_counter);
    let source_message_gid = crate::mapi::identity::source_key_for_object_id(source_message_id);
    let destination_message_gid =
        crate::mapi::identity::source_key_for_object_id(destination_message_id);
    let source_folder_gid = crate::mapi::identity::source_key_for_object_id(durable_outbox_id);
    let imported_change_key =
        mapi_mailstore::change_key_for_change_number(destination_global_counter);
    let imported_predecessor_change_list =
        mapi_mailstore::predecessor_change_list(destination_global_counter);
    assert!(test_mapi_pcl_includes_change_key(
        &imported_predecessor_change_list,
        &imported_change_key,
    ));
    let target_entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
        account.account_id,
        durable_outbox_id,
        source_message_id,
    )
    .expect("private MAPI TargetEntryId");
    assert_eq!(
        crate::mapi::identity::object_ids_from_message_entry_id(
            account.account_id,
            &target_entry_id,
        ),
        Some((durable_outbox_id, source_message_id)),
        "the raw TargetEntryId must retain Outlook's durable Outbox FID"
    );

    let submitted_messages = store.submitted_messages.clone();
    let imported_emails = store.imported_emails.clone();
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store.clone());
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x0037_001F,
        "Optimized send source message",
    );
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Outbox mirror body");
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_TARGET_ENTRY_ID,
        &target_entry_id,
    );
    let recipient = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let mut send_rops = Vec::new();
    append_rop_open_folder(&mut send_rops, 0, 1, durable_inbox_id);
    append_rop_create_message(&mut send_rops, 1, 2, durable_inbox_id);
    append_rop_set_properties(&mut send_rops, 2, 3, &property_values);
    append_rop_modify_recipients(&mut send_rops, 2, &[(1, 0x01, recipient.as_slice())]);
    append_rop_transport_send(&mut send_rops, 2);
    let send_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&send_rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(send_response.status(), StatusCode::OK);
    assert_eq!(send_response.headers().get("x-responsecode").unwrap(), "0");
    let send_response_rops = response_rops_from_execute_response(send_response).await;
    assert!(contains_bytes(
        &send_response_rops,
        &[0x4A, 0x02, 0, 0, 0, 0, 1]
    ));
    assert_eq!(submitted_messages.lock().unwrap().len(), 1);

    // A retransmitted optimized send uses a new Execute request and session,
    // so it must find the durable Outbox mirror instead of creating another
    // canonical Sent message or submission.
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;
    let retry_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&send_rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(retry_response.status(), StatusCode::OK);
    assert_eq!(retry_response.headers().get("x-responsecode").unwrap(), "0");
    let retry_response_rops = response_rops_from_execute_response(retry_response).await;
    assert!(contains_bytes(
        &retry_response_rops,
        &[0x4A, 0x02, 0, 0, 0, 0, 1]
    ));
    assert_eq!(submitted_messages.lock().unwrap().len(), 1);

    let canonical_id = Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();
    {
        let emails = store.emails.lock().unwrap();
        assert_eq!(
            emails.len(),
            1,
            "optimized send must create one canonical email"
        );
        let email = emails
            .iter()
            .find(|email| email.id == canonical_id)
            .unwrap();
        assert_eq!(email.mailbox_id, sent_id);
        assert!(email.mailbox_ids.contains(&sent_id));
        assert!(email.mailbox_ids.contains(&outbox_id));
        assert_eq!(
            email
                .mailbox_states
                .iter()
                .filter(|state| state.mailbox_id == sent_id)
                .count(),
            1,
            "the retry must not create a second Sent membership"
        );
        assert_eq!(
            email
                .mailbox_states
                .iter()
                .filter(|state| state.mailbox_id == outbox_id)
                .count(),
            1,
            "the durable optimized-send Outbox mirror must be unique"
        );
    }
    assert_eq!(
        store.mapi_identities.lock().unwrap()[&canonical_id],
        source_message_id
    );
    assert_eq!(
        store.mapi_identity_source_keys.lock().unwrap()[&canonical_id],
        source_message_gid
    );

    // Exchange captured the ImportMessageMove in a later MAPI session. A
    // fresh session proves the temporary Outbox identity is durable.
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;
    let mut identity_values = Vec::new();
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_SOURCE_KEY,
        &destination_message_gid,
    );
    append_mapi_i64_property(
        &mut identity_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        test_filetime("2026-07-31", "16:04"),
    );
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_CHANGE_KEY,
        &imported_change_key,
    );
    append_mapi_binary_property(
        &mut identity_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &imported_predecessor_change_list,
    );
    let mut final_properties = Vec::new();
    append_mapi_utf16_property(
        &mut final_properties,
        0x0037_001F,
        "Optimized send final Sent item",
    );
    let mut move_rops = Vec::new();
    append_rop_open_folder(&mut move_rops, 0, 1, durable_sent_id);
    move_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
        0x78, 0x00, 0x02, // RopSynchronizationImportMessageMove.
    ]);
    // [MS-OXCFXICS] section 2.2.3.2.4.4.1: SourceFolderId,
    // SourceMessageId, PCL, DestinationMessageId, and ChangeKey/XID are
    // non-empty length-prefixed fields. The folder GID is the physical
    // Outbox FID from the TargetEntryId, as captured from Exchange 2016.
    for field in [
        source_folder_gid.as_slice(),
        source_message_gid.as_slice(),
        imported_predecessor_change_list.as_slice(),
        destination_message_gid.as_slice(),
        imported_change_key.as_slice(),
    ] {
        move_rops.extend_from_slice(&(field.len() as u32).to_le_bytes());
        move_rops.extend_from_slice(field);
    }
    move_rops.extend_from_slice(&[
        0x72, 0x00, 0x02, 0x03, 0x00, // RopSynchronizationImportMessageChange.
    ]);
    move_rops.extend_from_slice(&4u16.to_le_bytes());
    move_rops.extend_from_slice(&identity_values);
    append_rop_set_properties(&mut move_rops, 3, 1, &final_properties);
    append_rop_save_changes_message_with_flags(&mut move_rops, 3, 3, 0x08);
    let move_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &move_rops,
                &[logon_handle, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(move_response.status(), StatusCode::OK);
    assert_eq!(move_response.headers().get("x-responsecode").unwrap(), "0");
    let move_response_rops = response_rops_from_execute_response(move_response).await;
    assert!(contains_bytes(
        &move_response_rops,
        &[0x78, 0x02, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &move_response_rops,
        &[0x72, 0x03, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &move_response_rops,
        &[0x0A, 0x03, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &move_response_rops,
        &[0x0C, 0x03, 0, 0, 0, 0]
    ));

    // The server must also acknowledge a later retry of the exact move: its
    // source Outbox membership is gone, while the destination identity is
    // already durable in Sent.
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;
    let mut move_replay_rops = Vec::new();
    append_rop_open_folder(&mut move_replay_rops, 0, 1, durable_sent_id);
    move_replay_rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
        0x78, 0x00, 0x02, // RopSynchronizationImportMessageMove.
    ]);
    for field in [
        source_folder_gid.as_slice(),
        source_message_gid.as_slice(),
        imported_predecessor_change_list.as_slice(),
        destination_message_gid.as_slice(),
        imported_change_key.as_slice(),
    ] {
        move_replay_rops.extend_from_slice(&(field.len() as u32).to_le_bytes());
        move_replay_rops.extend_from_slice(field);
    }
    let move_replay_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &move_replay_rops,
                &[logon_handle, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(move_replay_response.status(), StatusCode::OK);
    assert_eq!(
        move_replay_response
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
    let move_replay_response_rops = response_rops_from_execute_response(move_replay_response).await;
    assert!(contains_bytes(
        &move_replay_response_rops,
        &[0x78, 0x02, 0, 0, 0, 0]
    ));
    assert_eq!(submitted_messages.lock().unwrap().len(), 1);
    let replayed_email = {
        let emails = store.emails.lock().unwrap();
        assert_eq!(emails.len(), 1, "move replay must not clone the message");
        emails
            .iter()
            .find(|email| email.id == canonical_id)
            .cloned()
            .unwrap()
    };
    assert_eq!(replayed_email.mailbox_ids, vec![sent_id]);
    assert_eq!(replayed_email.mailbox_states.len(), 1);
    assert_eq!(replayed_email.mailbox_states[0].mailbox_id, sent_id);
    assert_eq!(
        store.mapi_identities.lock().unwrap()[&canonical_id],
        destination_message_id
    );

    let email = store
        .emails
        .lock()
        .unwrap()
        .iter()
        .find(|email| email.id == canonical_id)
        .cloned()
        .unwrap();
    assert_eq!(email.subject, "Optimized send final Sent item");
    assert_eq!(email.mailbox_id, sent_id);
    assert_eq!(email.mailbox_ids, vec![sent_id]);
    assert_eq!(email.mailbox_states.len(), 1);
    assert_eq!(email.mailbox_states[0].mailbox_id, sent_id);
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(canonical_id, sent_id)]
    );
    assert!(
        imported_emails.lock().unwrap().is_empty(),
        "ImportMessageChange must update the moved canonical email, not create a duplicate"
    );
    assert_eq!(
        store.mapi_identities.lock().unwrap()[&canonical_id],
        destination_message_id
    );
    assert_eq!(
        store.mapi_identity_source_keys.lock().unwrap()[&canonical_id],
        destination_message_gid
    );
    assert_eq!(
        store.mapi_identity_change_keys.lock().unwrap()[&canonical_id],
        imported_change_key
    );
    assert_eq!(
        store.mapi_identity_predecessor_change_lists.lock().unwrap()[&canonical_id],
        imported_predecessor_change_list
    );
    assert!(
        store.mapi_identity_change_numbers.lock().unwrap()[&canonical_id] >= reservation_end,
        "the imported Sent move must use a server CN outside Outlook's local replica range"
    );
}

#[tokio::test]
async fn mapi_over_http_transport_send_opened_draft_defers_canonical_attachment_hydration_and_preserves_bcc_guards(
) {
    let draft_id = Uuid::parse_str("20202020-2020-2020-2020-202020202020").unwrap();
    let draft_mailbox_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let sent_mailbox_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let mut draft = FakeStore::email(
        &draft_id.to_string(),
        &draft_mailbox_id.to_string(),
        "drafts",
        "Transport saved draft",
    );
    draft.body_text = "Draft body for transport send".to_string();
    draft.bcc.push(JmapEmailAddress {
        address: "transport-hidden@example.test".to_string(),
        display_name: Some("Transport Hidden".to_string()),
    });
    draft.has_attachments = true;
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let attachment_reference = format!("attachment:{draft_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&draft_mailbox_id.to_string(), "drafts", "Drafts"),
            FakeStore::mailbox(&sent_mailbox_id.to_string(), "sent", "Sent"),
        ])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            draft_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id: draft_id,
                file_name: "transport-inline.png".to_string(),
                media_type: "image/png".to_string(),
                disposition: Some("inline".to_string()),
                content_id: Some("transport-inline.png@01C86E1C.F1954390".to_string()),
                size_octets: 7,
                file_reference: attachment_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            attachment_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: attachment_reference,
                file_name: "transport-inline.png".to_string(),
                media_type: "image/png".to_string(),
                blob_bytes: b"PNGDATA".to_vec(),
                ..Default::default()
            },
        )]))),
        emails: Arc::new(Mutex::new(vec![draft])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let submitted_draft_messages = store.submitted_draft_messages.clone();
    let emails = store.emails.clone();
    let projection_store = store.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(14));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(14),
        test_mapi_message_id(&draft_id.to_string()),
    );
    append_rop_transport_send(&mut rops, 2);

    let request = execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x4A, 0x02, 0, 0, 0, 0, 1]));
    assert_eq!(
        submitted_draft_messages.lock().unwrap().as_slice(),
        &[draft_id],
        "opened Drafts messages must use the persisted-source submission API"
    );

    {
        let recorded = submitted_messages.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].source, "mapi-submit-message");
        assert_eq!(recorded[0].draft_message_id, Some(draft_id));
        assert_eq!(recorded[0].subject, "Transport saved draft");
        assert_eq!(recorded[0].bcc[0].address, "transport-hidden@example.test");
        assert!(
            recorded[0].attachments.is_empty(),
            "saved MAPI submit must let canonical storage hydrate the exact source MIME graph"
        );
    }

    let sent = {
        let canonical = emails.lock().unwrap();
        assert!(canonical.iter().all(|email| email.id != draft_id));
        let sent = canonical
            .iter()
            .filter(|email| {
                email.mailbox_role == "sent" && email.subject == "Transport saved draft"
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(sent.len(), 1);
        assert!(canonical.iter().all(|email| email.mailbox_role != "outbox"));
        sent[0].clone()
    };
    assert_eq!(sent.mailbox_id, sent_mailbox_id);
    assert_eq!(sent.mailbox_ids, vec![sent_mailbox_id]);
    assert_eq!(sent.mailbox_states[0].mailbox_id, sent_mailbox_id);
    assert_eq!(sent.mailbox_states[0].modseq, sent.modseq);
    assert!(sent.has_attachments);
    assert_eq!(sent.delivery_status, "queued");

    let visible = projection_store
        .fetch_jmap_emails(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert!(visible[0].bcc.is_empty());
    assert!(visible[0].has_attachments);
    let protected = projection_store
        .fetch_jmap_emails_with_protected_bcc(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(protected[0].bcc[0].address, "transport-hidden@example.test");
    let hidden_search = projection_store
        .query_jmap_email_ids(
            FakeStore::account().account_id,
            Some(sent_mailbox_id),
            Some("transport-hidden@example.test"),
            0,
            10,
        )
        .await
        .unwrap();
    assert!(hidden_search.ids.is_empty());
    let subject_search = projection_store
        .query_jmap_email_ids(
            FakeStore::account().account_id,
            Some(sent_mailbox_id),
            Some("Transport saved draft"),
            0,
            10,
        )
        .await
        .unwrap();
    assert_eq!(subject_search.ids, vec![sent.id]);
}

#[tokio::test]
async fn mapi_over_http_transport_send_opened_outbox_message_uses_canonical_submission() {
    let outbox_message_id = Uuid::parse_str("30303030-3030-3030-3030-303030303030").unwrap();
    let outbox_mailbox_id = Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap();
    let sent_mailbox_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let mut outbox = FakeStore::email(
        &outbox_message_id.to_string(),
        &outbox_mailbox_id.to_string(),
        "outbox",
        "Transport outbox send",
    );
    outbox.body_text = "Outbox body for transport send".to_string();
    outbox.bcc.push(JmapEmailAddress {
        address: "outbox-hidden@example.test".to_string(),
        display_name: Some("Outbox Hidden".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&outbox_mailbox_id.to_string(), "outbox", "Outbox"),
            FakeStore::mailbox(&sent_mailbox_id.to_string(), "sent", "Sent"),
        ])),
        emails: Arc::new(Mutex::new(vec![outbox])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let submitted_draft_messages = store.submitted_draft_messages.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(7));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(7),
        test_mapi_message_id(&outbox_message_id.to_string()),
    );
    append_rop_transport_send(&mut rops, 2);

    let request = execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x4A, 0x02, 0, 0, 0, 0, 1]));
    assert_eq!(
        submitted_draft_messages.lock().unwrap().as_slice(),
        &[outbox_message_id],
        "opened Outbox messages must use the persisted-source submission API"
    );

    {
        let recorded = submitted_messages.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].source, "mapi-submit-message");
        assert_eq!(recorded[0].draft_message_id, Some(outbox_message_id));
        assert_eq!(recorded[0].subject, "Transport outbox send");
        assert_eq!(recorded[0].body_text, "Outbox body for transport send");
        assert_eq!(recorded[0].bcc[0].address, "outbox-hidden@example.test");
    }

    let canonical = emails.lock().unwrap();
    assert!(canonical.iter().all(|email| email.id != outbox_message_id));
    let sent = canonical
        .iter()
        .find(|email| email.mailbox_role == "sent" && email.subject == "Transport outbox send")
        .expect("submitted outbox message is visible in canonical Sent");
    assert_eq!(sent.mailbox_id, sent_mailbox_id);
    assert_eq!(sent.delivery_status, "queued");
}

#[tokio::test]
async fn mapi_over_http_replayed_execute_request_id_does_not_resubmit_message() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let service = ExchangeService::new(store);
    let (mut execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Retry-safe submit");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Retry body");
    let to_row = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 2, &property_values);
    append_rop_modify_recipients(&mut rops, 2, &[(1, 0x01, to_row.as_slice())]);
    append_rop_submit_message(&mut rops, 2);

    execute_headers.insert(
        "x-requestid",
        HeaderValue::from_static("{11111111-2222-3333-4444-555555555555}:999999"),
    );
    let request = execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX]));
    let first = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    let first_body = response_bytes(first).await;
    let second = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    let second_body = response_bytes(second).await;

    assert_eq!(first_body, second_body);
    let recorded = submitted_messages.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].subject, "Retry-safe submit");
}

#[tokio::test]
async fn mapi_over_http_duplicate_execute_request_id_with_different_body_does_not_resubmit_message()
{
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox("22222222-2222-2222-2222-222222222222", "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    execute_headers.insert(
        "x-requestid",
        HeaderValue::from_static("{11111111-2222-3333-4444-555555555555}:999998"),
    );
    let first_request = mapi_submit_execute_body("Duplicate id first submit");
    let first = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &first_request)
        .await
        .unwrap();
    assert_eq!(first.headers().get("x-responsecode").unwrap(), "0");
    let refreshed_cookie = mapi_cookie_header(&first);

    let mut repeated_headers = execute_headers;
    repeated_headers.insert("cookie", HeaderValue::from_str(&refreshed_cookie).unwrap());
    let repeated = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &repeated_headers,
            &mapi_submit_execute_body("Duplicate id rejected submit"),
        )
        .await
        .unwrap();

    assert_eq!(repeated.status(), StatusCode::OK);
    assert_eq!(repeated.headers().get("x-responsecode").unwrap(), "12");
    assert!(String::from_utf8(response_bytes(repeated).await)
        .unwrap()
        .contains("reused MAPI Execute request id with a different ROP payload"));
    let recorded = submitted_messages.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].subject, "Duplicate id first submit");
    let sent = emails.lock().unwrap();
    assert_eq!(
        sent.iter()
            .filter(|email| email.subject == "Duplicate id first submit")
            .count(),
        1
    );
    assert_eq!(
        sent.iter()
            .filter(|email| email.subject == "Duplicate id rejected submit")
            .count(),
        0
    );
}

#[tokio::test]
async fn mapi_over_http_submit_opened_draft_defers_scheduling_mime_hydration_to_canonical_storage()
{
    let draft_id = Uuid::parse_str("20202020-2020-2020-2020-202020202020").unwrap();
    let draft_mailbox_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let mut draft = FakeStore::email(
        &draft_id.to_string(),
        &draft_mailbox_id.to_string(),
        "drafts",
        "Saved MAPI draft",
    );
    draft.body_text = "Draft body".to_string();
    draft.cc.push(JmapEmailAddress {
        address: "carol@example.test".to_string(),
        display_name: Some("Carol".to_string()),
    });
    draft.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden".to_string()),
    });
    draft.has_attachments = true;
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let attachment_reference = format!("attachment:{draft_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &draft_mailbox_id.to_string(),
            "drafts",
            "Drafts",
        )])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            draft_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id: draft_id,
                file_name: "response.ics".to_string(),
                media_type: "text/calendar; method=REPLY; charset=UTF-8".to_string(),
                disposition: Some("inline".to_string()),
                content_id: None,
                size_octets: 7,
                file_reference: attachment_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            attachment_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: attachment_reference,
                file_name: "response.ics".to_string(),
                media_type: "text/calendar; method=REPLY; charset=UTF-8".to_string(),
                blob_bytes: b"BEGIN:VCALENDAR\r\nMETHOD:REPLY\r\nEND:VCALENDAR\r\n".to_vec(),
                ..Default::default()
            },
        )]))),
        emails: Arc::new(Mutex::new(vec![draft])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let submitted_draft_messages = store.submitted_draft_messages.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Drafts
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(14));
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(14));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(&draft_id.to_string()));
    rops.extend_from_slice(&[
        0x32, 0x00, 0x02, 0x00, // RopSubmitMessage
    ]);

    let request = execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x32, 0x02, 0, 0, 0, 0]));
    assert_eq!(
        submitted_draft_messages.lock().unwrap().as_slice(),
        &[draft_id],
        "RopSubmitMessage on an opened draft must use the persisted-source submission API"
    );
    let recorded = submitted_messages.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].source, "mapi-submit-message");
    assert_eq!(recorded[0].draft_message_id, Some(draft_id));
    assert_eq!(recorded[0].subject, "Saved MAPI draft");
    assert_eq!(recorded[0].body_text, "Draft body");
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    assert_eq!(recorded[0].cc[0].address, "carol@example.test");
    assert_eq!(recorded[0].bcc[0].address, "hidden@example.test");
    assert!(
        recorded[0].attachments.is_empty(),
        "saved MAPI submit must not flatten the canonical source MIME graph"
    );
    let canonical = emails.lock().unwrap();
    let sent = canonical
        .iter()
        .find(|email| email.mailbox_role == "sent")
        .expect("submitted draft is visible in canonical Sent");
    assert!(sent.has_attachments);
}

#[tokio::test]
async fn mapi_over_http_submit_opened_draft_atomically_applies_unsaved_editor_overlay() {
    let draft_id = Uuid::parse_str("21212121-2121-2121-2121-212121212121").unwrap();
    let draft_mailbox_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let deleted_attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let deleted_reference = format!("attachment:{draft_id}:{deleted_attachment_id}");
    let mut draft = FakeStore::email(
        &draft_id.to_string(),
        &draft_mailbox_id.to_string(),
        "drafts",
        "Persisted subject",
    );
    draft.body_text = "Persisted body".to_string();
    draft.has_attachments = true;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &draft_mailbox_id.to_string(),
            "drafts",
            "Drafts",
        )])),
        emails: Arc::new(Mutex::new(vec![draft])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            draft_id,
            vec![ActiveSyncAttachment {
                id: deleted_attachment_id,
                message_id: draft_id,
                file_name: "old.txt".to_string(),
                media_type: "text/plain".to_string(),
                disposition: Some("attachment".to_string()),
                content_id: None,
                size_octets: 3,
                file_reference: deleted_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            deleted_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: deleted_reference,
                file_name: "old.txt".to_string(),
                media_type: "text/plain".to_string(),
                blob_bytes: b"old".to_vec(),
                ..Default::default()
            },
        )]))),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let submitted_source_patches = store.submitted_source_patches.clone();
    let submitted_draft_messages = store.submitted_draft_messages.clone();
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::text(), 0.8));
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut message_properties = Vec::new();
    append_mapi_utf16_property(&mut message_properties, 0x0037_001F, "Unsaved subject");
    append_mapi_utf16_property(&mut message_properties, 0x1000_001F, "Unsaved body");
    append_mapi_utf16_property(&mut message_properties, 0x9001_001F, "provider value");
    let replacement_recipient = mapi_recipient_row("Carol", "carol@example.test", 0x01);
    let mut new_attachment_properties = Vec::new();
    append_mapi_utf16_property(&mut new_attachment_properties, 0x3707_001F, "new.txt");
    append_mapi_utf16_property(&mut new_attachment_properties, 0x370E_001F, "text/plain");
    append_mapi_binary_property(
        &mut new_attachment_properties,
        0x3701_0102,
        b"new attachment",
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(14));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(14),
        test_mapi_message_id(&draft_id.to_string()),
    );
    append_rop_set_properties(&mut rops, 2, 3, &message_properties);
    append_rop_modify_recipients(&mut rops, 2, &[(0, 0x01, replacement_recipient.as_slice())]);
    append_rop_delete_properties(&mut rops, 2, &[0x9002_001F, 0x1090_0003]);
    rops.extend_from_slice(&[0x24, 0x00, 0x02]); // RopDeleteAttachment
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[0x23, 0x00, 0x02, 0x03]); // RopCreateAttachment
    append_rop_set_properties(&mut rops, 3, 3, &new_attachment_properties);
    rops.extend_from_slice(&[0x25, 0x00, 0x02, 0x03, 0x00]); // RopSaveChangesAttachment
    append_rop_submit_message(&mut rops, 2);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[logon_handle, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x32, 0x02, 0, 0, 0, 0]));
    assert!(submitted_draft_messages.lock().unwrap().is_empty());
    let submitted = submitted_messages.lock().unwrap();
    assert_eq!(
        submitted.len(),
        1,
        "unexpected submit inputs: {:?}",
        submitted
            .iter()
            .map(|input| (&input.subject, input.attachments.len()))
            .collect::<Vec<_>>()
    );
    assert_eq!(submitted[0].subject, "Unsaved subject");
    assert_eq!(submitted[0].body_text, "Unsaved body");
    assert_eq!(submitted[0].to.len(), 1);
    assert_eq!(submitted[0].to[0].address, "carol@example.test");
    assert_eq!(submitted[0].attachments.len(), 1);
    assert_eq!(submitted[0].attachments[0].file_name, "new.txt");
    assert_eq!(submitted[0].attachments[0].blob_bytes, b"new attachment");
    drop(submitted);

    let patches = submitted_source_patches.lock().unwrap();
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].expected_source_modseq, Some(41));
    assert_eq!(
        patches[0].delete_attachment_ids,
        vec![deleted_attachment_id]
    );
    assert_eq!(
        patches[0]
            .custom_property_upserts
            .iter()
            .map(|value| value.property_tag)
            .collect::<Vec<_>>(),
        vec![0x9001_001F]
    );
    assert_eq!(patches[0].delete_custom_property_tags, vec![0x9002_001F]);
    let followup = patches[0]
        .canonical_followup_update
        .as_ref()
        .expect("saved DeleteProperties is part of the source patch");
    assert_eq!(followup.flagged, Some(false));
    assert_eq!(followup.followup_flag_status.as_deref(), Some("none"));
}

#[tokio::test]
async fn mapi_over_http_submit_saved_counter_regenerates_only_pending_proposed_time() {
    let draft_id = Uuid::parse_str("31313131-3131-3131-3131-313131313131").unwrap();
    let draft_mailbox_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let transport_attachment_id = Uuid::parse_str("acacacac-acac-acac-acac-acacacacacac").unwrap();
    let transport_reference = format!("attachment:{draft_id}:{transport_attachment_id}");
    let mut draft = FakeStore::email(
        &draft_id.to_string(),
        &draft_mailbox_id.to_string(),
        "drafts",
        "New Time Proposed: Probe overlay",
    );
    draft.to[0].address = "organizer@example.test".to_string();
    draft.to[0].display_name = Some("Organizer".to_string());
    draft.has_attachments = true;
    draft.calendar_meeting_response = Some(lpe_storage::CalendarMeetingResponse {
        method: "COUNTER".to_string(),
        transport_attachment_id: Some(transport_attachment_id),
        server_processed: false,
        organizer: Some(lpe_storage::CalendarMeetingIdentity {
            email: "organizer@example.test".to_string(),
            display_name: "Organizer".to_string(),
        }),
        attendee_email: "alice@example.test".to_string(),
        attendee_name: "Alice".to_string(),
        partstat: "tentative".to_string(),
        uid: "saved-counter-overlay@example.test".to_string(),
        response_sent_at: Some("2026-08-24T08:00:00Z".to_string()),
        meeting_start: Some("2026-08-25T09:00:00Z".to_string()),
        meeting_end: Some("2026-08-25T09:30:00Z".to_string()),
        meeting_location: Some("Room 1".to_string()),
        meeting_sequence: Some(2),
        proposed_start: Some("2026-08-25T10:00:00Z".to_string()),
        proposed_end: Some("2026-08-25T10:30:00Z".to_string()),
        original_start: Some("2026-08-25T09:00:00Z".to_string()),
        original_end: Some("2026-08-25T09:30:00Z".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &draft_mailbox_id.to_string(),
            "drafts",
            "Drafts",
        )])),
        emails: Arc::new(Mutex::new(vec![draft])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            draft_id,
            vec![ActiveSyncAttachment {
                id: transport_attachment_id,
                message_id: draft_id,
                file_name: "response.ics".to_string(),
                media_type: "text/calendar; method=COUNTER; charset=UTF-8".to_string(),
                disposition: Some("inline".to_string()),
                content_id: None,
                size_octets: 64,
                file_reference: transport_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            transport_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: transport_reference,
                file_name: "response.ics".to_string(),
                media_type: "text/calendar; method=COUNTER; charset=UTF-8".to_string(),
                blob_bytes: b"BEGIN:VCALENDAR\r\nMETHOD:COUNTER\r\nEND:VCALENDAR\r\n".to_vec(),
                ..Default::default()
            },
        )]))),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let submitted_source_patches = store.submitted_source_patches.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut proposed_times = Vec::new();
    append_mapi_i64_property(
        &mut proposed_times,
        0x8250_0040,
        test_filetime("2026-08-25", "11:00"),
    );
    append_mapi_i64_property(
        &mut proposed_times,
        0x8251_0040,
        test_filetime("2026-08-25", "11:30"),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(14));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(14),
        test_mapi_message_id(&draft_id.to_string()),
    );
    append_rop_set_properties(&mut rops, 2, 2, &proposed_times);
    append_rop_submit_message(&mut rops, 2);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x32, 0x02, 0, 0, 0, 0]));

    let submitted = submitted_messages.lock().unwrap();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].attachments.len(), 1);
    let calendar = String::from_utf8_lossy(&submitted[0].attachments[0].blob_bytes);
    assert!(calendar.contains("METHOD:COUNTER"));
    assert!(calendar.contains("DTSTART:20260825T110000Z"));
    assert!(calendar.contains("DTEND:20260825T113000Z"));
    assert!(calendar.contains("X-MS-OLK-ORIGINALSTART:20260825T090000Z"));
    drop(submitted);
    assert_eq!(
        submitted_source_patches.lock().unwrap()[0].delete_attachment_ids,
        vec![transport_attachment_id]
    );
}

#[tokio::test]
async fn mapi_over_http_submit_saved_request_regenerates_pending_sequence() {
    let draft_id = Uuid::parse_str("41414141-4141-4141-4141-414141414141").unwrap();
    let draft_mailbox_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let mut draft = FakeStore::email(
        &draft_id.to_string(),
        &draft_mailbox_id.to_string(),
        "drafts",
        "Saved request sequence",
    );
    draft.calendar_invitation = true;
    draft.calendar_meeting_request = Some(lpe_storage::CalendarMeetingRequest {
        uid: "saved-request-sequence@example.test".to_string(),
        transport_attachment_id: None,
        client_processed: false,
        organizer: Some(lpe_storage::CalendarMeetingIdentity {
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
        }),
        attendees: vec![lpe_storage::CalendarMeetingAttendee {
            email: "bob@example.test".to_string(),
            display_name: "Bob".to_string(),
            cutype: "INDIVIDUAL".to_string(),
            role: "REQ-PARTICIPANT".to_string(),
            partstat: "needs-action".to_string(),
            rsvp: true,
        }],
        response_requested: true,
        sent_at: Some("2026-08-24T08:00:00Z".to_string()),
        meeting_start: "2026-08-25T09:00:00Z".to_string(),
        meeting_end: "2026-08-25T09:30:00Z".to_string(),
        meeting_location: Some("Room 1".to_string()),
        meeting_sequence: 2,
        intended_busy_status: 2,
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &draft_mailbox_id.to_string(),
            "drafts",
            "Drafts",
        )])),
        emails: Arc::new(Mutex::new(vec![draft])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let submitted_source_patches = store.submitted_source_patches.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut sequence = Vec::new();
    append_mapi_i32_property(&mut sequence, 0x8201_0003, 3);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(14));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(14),
        test_mapi_message_id(&draft_id.to_string()),
    );
    append_rop_set_properties(&mut rops, 2, 1, &sequence);
    append_rop_submit_message(&mut rops, 2);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x32, 0x02, 0, 0, 0, 0]));

    let submitted = submitted_messages.lock().unwrap();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].attachments.len(), 1);
    let calendar = String::from_utf8_lossy(&submitted[0].attachments[0].blob_bytes);
    assert!(calendar.contains("METHOD:REQUEST"));
    assert!(calendar.contains("SEQUENCE:3"));
    drop(submitted);
    assert_eq!(submitted_source_patches.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn mapi_over_http_submit_saved_request_regenerates_selected_ics_staged_for_deletion() {
    let draft_id = Uuid::parse_str("42424242-4242-4242-4242-424242424242").unwrap();
    let draft_mailbox_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let transport_attachment_id = Uuid::parse_str("aeaeaeae-aeae-aeae-aeae-aeaeaeaeaeae").unwrap();
    let transport_reference = format!("attachment:{draft_id}:{transport_attachment_id}");
    let mut draft = FakeStore::email(
        &draft_id.to_string(),
        &draft_mailbox_id.to_string(),
        "drafts",
        "Saved request selected body deletion",
    );
    draft.calendar_invitation = true;
    draft.has_attachments = true;
    draft.calendar_meeting_request = Some(lpe_storage::CalendarMeetingRequest {
        uid: "saved-request-deleted-body@example.test".to_string(),
        transport_attachment_id: None,
        client_processed: false,
        organizer: Some(lpe_storage::CalendarMeetingIdentity {
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
        }),
        attendees: vec![lpe_storage::CalendarMeetingAttendee {
            email: "bob@example.test".to_string(),
            display_name: "Bob".to_string(),
            cutype: "INDIVIDUAL".to_string(),
            role: "REQ-PARTICIPANT".to_string(),
            partstat: "needs-action".to_string(),
            rsvp: true,
        }],
        response_requested: true,
        sent_at: Some("2026-08-24T08:00:00Z".to_string()),
        meeting_start: "2026-08-25T09:00:00Z".to_string(),
        meeting_end: "2026-08-25T09:30:00Z".to_string(),
        meeting_location: Some("Room 1".to_string()),
        meeting_sequence: 2,
        intended_busy_status: 2,
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &draft_mailbox_id.to_string(),
            "drafts",
            "Drafts",
        )])),
        emails: Arc::new(Mutex::new(vec![draft])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            draft_id,
            vec![ActiveSyncAttachment {
                id: transport_attachment_id,
                message_id: draft_id,
                file_name: "invite.ics".to_string(),
                media_type: "text/calendar; method=REQUEST; charset=UTF-8".to_string(),
                disposition: Some("inline".to_string()),
                content_id: None,
                size_octets: 64,
                file_reference: transport_reference,
            }],
        )]))),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let submitted_source_patches = store.submitted_source_patches.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let (mut execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(14));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(14),
        test_mapi_message_id(&draft_id.to_string()),
    );
    rops.extend_from_slice(&[0x24, 0x00, 0x02]); // RopDeleteAttachment
    rops.extend_from_slice(&0u32.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(
        contains_bytes(&response_rops, &[0x24, 0x02, 0, 0, 0, 0]),
        "unexpected RopDeleteAttachment response: {response_rops:02x?}"
    );

    emails.lock().unwrap()[0]
        .calendar_meeting_request
        .as_mut()
        .unwrap()
        .transport_attachment_id = Some(transport_attachment_id);
    renew_mapi_request_id(&mut execute_headers);
    let mut submit_rops = Vec::new();
    append_rop_open_folder(&mut submit_rops, 0, 1, test_mapi_folder_id(14));
    append_rop_open_message(
        &mut submit_rops,
        1,
        2,
        test_mapi_folder_id(14),
        test_mapi_message_id(&draft_id.to_string()),
    );
    append_rop_submit_message(&mut submit_rops, 2);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &submit_rops,
                &[logon_handle, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x32, 0x02, 0, 0, 0, 0]));

    let submitted = submitted_messages.lock().unwrap();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].attachments.len(), 1);
    let calendar = String::from_utf8_lossy(&submitted[0].attachments[0].blob_bytes);
    assert!(calendar.contains("METHOD:REQUEST"));
    assert!(calendar.contains("UID:saved-request-deleted-body@example.test"));
    drop(submitted);
    assert_eq!(
        submitted_source_patches.lock().unwrap()[0].delete_attachment_ids,
        vec![transport_attachment_id]
    );
}

#[tokio::test]
async fn mapi_over_http_submit_saved_meeting_as_ordinary_deletes_stale_scheduling_body() {
    let draft_id = Uuid::parse_str("51515151-5151-5151-5151-515151515151").unwrap();
    let draft_mailbox_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let transport_attachment_id = Uuid::parse_str("adadadad-adad-adad-adad-adadadadadad").unwrap();
    let transport_reference = format!("attachment:{draft_id}:{transport_attachment_id}");
    let mut draft = FakeStore::email(
        &draft_id.to_string(),
        &draft_mailbox_id.to_string(),
        "drafts",
        "Meeting converted to note",
    );
    draft.calendar_invitation = true;
    draft.has_attachments = true;
    draft.calendar_meeting_request = Some(lpe_storage::CalendarMeetingRequest {
        uid: "meeting-converted-to-note@example.test".to_string(),
        transport_attachment_id: Some(transport_attachment_id),
        client_processed: false,
        organizer: Some(lpe_storage::CalendarMeetingIdentity {
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
        }),
        attendees: vec![lpe_storage::CalendarMeetingAttendee {
            email: "bob@example.test".to_string(),
            display_name: "Bob".to_string(),
            cutype: "INDIVIDUAL".to_string(),
            role: "REQ-PARTICIPANT".to_string(),
            partstat: "needs-action".to_string(),
            rsvp: true,
        }],
        response_requested: true,
        sent_at: Some("2026-08-24T08:00:00Z".to_string()),
        meeting_start: "2026-08-25T09:00:00Z".to_string(),
        meeting_end: "2026-08-25T09:30:00Z".to_string(),
        meeting_location: None,
        meeting_sequence: 2,
        intended_busy_status: 2,
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &draft_mailbox_id.to_string(),
            "drafts",
            "Drafts",
        )])),
        emails: Arc::new(Mutex::new(vec![draft])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            draft_id,
            vec![ActiveSyncAttachment {
                id: transport_attachment_id,
                message_id: draft_id,
                file_name: "invite.ics".to_string(),
                media_type: "text/calendar; method=REQUEST; charset=UTF-8".to_string(),
                disposition: Some("inline".to_string()),
                content_id: None,
                size_octets: 64,
                file_reference: transport_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            transport_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: transport_reference,
                file_name: "invite.ics".to_string(),
                media_type: "text/calendar; method=REQUEST; charset=UTF-8".to_string(),
                blob_bytes: b"BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nEND:VCALENDAR\r\n".to_vec(),
                ..Default::default()
            },
        )]))),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let submitted_source_patches = store.submitted_source_patches.clone();
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut message_class = Vec::new();
    append_mapi_utf16_property(&mut message_class, 0x001A_001F, "IPM.Note");
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(14));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(14),
        test_mapi_message_id(&draft_id.to_string()),
    );
    append_rop_set_properties(&mut rops, 2, 1, &message_class);
    append_rop_submit_message(&mut rops, 2);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x32, 0x02, 0, 0, 0, 0]));
    let submitted = submitted_messages.lock().unwrap();
    assert!(submitted[0].attachments.is_empty());
    assert_eq!(submitted[0].to.len(), 1);
    assert_eq!(submitted[0].to[0].address, "bob@example.test");
    drop(submitted);
    assert_eq!(
        submitted_source_patches.lock().unwrap()[0].delete_attachment_ids,
        vec![transport_attachment_id]
    );
}

#[tokio::test]
async fn mapi_over_http_open_message_returns_visible_recipient_rows() {
    let message_id = "22222222-2222-2222-2222-222222222222";
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(message_id, mailbox_id, "inbox", "Recipient message");
    email.cc.push(JmapEmailAddress {
        address: "carol@example.test".to_string(),
        display_name: Some("Carol".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(5),
        test_mapi_message_id(message_id),
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(response_rops[8], 0x03);
    let mut offset = 8 + 2 + 4 + 1;
    assert_eq!(response_rops[offset], 0x01); // Empty SubjectPrefix TypedString.
    offset += 1;
    assert_eq!(response_rops[offset], 0x04); // Unicode NormalizedSubject TypedString.
    offset += 1;
    assert_eq!(
        read_rop_utf16z(&response_rops, &mut offset).unwrap(),
        "Recipient message"
    );
    assert_eq!(
        u16::from_le_bytes(response_rops[offset..offset + 2].try_into().unwrap()),
        2
    );
    offset += 2;
    let recipient_column_count =
        u16::from_le_bytes(response_rops[offset..offset + 2].try_into().unwrap()) as usize;
    assert_eq!(
        recipient_column_count,
        crate::mapi::MESSAGE_RECIPIENT_COLUMNS.len()
    );
    offset += 2;
    let recipient_columns = response_rops[offset..offset + recipient_column_count * 4]
        .chunks_exact(4)
        .map(|bytes| u32::from_le_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(recipient_columns, crate::mapi::MESSAGE_RECIPIENT_COLUMNS);
    offset += recipient_column_count * 4;
    assert_eq!(response_rops[offset], 2);
    assert!(contains_bytes(
        &response_rops[offset + 1..],
        &utf16z("bob@example.test")
    ));
    assert!(contains_bytes(
        &response_rops[offset + 1..],
        &utf16z("carol@example.test")
    ));
}

#[tokio::test]
async fn mapi_over_http_read_recipients_returns_canonical_message_recipients() {
    let message_id = "22222222-2222-2222-2222-222222222222";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Recipient message",
    );
    email.cc.push(JmapEmailAddress {
        address: "carol@example.test".to_string(),
        display_name: Some("Carol".to_string()),
    });
    email.bcc.push(JmapEmailAddress {
        address: "erin@example.test".to_string(),
        display_name: Some("Erin".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x0F, 0x00, 0x02, // RopReadRecipients
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());

    let request = execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];
    let read_recipients_offset = response_rops
        .windows(6)
        .position(|window| window == [0x0F, 0x02, 0, 0, 0, 0])
        .unwrap();

    assert_eq!(response_rops[read_recipients_offset + 1], 0x02);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[read_recipients_offset + 2..read_recipients_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert!(contains_bytes(response_rops, &0u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &1u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &utf16z("bob@example.test")));
    assert!(contains_bytes(response_rops, &utf16z("Bob")));
    assert!(contains_bytes(response_rops, &utf16z("carol@example.test")));
    assert!(contains_bytes(response_rops, &utf16z("Carol")));
    assert!(!contains_bytes(response_rops, &utf16z("erin@example.test")));
}

#[tokio::test]
async fn mapi_over_http_microsoft_read_recipients_rejects_nonzero_reserved_field() {
    let message_id = "22222222-2222-4222-8222-222222222223";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Reserved ReadRecipients",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x0F, 0x00, 0x02, // RopReadRecipients
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x0F, 0x02, 0x57, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_read_recipients_returns_owner_sent_bcc_but_hides_draft_bcc() {
    // [MS-OXCROPS] section 2.2.6.6: RopReadRecipients returns recipient details for the open message.
    for (message_id, mailbox_id, role, display_name, folder_counter) in [
        (
            "23232323-2323-2323-2323-232323232323",
            "77777777-7777-7777-7777-777777777777",
            "sent",
            "Sent",
            7,
        ),
        (
            "24242424-2424-2424-2424-242424242424",
            "88888888-8888-8888-8888-888888888888",
            "drafts",
            "Drafts",
            14,
        ),
    ] {
        let mut mailbox = FakeStore::mailbox(mailbox_id, role, display_name);
        mailbox.total_emails = 1;
        let mut email = FakeStore::email(message_id, mailbox_id, role, "Owner recipient message");
        email.bcc.push(JmapEmailAddress {
            address: "erin@example.test".to_string(),
            display_name: Some("Erin".to_string()),
        });
        let store = FakeStore {
            session: Some(FakeStore::account()),
            mailboxes: Arc::new(Mutex::new(vec![mailbox])),
            emails: Arc::new(Mutex::new(vec![email])),
            ..Default::default()
        };
        let service = ExchangeService::new(store);
        let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

        let mut rops = vec![
            0x02, 0x00, 0x00, 0x01, // RopOpenFolder
        ];
        append_mapi_wire_id(&mut rops, test_mapi_folder_id(folder_counter));
        rops.push(0);
        rops.extend_from_slice(&[
            0x03, 0x00, 0x01, 0x02, // RopOpenMessage
        ]);
        rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
        append_mapi_wire_id(&mut rops, test_mapi_folder_id(folder_counter));
        rops.push(0);
        append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
        rops.extend_from_slice(&[
            0x0F, 0x00, 0x02, // RopReadRecipients
        ]);
        rops.extend_from_slice(&0u32.to_le_bytes());
        rops.extend_from_slice(&0u16.to_le_bytes());

        let request = execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX]));
        let response = service
            .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
        let body = response_bytes(response).await;
        let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
        let rop_buffer = &body[16..16 + rop_buffer_size];
        let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
        let response_rops = &rop_buffer[2..2 + response_rop_size];

        assert!(contains_bytes(response_rops, &utf16z("bob@example.test")));
        let exposes_bcc = role == "sent";
        assert_eq!(
            contains_bytes(response_rops, &utf16z("erin@example.test")),
            exposes_bcc
        );
        assert_eq!(contains_bytes(response_rops, &utf16z("Erin")), exposes_bcc);
    }
}

#[tokio::test]
async fn mapi_over_http_execute_returns_transport_folder_without_protocol_outbox_state() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let identity_codec =
        crate::mapi::load_mapi_identity_codec_for_test(&store, FakeStore::account().account_id)
            .await
            .unwrap();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut rops = vec![0xFE, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    rops.extend_from_slice(legacy_dn);
    rops.extend_from_slice(&[
        0x6D, 0x00, 0x00, // RopGetTransportFolder against the logon handle.
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let transport_frame: Vec<u8> = {
        let mut expected = vec![0x6D, 0x00, 0, 0, 0, 0];
        append_mapi_wire_id(
            &mut expected,
            identity_codec
                .actual_object_id(crate::mapi::identity::OUTBOX_FOLDER_ID)
                .unwrap(),
        );
        expected
    };
    let transport_offset = response_rops
        .windows(transport_frame.len())
        .position(|window| window == transport_frame.as_slice())
        .unwrap_or_else(|| panic!("transport folder response frame: {response_rops:02x?}"));
    assert_eq!(response_rops[transport_offset], 0x6D);
    assert_eq!(response_rops[transport_offset + 1], 0x00);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[transport_offset + 2..transport_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(
            &response_rops[transport_offset + 6..transport_offset + 14]
        )
        .unwrap(),
        identity_codec
            .actual_object_id(crate::mapi::identity::OUTBOX_FOLDER_ID)
            .unwrap()
    );
}

#[tokio::test]
async fn mapi_over_http_microsoft_transport_spooler_rops_keep_batch_aligned_without_mutation() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let saved_drafts = store.saved_drafts.clone();
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let message_id = test_mapi_message_id("87878787-8787-8787-8787-878787878787");
    let folder_id = test_mapi_folder_id(5);

    let mut rops = mapi_private_logon_rops("alice");
    rops.extend_from_slice(&[0x34, 0x00, 0x00]); // RopAbortSubmit.
    append_mapi_wire_id(&mut rops, folder_id);
    append_mapi_wire_id(&mut rops, message_id);
    rops.extend_from_slice(&[0x47, 0x00, 0x00]); // RopSetSpooler.
    rops.extend_from_slice(&[0x48, 0x00, 0x00]); // RopSpoolerLockMessage.
    rops.extend_from_slice(&message_id.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x51, 0x00, 0x00]); // RopTransportNewMail.
    rops.extend_from_slice(&message_id.to_le_bytes());
    append_mapi_wire_id(&mut rops, folder_id);
    rops.extend_from_slice(b"IPM.Note\0");
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[0x57, 0x00, 0x00]); // RopUpdateDeferredActionMessages.
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&[0x01, 0x02]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&[0x03, 0x04]);
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // RopGetStoreState proves the batch stayed aligned.

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x34, 0x00, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(&response_rops, &[0x47, 0x00, 0, 0, 0, 0]));
    for rop_id in [0x48, 0x51] {
        assert!(
            contains_bytes(&response_rops, &[rop_id, 0x00, 0, 0, 0, 0]),
            "{response_rops:02x?}"
        );
    }
    assert!(contains_bytes(
        &response_rops,
        &[0x57, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(submitted_messages.lock().unwrap().is_empty());
    assert!(saved_drafts.lock().unwrap().is_empty());
    assert!(imported_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_microsoft_abort_submit_cancels_pre_handoff_submission() {
    for status in ["queued", "ready", "deferred", "cancelled"] {
        let (response_rops, cancelled) = abort_submit_response(status).await;
        assert_eq!(cancelled.len(), 1);
        assert!(
            contains_bytes(&response_rops, &[0x34, 0x00, 0, 0, 0, 0]),
            "{response_rops:02x?}; cancelled={cancelled:?}"
        );
    }
}

#[tokio::test]
async fn mapi_over_http_microsoft_abort_submit_rejects_handed_off_and_terminal_submissions() {
    for status in ["handed_off", "relayed", "bounced", "failed"] {
        let (response_rops, cancelled) = abort_submit_response(status).await;
        assert_eq!(cancelled.len(), 1);
        assert!(
            contains_bytes(&response_rops, &[0x34, 0x00, 0x02, 0x01, 0x04, 0x80]),
            "{response_rops:02x?}; cancelled={cancelled:?}"
        );
    }
}
