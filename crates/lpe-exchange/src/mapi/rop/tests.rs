use super::super::transport::MAPI_SESSION_MAX_AGE_SECONDS;
use super::*;

fn test_hex_bytes(value: &str) -> Vec<u8> {
    let hex = value
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect::<String>();
    assert_eq!(hex.len() % 2, 0);
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
        .collect()
}

#[test]
fn split_rop_buffer_accepts_microsoft_spec_framing_examples() {
    let empty = [0x02, 0x00];
    let (requests, handles) = split_rop_buffer(&empty).unwrap();
    assert!(requests.is_empty());
    assert!(handles.is_empty());

    let single = [
        0x09, 0x00, 0x15, 0x01, 0x01, 0x02, 0x01, 0xFF, 0x0F, 0x6D, 0x00, 0x00, 0x00, 0x56, 0x00,
        0x00, 0x00,
    ];
    let (requests, handles) = split_rop_buffer(&single).unwrap();
    assert_eq!(requests, &[0x15, 0x01, 0x01, 0x02, 0x01, 0xFF, 0x0F]);
    assert_eq!(handles, &[0x6D, 0x00, 0x00, 0x00, 0x56, 0x00, 0x00, 0x00]);

    let multiple = [
        0x14, 0x00, 0x02, 0x00, 0x00, 0x01, 0x01, 0x00, 0x59, 0x65, 0x73, 0x73, 0x69, 0x72, 0x00,
        0x04, 0x00, 0x01, 0x02, 0x04, 0x6E, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF,
    ];
    let (requests, handles) = split_rop_buffer(&multiple).unwrap();
    assert_eq!(
        requests,
        &[
            0x02, 0x00, 0x00, 0x01, 0x01, 0x00, 0x59, 0x65, 0x73, 0x73, 0x69, 0x72, 0x00, 0x04,
            0x00, 0x01, 0x02, 0x04,
        ]
    );
    assert_eq!(
        handles,
        &[0x6E, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,]
    );

    let release = [
        0x08, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x6F, 0x00, 0x00, 0x00, 0x6E, 0x00, 0x00,
        0x00,
    ];
    let (requests, handles) = split_rop_buffer(&release).unwrap();
    assert_eq!(requests, &[0x01, 0x00, 0x00, 0x01, 0x00, 0x01]);
    assert_eq!(handles, &[0x6F, 0x00, 0x00, 0x00, 0x6E, 0x00, 0x00, 0x00]);
}

#[test]
fn split_rop_buffer_preserves_legacy_framing_when_handle_table_is_valid() {
    let legacy = rop_buffer_with_response(vec![0x01, 0x00, 0x00], &[0x34]);
    let (requests, handles) = split_rop_buffer(&legacy).unwrap();

    assert_eq!(requests, &[0x01, 0x00, 0x00]);
    assert_eq!(handles, &[0x34, 0x00, 0x00, 0x00]);
}

#[test]
fn message_create_and_save_responses_match_microsoft_message_examples() {
    let create = RopRequest {
        rop_id: RopId::CreateMessage.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: Vec::new(),
    };
    assert_eq!(
        rop_create_message_response(&create),
        vec![0x06, 0x01, 0, 0, 0, 0, 0]
    );

    let save = RopRequest {
        rop_id: RopId::SaveChangesMessage.as_u8(),
        input_handle_index: Some(1),
        output_handle_index: Some(0),
        payload: vec![0x0A],
    };
    assert_eq!(
        rop_save_changes_message_response(&save, 0x3986_F000_0000_0101),
        vec![0x0C, 0x00, 0, 0, 0, 0, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0xF0, 0x86, 0x39,]
    );
}

#[test]
fn attachment_create_and_save_responses_match_microsoft_message_examples() {
    let table = RopRequest {
        rop_id: RopId::GetAttachmentTable.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: vec![0x00],
    };
    assert_eq!(
        rop_get_attachment_table_response(&table),
        vec![0x21, 0x01, 0, 0, 0, 0]
    );

    let create = RopRequest {
        rop_id: RopId::CreateAttachment.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: Some(3),
        payload: Vec::new(),
    };
    assert_eq!(
        rop_create_attachment_response(&create, 1),
        vec![0x23, 0x03, 0, 0, 0, 0, 1, 0, 0, 0]
    );

    let save = RopRequest {
        rop_id: RopId::SaveChangesAttachment.as_u8(),
        input_handle_index: Some(1),
        output_handle_index: Some(2),
        payload: vec![0x0A],
    };
    assert_eq!(
        rop_simple_success_response(&save),
        vec![0x25, 0x02, 0, 0, 0, 0]
    );
}

#[test]
fn microsoft_oxcmsg_core_request_examples_parse_expected_fields() {
    let create_message_golden = vec![
        0x06, 0x00, 0x00, 0x01, 0xFF, 0x0F, 0x01, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x79, 0x93, 0x00,
    ];
    let mut create_message_cursor = Cursor::new(&create_message_golden);
    let create_message = read_rop_request(&mut create_message_cursor).unwrap();

    assert_eq!(
        RopId::from_u8(create_message.rop_id),
        Some(RopId::CreateMessage)
    );
    assert_eq!(create_message.input_handle_index, Some(0));
    assert_eq!(create_message.output_handle_index, Some(1));
    assert_eq!(
        create_message.payload,
        vec![0x01, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x79, 0x93, 0x00]
    );
    assert!(!create_message.create_message_associated());
    assert_eq!(create_message_cursor.remaining(), 0);

    let attachment_table_golden = vec![0x21, 0x00, 0x00, 0x01, 0x00];
    let mut attachment_table_cursor = Cursor::new(&attachment_table_golden);
    let attachment_table = read_rop_request(&mut attachment_table_cursor).unwrap();

    assert_eq!(
        attachment_table.typed(),
        TypedRopRequest::OpenTable(RopOpenTableRequest {
            rop_id: RopId::GetAttachmentTable.as_u8(),
            input_handle_index: 0,
            output_handle_index: 1,
            table_flags: 0,
        })
    );
    assert_eq!(
        serialize_rop_request(&attachment_table).unwrap(),
        attachment_table_golden
    );
    assert_eq!(attachment_table_cursor.remaining(), 0);

    let save_message_golden = vec![0x0C, 0x00, 0x00, 0x01, 0x0A];
    let mut save_message_cursor = Cursor::new(&save_message_golden);
    let save_message = read_rop_request(&mut save_message_cursor).unwrap();

    assert_eq!(
        save_message.typed(),
        TypedRopRequest::SaveChangesMessage(RopSaveChangesMessageRequest {
            response_handle_index: 0,
            input_handle_index: 1,
            save_flags: 0x0A,
        })
    );
    assert_eq!(
        serialize_rop_request(&save_message).unwrap(),
        save_message_golden
    );
    assert_eq!(save_message_cursor.remaining(), 0);
}

#[test]
fn microsoft_oxcmsg_attachment_request_examples_parse_expected_fields() {
    for (golden, output_handle_index) in [
        (vec![0x23, 0x00, 0x00, 0x01], 1),
        (vec![0x23, 0x00, 0x00, 0x03], 3),
    ] {
        let mut cursor = Cursor::new(&golden);
        let request = read_rop_request(&mut cursor).unwrap();

        assert_eq!(
            RopId::from_u8(request.rop_id),
            Some(RopId::CreateAttachment)
        );
        assert_eq!(request.input_handle_index, Some(0));
        assert_eq!(request.output_handle_index, Some(output_handle_index));
        assert!(request.payload.is_empty());
        assert_eq!(cursor.remaining(), 0);
    }

    for (golden, response_handle_index, input_handle_index) in [
        (vec![0x25, 0x00, 0x01, 0x00, 0x0A], 1, 0),
        (vec![0x25, 0x00, 0x02, 0x01, 0x0A], 2, 1),
    ] {
        let mut cursor = Cursor::new(&golden);
        let request = read_rop_request(&mut cursor).unwrap();

        assert_eq!(
            RopId::from_u8(request.rop_id),
            Some(RopId::SaveChangesAttachment)
        );
        assert_eq!(request.input_handle_index, Some(input_handle_index));
        assert_eq!(request.output_handle_index, Some(response_handle_index));
        assert_eq!(request.payload, vec![0x0A]);
        assert_eq!(request.response_handle_index(), response_handle_index);
        assert_eq!(cursor.remaining(), 0);
    }
}

#[test]
fn microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row() {
    let golden = test_hex_bytes(
        "\
            0e00080c000300fe0f030000391f00ff391f00fe390300713a030005391f00f6\
            5f0300fd5f0300ff5f0300de5f0300df5f0201f75f010000000000012701\
            51065a00557365723200750073006500720032000000750073006500720032000000\
            0c0000060000000000000075007300650072003200000075007300650072003200\
            400073007a0066006b0075006b002d0064006f006d002e006500780074006500\
            730074002e006d006900630072006f0073006f00660074002e0063006f006d00\
            000000000000000000407500730065007200320000000100000000000000000000\
            00000000007c0000000000dca740c8c042101ab4b908002b2fe1820100000000\
            0000002f6f3d4669727374204f7267616e697a6174696f6e2f6f753d45786368\
            616e67652041646d696e6973747261746976652047726f757020284659444942\
            4f484632335350444c54292f636e3d526563697069656e74732f636e3d757365\
            723200",
    );
    let mut cursor = Cursor::new(&golden);
    let request = read_rop_request(&mut cursor).unwrap();

    assert_eq!(
        RopId::from_u8(request.rop_id),
        Some(RopId::ModifyRecipients)
    );
    assert_eq!(request.input_handle_index, Some(8));
    assert_eq!(request.output_handle_index, None);
    assert_eq!(request.property_tags().len(), 12);
    assert_eq!(
        request.property_tags(),
        vec![
            0x0FFE_0003,
            0x3900_0003,
            0x39FF_001F,
            0x39FE_001F,
            0x3A71_0003,
            0x3905_0003,
            0x5FF6_001F,
            0x5FFD_0003,
            0x5FFF_0003,
            0x5FDE_0003,
            0x5FDF_0003,
            0x5FF7_0102,
        ]
    );

    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "sender@example.test".to_string(),
        display_name: "Sender".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let changes = request.modify_recipients(&principal, &[]).unwrap();

    assert_eq!(changes.len(), 1);
    let PendingRecipientChange::Upsert(recipient) = &changes[0] else {
        panic!("MS-OXCMSG 4.7 row should upsert one recipient");
    };
    assert_eq!(recipient.recipient_type, 1);
    assert_eq!(recipient.address, "user2@szfkuk-dom.extest.microsoft.com");
    assert_eq!(recipient.display_name.as_deref(), Some("user2"));
    assert_eq!(cursor.remaining(), 0);
}

#[test]
fn folder_create_and_hierarchy_table_responses_match_microsoft_folder_examples() {
    let create = RopRequest {
        rop_id: RopId::CreateFolder.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: Vec::new(),
    };
    assert_eq!(
        rop_create_folder_response(
            &create,
            crate::mapi::identity::mapi_store_id(0x0E91_5212),
            false,
        ),
        vec![0x1C, 0x01, 0, 0, 0, 0, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x91, 0x52, 0x12, 0x00]
    );

    let hierarchy = RopRequest {
        rop_id: RopId::GetHierarchyTable.as_u8(),
        input_handle_index: Some(1),
        output_handle_index: Some(2),
        payload: vec![0x00],
    };
    assert_eq!(
        rop_get_hierarchy_table_response(&hierarchy, 21),
        vec![0x04, 0x02, 0, 0, 0, 0, 0x15, 0x00, 0x00, 0x00]
    );
}

#[test]
fn folder_mutation_responses_match_microsoft_folder_examples() {
    for (rop_id, handle_index, expected) in [
        (
            RopId::DeleteFolder.as_u8(),
            1,
            vec![0x1D, 0x01, 0, 0, 0, 0, 0],
        ),
        (
            RopId::DeleteMessages.as_u8(),
            0,
            vec![0x1E, 0x00, 0, 0, 0, 0, 0],
        ),
        (
            RopId::MoveCopyMessages.as_u8(),
            0,
            vec![0x33, 0x00, 0, 0, 0, 0, 0],
        ),
        (
            RopId::MoveFolder.as_u8(),
            1,
            vec![0x35, 0x01, 0, 0, 0, 0, 0],
        ),
        (
            RopId::CopyFolder.as_u8(),
            0,
            vec![0x36, 0x00, 0, 0, 0, 0, 0],
        ),
    ] {
        let request = RopRequest {
            rop_id,
            input_handle_index: Some(handle_index),
            output_handle_index: None,
            payload: Vec::new(),
        };
        assert_eq!(
            rop_partial_completion_response(rop_id, request.response_handle_index(), false),
            expected
        );
    }

    let set_search = RopRequest {
        rop_id: RopId::SetSearchCriteria.as_u8(),
        input_handle_index: Some(1),
        output_handle_index: None,
        payload: Vec::new(),
    };
    assert_eq!(
        rop_simple_success_response(&set_search),
        vec![0x30, 0x01, 0, 0, 0, 0]
    );
}

#[test]
fn microsoft_oxcfold_create_and_hierarchy_examples_parse_through_typed_parser() {
    let create_golden = vec![
        0x1C, 0x00, 0x00, 0x01, 0x01, 0x01, 0x00, 0x00, 0x46, 0x00, 0x6F, 0x00, 0x6C, 0x00, 0x64,
        0x00, 0x65, 0x00, 0x72, 0x00, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    let mut create_cursor = Cursor::new(&create_golden);
    let create = read_rop_request(&mut create_cursor).unwrap();

    assert_eq!(RopId::from_u8(create.rop_id), Some(RopId::CreateFolder));
    assert_eq!(create.input_handle_index, Some(0));
    assert_eq!(create.output_handle_index, Some(1));
    assert_eq!(create.create_folder_type(), 1);
    assert!(!create.create_folder_open_existing());
    assert_eq!(create.create_folder_reserved(), 0);
    assert_eq!(create.create_folder_display_name(), "Folder1");
    assert_eq!(create.payload.get(13..15), Some(&[0x00, 0x00][..]));
    assert_eq!(create_cursor.remaining(), 0);

    let hierarchy_golden = vec![0x04, 0x00, 0x01, 0x02, 0x00];
    let mut hierarchy_cursor = Cursor::new(&hierarchy_golden);
    let hierarchy = read_rop_request(&mut hierarchy_cursor).unwrap();

    assert_eq!(
        hierarchy.typed(),
        TypedRopRequest::OpenTable(RopOpenTableRequest {
            rop_id: RopId::GetHierarchyTable.as_u8(),
            input_handle_index: 1,
            output_handle_index: 2,
            table_flags: 0,
        })
    );
    assert_eq!(serialize_rop_request(&hierarchy).unwrap(), hierarchy_golden);
    assert_eq!(hierarchy_cursor.remaining(), 0);
}

#[test]
fn microsoft_oxcfold_folder_mutation_examples_parse_expected_fields() {
    let delete_folder_golden = vec![
        0x1D, 0x00, 0x01, 0x05, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xDF, 0x36,
    ];
    let mut delete_folder_cursor = Cursor::new(&delete_folder_golden);
    let delete_folder = read_rop_request(&mut delete_folder_cursor).unwrap();

    assert_eq!(
        RopId::from_u8(delete_folder.rop_id),
        Some(RopId::DeleteFolder)
    );
    assert_eq!(delete_folder.input_handle_index, Some(1));
    assert_eq!(delete_folder.delete_folder_flags(), Some(0x05));
    assert_eq!(
        delete_folder.payload,
        vec![0x05, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xDF, 0x36]
    );
    assert_eq!(delete_folder_cursor.remaining(), 0);

    let delete_messages_golden = vec![
        0x1E, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xF1, 0x48,
        0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xC3, 0x02,
    ];
    let mut delete_messages_cursor = Cursor::new(&delete_messages_golden);
    let delete_messages = read_rop_request(&mut delete_messages_cursor).unwrap();

    assert_eq!(
        RopId::from_u8(delete_messages.rop_id),
        Some(RopId::DeleteMessages)
    );
    assert_eq!(delete_messages.input_handle_index, Some(0));
    assert_eq!(delete_messages.delete_messages_want_asynchronous(), Some(0));
    assert_eq!(delete_messages.delete_messages_notify_non_read(), Some(1));
    assert_eq!(&delete_messages.payload[..4], &[0x00, 0x01, 0x02, 0x00]);
    assert_eq!(delete_messages.payload.len(), 20);
    assert_eq!(delete_messages_cursor.remaining(), 0);

    let move_copy_messages_golden = vec![
        0x33, 0x00, 0x00, 0x01, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xEC, 0x5D, 0x00,
        0x00,
    ];
    let mut move_copy_messages_cursor = Cursor::new(&move_copy_messages_golden);
    let move_copy_messages = read_rop_request(&mut move_copy_messages_cursor).unwrap();

    assert_eq!(
        RopId::from_u8(move_copy_messages.rop_id),
        Some(RopId::MoveCopyMessages)
    );
    assert_eq!(move_copy_messages.input_handle_index, Some(0));
    assert_eq!(move_copy_messages.output_handle_index, Some(1));
    assert_eq!(move_copy_messages.move_copy_want_asynchronous(), Some(0));
    assert_eq!(move_copy_messages.move_copy_want_copy_raw(), Some(0));
    assert!(!move_copy_messages.move_copy_want_copy());
    assert_eq!(
        move_copy_messages.payload,
        vec![0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xEC, 0x5D, 0x00, 0x00]
    );
    assert_eq!(move_copy_messages_cursor.remaining(), 0);
}

#[test]
fn microsoft_oxcfold_folder_move_copy_and_search_examples_parse_expected_fields() {
    let move_folder_golden = vec![
        0x35, 0x00, 0x01, 0x02, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xDF, 0x36, 0x46,
        0x00, 0x6F, 0x00, 0x6C, 0x00, 0x64, 0x00, 0x65, 0x00, 0x72, 0x00, 0x31, 0x00, 0x00, 0x00,
    ];
    let mut move_folder_cursor = Cursor::new(&move_folder_golden);
    let move_folder = read_rop_request(&mut move_folder_cursor).unwrap();

    assert_eq!(RopId::from_u8(move_folder.rop_id), Some(RopId::MoveFolder));
    assert_eq!(move_folder.input_handle_index, Some(1));
    assert_eq!(move_folder.output_handle_index, Some(2));
    assert_eq!(move_folder.folder_move_copy_want_asynchronous(), Some(1));
    assert_eq!(move_folder.folder_move_copy_use_unicode(), Some(1));
    assert_eq!(move_folder.folder_move_copy_display_name(), "Folder1");
    assert_eq!(move_folder_cursor.remaining(), 0);

    let copy_folder_golden = vec![
        0x36, 0x00, 0x00, 0x01, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x0E, 0x8E, 0xDF, 0x36,
        0x46, 0x00, 0x6F, 0x00, 0x6C, 0x00, 0x64, 0x00, 0x65, 0x00, 0x72, 0x00, 0x31, 0x00, 0x00,
        0x00,
    ];
    let mut copy_folder_cursor = Cursor::new(&copy_folder_golden);
    let copy_folder = read_rop_request(&mut copy_folder_cursor).unwrap();

    assert_eq!(RopId::from_u8(copy_folder.rop_id), Some(RopId::CopyFolder));
    assert_eq!(copy_folder.input_handle_index, Some(0));
    assert_eq!(copy_folder.output_handle_index, Some(1));
    assert_eq!(copy_folder.folder_move_copy_want_asynchronous(), Some(1));
    assert_eq!(copy_folder.folder_move_copy_want_recursive(), Some(1));
    assert_eq!(copy_folder.folder_move_copy_use_unicode(), Some(1));
    assert_eq!(copy_folder.folder_move_copy_display_name(), "Folder1");
    assert_eq!(copy_folder_cursor.remaining(), 0);

    let get_search_golden = vec![0x31, 0x00, 0x00, 0x01, 0x01, 0x00];
    let mut get_search_cursor = Cursor::new(&get_search_golden);
    let get_search = read_rop_request(&mut get_search_cursor).unwrap();

    assert_eq!(
        RopId::from_u8(get_search.rop_id),
        Some(RopId::GetSearchCriteria)
    );
    assert_eq!(get_search.input_handle_index, Some(0));
    assert!(get_search.get_search_criteria_use_unicode());
    assert!(get_search.get_search_criteria_include_restriction());
    assert!(!get_search.get_search_criteria_include_folders());
    assert_eq!(get_search_cursor.remaining(), 0);
}

#[test]
fn microsoft_oxcfold_set_search_criteria_example_parses_scope_and_flags() {
    let golden = vec![
        0x30, 0x00, 0x01, 0x29, 0x01, 0x00, 0x02, 0x00, 0x00, 0x07, 0x00, 0x02, 0x03, 0x02, 0x00,
        0x01, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x49, 0x00, 0x50, 0x00, 0x4D,
        0x00, 0x2E, 0x00, 0x41, 0x00, 0x70, 0x00, 0x70, 0x00, 0x6F, 0x00, 0x69, 0x00, 0x6E, 0x00,
        0x74, 0x00, 0x6D, 0x00, 0x65, 0x00, 0x6E, 0x00, 0x74, 0x00, 0x00, 0x00, 0x02, 0x03, 0x02,
        0x00, 0x01, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x49, 0x00, 0x50, 0x00,
        0x4D, 0x00, 0x2E, 0x00, 0x43, 0x00, 0x6F, 0x00, 0x6E, 0x00, 0x74, 0x00, 0x61, 0x00, 0x63,
        0x00, 0x74, 0x00, 0x00, 0x00, 0x02, 0x03, 0x02, 0x00, 0x01, 0x00, 0x1F, 0x00, 0x1A, 0x00,
        0x1F, 0x00, 0x1A, 0x00, 0x49, 0x00, 0x50, 0x00, 0x4D, 0x00, 0x2E, 0x00, 0x44, 0x00, 0x69,
        0x00, 0x73, 0x00, 0x74, 0x00, 0x4C, 0x00, 0x69, 0x00, 0x73, 0x00, 0x74, 0x00, 0x00, 0x00,
        0x02, 0x03, 0x02, 0x00, 0x01, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x49,
        0x00, 0x50, 0x00, 0x4D, 0x00, 0x2E, 0x00, 0x41, 0x00, 0x63, 0x00, 0x74, 0x00, 0x69, 0x00,
        0x76, 0x00, 0x69, 0x00, 0x74, 0x00, 0x79, 0x00, 0x00, 0x00, 0x02, 0x03, 0x02, 0x00, 0x01,
        0x00, 0x1F, 0x00, 0x1A, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x49, 0x00, 0x50, 0x00, 0x4D, 0x00,
        0x2E, 0x00, 0x53, 0x00, 0x74, 0x00, 0x69, 0x00, 0x63, 0x00, 0x6B, 0x00, 0x79, 0x00, 0x4E,
        0x00, 0x6F, 0x00, 0x74, 0x00, 0x65, 0x00, 0x00, 0x00, 0x02, 0x03, 0x00, 0x00, 0x01, 0x00,
        0x1F, 0x00, 0x1A, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x49, 0x00, 0x50, 0x00, 0x4D, 0x00, 0x2E,
        0x00, 0x54, 0x00, 0x61, 0x00, 0x73, 0x00, 0x6B, 0x00, 0x00, 0x00, 0x02, 0x03, 0x02, 0x00,
        0x01, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x1F, 0x00, 0x1A, 0x00, 0x49, 0x00, 0x50, 0x00, 0x4D,
        0x00, 0x2E, 0x00, 0x54, 0x00, 0x61, 0x00, 0x73, 0x00, 0x6B, 0x00, 0x2E, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x04, 0x04, 0x03, 0x00, 0x17, 0x00, 0x03, 0x00, 0x17, 0x00, 0x02, 0x00,
        0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x88, 0x2A, 0x00, 0x02,
        0x00,
    ];
    let mut cursor = Cursor::new(&golden);
    let request = read_rop_request(&mut cursor).unwrap();

    assert_eq!(
        RopId::from_u8(request.rop_id),
        Some(RopId::SetSearchCriteria)
    );
    assert_eq!(request.input_handle_index, Some(1));
    assert_eq!(request.payload.get(..2), Some(&[0x29, 0x01][..]));
    assert_eq!(request.payload.get(299..301), Some(&[0x01, 0x00][..]));
    assert_eq!(
        request.payload.get(301..309),
        Some(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x88][..])
    );
    assert_eq!(
        request.payload.get(309..313),
        Some(&[0x2A, 0x00, 0x02, 0x00][..])
    );
    assert_eq!(request.payload.len(), 313);
    assert_eq!(cursor.remaining(), 0);
}

#[test]
fn contents_table_responses_match_microsoft_table_examples() {
    let open = RopRequest {
        rop_id: RopId::GetContentsTable.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: vec![0],
    };
    assert_eq!(
        rop_get_contents_table_response(&open, 4),
        vec![0x05, 0x01, 0, 0, 0, 0, 4, 0, 0, 0]
    );

    let set_columns = RopRequest {
        rop_id: RopId::SetColumns.as_u8(),
        input_handle_index: Some(1),
        output_handle_index: None,
        payload: Vec::new(),
    };
    assert_eq!(
        rop_set_columns_response(&set_columns),
        vec![0x12, 0x01, 0, 0, 0, 0, 0]
    );

    let sort = RopRequest {
        rop_id: RopId::SortTable.as_u8(),
        input_handle_index: Some(1),
        output_handle_index: None,
        payload: Vec::new(),
    };
    assert_eq!(
        rop_sort_table_response(&sort),
        vec![0x13, 0x01, 0, 0, 0, 0, 0]
    );
}

#[test]
fn expand_row_response_matches_microsoft_category_example() {
    let request = RopRequest {
        rop_id: RopId::ExpandRow.as_u8(),
        input_handle_index: Some(1),
        output_handle_index: None,
        payload: Vec::new(),
    };

    assert_eq!(
        rop_expand_row_success_response(&request, 3, Vec::new()),
        vec![0x59, 0x01, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0]
    );
}

#[test]
fn microsoft_oxcrops_rop_buffer_request_examples_parse_expected_fields() {
    fn split_rop_buffer(buffer: &[u8]) -> (&[u8], &[u8]) {
        let rop_size = u16::from_le_bytes(buffer[0..2].try_into().unwrap()) as usize;
        (&buffer[2..rop_size], &buffer[rop_size..])
    }

    let empty = [0x02, 0x00];
    let (rops, handles) = split_rop_buffer(&empty);
    assert!(rops.is_empty());
    assert!(handles.is_empty());

    let single = [
        0x09, 0x00, 0x15, 0x01, 0x01, 0x02, 0x01, 0xFF, 0x0F, 0x6D, 0x00, 0x00, 0x00, 0x56, 0x00,
        0x00, 0x00,
    ];
    let (single_rops, single_handles) = split_rop_buffer(&single);
    let mut cursor = Cursor::new(single_rops);
    let query_rows = read_rop_request(&mut cursor).unwrap();
    assert_eq!(
        query_rows.typed(),
        TypedRopRequest::QueryRows(RopQueryRowsRequest {
            input_handle_index: 1,
            flags: 2,
            forward_read: true,
            row_count: 0x0FFF,
        })
    );
    let mut serialized_query_rows = serialize_rop_request(&query_rows).unwrap();
    // RopRequest does not retain LogonId; this assertion still verifies every modeled field.
    serialized_query_rows[1] = single_rops[1];
    assert_eq!(serialized_query_rows, single_rops);
    assert_eq!(cursor.remaining(), 0);
    assert_eq!(
        single_handles
            .chunks_exact(4)
            .map(|handle| u32::from_le_bytes(handle.try_into().unwrap()))
            .collect::<Vec<_>>(),
        vec![0x6D, 0x56]
    );

    let multiple = [
        0x14, 0x00, 0x02, 0x00, 0x00, 0x01, 0x01, 0x00, 0x59, 0x65, 0x73, 0x73, 0x69, 0x72, 0x00,
        0x04, 0x00, 0x01, 0x02, 0x04, 0x6E, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF,
    ];
    let (multiple_rops, multiple_handles) = split_rop_buffer(&multiple);
    let mut cursor = Cursor::new(multiple_rops);
    let open_folder = read_rop_request(&mut cursor).unwrap();
    let hierarchy_table = read_rop_request(&mut cursor).unwrap();
    assert_eq!(
        open_folder.typed(),
        TypedRopRequest::OpenFolder(RopOpenFolderRequest {
            input_handle_index: 0,
            output_handle_index: 1,
            folder_id: 0x5965_7373_6972_0001,
            open_mode_flags: 0,
        })
    );
    assert_eq!(
        hierarchy_table.typed(),
        TypedRopRequest::OpenTable(RopOpenTableRequest {
            rop_id: RopId::GetHierarchyTable.as_u8(),
            input_handle_index: 1,
            output_handle_index: 2,
            table_flags: 4,
        })
    );
    let mut serialized = serialize_rop_request(&open_folder).unwrap();
    serialized.extend_from_slice(&serialize_rop_request(&hierarchy_table).unwrap());
    assert_eq!(serialized, multiple_rops);
    assert_eq!(cursor.remaining(), 0);
    assert_eq!(
        multiple_handles
            .chunks_exact(4)
            .map(|handle| u32::from_le_bytes(handle.try_into().unwrap()))
            .collect::<Vec<_>>(),
        vec![0x6E, u32::MAX, u32::MAX]
    );

    let sync_then_upload = [
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x02, 0x09, 0x01, 0x01, // type, send options, flags
        0x00, 0x00, // RestrictionDataSize
        0x01, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
        0x17, 0x40, 0x00, 0x03, // MetaTagIdsetGiven
        0x00, 0x00, 0x00, 0x00, // stream size
    ];
    let mut cursor = Cursor::new(&sync_then_upload);
    let sync_configure = read_rop_request(&mut cursor).unwrap();
    let upload_begin = read_rop_request(&mut cursor).unwrap();
    assert_eq!(
        sync_configure.rop_id,
        RopId::SynchronizationConfigure.as_u8()
    );
    assert_eq!(
        sync_configure.payload,
        vec![0x02, 0x09, 0x01, 0x01, 0, 0, 1, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        upload_begin.rop_id,
        RopId::SynchronizationUploadStateStreamBegin.as_u8()
    );
    assert_eq!(cursor.remaining(), 0);

    let release_pair = [
        0x08, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x6F, 0x00, 0x00, 0x00, 0x6E, 0x00, 0x00,
        0x00,
    ];
    let (release_rops, release_handles) = split_rop_buffer(&release_pair);
    let mut cursor = Cursor::new(release_rops);
    let first_release = read_rop_request(&mut cursor).unwrap();
    let second_release = read_rop_request(&mut cursor).unwrap();
    assert_eq!(
        first_release.typed(),
        TypedRopRequest::Release(RopInputOnlyRequest {
            rop_id: RopId::Release.as_u8(),
            input_handle_index: 0,
        })
    );
    assert_eq!(
        second_release.typed(),
        TypedRopRequest::Release(RopInputOnlyRequest {
            rop_id: RopId::Release.as_u8(),
            input_handle_index: 1,
        })
    );
    let mut serialized = serialize_rop_request(&first_release).unwrap();
    serialized.extend_from_slice(&serialize_rop_request(&second_release).unwrap());
    assert_eq!(serialized, release_rops);
    assert_eq!(cursor.remaining(), 0);
    assert_eq!(
        release_handles
            .chunks_exact(4)
            .map(|handle| u32::from_le_bytes(handle.try_into().unwrap()))
            .collect::<Vec<_>>(),
        vec![0x6F, 0x6E]
    );
}

#[test]
fn buffer_too_small_response_matches_microsoft_rop_layout() {
    let request = [
        0x03, 0x00, 0x00, 0x01, 0xFF, 0x0F, 0x01, 0x00, 0x15, 0x89, 0x00, 0x78, 0x27, 0x1E, 0x03,
        0x01, 0x00, 0x15, 0x89, 0x00, 0x78, 0x2F, 0xBB,
    ];
    let handles = [0x12, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF];

    let response = rop_buffer_too_small_response(0x002C, &request, &handles);

    assert_eq!(&response[..3], &[0x1C, 0x00, 0xFF]);
    assert_eq!(&response[3..5], &0x002Cu16.to_le_bytes());
    assert_eq!(&response[5..28], request.as_slice());
    assert_eq!(&response[28..], handles.as_slice());
}

#[test]
fn backoff_response_matches_microsoft_logon_example() {
    let set_columns = RopRequest {
        rop_id: RopId::SetColumns.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: Vec::new(),
    };
    let mut response = rop_set_columns_response(&set_columns);
    response.extend_from_slice(&rop_backoff_response(0, 0x1234, &[], &[]));

    assert_eq!(
        rop_buffer_with_response_spec(response, &[0x28]),
        vec![
            0x12, 0x00, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF9, 0x00, 0x34, 0x12, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00,
        ]
    );
}

#[test]
fn backoff_response_matches_microsoft_targeted_rop_example() {
    let open_folder = RopRequest {
        rop_id: RopId::OpenFolder.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: Vec::new(),
    };
    let mut response = rop_open_folder_response(&open_folder, false);
    response.extend_from_slice(&rop_backoff_response(0, 0, &[(0x1C, 0x0004_4F17)], &[]));

    assert_eq!(
        rop_buffer_with_response_spec(response, &[0x0A, 0x24]),
        vec![
            0x18, 0x00, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF9, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x1C, 0x17, 0x4F, 0x04, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00,
            0x24, 0x00, 0x00, 0x00,
        ]
    );
}

#[test]
fn get_properties_specific_returns_typed_value_for_unspecified_subject() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
        email: "alice@example.test".to_string(),
        display_name: "Alice".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mailbox_id = Uuid::from_u128(0x11111111111111111111111111111111);
    let email_id = Uuid::from_u128(0x22222222222222222222222222222222);
    let message_id = crate::mapi::identity::mapi_store_id(0x2222);
    crate::mapi::identity::remember_mapi_identity(mailbox_id, INBOX_FOLDER_ID);
    crate::mapi::identity::remember_mapi_identity(email_id, message_id);
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 10,
        modseq: 1,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let emails = vec![JmapEmail {
        id: email_id,
        thread_id: email_id,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: Vec::new(),
        mailbox_id,
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 7,
        received_at: "2026-06-07T19:56:00Z".to_string(),
        sent_at: None,
        from_address: "sender@example.test".to_string(),
        from_display: Some("Sender".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "author".to_string(),
        submitted_by_account_id: Uuid::nil(),
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Hello".to_string(),
        preview: String::new(),
        body_text: "Body".to_string(),
        body_html_sanitized: None,
        unread: false,
        flagged: false,
        followup_flag_status: "none".to_string(),
        followup_icon: 0,
        todo_item_flags: 0,
        followup_request: String::new(),
        followup_start_at: None,
        followup_due_at: None,
        followup_completed_at: None,
        reminder_set: false,
        reminder_at: None,
        reminder_dismissed_at: None,
        swapped_todo_store_id: None,
        swapped_todo_data: None,
        categories: Vec::new(),
        has_attachments: false,
        size_octets: 512,
        internet_message_id: Some("<hello@example.test>".to_string()),
        mime_blob_ref: None,
        delivery_status: "delivered".to_string(),
    }];
    let mut payload = Vec::new();
    write_u16(&mut payload, 4096);
    write_u16(&mut payload, 2);
    write_u32(&mut payload, PID_TAG_MESSAGE_FLAGS);
    write_u32(&mut payload, 0x0037_0001);
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(3),
        output_handle_index: None,
        payload,
    };
    let object = MapiObject::Message {
        folder_id: INBOX_FOLDER_ID,
        message_id,
        saved_email: None,
        pending_properties: HashMap::new(),
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &mailboxes,
        &emails,
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..7], &[0x07, 0x03, 0, 0, 0, 0, 0x01]);
    assert_eq!(response[7], 0);
    assert_eq!(&response[12..15], &[0x1F, 0x00, 0]);
    assert_eq!(&response[15..], utf16z_bytes("Hello").as_slice());
}

#[test]
fn get_properties_specific_resolves_unspecified_modeled_message_properties() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
        email: "alice@example.test".to_string(),
        display_name: "Alice".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mailbox_id = Uuid::from_u128(0x11111111111111111111111111111111);
    let email_id = Uuid::from_u128(0x33333333333333333333333333333333);
    let message_id = crate::mapi::identity::mapi_store_id(0x3333);
    crate::mapi::identity::remember_mapi_identity(mailbox_id, INBOX_FOLDER_ID);
    crate::mapi::identity::remember_mapi_identity(email_id, message_id);
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 10,
        modseq: 1,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let emails = vec![JmapEmail {
        id: email_id,
        thread_id: email_id,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: Vec::new(),
        mailbox_id,
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 7,
        received_at: "2026-06-07T19:56:00Z".to_string(),
        sent_at: None,
        from_address: "sender@example.test".to_string(),
        from_display: Some("Sender".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "author".to_string(),
        submitted_by_account_id: Uuid::nil(),
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Unspecified".to_string(),
        preview: String::new(),
        body_text: "Body".to_string(),
        body_html_sanitized: None,
        unread: false,
        flagged: false,
        followup_flag_status: "none".to_string(),
        followup_icon: 0,
        todo_item_flags: 0,
        followup_request: String::new(),
        followup_start_at: None,
        followup_due_at: None,
        followup_completed_at: None,
        reminder_set: false,
        reminder_at: None,
        reminder_dismissed_at: None,
        swapped_todo_store_id: None,
        swapped_todo_data: None,
        categories: Vec::new(),
        has_attachments: true,
        size_octets: 2048,
        internet_message_id: Some("<unspecified@example.test>".to_string()),
        mime_blob_ref: None,
        delivery_status: "delivered".to_string(),
    }];
    let mut payload = Vec::new();
    write_u16(&mut payload, 4096);
    write_u16(&mut payload, 3);
    write_u32(&mut payload, 0x0E1B_0000);
    write_u32(&mut payload, 0x0E08_0000);
    write_u32(&mut payload, 0x1035_0000);
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(3),
        output_handle_index: None,
        payload,
    };
    let object = MapiObject::Message {
        folder_id: INBOX_FOLDER_ID,
        message_id,
        saved_email: None,
        pending_properties: HashMap::new(),
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &mailboxes,
        &emails,
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..7], &[0x07, 0x03, 0, 0, 0, 0, 0x01]);
    let mut offset = 7;
    assert_eq!(
        u16::from_le_bytes(response[offset..offset + 2].try_into().unwrap()),
        0x000B
    );
    offset += 2;
    assert_eq!(&response[offset..offset + 2], &[0, 1]);
    offset += 2;
    assert_eq!(
        u16::from_le_bytes(response[offset..offset + 2].try_into().unwrap()),
        0x0003
    );
    offset += 2;
    assert_eq!(response[offset], 0);
    offset += 1;
    assert_eq!(
        u32::from_le_bytes(response[offset..offset + 4].try_into().unwrap()),
        2048
    );
    offset += 4;
    assert_eq!(
        u16::from_le_bytes(response[offset..offset + 2].try_into().unwrap()),
        0x001F
    );
    offset += 2;
    assert_eq!(response[offset], 0);
    offset += 1;
    assert_eq!(
        &response[offset..],
        utf16z_bytes("<unspecified@example.test>").as_slice()
    );
}

#[test]
fn common_view_descriptor_getprops_contract_reports_current_inbox_compact_shape() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::from_u128(0x9191),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let view_id = crate::mapi_store::outlook_default_folder_named_view_id(INBOX_FOLDER_ID);
    let object = MapiObject::CommonViewNamedView {
        folder_id: INBOX_FOLDER_ID,
        view_id,
    };
    let columns = [
        OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835,
        OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C,
    ];

    let contract = format_common_view_descriptor_getprops_contract(
        Some(&object),
        &principal,
        &columns,
        &MapiMailStoreSnapshot::empty(),
    );

    assert!(contract.contains("found=true"));
    assert!(contract.contains("view_name=Compact"));
    assert!(contract.contains("descriptor_column_count=11"));
    assert!(contract.contains("descriptor_strings_terminators=11"));
    assert!(contract.contains("descriptor_strings_starts_with_terminator=true"));
    assert!(contract.contains("descriptor_strings_ends_with_terminator=true"));
    assert!(contract.contains("descriptor_strings_trailing_nul=false"));
    assert!(
        contract.contains("0x68350102:OutlookCommonViewDescriptorBinary6835:binary_bytes=510"),
        "{contract}"
    );
    assert!(
        contract.contains("0x683c0102:OutlookCommonViewDescriptorStrings683C:binary_bytes=174"),
        "{contract}"
    );
}

#[test]
fn get_properties_specific_returns_not_enough_memory_for_size_limited_value() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "sender@example.test".to_string(),
        display_name: "Sender".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_SUBJECT_W,
        MapiValue::String("Large subject".to_string()),
    );
    let object = MapiObject::PendingMessage {
        folder_id: DRAFTS_FOLDER_ID,
        properties,
        recipients: Vec::new(),
    };
    let mut payload = Vec::new();
    write_u16(&mut payload, 4);
    write_u16(&mut payload, 1);
    write_u32(&mut payload, PID_TAG_SUBJECT_W);
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(3),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..6], &[0x07, 0x03, 0, 0, 0, 0]);
    assert_eq!(response[6], 0x01);
    assert_eq!(response[7], 0x0A);
    assert_eq!(
        u32::from_le_bytes(response[8..12].try_into().unwrap()),
        0x8007_000E
    );
}

#[test]
fn get_properties_specific_size_limit_preserves_unspecified_property_type() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "sender@example.test".to_string(),
        display_name: "Sender".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_SUBJECT_W,
        MapiValue::String("Large subject".to_string()),
    );
    let object = MapiObject::PendingMessage {
        folder_id: DRAFTS_FOLDER_ID,
        properties,
        recipients: Vec::new(),
    };
    let mut payload = Vec::new();
    write_u16(&mut payload, 4);
    write_u16(&mut payload, 1);
    write_u32(&mut payload, 0x0037_0001);
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(3),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..6], &[0x07, 0x03, 0, 0, 0, 0]);
    assert_eq!(response[6], 0x01);
    assert_eq!(&response[7..9], &0x001Fu16.to_le_bytes());
    assert_eq!(response[9], 0x0A);
    assert_eq!(
        u32::from_le_bytes(response[10..14].try_into().unwrap()),
        0x8007_000E
    );
}

#[test]
pub(in crate::mapi) fn session_idle_expiry_follows_cookie_max_age() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
    let fresh = MapiSession {
        endpoint: MapiEndpoint::Emsmdb,
        tenant_id: Uuid::from_u128(0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa),
        account_id: Uuid::nil(),
        email: "user@example.test".to_string(),
        created_at: now,
        last_seen_at: now - Duration::from_secs(u64::from(MAPI_SESSION_MAX_AGE_SECONDS)),
        first_request_type: "Connect".to_string(),
        first_request_id: "test:1".to_string(),
        last_request_type: "Connect".to_string(),
        last_request_id: "test:1".to_string(),
        request_count: 1,
        execute_request_count: 0,
        next_handle: 1,
        handles: HashMap::new(),
        message_statuses: HashMap::new(),
        message_save_generations: HashMap::new(),
        message_handle_generations: HashMap::new(),
        pending_message_recipient_replacements: HashMap::new(),
        pending_message_attachments: HashMap::new(),
        pending_attachment_parent_messages: HashMap::new(),
        pending_attachment_deletions: HashSet::new(),
        pending_embedded_message_ids: HashMap::new(),
        pending_embedded_message_attachments: HashMap::new(),
        saved_embedded_messages: HashMap::new(),
        saved_search_folder_definitions: HashMap::new(),
        special_folder_aliases: HashMap::new(),
        deleted_advertised_special_folders: HashSet::new(),
        deleted_search_folder_definitions: HashSet::new(),
        named_properties: HashMap::new(),
        named_property_ids: HashMap::new(),
        next_named_property_id: FIRST_NAMED_PROPERTY_ID,
        next_local_replica_sequence: 1,
        notification_cursor: None,
        pending_notifications: VecDeque::new(),
        completed_execute_requests: HashMap::new(),
        completed_execute_request_order: VecDeque::new(),
        post_hierarchy_actions: PostHierarchyActionState::default(),
        default_view_advertisements: HashMap::new(),
        inbox_associated_config_stream_handles: HashSet::new(),
        inbox_rule_organizer_stream_handles: HashSet::new(),
        logon_identity: None,
        outlook_smart_input_variant: "none".to_string(),
        outlook_smart_input_variant_applied: false,
    };
    let stale = MapiSession {
        last_seen_at: now - Duration::from_secs(u64::from(MAPI_SESSION_MAX_AGE_SECONDS) + 1),
        ..fresh.clone()
    };

    assert!(!session_is_expired(&fresh, now));
    assert!(session_is_expired(&stale, now));
}

#[test]
pub(in crate::mapi) fn logon_time_bytes_encode_valid_utc_calendar_fields() {
    let bytes = logon_time_bytes(SystemTime::UNIX_EPOCH + Duration::from_secs(1_778_046_495));

    assert_eq!(bytes, [15, 48, 5, 3, 6, 5, 0xEA, 0x07]);
}

#[test]
pub(in crate::mapi) fn create_folder_private_response_stops_after_non_existing_flag() {
    let request = RopRequest {
        rop_id: RopId::CreateFolder.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: Vec::new(),
    };

    let created = rop_create_folder_response(&request, QUICK_STEP_SETTINGS_FOLDER_ID, false);
    assert_eq!(created.len(), 15);
    assert_eq!(created[14], 0);

    let existing = rop_create_folder_response(&request, QUICK_STEP_SETTINGS_FOLDER_ID, true);
    assert_eq!(existing.len(), 16);
    assert_eq!(existing[14], 1);
}

#[test]
fn open_message_response_does_not_advertise_missing_recipient_rows() {
    let request = RopRequest {
        rop_id: RopId::OpenMessage.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: Some(2),
        payload: Vec::new(),
    };

    let response = rop_open_message_response(&request, "Subject", 3);

    assert_eq!(response[0], RopId::OpenMessage.as_u8());
    assert_eq!(response[1], 2);
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(&response[response.len() - 5..response.len() - 3], &[3, 0]);
    assert_eq!(&response[response.len() - 3..response.len() - 1], &[0, 0]);
    assert_eq!(response[response.len() - 1], 0);
}

#[test]
fn microsoft_reload_cached_information_matches_open_message_shape() {
    let reload_request = RopRequest {
        rop_id: RopId::ReloadCachedInformation.as_u8(),
        input_handle_index: Some(2),
        output_handle_index: None,
        payload: Vec::new(),
    };
    let open_request = RopRequest {
        rop_id: RopId::OpenMessage.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: Some(2),
        payload: Vec::new(),
    };
    let object = MapiObject::PendingMessage {
        folder_id: INBOX_FOLDER_ID,
        properties: HashMap::from([(PID_TAG_SUBJECT_W, MapiValue::String("Subject".into()))]),
        recipients: vec![PendingRecipient {
            row_id: 0,
            recipient_type: 1,
            address: "alice@example.test".into(),
            display_name: Some("Alice".into()),
        }],
    };
    let snapshot = MapiMailStoreSnapshot::empty();

    let response =
        rop_reload_cached_information_response(&reload_request, Some(&object), &[], &[], &snapshot);
    let open_response = rop_open_message_response(&open_request, "Subject", 1);

    assert_eq!(response[0], RopId::ReloadCachedInformation.as_u8());
    assert_eq!(response[1], 2);
    assert_eq!(&response[2..], &open_response[2..]);
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(&response[response.len() - 5..response.len() - 3], &[1, 0]);
    assert_eq!(&response[response.len() - 3..response.len() - 1], &[0, 0]);
    assert_eq!(response[response.len() - 1], 0);
}

#[test]
fn modify_recipients_accepts_microsoft_message_example_columns() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let columns = [
        PID_TAG_OBJECT_TYPE,
        PID_TAG_DISPLAY_TYPE,
        PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W,
        PID_TAG_SMTP_ADDRESS_W,
        PID_TAG_SEND_INTERNET_ENCODING,
        PID_TAG_DISPLAY_TYPE_EX,
        PID_TAG_RECIPIENT_DISPLAY_NAME_W,
        PID_TAG_RECIPIENT_FLAGS,
        PID_TAG_RECIPIENT_TRACK_STATUS,
        OUTLOOK_RECIPIENT_5FDE,
        PID_TAG_RECIPIENT_ORDER,
        PID_TAG_RECIPIENT_ENTRY_ID,
    ];
    let mut row = Vec::new();
    write_u16(&mut row, 0x0651);
    row.push(b'Z');
    row.push(0);
    write_ascii_z(&mut row, "User2");
    write_utf16z(&mut row, "User2");
    write_utf16z(&mut row, "user2");
    write_u16(&mut row, columns.len() as u16);
    row.push(0);
    for (tag, value) in [
        (PID_TAG_OBJECT_TYPE, MapiValue::U32(6)),
        (PID_TAG_DISPLAY_TYPE, MapiValue::U32(0)),
        (
            PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W,
            MapiValue::String("user2".to_string()),
        ),
        (
            PID_TAG_SMTP_ADDRESS_W,
            MapiValue::String("user2@szfkuk-dom.extest.microsoft.com".to_string()),
        ),
        (PID_TAG_SEND_INTERNET_ENCODING, MapiValue::U32(0)),
        (PID_TAG_DISPLAY_TYPE_EX, MapiValue::U32(0x4000_0000)),
        (
            PID_TAG_RECIPIENT_DISPLAY_NAME_W,
            MapiValue::String("user2".to_string()),
        ),
        (PID_TAG_RECIPIENT_FLAGS, MapiValue::U32(1)),
        (PID_TAG_RECIPIENT_TRACK_STATUS, MapiValue::U32(0)),
        (OUTLOOK_RECIPIENT_5FDE, MapiValue::U32(0)),
        (PID_TAG_RECIPIENT_ORDER, MapiValue::U32(0)),
        (PID_TAG_RECIPIENT_ENTRY_ID, MapiValue::Binary(vec![0; 124])),
    ] {
        write_mapi_value(&mut row, tag, &value);
    }

    let recipient = parse_pending_recipient_row(0, 1, &columns, &row, &principal, &[]).unwrap();

    assert_eq!(recipient.row_id, 0);
    assert_eq!(recipient.recipient_type, 1);
    assert_eq!(recipient.address, "user2@szfkuk-dom.extest.microsoft.com");
    assert_eq!(recipient.display_name.as_deref(), Some("user2"));
}

#[test]
pub(in crate::mapi) fn gwart_time_marker_uses_real_timestamp_and_stays_nonzero() {
    assert_eq!(
        gwart_time_marker(SystemTime::UNIX_EPOCH + Duration::from_secs(1_778_046_495)),
        1_778_046_495
    );
    assert_eq!(gwart_time_marker(SystemTime::UNIX_EPOCH), 1);
}

#[test]
pub(in crate::mapi) fn property_debug_names_cover_recent_outlook_folder_probes() {
    assert_eq!(
        property_tag_debug_name(PID_TAG_LOCAL_COMMIT_TIME_MAX),
        "PidTagLocalCommitTimeMax"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_DELETED_COUNT_TOTAL),
        "PidTagDeletedCountTotal"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_CONTENT_UNREAD_COUNT),
        "PidTagContentUnreadCount"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_CONTENT_COUNT),
        "PidTagContentCount"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_PARENT_FOLDER_ID),
        "PidTagParentFolderId"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_SUBFOLDERS),
        "PidTagSubfolders"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_FOLDER_TYPE),
        "PidTagFolderType"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_CHANGE_KEY),
        "PidTagChangeKey"
    );
    assert_eq!(property_tag_debug_name(PID_TAG_ACCESS), "PidTagAccess");
    assert_eq!(
        property_tag_debug_name(PID_TAG_CONVERSATION_TOPIC_W),
        "PidTagConversationTopic"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_CONVERSATION_INDEX),
        "PidTagConversationIndex"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_MESSAGE_CLASS_W),
        "PidTagMessageClass"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_ORIGINAL_MESSAGE_CLASS_W),
        "PidTagOriginalMessageClass"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_ACCESS_LEVEL),
        "PidTagAccessLevel"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_SENDER_ADDRESS_TYPE_W),
        "PidTagSenderAddressType"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_SENDER_SMTP_ADDRESS_W),
        "PidTagSenderSmtpAddress"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_MESSAGE_STATUS),
        "PidTagMessageStatus"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_SEARCH_KEY),
        "PidTagSearchKey"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_DISPLAY_BCC_W),
        "PidTagDisplayBcc"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_DISPLAY_TO_W),
        "PidTagDisplayTo"
    );
    assert_eq!(property_tag_debug_name(PID_TAG_SUBJECT_W), "PidTagSubject");
    assert_eq!(
        property_tag_debug_name(PID_TAG_SUBJECT_PREFIX_W),
        "PidTagSubjectPrefix"
    );
    assert_eq!(property_tag_debug_name(PID_TAG_BODY_W), "PidTagBody");
    assert_eq!(
        property_tag_debug_name(PID_TAG_RTF_COMPRESSED),
        "PidTagRtfCompressed"
    );
    assert_eq!(property_tag_debug_name(PID_TAG_HTML_BINARY), "PidTagHtml");
    assert_eq!(
        property_tag_debug_name(PID_TAG_HAS_ATTACHMENTS),
        "PidTagHasAttachments"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_MESSAGE_FLAGS),
        "PidTagMessageFlags"
    );
    assert_eq!(property_tag_debug_name(PID_TAG_READ), "PidTagRead");
    assert_eq!(
        property_tag_debug_name(PID_TAG_TRANSPORT_MESSAGE_HEADERS_W),
        "PidTagTransportMessageHeaders"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_RTF_IN_SYNC),
        "PidTagRtfInSync"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_NATIVE_BODY),
        "PidTagNativeBody"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_INTERNET_CODEPAGE),
        "PidTagInternetCodepage"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_MESSAGE_LOCALE_ID),
        "PidTagMessageLocaleId"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_EXTENDED_RULE_MESSAGE_ACTIONS),
        "PidTagExtendedRuleMessageActions"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_INTERNET_MESSAGE_ID_W),
        "PidTagInternetMessageId"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_FLAG_STATUS),
        "PidTagFlagStatus"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_SWAPPED_TODO_STORE),
        "PidTagSwappedToDoStore"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_LAST_MODIFICATION_TIME),
        "PidTagLastModificationTime"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_CLSID),
        "PidTagViewDescriptorCLSID"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_FLAGS),
        "PidTagViewDescriptorFlags"
    );
    assert_eq!(
        property_tag_debug_name(OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835),
        "OutlookCommonViewDescriptorBinary6835"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_VERSION),
        "PidTagViewDescriptorVersion"
    );
    assert_eq!(
        property_tag_debug_name(OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C),
        "OutlookCommonViewDescriptorStrings683C"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE),
        "PidTagViewDescriptorFolderType"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE),
        "PidTagViewDescriptorViewMode"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_BINARY),
        "PidTagViewDescriptorBinary"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_STRINGS_W),
        "PidTagViewDescriptorStrings"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_NAME_W),
        "PidTagViewDescriptorName"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL),
        "PidTagViewDescriptorVersionCanonical"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_WLINK_GROUP_HEADER_ID),
        "PidTagWlinkGroupHeaderId"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_WLINK_SAVE_STAMP),
        "PidTagWlinkSaveStamp"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_WLINK_TYPE),
        "PidTagWlinkType"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_WLINK_FLAGS),
        "PidTagWlinkFlags"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_WLINK_ORDINAL),
        "PidTagWlinkOrdinal"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_WLINK_ENTRY_ID),
        "PidTagWlinkEntryId"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_WLINK_RECORD_KEY),
        "PidTagWlinkRecordKey"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_WLINK_STORE_ENTRY_ID),
        "PidTagWlinkStoreEntryId"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_WLINK_FOLDER_TYPE),
        "PidTagWlinkFolderType"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_WLINK_GROUP_CLSID),
        "PidTagWlinkGroupClsid"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_WLINK_GROUP_NAME_W),
        "PidTagWlinkGroupName"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_WLINK_SECTION),
        "PidTagWlinkSection"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID),
        "PidTagWlinkAddressBookStoreEid"
    );
    assert_eq!(
        property_tag_debug_name(OUTLOOK_STALE_SHARING_LOCAL_FOLDER_ID_TAG),
        "OutlookStaleSharingCalendarGroupEntryAssociatedLocalFolderId"
    );
    assert_eq!(
        property_tag_debug_name(OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B),
        "OutlookAssociatedConfigBinary0E0B"
    );
    assert_eq!(
        property_tag_debug_name(PID_NAME_CONTENT_CLASS_W_TAG),
        "PidNameContentClass"
    );
    assert_eq!(
        property_tag_debug_name(PID_NAME_CONTENT_TYPE_W_TAG),
        "PidNameContentType"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_ROAMING_DATATYPES),
        "PidTagRoamingDatatypes"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_ROAMING_DICTIONARY),
        "PidTagRoamingDictionary"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_ROAMING_XML_STREAM),
        "PidTagRoamingXmlStream"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W),
        "PidTagDefaultPostMessageClass"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8),
        "PidTagDefaultPostMessageClass"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_ADDITIONAL_REN_ENTRY_IDS),
        "PidTagAdditionalRenEntryIds"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX),
        "PidTagAdditionalRenEntryIdsEx"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_FREE_BUSY_ENTRY_IDS),
        "PidTagFreeBusyEntryIds"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_ASSOCIATED_SHARING_PROVIDER),
        "PidTagAssociatedSharingProvider"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_SEARCH_FOLDER_ID),
        "PidTagSearchFolderId"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_SEARCH_FOLDER_STORAGE_TYPE),
        "PidTagSearchFolderStorageType"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_SEARCH_FOLDER_EFP_FLAGS),
        "PidTagSearchFolderEfpFlags"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_SEARCH_FOLDER_DEFINITION),
        "PidTagSearchFolderDefinition"
    );
    assert_eq!(property_tag_debug_name(PID_TAG_PST_PATH_W), "PidTagPstPath");
    assert_eq!(
        property_tag_debug_name(PID_TAG_EXTENDED_RULE_SIZE_LIMIT),
        "PidTagExtendedRuleSizeLimit"
    );
    assert_eq!(
        property_tag_debug_name(PID_TAG_ATTACH_NUM),
        "PidTagAttachNumber"
    );
    assert_eq!(property_tag_debug_name(0x6707_001F), "PidTagUserGuid");
    assert_eq!(
        property_tag_debug_name(0x6842_000B),
        "PidTagWlinkGroupHeaderId"
    );
    assert_eq!(property_tag_debug_name(0x684A_101F), "PidTagWlinkFlags");
    assert_eq!(property_tag_debug_name(0x684B_000B), "PidTagWlinkOrdinal");
    assert_eq!(
        property_tag_debug_name(0x6841_001F),
        "PidTagViewDescriptorViewMode"
    );
}

#[test]
pub(in crate::mapi) fn set_search_criteria_rejects_invalid_folder_id_scope() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&0u16.to_le_bytes());
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&0u64.to_le_bytes());
    payload.extend_from_slice(&1u32.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::SetSearchCriteria.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    assert_eq!(request.search_criteria_folder_ids(), None);
}

#[test]
fn logon_getprops_projects_extended_rule_size_limit() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_EXTENDED_RULE_SIZE_LIMIT.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&MapiObject::Logon),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(response[0], RopId::GetPropertiesSpecific.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(response[6], 0);
    assert_eq!(
        u32::from_le_bytes(response[7..11].try_into().unwrap()),
        35 * 1024
    );
}

#[test]
fn associated_config_0e0b_debug_reports_stored_value_and_fallback() {
    let message = crate::mapi_store::MapiAssociatedConfigMessage {
        id: 0x7fff_ffff_fffb_0001,
        folder_id: INBOX_FOLDER_ID,
        canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
        message_class: "IPM.Configuration.AccountPrefs".to_string(),
        subject: "Account preferences".to_string(),
        properties_json: serde_json::json!({
            "0x0e0b0102": {"type": "binary", "value": "01020304"}
        }),
    };

    let summary = format_associated_config_0e0b_debug(
        &[OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B],
        &message,
        &[OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B],
    );

    assert!(summary.contains("requested=true"));
    assert!(summary.contains("public_ms_oxprops_name=unmapped"));
    assert!(summary.contains("stored=true"));
    assert!(summary.contains("stored_shape=binary:bytes=4:preview=01020304"));
    assert!(summary.contains("semantic_shape=binary:bytes=4:preview=01020304"));
    assert!(summary.contains("roaming_datatypes=0x00000004"));
    assert!(summary.contains("dictionary_advertised=true"));
    assert!(summary.contains("roaming_dictionary_shape=binary:bytes="));
    assert!(summary.contains("dictionary_payload_consistent=true"));
    assert!(summary.contains("fallback_default=true"));
    assert!(summary.contains("property_json_tags=0x0e0b0102"));
}

#[test]
fn associated_config_zero_metadata_defaults_are_intentional() {
    let object = MapiObject::AssociatedConfig {
        folder_id: INBOX_FOLDER_ID,
        config_id: 0x7fff_ffff_fffb_0001,
        saved_message: None,
    };

    assert!(modeled_zero_or_default_property(
        Some(&object),
        PID_TAG_MESSAGE_STATUS
    ));
    assert!(modeled_zero_or_default_property(
        Some(&object),
        PID_TAG_SENT_MAIL_SVR_EID
    ));
    assert!(modeled_zero_or_default_property(
        Some(&object),
        OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
    ));
    assert!(!modeled_zero_or_default_property(
        Some(&object),
        PID_TAG_ORIGINAL_MESSAGE_CLASS_W
    ));
    assert!(!modeled_zero_or_default_property(
        Some(&object),
        0x801D_0003
    ));
    assert!(!modeled_zero_or_default_property(
        Some(&object),
        0x801D_0000
    ));
}

#[test]
fn associated_config_absent_optional_getprops_returns_not_found() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let object = MapiObject::AssociatedConfig {
        folder_id: INBOX_FOLDER_ID,
        config_id: 0x7fff_ffff_fffb_0001,
        saved_message: Some(crate::mapi_store::MapiAssociatedConfigMessage {
            id: 0x7fff_ffff_fffb_0001,
            folder_id: INBOX_FOLDER_ID,
            canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
            message_class: "IPM.Configuration.Unknown".to_string(),
            subject: "IPM.Configuration.Unknown".to_string(),
            properties_json: serde_json::json!({
                "0x000b0102": {"type": "binary", "value": ""}
            }),
        }),
    };

    assert!(!fallback_default_specific_property(
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        0x000B_0102,
    ));
    assert!(fallback_default_specific_property(
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        0x0014_000B,
    ));

    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&0x0014_000B_u32.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(response[0], RopId::GetPropertiesSpecific.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(response[6], 1);
    assert_eq!(response[7], 0x0A);
    assert_eq!(
        u32::from_le_bytes(response[8..12].try_into().unwrap()),
        0x8004_010F
    );
}

#[test]
fn quick_step_custom_action_defaults_undocumented_0e0b_to_empty_binary() {
    let object = MapiObject::AssociatedConfig {
        folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
        config_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF4),
        saved_message: None,
    };

    assert!(modeled_zero_or_default_property(
        Some(&object),
        OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B
    ));
}

#[test]
fn associated_config_getprops_rejects_default_from_wrong_folder() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let object = MapiObject::AssociatedConfig {
        folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
        config_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFC),
        saved_message: None,
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(response[0], RopId::GetPropertiesSpecific.as_u8());
    assert_eq!(
        u32::from_le_bytes(response[2..6].try_into().unwrap()),
        0x8004_010F
    );
}

#[test]
fn folder_default_named_view_getprops_projects_message_class() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let object = MapiObject::CommonViewNamedView {
        folder_id: INBOX_FOLDER_ID,
        view_id: crate::mapi_store::OUTLOOK_DEFAULT_FOLDER_NAMED_VIEW_ID,
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(3),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(response[0], RopId::GetPropertiesSpecific.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert!(response.windows(2).any(|bytes| bytes == b"I\0"));
}

fn test_accessible_calendar_event(
    id: Uuid,
    account_id: Uuid,
    title: &str,
) -> lpe_storage::AccessibleEvent {
    lpe_storage::AccessibleEvent {
        id,
        uid: format!("uid-{id}"),
        collection_id: "default".to_string(),
        owner_account_id: account_id,
        owner_email: "test@example.test".to_string(),
        owner_display_name: "Test".to_string(),
        rights: lpe_storage::CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: false,
        },
        date: "2026-06-01".to_string(),
        time: "10:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 60,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: title.to_string(),
        location: String::new(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "[]".to_string(),
        notes: String::new(),
        body_html: String::new(),
    }
}

fn contains_utf16(bytes: &[u8], value: &str) -> bool {
    let needle = value
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    bytes
        .windows(needle.len())
        .any(|window| window == needle.as_slice())
}

fn contains_ascii_z(bytes: &[u8], value: &str) -> bool {
    let mut needle = value.as_bytes().to_vec();
    needle.push(0);
    contains_bytes(bytes, &needle)
}

fn contains_bytes(bytes: &[u8], needle: &[u8]) -> bool {
    bytes.windows(needle.len()).any(|window| window == needle)
}

#[test]
fn get_properties_all_honors_non_unicode_string_request() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "sender@example.test".to_string(),
        display_name: "Sender".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_SUBJECT_W,
        MapiValue::String("String8 subject".to_string()),
    );
    properties.insert(
        PID_TAG_BODY_W,
        MapiValue::String("String8 body".to_string()),
    );
    let object = MapiObject::PendingMessage {
        folder_id: DRAFTS_FOLDER_ID,
        properties,
        recipients: Vec::new(),
    };
    let request = RopRequest {
        rop_id: RopId::GetPropertiesAll.as_u8(),
        input_handle_index: Some(2),
        output_handle_index: None,
        payload: [0x00, 0x10, 0x00, 0x00].to_vec(),
    };

    let response = rop_get_properties_all_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..6], &[0x08, 0x02, 0, 0, 0, 0]);
    assert!(contains_bytes(
        &response,
        &((PID_TAG_SUBJECT_W & 0xFFFF_0000) | 0x001E).to_le_bytes()
    ));
    assert!(contains_bytes(
        &response,
        &((PID_TAG_BODY_W & 0xFFFF_0000) | 0x001E).to_le_bytes()
    ));
    assert!(contains_ascii_z(&response, "String8 subject"));
    assert!(contains_ascii_z(&response, "String8 body"));
}

#[test]
fn get_properties_all_returns_error_tag_for_size_limited_value() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "sender@example.test".to_string(),
        display_name: "Sender".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_SUBJECT_W,
        MapiValue::String("Oversized subject".to_string()),
    );
    let object = MapiObject::PendingMessage {
        folder_id: DRAFTS_FOLDER_ID,
        properties,
        recipients: Vec::new(),
    };
    let request = RopRequest {
        rop_id: RopId::GetPropertiesAll.as_u8(),
        input_handle_index: Some(2),
        output_handle_index: None,
        payload: [0x04, 0x00, 0x01, 0x00].to_vec(),
    };

    let response = rop_get_properties_all_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );
    let mut expected = property_error_tag(PID_TAG_SUBJECT_W).to_le_bytes().to_vec();
    expected.extend_from_slice(&0x8007_000E_u32.to_le_bytes());

    assert_eq!(&response[..6], &[0x08, 0x02, 0, 0, 0, 0]);
    assert!(contains_bytes(&response, &expected));
}

#[test]
fn calendar_event_getprops_specific_projects_visible_event() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::from_u128(0x8181),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let event_id = Uuid::from_u128(0x8182);
    crate::mapi::identity::remember_mapi_identity(
        event_id,
        crate::mapi::identity::mapi_store_id(0x8182),
    );
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![test_accessible_calendar_event(
            event_id,
            principal.account_id,
            "Projected event",
        )],
        Vec::new(),
        Vec::new(),
    );
    let object = MapiObject::Event {
        folder_id: CALENDAR_FOLDER_ID,
        event_id: crate::mapi::identity::mapi_store_id(0x8182),
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&2u16.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &snapshot,
    );

    assert_eq!(response[0], RopId::GetPropertiesSpecific.as_u8());
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert!(contains_utf16(&response, "IPM.Appointment"));
    assert!(contains_utf16(&response, "Projected event"));
}

#[test]
fn calendar_event_getprops_all_rejects_missing_event_handle() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let object = MapiObject::Event {
        folder_id: CALENDAR_FOLDER_ID,
        event_id: crate::mapi::identity::mapi_store_id(0x43),
    };
    let request = RopRequest {
        rop_id: RopId::GetPropertiesAll.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: Vec::new(),
    };

    let response = rop_get_properties_all_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(response[0], RopId::GetPropertiesAll.as_u8());
    assert_eq!(
        u32::from_le_bytes(response[2..6].try_into().unwrap()),
        0x8004_010F
    );
}

#[test]
fn calendar_event_get_valid_attachments_rejects_missing_event_handle() {
    let object = MapiObject::Event {
        folder_id: CALENDAR_FOLDER_ID,
        event_id: crate::mapi::identity::mapi_store_id(0x43),
    };
    let request = RopRequest {
        rop_id: RopId::GetValidAttachments.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: Vec::new(),
    };

    let response = rop_get_valid_attachments_response(
        &request,
        Some(&object),
        &MapiMailStoreSnapshot::empty(),
        &HashSet::new(),
    );

    assert_eq!(response[0], RopId::GetValidAttachments.as_u8());
    assert_eq!(
        u32::from_le_bytes(response[2..6].try_into().unwrap()),
        0x8004_010F
    );
}

#[test]
fn conversation_action_getprops_rejects_default_from_wrong_folder() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let object = MapiObject::ConversationAction {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        conversation_action_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2),
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(response[0], RopId::GetPropertiesSpecific.as_u8());
    assert_eq!(
        u32::from_le_bytes(response[2..6].try_into().unwrap()),
        0x8004_010F
    );
}

#[test]
fn delegate_freebusy_getprops_rejects_message_from_wrong_folder() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let message_id = Uuid::parse_str("56565656-5656-4656-8656-565656565656").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        message_id,
        crate::mapi::identity::mapi_store_id(610),
    );
    let snapshot = MapiMailStoreSnapshot::empty().with_delegate_freebusy_messages(vec![
        lpe_storage::DelegateFreeBusyMessageObject {
            id: message_id,
            account_id: Uuid::nil(),
            owner_account_id: Uuid::nil(),
            owner_email: "owner@example.test".to_string(),
            message_kind: "freebusy".to_string(),
            subject: "owner@example.test: busy".to_string(),
            body_text: "busy".to_string(),
            starts_at: None,
            ends_at: None,
            busy_status: None,
            payload_json: "{}".to_string(),
            updated_at: "2026-05-26T08:00:00Z".to_string(),
        },
    ]);
    let object = MapiObject::DelegateFreeBusyMessage {
        folder_id: INBOX_FOLDER_ID,
        message_id: snapshot.delegate_freebusy_messages()[0].id,
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(0),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &snapshot,
    );

    assert_eq!(response[0], RopId::GetPropertiesSpecific.as_u8());
    assert_eq!(
        u32::from_le_bytes(response[2..6].try_into().unwrap()),
        0x8004_010F
    );
}

#[test]
fn message_body_getprops_contract_reports_canonical_body_shape() {
    let mailbox_id = Uuid::parse_str("10101010-1010-1010-1010-101010101010").unwrap();
    let email_id = Uuid::parse_str("20202020-2020-2020-2020-202020202020").unwrap();
    crate::mapi::identity::remember_mapi_identity(mailbox_id, INBOX_FOLDER_ID);
    crate::mapi::identity::remember_mapi_identity(
        email_id,
        crate::mapi::identity::mapi_store_id(0x99),
    );
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 10,
        modseq: 1,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let emails = vec![JmapEmail {
        id: email_id,
        thread_id: email_id,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: Vec::new(),
        mailbox_id,
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 7,
        received_at: "2026-06-07T19:56:00Z".to_string(),
        sent_at: None,
        from_address: "sender@example.test".to_string(),
        from_display: Some("Sender".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "author".to_string(),
        submitted_by_account_id: Uuid::nil(),
        to: vec![JmapEmailAddress {
            address: "test@example.test".to_string(),
            display_name: Some("Test".to_string()),
        }],
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Body check".to_string(),
        preview: "Plain body".to_string(),
        body_text: "Plain body".to_string(),
        body_html_sanitized: Some("<p>Plain body</p>".to_string()),
        unread: false,
        flagged: false,
        followup_flag_status: "none".to_string(),
        followup_icon: 0,
        todo_item_flags: 0,
        followup_request: String::new(),
        followup_start_at: None,
        followup_due_at: None,
        followup_completed_at: None,
        reminder_set: false,
        reminder_at: None,
        reminder_dismissed_at: None,
        swapped_todo_store_id: None,
        swapped_todo_data: None,
        categories: Vec::new(),
        has_attachments: false,
        size_octets: 512,
        internet_message_id: Some("<body-check@example.test>".to_string()),
        mime_blob_ref: None,
        delivery_status: "delivered".to_string(),
    }];
    let object = MapiObject::Message {
        folder_id: INBOX_FOLDER_ID,
        message_id: crate::mapi::identity::mapi_store_id(0x99),
        saved_email: None,
        pending_properties: HashMap::new(),
    };
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    let contract = format_message_body_getprops_contract(
        Some(&object),
        &[
            PID_TAG_BODY_W,
            PID_TAG_RTF_COMPRESSED,
            PID_TAG_HTML_BINARY,
            PID_TAG_NATIVE_BODY,
        ],
        &mailboxes,
        &emails,
        &snapshot,
    );

    assert!(contract.contains("message_found=true"));
    assert!(contract.contains("source=mailbox"));
    assert!(contract.contains("subject_chars=10"));
    assert!(contract.contains("body_text_chars=10"));
    assert!(contract.contains("body_text_empty=false"));
    assert!(contract.contains("body_html_bytes=17"));
    assert!(contract.contains("native_body=3"));
    assert!(contract.contains("requested_body_tags=0x1000001f,0x10090102,0x10130102,0x10160003"));
}

#[test]
fn microsoft_oxcdata_property_row_example_streams_oversized_body() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::from_u128(0x8181),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mailbox_id = Uuid::from_u128(0x9001);
    let email_id = Uuid::from_u128(0x9002);
    let message_id = crate::mapi::identity::mapi_store_id(0x9002);
    crate::mapi::identity::remember_mapi_identity(mailbox_id, INBOX_FOLDER_ID);
    crate::mapi::identity::remember_mapi_identity(email_id, message_id);
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 10,
        modseq: 1,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let emails = vec![JmapEmail {
        id: email_id,
        thread_id: email_id,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: Vec::new(),
        mailbox_id,
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 7,
        received_at: "2026-06-07T19:56:00Z".to_string(),
        sent_at: None,
        from_address: "sender@example.test".to_string(),
        from_display: Some("Sender".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "author".to_string(),
        submitted_by_account_id: Uuid::nil(),
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Hello".to_string(),
        preview: "Large body".to_string(),
        body_text: "Large body ".repeat(32),
        body_html_sanitized: None,
        unread: false,
        flagged: false,
        followup_flag_status: "none".to_string(),
        followup_icon: 0,
        todo_item_flags: 0,
        followup_request: String::new(),
        followup_start_at: None,
        followup_due_at: None,
        followup_completed_at: None,
        reminder_set: false,
        reminder_at: None,
        reminder_dismissed_at: None,
        swapped_todo_store_id: None,
        swapped_todo_data: None,
        categories: Vec::new(),
        has_attachments: true,
        size_octets: 4096,
        internet_message_id: Some("<body-check@example.test>".to_string()),
        mime_blob_ref: None,
        delivery_status: "delivered".to_string(),
    }];
    let object = MapiObject::Message {
        folder_id: INBOX_FOLDER_ID,
        message_id,
        saved_email: None,
        pending_properties: HashMap::new(),
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&16u16.to_le_bytes());
    payload.extend_from_slice(&3u16.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_MESSAGE_FLAGS.to_le_bytes());
    payload.extend_from_slice(&0x0037_0001u32.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_BODY_W.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific.as_u8(),
        input_handle_index: Some(2),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &mailboxes,
        &emails,
        &MapiMailStoreSnapshot::empty(),
    );

    let mut expected_row = vec![0x00];
    expected_row.extend_from_slice(&0x13u32.to_le_bytes());
    expected_row.extend_from_slice(&0x001Fu16.to_le_bytes());
    expected_row.push(0x00);
    expected_row.extend_from_slice(&utf16z_bytes("Hello"));
    expected_row.push(0x0A);
    expected_row.extend_from_slice(&0x8007_000E_u32.to_le_bytes());

    assert_eq!(&response[..7], &[0x07, 0x02, 0, 0, 0, 0, 1]);
    assert_eq!(&response[7..], expected_row.as_slice());
}

#[test]
fn saved_message_handle_getprops_uses_same_batch_email() {
    let account_id = Uuid::parse_str("10101010-1010-1010-1010-101010101010").unwrap();
    let email_id = Uuid::parse_str("20202020-2020-2020-2020-202020202020").unwrap();
    let message_id = crate::mapi::identity::mapi_store_id(0x99);
    let email = JmapEmail {
        id: email_id,
        thread_id: email_id,
        mailbox_ids: vec![account_id],
        mailbox_states: Vec::new(),
        mailbox_id: account_id,
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 7,
        received_at: "2026-06-07T19:56:00Z".to_string(),
        sent_at: None,
        from_address: "sender@example.test".to_string(),
        from_display: Some("Sender".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "author".to_string(),
        submitted_by_account_id: account_id,
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Saved batch".to_string(),
        preview: "Saved body".to_string(),
        body_text: "Saved body".to_string(),
        body_html_sanitized: None,
        unread: false,
        flagged: false,
        followup_flag_status: "none".to_string(),
        followup_icon: 0,
        todo_item_flags: 0,
        followup_request: String::new(),
        followup_start_at: None,
        followup_due_at: None,
        followup_completed_at: None,
        reminder_set: false,
        reminder_at: None,
        reminder_dismissed_at: None,
        swapped_todo_store_id: None,
        swapped_todo_data: None,
        categories: Vec::new(),
        has_attachments: false,
        size_octets: 128,
        internet_message_id: Some("<saved-batch@example.test>".to_string()),
        mime_blob_ref: None,
        delivery_status: "delivered".to_string(),
    };
    let object = MapiObject::Message {
        folder_id: INBOX_FOLDER_ID,
        message_id,
        saved_email: Some(MapiSavedEmail { email }),
        pending_properties: HashMap::new(),
    };
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id,
        email: "test@example.test".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&2u16.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_BODY_W.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific as u8,
        input_handle_index: Some(1),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..7], &[0x07, 0x01, 0, 0, 0, 0, 0]);
    assert!(response
        .windows(utf16z_bytes("Saved batch").len())
        .any(|window| window == utf16z_bytes("Saved batch").as_slice()));
    assert!(response
        .windows(utf16z_bytes("Saved body").len())
        .any(|window| window == utf16z_bytes("Saved body").as_slice()));
}

#[test]
pub(in crate::mapi) fn persisted_message_getprops_returns_body_values() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let mailbox_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    crate::mapi::identity::remember_mapi_identity(mailbox_id, INBOX_FOLDER_ID);
    let email_id = Uuid::parse_str("99999999-9999-4999-8999-999999999999").unwrap();
    let message_id = crate::mapi::identity::mapi_store_id(0x99);
    crate::mapi::identity::remember_mapi_identity(email_id, message_id);
    let mailboxes = vec![JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        sort_order: 10,
        modseq: 1,
        total_emails: 1,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    }];
    let emails = vec![JmapEmail {
        id: email_id,
        thread_id: email_id,
        mailbox_ids: vec![mailbox_id],
        mailbox_states: Vec::new(),
        mailbox_id,
        mailbox_role: "inbox".to_string(),
        mailbox_name: "Inbox".to_string(),
        modseq: 7,
        received_at: "2026-06-07T19:56:00Z".to_string(),
        sent_at: None,
        from_address: "sender@example.test".to_string(),
        from_display: Some("Sender".to_string()),
        sender_address: None,
        sender_display: None,
        sender_authorization_kind: "author".to_string(),
        submitted_by_account_id: Uuid::nil(),
        to: Vec::new(),
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: "Body check".to_string(),
        preview: "Plain body".to_string(),
        body_text: "Plain body".to_string(),
        body_html_sanitized: Some("<p>Plain body</p>".to_string()),
        unread: false,
        flagged: false,
        followup_flag_status: "none".to_string(),
        followup_icon: 0,
        todo_item_flags: 0,
        followup_request: String::new(),
        followup_start_at: None,
        followup_due_at: None,
        followup_completed_at: None,
        reminder_set: false,
        reminder_at: None,
        reminder_dismissed_at: None,
        swapped_todo_store_id: None,
        swapped_todo_data: None,
        categories: Vec::new(),
        has_attachments: false,
        size_octets: 512,
        internet_message_id: Some("<body-check@example.test>".to_string()),
        mime_blob_ref: None,
        delivery_status: "delivered".to_string(),
    }];
    let object = MapiObject::Message {
        folder_id: INBOX_FOLDER_ID,
        message_id,
        saved_email: None,
        pending_properties: HashMap::new(),
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&4u16.to_le_bytes());
    for tag in [
        PID_TAG_BODY_W,
        PID_TAG_RTF_COMPRESSED,
        PID_TAG_HTML_BINARY,
        PID_TAG_NATIVE_BODY,
    ] {
        payload.extend_from_slice(&tag.to_le_bytes());
    }
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific as u8,
        input_handle_index: Some(1),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &mailboxes,
        &emails,
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..7], &[0x07, 0x01, 0, 0, 0, 0, 0]);
    assert!(response
        .windows(utf16z_bytes("Plain body").len())
        .any(|window| window == utf16z_bytes("Plain body").as_slice()));
    assert!(response
        .windows("<p>Plain body</p>".len())
        .any(|window| window == b"<p>Plain body</p>"));
    assert!(response.windows(5).any(|window| window == b"{\\rtf"));
    assert!(response
        .windows(4)
        .any(|window| window == 3u32.to_le_bytes()));
}

#[test]
fn saved_associated_config_getprops_uses_same_batch_saved_message() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let config_id = crate::mapi::identity::mapi_store_id(0x4321);
    let object = MapiObject::AssociatedConfig {
        folder_id: CALENDAR_FOLDER_ID,
        config_id,
        saved_message: Some(crate::mapi_store::MapiAssociatedConfigMessage {
            id: config_id,
            folder_id: CALENDAR_FOLDER_ID,
            canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
            message_class: "IPM.Configuration.Calendar".to_string(),
            subject: "Calendar config".to_string(),
            properties_json: serde_json::json!({}),
        }),
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&2u16.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_CHANGE_KEY.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific as u8,
        input_handle_index: Some(3),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..7], &[0x07, 0x03, 0, 0, 0, 0, 0]);
    assert!(response
        .windows(utf16z_bytes("IPM.Configuration.Calendar").len())
        .any(|window| window == utf16z_bytes("IPM.Configuration.Calendar").as_slice()));
    let expected_change_key = mapi_mailstore::change_key_for_change_number(
        mapi_mailstore::change_number_for_store_id(config_id),
    );
    assert!(response
        .windows(expected_change_key.len())
        .any(|window| window == expected_change_key.as_slice()));
}

#[test]
fn saved_umolk_associated_config_getprops_projects_roaming_dictionary_stream() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let config_id = crate::mapi::identity::mapi_store_id(0x4322);
    let object = MapiObject::AssociatedConfig {
        folder_id: INBOX_FOLDER_ID,
        config_id,
        saved_message: Some(crate::mapi_store::MapiAssociatedConfigMessage {
            id: config_id,
            folder_id: INBOX_FOLDER_ID,
            canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
            message_class: "IPM.Configuration.UMOLK.UserOptions".to_string(),
            subject: "IPM.Configuration.UMOLK.UserOptions".to_string(),
            properties_json: serde_json::json!({}),
        }),
    };
    let tags = [
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_MESSAGE_STATUS,
        OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
    ];
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for tag in tags {
        payload.extend_from_slice(&tag.to_le_bytes());
    }
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific as u8,
        input_handle_index: Some(3),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..7], &[0x07, 0x03, 0, 0, 0, 0, 0]);
    let mut cursor = Cursor::new(&response[7..]);
    assert_eq!(
        parse_property_value_for_tag(&mut cursor, PID_TAG_MESSAGE_CLASS_W).unwrap(),
        MapiValue::String("IPM.Configuration.UMOLK.UserOptions".to_string())
    );
    assert_eq!(
        parse_property_value_for_tag(&mut cursor, PID_TAG_MESSAGE_FLAGS).unwrap(),
        MapiValue::I32(64)
    );
    assert_eq!(
        parse_property_value_for_tag(&mut cursor, PID_TAG_MESSAGE_STATUS).unwrap(),
        MapiValue::I32(0)
    );
    assert!(matches!(
        parse_property_value_for_tag(&mut cursor, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B).unwrap(),
        MapiValue::Binary(value)
            if value.starts_with(br#"<?xml version="1.0" encoding="utf-8"?>"#)
                && value.windows(b"18-OLPrefsVersion".len()).any(|window| window == b"18-OLPrefsVersion")
    ));
}

#[test]
fn umolk_associated_config_property_burst_reports_absent_values_not_found() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let config_id = crate::mapi::identity::mapi_store_id(0x4324);
    let object = MapiObject::AssociatedConfig {
        folder_id: INBOX_FOLDER_ID,
        config_id,
        saved_message: Some(crate::mapi_store::MapiAssociatedConfigMessage {
            id: config_id,
            folder_id: INBOX_FOLDER_ID,
            canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555556").unwrap(),
            message_class: "IPM.Configuration.UMOLK.UserOptions".to_string(),
            subject: "IPM.Configuration.UMOLK.UserOptions".to_string(),
            properties_json: serde_json::json!({}),
        }),
    };
    let tags = [
        PID_TAG_ROAMING_DATATYPES,
        PID_TAG_ROAMING_DICTIONARY,
        0x9001_0003,
        0x9020_0102,
        0x85B2_1102,
        0x9269_000B,
    ];
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for tag in tags {
        payload.extend_from_slice(&tag.to_le_bytes());
    }
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific as u8,
        input_handle_index: Some(3),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..7], &[0x07, 0x03, 0, 0, 0, 0, 1]);
    let mut cursor = Cursor::new(&response[7..]);
    assert_eq!(cursor.read_u8().unwrap(), 0);
    assert_eq!(
        parse_property_value_for_tag(&mut cursor, PID_TAG_ROAMING_DATATYPES).unwrap(),
        MapiValue::I32(4)
    );
    assert_eq!(cursor.read_u8().unwrap(), 0);
    assert!(matches!(
        parse_property_value_for_tag(&mut cursor, PID_TAG_ROAMING_DICTIONARY).unwrap(),
        MapiValue::Binary(value) if value.windows(b"18-OLPrefsVersion".len()).any(|window| window == b"18-OLPrefsVersion")
    ));
    for tag in tags.iter().skip(2) {
        assert_eq!(cursor.read_u8().unwrap(), 0x0A, "tag {tag:#010x}");
        assert_eq!(cursor.read_u32().unwrap(), 0x8004_010F, "tag {tag:#010x}");
    }
}

#[test]
fn umolk_trace_property_burst_does_not_fabricate_optional_standard_values() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let config_id = crate::mapi::identity::mapi_store_id(0x4325);
    let object = MapiObject::AssociatedConfig {
        folder_id: INBOX_FOLDER_ID,
        config_id,
        saved_message: Some(crate::mapi_store::MapiAssociatedConfigMessage {
            id: config_id,
            folder_id: INBOX_FOLDER_ID,
            canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555557").unwrap(),
            message_class: "IPM.Configuration.UMOLK.UserOptions".to_string(),
            subject: "IPM.Configuration.UMOLK.UserOptions".to_string(),
            properties_json: serde_json::json!({
                "0x7c060003": {"type": "u32", "value": 4},
                "0x7c070102": {"type": "binary", "value": "0102"}
            }),
        }),
    };
    // This is the optional-standard-property portion of Outlook 16 request :109
    // from the 202607111201 startup trace. Missing values require a flagged row;
    // see [MS-OXCROPS] 2.2.8.3.2 and [MS-OXCDATA] 2.8.1.2.
    let absent_tags = [
        PID_TAG_RTF_IN_SYNC,
        PID_TAG_RTF_COMPRESSED,
        PID_TAG_DISPLAY_TO_W,
        PID_TAG_REPLY_TIME,
        PID_TAG_PRIORITY,
        PID_TAG_NATIVE_BODY,
    ];
    let mut tags = absent_tags.to_vec();
    tags.extend([PID_TAG_MESSAGE_CLASS_W, PID_TAG_ROAMING_DATATYPES]);
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for tag in &tags {
        payload.extend_from_slice(&tag.to_le_bytes());
    }
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific as u8,
        input_handle_index: Some(3),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..7], &[0x07, 0x03, 0, 0, 0, 0, 1]);
    let mut cursor = Cursor::new(&response[7..]);
    for tag in absent_tags {
        assert_eq!(cursor.read_u8().unwrap(), 0x0A, "tag {tag:#010x}");
        assert_eq!(cursor.read_u32().unwrap(), 0x8004_010F, "tag {tag:#010x}");
    }
    assert_eq!(cursor.read_u8().unwrap(), 0);
    assert_eq!(
        parse_property_value_for_tag(&mut cursor, PID_TAG_MESSAGE_CLASS_W).unwrap(),
        MapiValue::String("IPM.Configuration.UMOLK.UserOptions".to_string())
    );
    assert_eq!(cursor.read_u8().unwrap(), 0);
    assert_eq!(
        parse_property_value_for_tag(&mut cursor, PID_TAG_ROAMING_DATATYPES).unwrap(),
        MapiValue::I32(4)
    );
}

#[test]
fn contacts_helper_associated_getprops_projects_empty_modeled_values() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let config_id = crate::mapi::identity::mapi_store_id(0x4323);
    let object = MapiObject::AssociatedConfig {
        folder_id: CONTACTS_FOLDER_ID,
        config_id,
        saved_message: Some(crate::mapi_store::MapiAssociatedConfigMessage {
            id: config_id,
            folder_id: CONTACTS_FOLDER_ID,
            canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
            message_class: "IPM.Microsoft.OSC.ContactSync".to_string(),
            subject: "IPM.Microsoft.OSC.ContactSync".to_string(),
            properties_json: serde_json::json!({}),
        }),
    };
    let tags = [
        PID_NAME_OSC_CONTACT_SOURCES_TAG,
        PID_TAG_ENTRY_ID,
        OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
        PID_TAG_MESSAGE_CLASS_W,
    ];
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for tag in tags {
        payload.extend_from_slice(&tag.to_le_bytes());
    }
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific as u8,
        input_handle_index: Some(3),
        output_handle_index: None,
        payload,
    };

    assert!(!fallback_default_specific_property(
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        PID_NAME_OSC_CONTACT_SOURCES_TAG,
    ));

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..7], &[0x07, 0x03, 0, 0, 0, 0, 0]);
    let mut cursor = Cursor::new(&response[7..]);
    assert_eq!(
        parse_property_value_for_tag(&mut cursor, PID_NAME_OSC_CONTACT_SOURCES_TAG).unwrap(),
        MapiValue::MultiString(Vec::new())
    );
    assert!(matches!(
        parse_property_value_for_tag(&mut cursor, PID_TAG_ENTRY_ID).unwrap(),
        MapiValue::Binary(bytes) if !bytes.is_empty()
    ));
    assert_eq!(
        parse_property_value_for_tag(&mut cursor, OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B).unwrap(),
        MapiValue::Binary(Vec::new())
    );
    assert_eq!(
        parse_property_value_for_tag(&mut cursor, PID_TAG_MESSAGE_CLASS_W).unwrap(),
        MapiValue::String("IPM.Microsoft.OSC.ContactSync".to_string())
    );

    let contact_link = MapiObject::AssociatedConfig {
        folder_id: CONTACTS_FOLDER_ID,
        config_id,
        saved_message: Some(crate::mapi_store::MapiAssociatedConfigMessage {
            id: config_id,
            folder_id: CONTACTS_FOLDER_ID,
            canonical_id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
            message_class: "IPM.Microsoft.ContactLink.TimeStamp".to_string(),
            subject: "IPM.Microsoft.ContactLink.TimeStamp".to_string(),
            properties_json: serde_json::json!({}),
        }),
    };
    assert!(!fallback_default_specific_property(
        Some(&contact_link),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        (PID_LID_OUTLOOK_OSC_CONTACT_SOURCE_80EC << 16) | 0x0003,
    ));
}

#[test]
fn property_row_kind_reports_fallback_defaults_as_flagged() {
    const UNKNOWN_FOLDER_INTEGER: u32 = 0x801D_0003;
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let object = MapiObject::Folder {
        folder_id: INBOX_FOLDER_ID,
        properties: HashMap::new(),
    };

    assert_eq!(
        property_row_kind_for_debug(
            Some(&object),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            &[UNKNOWN_FOLDER_INTEGER],
        ),
        "flagged"
    );

    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&UNKNOWN_FOLDER_INTEGER.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific as u8,
        input_handle_index: Some(3),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..7], &[0x07, 0x03, 0, 0, 0, 0, 1]);
    assert_eq!(response[7], 0x0A);
    assert_eq!(
        u32::from_le_bytes(response[8..12].try_into().unwrap()),
        0x8004_010F
    );
}

#[test]
fn undocumented_folder_binary_120c_returns_empty_binary() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let folder = MapiObject::Folder {
        folder_id: INBOX_FOLDER_ID,
        properties: HashMap::new(),
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific as u8,
        input_handle_index: Some(1),
        output_handle_index: None,
        payload,
    };

    assert_eq!(
        property_tag_debug_name(OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C),
        "OutlookUndocumentedFolderBinary120C"
    );
    assert!(!fallback_default_specific_property(
        Some(&folder),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C,
    ));

    let response = rop_get_properties_specific_response(
        &request,
        Some(&folder),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..6], &[0x07, 0x01, 0, 0, 0, 0]);
    assert_eq!(&response[6..], &[0, 0, 0]);

    for folder_id in [
        CALENDAR_FOLDER_ID,
        CONTACTS_FOLDER_ID,
        TASKS_FOLDER_ID,
        NOTES_FOLDER_ID,
        JOURNAL_FOLDER_ID,
    ] {
        let folder = MapiObject::Folder {
            folder_id,
            properties: HashMap::new(),
        };
        let mut default_view_payload = Vec::new();
        default_view_payload.extend_from_slice(&4096u16.to_le_bytes());
        default_view_payload.extend_from_slice(&1u16.to_le_bytes());
        default_view_payload.extend_from_slice(&PID_TAG_DEFAULT_VIEW_ENTRY_ID.to_le_bytes());
        let default_view_request = RopRequest {
            rop_id: RopId::GetPropertiesSpecific as u8,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: default_view_payload,
        };

        assert!(!fallback_default_specific_property(
            Some(&folder),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            PID_TAG_DEFAULT_VIEW_ENTRY_ID,
        ));

        let response = rop_get_properties_specific_response(
            &default_view_request,
            Some(&folder),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..7], &[0x07, 0x01, 0, 0, 0, 0, 0]);
        assert!(response.len() > 7);
    }

    let mut default_view_payload = Vec::new();
    default_view_payload.extend_from_slice(&4096u16.to_le_bytes());
    default_view_payload.extend_from_slice(&1u16.to_le_bytes());
    default_view_payload.extend_from_slice(&PID_TAG_DEFAULT_VIEW_ENTRY_ID.to_le_bytes());
    let default_view_request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific as u8,
        input_handle_index: Some(1),
        output_handle_index: None,
        payload: default_view_payload,
    };

    for folder_id in [IPM_SUBTREE_FOLDER_ID] {
        let normal_view_folder = MapiObject::Folder {
            folder_id,
            properties: HashMap::new(),
        };
        assert!(fallback_default_specific_property(
            Some(&normal_view_folder),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
            PID_TAG_DEFAULT_VIEW_ENTRY_ID,
        ));

        let response = rop_get_properties_specific_response(
            &default_view_request,
            Some(&normal_view_folder),
            &principal,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert_eq!(&response[..7], &[0x07, 0x01, 0, 0, 0, 0, 1]);
        assert_eq!(response[7], 0x0A);
        assert_eq!(
            u32::from_le_bytes(response[8..12].try_into().unwrap()),
            ROP_ERROR_NOT_FOUND
        );
    }

    let quick_step_settings = MapiObject::Folder {
        folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
        properties: HashMap::new(),
    };
    assert!(fallback_default_specific_property(
        Some(&quick_step_settings),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        PID_TAG_DEFAULT_VIEW_ENTRY_ID,
    ));

    let response = rop_get_properties_specific_response(
        &default_view_request,
        Some(&quick_step_settings),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert_eq!(&response[..7], &[0x07, 0x01, 0, 0, 0, 0, 1]);
    assert_eq!(response[7], 0x0A);
    assert_eq!(
        u32::from_le_bytes(response[8..12].try_into().unwrap()),
        ROP_ERROR_NOT_FOUND
    );
}

#[test]
fn fallback_property_errors_for_debug_match_wire_error_codes() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let folder = MapiObject::Folder {
        folder_id: INBOX_FOLDER_ID,
        properties: HashMap::new(),
    };
    let folder_error_tags = unsupported_specific_property_tags(
        Some(&folder),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        &[OUTLOOK_UNDOCUMENTED_FOLDER_BINARY_120C],
    );
    let folder_errors = format_property_errors_for_debug(
        Some(&folder),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        &folder_error_tags,
    );
    assert_eq!(folder_errors, "");

    let config = MapiObject::AssociatedConfig {
        folder_id: INBOX_FOLDER_ID,
        config_id: crate::mapi::identity::mapi_store_id(0x4322),
        saved_message: None,
    };
    assert!(fallback_default_specific_property(
        Some(&config),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        0x801D_0003,
    ));
    let missing_errors = format_property_errors_for_debug(
        Some(&config),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        &[0x801D_0003],
    );
    assert!(missing_errors.contains("0x801d0003:unknown:0x8004010f"));

    let unsupported_errors = format_property_errors_for_debug(
        Some(&config),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        &[0x801D_0000],
    );
    assert!(unsupported_errors.contains("0x801d0000:unknown:0x80040102"));
}

#[test]
pub(in crate::mapi) fn folder_deleted_count_total_zero_is_modeled_not_fallback() {
    let folder = MapiObject::Folder {
        folder_id: COMMON_VIEWS_FOLDER_ID,
        properties: HashMap::new(),
    };

    assert!(modeled_zero_or_default_property(
        Some(&folder),
        PID_TAG_DELETED_COUNT_TOTAL
    ));
}

#[test]
fn logon_empty_pst_path_is_modeled_not_fallback() {
    assert!(modeled_zero_or_default_property(
        Some(&MapiObject::Logon),
        PID_TAG_PST_PATH_W
    ));
}

#[test]
fn folder_archive_policy_empty_defaults_are_modeled_not_fallback() {
    let folder = MapiObject::Folder {
        folder_id: INBOX_FOLDER_ID,
        properties: HashMap::new(),
    };

    for property_tag in [
        PID_TAG_ARCHIVE_TAG,
        PID_TAG_POLICY_TAG,
        PID_TAG_RETENTION_PERIOD,
        PID_TAG_RETENTION_FLAGS,
        PID_TAG_ARCHIVE_PERIOD,
    ] {
        assert!(modeled_zero_or_default_property(
            Some(&folder),
            property_tag
        ));
    }
}

#[test]
fn folder_view_empty_defaults_are_modeled_not_fallback() {
    let folder = MapiObject::Folder {
        folder_id: INBOX_FOLDER_ID,
        properties: HashMap::new(),
    };

    for property_tag in [
        PID_TAG_FOLDER_FORM_FLAGS,
        PID_TAG_FOLDER_WEBVIEWINFO,
        PID_TAG_FOLDER_XVIEWINFO_E,
        PID_TAG_FOLDER_VIEWS_ONLY,
        PID_TAG_DEFAULT_FORM_NAME_W,
        PID_TAG_FOLDER_FORM_STORAGE,
        PID_TAG_ACL_MEMBER_NAME_W,
        0x6672_0102,
        PID_TAG_FOLDER_VIEWLIST_FLAGS,
    ] {
        assert!(modeled_zero_or_default_property(
            Some(&folder),
            property_tag
        ));
    }

    assert!(!modeled_zero_or_default_property(
        Some(&folder),
        PID_TAG_DEFAULT_VIEW_ENTRY_ID
    ));
}

#[test]
fn empty_class_defaults_are_modeled_only_for_none_special_folders() {
    for folder_id in [
        ROOT_FOLDER_ID,
        DEFERRED_ACTION_FOLDER_ID,
        SPOOLER_QUEUE_FOLDER_ID,
        COMMON_VIEWS_FOLDER_ID,
        VIEWS_FOLDER_ID,
    ] {
        let folder = MapiObject::Folder {
            folder_id,
            properties: HashMap::new(),
        };

        for property_tag in [
            PID_TAG_CONTAINER_CLASS_W,
            PID_TAG_MESSAGE_CLASS_W,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        ] {
            assert!(modeled_zero_or_default_property(
                Some(&folder),
                property_tag
            ));
        }
    }

    let freebusy = MapiObject::Folder {
        folder_id: FREEBUSY_DATA_FOLDER_ID,
        properties: HashMap::new(),
    };

    for property_tag in [
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
    ] {
        assert!(!modeled_zero_or_default_property(
            Some(&freebusy),
            property_tag
        ));
    }

    let inbox = MapiObject::Folder {
        folder_id: INBOX_FOLDER_ID,
        properties: HashMap::new(),
    };

    for property_tag in [
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_MESSAGE_CLASS_W,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8,
        PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
    ] {
        assert!(!modeled_zero_or_default_property(
            Some(&inbox),
            property_tag
        ));
    }
}

#[test]
fn root_folder_type_zero_is_modeled_not_fallback() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "alice@example.test".to_string(),
        display_name: "Alice".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let folder = MapiObject::Folder {
        folder_id: ROOT_FOLDER_ID,
        properties: HashMap::new(),
    };

    assert!(modeled_zero_or_default_property(
        Some(&folder),
        PID_TAG_FOLDER_TYPE
    ));
    assert!(!fallback_default_specific_property(
        Some(&folder),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        PID_TAG_FOLDER_TYPE
    ));
}

#[test]
fn folder_type_getprops_contract_reports_loaded_inbox() {
    let account_id = Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb);
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id,
        email: "alice@example.test".to_string(),
        display_name: "Alice".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let inbox_id = Uuid::from_u128(0x1111);
    crate::mapi::identity::remember_mapi_identity(inbox_id, INBOX_FOLDER_ID);
    let inbox = JmapMailbox {
        id: inbox_id,
        parent_id: None,
        role: "inbox".to_string(),
        name: "INBOX".to_string(),
        sort_order: 0,
        modseq: 42,
        total_emails: 221,
        unread_emails: 17,
        size_octets: 0,
        is_subscribed: true,
    };
    let object = MapiObject::Folder {
        folder_id: INBOX_FOLDER_ID,
        properties: HashMap::new(),
    };

    let contract = format_folder_type_getprops_contract(
        Some(&object),
        &principal,
        &[PID_TAG_FOLDER_TYPE],
        &[inbox],
        &MapiMailStoreSnapshot::empty(),
    );

    assert!(contract.contains("mailbox_folder_found=true"));
    assert!(contract.contains("property_source=mailbox"));
    assert!(contract.contains("returned_value=1"));
    assert!(contract.contains("returned_kind=generic"));
    assert!(contract.contains("expected_kind=generic"));
    assert!(contract.ends_with("issues="));
}

#[test]
fn folder_type_getprops_contract_flags_inbox_without_snapshot() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
        email: "alice@example.test".to_string(),
        display_name: "Alice".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let object = MapiObject::Folder {
        folder_id: INBOX_FOLDER_ID,
        properties: HashMap::new(),
    };

    let contract = format_folder_type_getprops_contract(
        Some(&object),
        &principal,
        &[PID_TAG_FOLDER_TYPE],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert!(contract.contains("mailbox_folder_found=false"));
    assert!(contract.contains("property_source=special_folder_fallback"));
    assert!(contract.contains("returned_value=1"));
    assert!(contract
        .contains("issues=inbox_without_loaded_mailbox|inbox_answered_from_special_fallback"));
}

#[test]
fn folder_type_getprops_contract_accepts_advertised_search_folder() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
        email: "alice@example.test".to_string(),
        display_name: "Alice".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let object = MapiObject::Folder {
        folder_id: CONTACTS_SEARCH_FOLDER_ID,
        properties: HashMap::from([(PID_TAG_FOLDER_TYPE, MapiValue::U32(FOLDER_SEARCH))]),
    };

    let contract = format_folder_type_getprops_contract(
        Some(&object),
        &principal,
        &[PID_TAG_FOLDER_TYPE],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert!(contract.contains("advertised_special_folder=true"));
    assert!(contract.contains("property_source=opened_handle"));
    assert!(contract.contains("returned_value=2"));
    assert!(contract.contains("returned_kind=search"));
    assert!(contract.contains("expected_kind=search"));
    assert!(contract.ends_with("issues="));

    let finder_root = MapiObject::Folder {
        folder_id: SEARCH_FOLDER_ID,
        properties: HashMap::from([(PID_TAG_FOLDER_TYPE, MapiValue::U32(FOLDER_SEARCH))]),
    };
    let contract = format_folder_type_getprops_contract(
        Some(&finder_root),
        &principal,
        &[PID_TAG_FOLDER_TYPE],
        &[],
        &MapiMailStoreSnapshot::empty(),
    );

    assert!(contract.contains("folder_id=0x00000000000b0001"));
    assert!(contract.contains("returned_kind=search"));
    assert!(contract.contains("expected_kind=search"));
    assert!(contract.ends_with("issues="));
}

#[test]
fn folder_type_getprops_contract_prefers_saved_search_definition() {
    let account_id = Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb);
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id,
        email: "alice@example.test".to_string(),
        display_name: "Alice".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let folder_id = crate::mapi::identity::mapi_store_id(0x165);
    let mailbox_id = Uuid::from_u128(0x165);
    crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: String::new(),
        name: "People Search".to_string(),
        sort_order: 0,
        modseq: 42,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
        lpe_storage::SearchFolderDefinition {
            id: mailbox_id,
            account_id,
            role: "contacts_search".to_string(),
            display_name: "People Search".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "contact".to_string(),
            scope_json: serde_json::json!({"scope": "contacts"}),
            restriction_json: serde_json::json!({"kind": "contacts_search"}),
            excluded_folder_roles: Vec::new(),
            is_builtin: true,
        },
    ]);
    let object = MapiObject::Folder {
        folder_id,
        properties: HashMap::from([(PID_TAG_FOLDER_TYPE, MapiValue::U32(FOLDER_GENERIC))]),
    };

    let contract = format_folder_type_getprops_contract(
        Some(&object),
        &principal,
        &[PID_TAG_FOLDER_TYPE],
        &[mailbox],
        &snapshot,
    );

    assert!(contract.contains("search_folder_definition_found=true"));
    assert!(contract.contains("property_source=search_folder_definition"));
    assert!(contract.contains("returned_value=2"));
    assert!(contract.contains("returned_kind=search"));
    assert!(contract.contains("expected_kind=search"));
    assert!(contract.ends_with("issues="));
}

#[test]
fn folder_type_getprops_contract_accepts_projected_search_folder_role() {
    let account_id = Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb);
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id,
        email: "alice@example.test".to_string(),
        display_name: "Alice".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let folder_id = crate::mapi::identity::mapi_store_id(0x195);
    let mailbox_id = Uuid::from_u128(0x195);
    crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: "__mapi_search_folder_message".to_string(),
        name: "Categories Rename Search Folder".to_string(),
        sort_order: 0,
        modseq: 42,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let object = MapiObject::Folder {
        folder_id,
        properties: HashMap::from([(PID_TAG_FOLDER_TYPE, MapiValue::U32(FOLDER_SEARCH))]),
    };

    let contract = format_folder_type_getprops_contract(
        Some(&object),
        &principal,
        &[PID_TAG_FOLDER_TYPE],
        &[mailbox],
        &MapiMailStoreSnapshot::empty(),
    );

    assert!(contract.contains("returned_kind=search"));
    assert!(contract.contains("expected_kind=search"));
    assert!(contract.ends_with("issues="));
}

#[test]
fn folder_getprops_returns_search_type_for_saved_search_definition() {
    let account_id = Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb);
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id,
        email: "alice@example.test".to_string(),
        display_name: "Alice".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let folder_id = crate::mapi::identity::mapi_store_id(0x168);
    let mailbox_id = Uuid::from_u128(0x168);
    crate::mapi::identity::remember_mapi_identity(mailbox_id, folder_id);
    let mailbox = JmapMailbox {
        id: mailbox_id,
        parent_id: None,
        role: String::new(),
        name: "Category Search".to_string(),
        sort_order: 0,
        modseq: 42,
        total_emails: 0,
        unread_emails: 0,
        size_octets: 0,
        is_subscribed: true,
    };
    let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
        lpe_storage::SearchFolderDefinition {
            id: mailbox_id,
            account_id,
            role: "category_search".to_string(),
            display_name: "Category Search".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"scope": "mail"}),
            restriction_json: serde_json::json!({"kind": "category_search"}),
            excluded_folder_roles: Vec::new(),
            is_builtin: true,
        },
    ]);
    let object = MapiObject::Folder {
        folder_id,
        properties: HashMap::from([(PID_TAG_FOLDER_TYPE, MapiValue::U32(FOLDER_GENERIC))]),
    };

    let row = serialize_object_property(
        Some(&object),
        &principal,
        &[mailbox],
        &[],
        &snapshot,
        PID_TAG_FOLDER_TYPE,
    );

    assert_eq!(u32::from_le_bytes(row.try_into().unwrap()), FOLDER_SEARCH);
}

#[test]
fn default_view_entry_id_debug_decodes_message_target_ids() {
    let entry_id = crate::mapi::identity::message_entry_id_from_object_ids(
        Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
        INBOX_FOLDER_ID,
        crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_ID,
    )
    .unwrap();

    assert_eq!(
        default_view_message_entry_id_target(&entry_id),
        Some((
            INBOX_FOLDER_ID,
            crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_ID
        ))
    );
    assert_eq!(default_view_message_entry_id_target(&entry_id[..46]), None);
}

#[test]
fn folder_getprops_projects_saved_search_definition_metadata() {
    let account_id = Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb);
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id,
        email: "alice@example.test".to_string(),
        display_name: "Alice".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let folder_id = crate::mapi::identity::mapi_store_id(0x1db);
    let definition_id = Uuid::from_u128(0x1db);
    crate::mapi::identity::remember_mapi_identity(definition_id, folder_id);
    let snapshot = MapiMailStoreSnapshot::empty().with_search_folder_definitions(vec![
        lpe_storage::SearchFolderDefinition {
            id: definition_id,
            account_id,
            role: "custom".to_string(),
            display_name: "Categories Rename Search Folder".to_string(),
            definition_kind: "user_saved".to_string(),
            result_object_kind: "message".to_string(),
            scope_json: serde_json::json!({"scope": "mail"}),
            restriction_json: serde_json::json!({"kind": "mapi_bounded", "all": []}),
            excluded_folder_roles: Vec::new(),
            is_builtin: false,
        },
    ]);
    let object = MapiObject::Folder {
        folder_id,
        properties: HashMap::new(),
    };

    assert_eq!(
        serialize_object_property(
            Some(&object),
            &principal,
            &[],
            &[],
            &snapshot,
            PID_TAG_DISPLAY_NAME_W,
        ),
        utf16z_bytes("Categories Rename Search Folder")
    );
    assert_eq!(
        serialize_object_property(
            Some(&object),
            &principal,
            &[],
            &[],
            &snapshot,
            PID_TAG_CONTAINER_CLASS_W,
        ),
        utf16z_bytes("IPF.Note")
    );
    assert_eq!(
        serialize_object_property(
            Some(&object),
            &principal,
            &[],
            &[],
            &snapshot,
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W,
        ),
        utf16z_bytes("IPM.Note")
    );
    let rights = serialize_object_property(
        Some(&object),
        &principal,
        &[],
        &[],
        &snapshot,
        PID_TAG_RIGHTS,
    );
    assert_eq!(
        u32::from_le_bytes(rights.try_into().unwrap()),
        MAPI_FOLDER_ACCESS
    );
    let mut expected_extended_flags = Vec::new();
    let mut extended_flags = extended_folder_flags();
    extended_flags.extend_from_slice(&[0x03, 0x04]);
    extended_flags.extend_from_slice(&0u32.to_le_bytes());
    extended_flags.extend_from_slice(&[0x02, 0x10]);
    extended_flags.extend_from_slice(definition_id.as_bytes());
    write_mapi_value(
        &mut expected_extended_flags,
        PID_TAG_EXTENDED_FOLDER_FLAGS,
        &MapiValue::Binary(extended_flags),
    );
    assert_eq!(
        serialize_object_property(
            Some(&object),
            &principal,
            &[],
            &[],
            &snapshot,
            PID_TAG_EXTENDED_FOLDER_FLAGS,
        ),
        expected_extended_flags
    );
}

#[test]
pub(in crate::mapi) fn microsoft_get_message_status_response_uses_set_status_opcode() {
    let request = RopRequest {
        rop_id: RopId::GetMessageStatus.as_u8(),
        input_handle_index: Some(1),
        output_handle_index: None,
        payload: Vec::new(),
    };

    let response = rop_message_status_response(&request, 0);

    assert_eq!(
        response,
        vec![RopId::SetMessageStatus.as_u8(), 1, 0, 0, 0, 0, 0, 0, 0, 0]
    );
}

#[test]
pub(in crate::mapi) fn microsoft_open_embedded_message_response_includes_message_id() {
    let request = RopRequest {
        rop_id: RopId::OpenEmbeddedMessage.as_u8(),
        input_handle_index: Some(3),
        output_handle_index: Some(4),
        payload: Vec::new(),
    };
    let message_id = crate::mapi::identity::mapi_store_id(0x44);

    let response = rop_open_embedded_message_response(&request, message_id, "Embedded", 0);

    assert_eq!(response[0], RopId::OpenEmbeddedMessage.as_u8());
    assert_eq!(response[1], 4);
    assert_eq!(u32::from_le_bytes(response[2..6].try_into().unwrap()), 0);
    assert_eq!(response[6], 0);
    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(&response[7..15]),
        Some(message_id)
    );
    assert!(response
        .windows(utf16z_bytes("Embedded").len())
        .any(|window| window == utf16z_bytes("Embedded").as_slice()));
}

#[test]
pub(in crate::mapi) fn restriction_parser_preserves_content_fuzzy_levels() {
    let mut restriction = vec![MapiRestrictionType::Content as u8];
    restriction.extend_from_slice(&0x0002u16.to_le_bytes());
    restriction.extend_from_slice(&0x0001u16.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    write_utf16z(&mut restriction, "IPM.Schedule");

    assert_eq!(
        parse_mapi_restriction(&restriction).unwrap(),
        MapiRestriction::Content {
            property_tag: PID_TAG_MESSAGE_CLASS_W,
            value: "IPM.Schedule".to_string(),
            fuzzy_level_low: 0x0002,
            fuzzy_level_high: 0x0001,
        }
    );
}

#[test]
pub(in crate::mapi) fn restriction_parser_rejects_trailing_bytes() {
    let mut restriction = vec![MapiRestrictionType::Exist as u8];
    restriction.extend_from_slice(&PID_TAG_HAS_ATTACHMENTS.to_le_bytes());

    assert_eq!(
        parse_mapi_restriction(&restriction).unwrap(),
        MapiRestriction::Exist {
            property_tag: PID_TAG_HAS_ATTACHMENTS
        }
    );

    restriction.extend_from_slice(&[0xEE, 0xEE]);

    assert!(parse_mapi_restriction(&restriction).is_err());
}

#[test]
pub(in crate::mapi) fn outlook_logon_bootstrap_details_use_valid_store_icons() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let columns = [
        PID_TAG_MAILBOX_OWNER_NAME_W,
        PID_TAG_MAILBOX_OWNER_ENTRY_ID,
        PID_TAG_SERVER_TYPE_DISPLAY_NAME_W,
        PID_TAG_SERVER_CONNECTED_ICON,
        PID_TAG_SERVER_ACCOUNT_ICON,
        PID_TAG_PRIVATE,
        PID_TAG_OUTLOOK_STORE_STATE,
        PID_TAG_USER_GUID,
        PID_TAG_MAX_SUBMIT_MESSAGE_SIZE,
    ];

    assert!(is_outlook_logon_bootstrap_getprops(
        Some(&MapiObject::Logon),
        &columns
    ));
    let details = format_outlook_logon_bootstrap_property_details(&principal, &columns);
    let row_shape = outlook_logon_bootstrap_row_shape(&principal, &columns);

    assert!(details.contains("provider_uid_matches_nspi=true"));
    assert!(details.contains("r4=0x00000001"));
    assert!(details.contains("dn_null_terminated=true"));
    assert!(details.contains("private=true"));
    assert!(details.contains("max_submit_message_size_kb=35840"));
    assert!(details.contains("ico_len=70"));
    assert!(details.contains("reserved=0x0000"));
    assert!(details.contains("type=0x0001"));
    assert!(details.contains("count=1"));
    assert!(details.contains("bit_count=32"));
    assert!(details.contains("length_matches_directory=true"));
    assert_eq!(row_shape.estimated_rop_payload_bytes, 297);
    assert_eq!(row_shape.property_row_bytes, 290);
    assert_eq!(row_shape.icon_row_bytes, 144);
    assert_eq!(row_shape.non_icon_row_bytes, 146);
}

#[test]
fn contacts_search_getprops_content_count_matches_projected_results() {
    let account_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id,
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let rights = lpe_storage::CollaborationRights {
        may_read: true,
        may_write: true,
        may_delete: true,
        may_share: true,
    };
    let collection = lpe_storage::CollaborationCollection {
        id: "default".to_string(),
        kind: "contacts".to_string(),
        owner_account_id: account_id,
        owner_email: principal.email.clone(),
        owner_display_name: principal.display_name.clone(),
        display_name: "Contacts".to_string(),
        is_owned: true,
        rights: rights.clone(),
    };
    let contact_id = Uuid::parse_str("71717171-7171-7171-7171-717171717171").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        contact_id,
        crate::mapi::identity::mapi_store_id(67),
    );
    let contact = lpe_storage::AccessibleContact {
        id: contact_id,
        collection_id: collection.id.clone(),
        owner_account_id: account_id,
        owner_email: principal.email.clone(),
        owner_display_name: principal.display_name.clone(),
        rights,
        name: "Denis Ducret".to_string(),
        role: String::new(),
        email: "denis@example.test".to_string(),
        phone: String::new(),
        team: String::new(),
        notes: String::new(),
        ..Default::default()
    };
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![collection],
        Vec::new(),
        Vec::new(),
        vec![contact],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_search_folder_definitions(vec![lpe_storage::SearchFolderDefinition {
        id: Uuid::parse_str("34343434-3434-4434-8434-343434343402").unwrap(),
        account_id,
        role: "contacts_search".to_string(),
        display_name: "Contacts Search".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "contact".to_string(),
        scope_json: serde_json::json!({"scope": "contacts"}),
        restriction_json: serde_json::json!({"kind": "contacts_search"}),
        excluded_folder_roles: Vec::new(),
        is_builtin: true,
    }]);
    let object = MapiObject::Folder {
        folder_id: CONTACTS_SEARCH_FOLDER_ID,
        properties: HashMap::new(),
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&4096u16.to_le_bytes());
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&PID_TAG_CONTENT_COUNT.to_le_bytes());
    let request = RopRequest {
        rop_id: RopId::GetPropertiesSpecific as u8,
        input_handle_index: Some(1),
        output_handle_index: None,
        payload,
    };

    let response = rop_get_properties_specific_response(
        &request,
        Some(&object),
        &principal,
        &[],
        &[],
        &snapshot,
    );

    assert_eq!(&response[..7], &[0x07, 0x01, 0, 0, 0, 0, 0]);
    assert_eq!(u32::from_le_bytes(response[7..11].try_into().unwrap()), 1);
}

#[test]
fn public_folder_replica_responses_match_microsoft_counter_shape() {
    let request = RopRequest {
        rop_id: 0x42,
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: Vec::new(),
    };

    assert_eq!(
        rop_get_owning_servers_response(&request, &["LPE-MBX-01".to_string()]),
        [
            0x42, 0x00, 0, 0, 0, 0, 1, 0, 1, 0, b'L', b'P', b'E', b'-', b'M', b'B', b'X', b'-',
            b'0', b'1', 0,
        ]
    );
    assert_eq!(
        rop_public_folder_is_ghosted_response(&request, true),
        [0x45, 0x00, 0, 0, 0, 0, 1, 0, 0, 0, 0]
    );
    assert_eq!(
        rop_public_folder_is_ghosted_response(&request, false),
        [0x45, 0x00, 0, 0, 0, 0, 0]
    );
}

#[test]
pub(in crate::mapi) fn private_logon_places_exactly_13_folder_ids_before_response_flags() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let request = RopRequest {
        rop_id: 0xFE,
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: vec![0x01],
    };

    let response = rop_logon_response_body(&principal, &request);
    let response_flags_offset = 7 + PRIVATE_LOGON_SPECIAL_FOLDER_IDS.len() * 8;

    assert_eq!(PRIVATE_LOGON_SPECIAL_FOLDER_IDS.len(), 13);
    assert_eq!(response[response_flags_offset], 0x07);
    assert_eq!(
        &response[response_flags_offset + 1..response_flags_offset + 17],
        &principal.account_id.to_bytes_le()
    );
    assert_eq!(
        &response[7..response_flags_offset],
        PRIVATE_LOGON_SPECIAL_FOLDER_IDS
            .iter()
            .flat_map(|folder_id| {
                crate::mapi::identity::wire_id_bytes_from_object_id(*folder_id)
                    .unwrap()
                    .to_vec()
            })
            .collect::<Vec<_>>()
            .as_slice()
    );
}

#[test]
pub(in crate::mapi) fn private_logon_preserves_undercover_logon_flag_0x09() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let request = RopRequest {
        rop_id: 0xFE,
        input_handle_index: Some(0),
        output_handle_index: Some(1),
        payload: vec![0x09],
    };

    let response = rop_logon_response_body(&principal, &request);
    let response_flags_offset = 7 + PRIVATE_LOGON_SPECIAL_FOLDER_IDS.len() * 8;

    assert_eq!(response[6], 0x09);
    assert_eq!(response[response_flags_offset], 0x07);
}

#[test]
pub(in crate::mapi) fn long_term_id_from_id_accepts_outlook_and_emitted_counter_forms() {
    let canonical_id = crate::mapi::identity::CALENDAR_FOLDER_ID;
    let dynamic_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 3,
    );
    let cases = [
        (
            crate::mapi::identity::wire_id_bytes_from_object_id(canonical_id)
                .unwrap()
                .to_vec(),
            canonical_id,
        ),
        (
            {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&crate::mapi::identity::globcnt_bytes(
                    crate::mapi::identity::CALENDAR_FOLDER_COUNTER,
                ));
                bytes.extend_from_slice(&1u16.to_le_bytes());
                bytes
            },
            canonical_id,
        ),
        (
            {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&999u16.to_le_bytes());
                bytes.extend_from_slice(&crate::mapi::identity::globcnt_bytes(
                    crate::mapi::identity::CALENDAR_FOLDER_COUNTER,
                ));
                bytes
            },
            canonical_id,
        ),
        (
            {
                let mut bytes = crate::mapi::identity::globcnt_bytes(
                    crate::mapi::identity::CALENDAR_FOLDER_COUNTER,
                )
                .to_vec();
                bytes.reverse();
                bytes.extend_from_slice(&999u16.to_le_bytes());
                bytes
            },
            canonical_id,
        ),
        (
            {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(
                    &crate::mapi::identity::CONFLICTS_FOLDER_COUNTER.to_le_bytes()[..6],
                );
                bytes.extend_from_slice(&0u16.to_le_bytes());
                bytes
            },
            crate::mapi::identity::CONFLICTS_FOLDER_ID,
        ),
        (
            {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(
                    &crate::mapi::identity::global_counter_from_store_id(dynamic_id)
                        .unwrap()
                        .to_le_bytes()[..6],
                );
                bytes.extend_from_slice(&0u16.to_le_bytes());
                bytes
            },
            dynamic_id,
        ),
    ];

    for (bytes, expected_id) in cases {
        let request = RopRequest {
            rop_id: 0x43,
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: bytes,
        };
        let response = rop_long_term_id_from_id_response(&request);

        assert_eq!(&response[..6], &[0x43, 0x00, 0, 0, 0, 0]);
        assert_eq!(
            &response[6..30],
            &crate::mapi::identity::long_term_id_from_object_id(expected_id).unwrap()
        );
    }
}

#[test]
pub(in crate::mapi) fn long_term_id_from_id_unmapped_values_return_ec_not_found() {
    for bytes in [[0; 8], [0xFF; 8], [0x01, 0, 0, 0, 0, 0, 0, 0]] {
        let request = RopRequest {
            rop_id: 0x43,
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: bytes.to_vec(),
        };

        assert_eq!(
            rop_long_term_id_from_id_response(&request),
            vec![0x43, 0x00, 0x0F, 0x01, 0x04, 0x80]
        );
    }
}

#[test]
pub(in crate::mapi) fn id_from_long_term_id_accepts_mailbox_guid_aliases_and_special_stale_guid() {
    let principal_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let aliases = [*principal_guid.as_bytes(), principal_guid.to_bytes_le()];
    let normal_id = crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 4,
    );
    let special_id = crate::mapi::identity::CALENDAR_FOLDER_ID;
    let mut aliased = crate::mapi::identity::long_term_id_from_object_id(normal_id).unwrap();
    aliased[..16].copy_from_slice(&principal_guid.to_bytes_le());
    let mut stale_special = crate::mapi::identity::long_term_id_from_object_id(special_id).unwrap();
    stale_special[..16].copy_from_slice(&[0xA5; 16]);
    let mut stale_normal = crate::mapi::identity::long_term_id_from_object_id(normal_id).unwrap();
    stale_normal[..16].copy_from_slice(&[0xA5; 16]);

    for (long_term_id, expected_id) in [(aliased, normal_id), (stale_special, special_id)] {
        let request = RopRequest {
            rop_id: 0x44,
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: long_term_id.to_vec(),
        };
        let response = rop_id_from_long_term_id_response(&request, &aliases);

        assert_eq!(&response[..6], &[0x44, 0x00, 0, 0, 0, 0]);
        assert_eq!(
            &response[6..14],
            &crate::mapi::identity::wire_id_bytes_from_object_id(expected_id).unwrap()
        );
    }

    let request = RopRequest {
        rop_id: 0x44,
        input_handle_index: Some(0),
        output_handle_index: None,
        payload: stale_normal.to_vec(),
    };
    assert_eq!(
        rop_id_from_long_term_id_response(&request, &aliases),
        vec![0x44, 0x00, 0x0F, 0x01, 0x04, 0x80]
    );
}

#[test]
pub(in crate::mapi) fn ipm_subtree_ostid_read_prefers_session_client_write() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap(),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let client_ostid = vec![0x42; 40];
    let mut folder = MapiObject::Folder {
        folder_id: IPM_SUBTREE_FOLDER_ID,
        properties: HashMap::new(),
    };

    apply_mapi_property_values(
        Some(&mut folder),
        vec![(PID_TAG_OST_OSTID, MapiValue::Binary(client_ostid.clone()))],
    )
    .unwrap();
    let row = serialize_object_property(
        Some(&folder),
        &principal,
        &[],
        &[],
        &MapiMailStoreSnapshot::empty(),
        PID_TAG_OST_OSTID,
    );

    assert_eq!(u16::from_le_bytes(row[0..2].try_into().unwrap()), 40);
    assert_eq!(&row[2..], client_ostid.as_slice());
}

#[test]
pub(in crate::mapi) fn golden_open_folder_rop_round_trips_through_typed_parser() {
    let folder_id = crate::mapi::identity::mapi_store_id(0x1122_3344_5566);
    let mut golden = vec![0x02, 0x00, 0x00, 0x01];
    golden.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(folder_id).unwrap(),
    );
    golden.push(0x00);

    let mut cursor = Cursor::new(&golden);
    let request = read_rop_request(&mut cursor).unwrap();

    assert_eq!(
        request.typed(),
        TypedRopRequest::OpenFolder(RopOpenFolderRequest {
            input_handle_index: 0,
            output_handle_index: 1,
            folder_id,
            open_mode_flags: 0,
        })
    );
    assert_eq!(serialize_rop_request(&request).unwrap(), golden);
    assert_eq!(cursor.remaining(), 0);
}

#[test]
pub(in crate::mapi) fn golden_set_columns_rop_round_trips_through_typed_parser() {
    let golden = vec![
        0x12, 0x00, 0x02, 0x00, 0x02, 0x00, 0x1F, 0x00, 0x37, 0x00, 0x03, 0x00, 0x0E, 0x0C,
    ];

    let mut cursor = Cursor::new(&golden);
    let request = read_rop_request(&mut cursor).unwrap();

    assert_eq!(
        request.typed(),
        TypedRopRequest::SetColumns(RopSetColumnsRequest {
            input_handle_index: 2,
            flags: 0,
            property_tags: vec![0x0037_001F, 0x0C0E_0003],
        })
    );
    assert_eq!(serialize_rop_request(&request).unwrap(), golden);
    assert_eq!(cursor.remaining(), 0);
}

#[test]
fn microsoft_oxctabl_get_contents_table_example_round_trips_through_typed_parser() {
    let golden = vec![0x05, 0x00, 0x00, 0x01, 0x00];
    let mut cursor = Cursor::new(&golden);
    let request = read_rop_request(&mut cursor).unwrap();

    assert_eq!(
        request.typed(),
        TypedRopRequest::OpenTable(RopOpenTableRequest {
            rop_id: RopId::GetContentsTable.as_u8(),
            input_handle_index: 0,
            output_handle_index: 1,
            table_flags: 0,
        })
    );
    assert_eq!(serialize_rop_request(&request).unwrap(), golden);
    assert_eq!(cursor.remaining(), 0);
}

#[test]
fn microsoft_oxctabl_set_columns_example_round_trips_through_typed_parser() {
    let golden = vec![
        0x12, 0x00, 0x01, 0x00, 0x06, 0x00, 0x14, 0x00, 0x48, 0x67, 0x14, 0x00, 0x4A, 0x67, 0x14,
        0x00, 0x4D, 0x67, 0x03, 0x00, 0x4E, 0x67, 0x1F, 0x00, 0x37, 0x00, 0x40, 0x00, 0x06, 0x0E,
    ];

    let mut cursor = Cursor::new(&golden);
    let request = read_rop_request(&mut cursor).unwrap();

    assert_eq!(
        request.typed(),
        TypedRopRequest::SetColumns(RopSetColumnsRequest {
            input_handle_index: 1,
            flags: 0,
            property_tags: vec![
                0x6748_0014,
                0x674A_0014,
                0x674D_0014,
                0x674E_0003,
                0x0037_001F,
                0x0E06_0040,
            ],
        })
    );
    assert_eq!(serialize_rop_request(&request).unwrap(), golden);
    assert_eq!(cursor.remaining(), 0);
}

#[test]
fn microsoft_oxctabl_sort_and_query_rows_examples_parse_through_typed_parser() {
    let sort_golden = vec![
        0x13, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x06, 0x0E, 0x01,
    ];
    let mut sort_cursor = Cursor::new(&sort_golden);
    let sort = read_rop_request(&mut sort_cursor).unwrap();

    assert_eq!(RopId::from_u8(sort.rop_id), Some(RopId::SortTable));
    assert_eq!(sort.input_handle_index, Some(1));
    assert_eq!(sort.sort_orders().len(), 1);
    assert_eq!(sort.sort_orders()[0].property_tag, 0x0E06_0040);
    assert_eq!(sort.sort_orders()[0].order, 1);
    assert_eq!(sort.sort_category_count(), 0);
    assert_eq!(sort_cursor.remaining(), 0);

    let query_golden = vec![0x15, 0x00, 0x01, 0x00, 0x01, 0x32, 0x00];
    let mut query_cursor = Cursor::new(&query_golden);
    let query = read_rop_request(&mut query_cursor).unwrap();

    assert_eq!(
        query.typed(),
        TypedRopRequest::QueryRows(RopQueryRowsRequest {
            input_handle_index: 1,
            flags: 0,
            forward_read: true,
            row_count: 0x32,
        })
    );
    assert_eq!(serialize_rop_request(&query).unwrap(), query_golden);
    assert_eq!(query_cursor.remaining(), 0);
}

#[test]
fn microsoft_oxctabl_category_examples_parse_expected_fields() {
    let sort_golden = vec![
        0x13, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00, 0x01, 0x00, 0x1F, 0x30, 0x08, 0x80, 0x00,
        0x40, 0x00, 0x06, 0x0E, 0x01,
    ];
    let mut sort_cursor = Cursor::new(&sort_golden);
    let sort = read_rop_request(&mut sort_cursor).unwrap();

    assert_eq!(RopId::from_u8(sort.rop_id), Some(RopId::SortTable));
    assert_eq!(sort.input_handle_index, Some(0));
    assert_eq!(sort.sort_category_count(), 1);
    assert_eq!(sort.sort_expanded_count(), 1);
    assert_eq!(sort.sort_orders().len(), 2);
    assert_eq!(sort.sort_orders()[0].property_tag, 0x8008_301F);
    assert_eq!(sort.sort_orders()[0].order, 0);
    assert_eq!(sort.sort_orders()[1].property_tag, 0x0E06_0040);
    assert_eq!(sort.sort_orders()[1].order, 1);
    assert_eq!(sort_cursor.remaining(), 0);

    let expand_golden = vec![
        0x59, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0xF1, 0x88, 0xBD,
    ];
    let mut expand_cursor = Cursor::new(&expand_golden);
    let expand = read_rop_request(&mut expand_cursor).unwrap();

    assert_eq!(RopId::from_u8(expand.rop_id), Some(RopId::ExpandRow));
    assert_eq!(expand.input_handle_index, Some(1));
    assert_eq!(expand.expand_max_row_count(), 0);
    assert_eq!(expand.category_id(), Some(0xBD88_F100_0000_0001));
    assert_eq!(expand_cursor.remaining(), 0);

    let query_golden = vec![0x15, 0x00, 0x00, 0x00, 0x01, 0x32, 0x00];
    let mut query_cursor = Cursor::new(&query_golden);
    let query = read_rop_request(&mut query_cursor).unwrap();

    assert_eq!(
        query.typed(),
        TypedRopRequest::QueryRows(RopQueryRowsRequest {
            input_handle_index: 0,
            flags: 0,
            forward_read: true,
            row_count: 0x32,
        })
    );
    assert_eq!(serialize_rop_request(&query).unwrap(), query_golden);
    assert_eq!(query_cursor.remaining(), 0);
}

#[test]
pub(in crate::mapi) fn expand_row_payload_never_decodes_as_message_ids() {
    let category_id = crate::mapi::identity::mapi_store_id(0x0102_0304_0506);
    let mut golden = vec![RopId::ExpandRow.as_u8(), 0x00, 0x00];
    golden.extend_from_slice(&1u16.to_le_bytes());
    golden.extend_from_slice(
        &crate::mapi::identity::wire_id_bytes_from_object_id(category_id).unwrap(),
    );

    let mut cursor = Cursor::new(&golden);
    let request = read_rop_request(&mut cursor).unwrap();

    assert_eq!(RopId::from_u8(request.rop_id), Some(RopId::ExpandRow));
    assert_eq!(request.message_ids(), Vec::<u64>::new());
    assert_eq!(cursor.remaining(), 0);
}

#[test]
pub(in crate::mapi) fn sync_import_message_move_uses_length_prefixed_source_ids() {
    let source_folder_id = crate::mapi::identity::mapi_store_id(0x0102_0304_0507);
    let source_message_id = crate::mapi::identity::mapi_store_id(0x0102_0304_0508);
    let destination_message_id = crate::mapi::identity::mapi_store_id(0x0102_0304_0509);
    let change_number = crate::mapi::identity::mapi_store_id(0x0102_0304_0510);
    let source_folder_wire =
        crate::mapi::identity::wire_id_bytes_from_object_id(source_folder_id).unwrap();
    let source_message_wire =
        crate::mapi::identity::wire_id_bytes_from_object_id(source_message_id).unwrap();
    let destination_message_wire =
        crate::mapi::identity::wire_id_bytes_from_object_id(destination_message_id).unwrap();
    let change_number_wire =
        crate::mapi::identity::wire_id_bytes_from_object_id(change_number).unwrap();
    let predecessor_change_list = [0x01, 0x02, 0x03, 0x04];
    let mut golden = vec![RopId::SynchronizationImportMessageMove.as_u8(), 0x00, 0x00];
    for field in [
        source_folder_wire.as_slice(),
        source_message_wire.as_slice(),
        predecessor_change_list.as_slice(),
        destination_message_wire.as_slice(),
        change_number_wire.as_slice(),
    ] {
        golden.extend_from_slice(&(field.len() as u32).to_le_bytes());
        golden.extend_from_slice(field);
    }

    let mut cursor = Cursor::new(&golden);
    let request = read_rop_request(&mut cursor).unwrap();

    assert_eq!(
        RopId::from_u8(request.rop_id),
        Some(RopId::SynchronizationImportMessageMove)
    );
    assert_eq!(
        request.import_move(),
        Some((source_folder_id, source_message_id))
    );
    assert_eq!(cursor.remaining(), 0);

    let mut truncated = Cursor::new(&golden[..golden.len() - 1]);
    assert!(read_rop_request(&mut truncated).is_err());
}

#[test]
pub(in crate::mapi) fn malformed_supported_rop_buffer_fails_without_partial_request() {
    let mut cursor = Cursor::new(&[0x02, 0x00, 0x00, 0x01, 0x88, 0x77]);

    assert!(read_rop_request(&mut cursor).is_err());
}

#[test]
pub(in crate::mapi) fn supported_rop_uses_enum_classification_without_terminal_stop() {
    let mut cursor = Cursor::new(&[0x04, 0x00, 0x01, 0x02, 0x04]);
    let request = read_rop_request(&mut cursor).unwrap();

    assert_eq!(
        RopId::from_u8(request.rop_id),
        Some(RopId::GetHierarchyTable)
    );
    assert_eq!(request.typed().rop_id(), RopId::GetHierarchyTable.as_u8());
    assert!(!request.typed().unsupported_is_terminal());
    assert_eq!(request.input_handle_index(), Some(1));
    assert_eq!(request.output_handle_index, Some(2));
    assert_eq!(cursor.remaining(), 0);
}

#[test]
pub(in crate::mapi) fn unsupported_rop_is_terminal_without_consuming_later_rop_bytes() {
    let mut cursor = Cursor::new(&[0xAA, 0x00, 0x03, 0x01, 0x00, 0x00]);
    let request = read_rop_request(&mut cursor).unwrap();

    assert_eq!(RopId::from_u8(request.rop_id), None);
    assert!(request.typed().unsupported_is_terminal());
    assert_eq!(request.input_handle_index(), Some(3));
    assert_eq!(cursor.remaining(), 3);
    assert!(serialize_rop_request(&request).is_err());
    assert_eq!(
        unsupported_rop_response(0xAA, request.response_handle_index()),
        vec![0xAA, 0x03, 0x02, 0x01, 0x04, 0x80]
    );
}

#[test]
pub(in crate::mapi) fn malformed_handle_table_is_rejected() {
    assert!(read_handle_table(&[0x01, 0x02, 0x03]).is_err());
    assert_eq!(
        read_handle_table(&[0x6E, 0x00, 0x00, 0x00]).unwrap(),
        vec![0x6E]
    );
}

#[test]
pub(in crate::mapi) fn invalid_input_handle_index_serializes_common_rop_error() {
    let request = RopRequest {
        rop_id: 0x04,
        input_handle_index: Some(7),
        output_handle_index: Some(1),
        payload: vec![0],
    };
    let handles = read_handle_table(&[0x6E, 0x00, 0x00, 0x00]).unwrap();

    assert_eq!(input_handle(&handles, &request), None);
    assert_eq!(
        rop_handle_index_error_response(&request),
        vec![0x04, 0x01, 0x0F, 0x01, 0x04, 0x80]
    );
}

#[test]
pub(in crate::mapi) fn upload_state_success_response_uses_input_handle_index() {
    for rop_id in [0x75, 0x76, 0x77] {
        let request = RopRequest {
            rop_id,
            input_handle_index: Some(3),
            output_handle_index: Some(9),
            payload: Vec::new(),
        };

        assert_eq!(
            rop_upload_state_success_response(&request),
            vec![rop_id, 3, 0, 0, 0, 0]
        );
    }
}

#[test]
pub(in crate::mapi) fn get_address_types_success_response_uses_input_handle_index() {
    let request = RopRequest {
        rop_id: RopId::GetAddressTypes.as_u8(),
        input_handle_index: Some(3),
        output_handle_index: Some(9),
        payload: Vec::new(),
    };

    assert_eq!(
        rop_get_address_types_response(&request),
        vec![0x49, 0x03, 0, 0, 0, 0, 2, 0, 8, 0, b'E', b'X', 0, b'S', b'M', b'T', b'P', 0,]
    );
}

#[test]
pub(in crate::mapi) fn note_and_journal_message_handles_serialize_object_properties() {
    let note_id = Uuid::parse_str("51515151-5151-5151-5151-515151515151").unwrap();
    let journal_id = Uuid::parse_str("61616161-6161-6161-6161-616161616161").unwrap();
    crate::mapi::identity::remember_mapi_identity(
        note_id,
        crate::mapi::identity::mapi_store_id(90),
    );
    crate::mapi::identity::remember_mapi_identity(
        journal_id,
        crate::mapi::identity::mapi_store_id(91),
    );
    let snapshot = MapiMailStoreSnapshot::new(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_notes_and_journal(
        vec![ClientNote {
            id: note_id,
            title: "Sticky note".to_string(),
            body_text: "Remember Outlook open-message reads".to_string(),
            color: "yellow".to_string(),
            categories_json: "[]".to_string(),
            created_at: "2026-05-19T12:00:00Z".to_string(),
            updated_at: "2026-05-19T12:30:00Z".to_string(),
        }],
        vec![JournalEntry {
            id: journal_id,
            subject: "Support call".to_string(),
            body_text: "Call notes".to_string(),
            entry_type: "phone-call".to_string(),
            message_class: "IPM.Activity".to_string(),
            starts_at: Some("2026-05-19T13:00:00Z".to_string()),
            ends_at: Some("2026-05-19T13:15:00Z".to_string()),
            occurred_at: None,
            companies_json: "[]".to_string(),
            contacts_json: "[]".to_string(),
            created_at: "2026-05-19T12:55:00Z".to_string(),
            updated_at: "2026-05-19T13:15:00Z".to_string(),
        }],
    );
    let principal = AccountPrincipal {
        tenant_id: Uuid::nil(),
        account_id: Uuid::nil(),
        email: "test@example.test".to_string(),
        display_name: "Test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };

    let note_object = MapiObject::Note {
        folder_id: NOTES_FOLDER_ID,
        note_id: crate::mapi::identity::mapi_store_id(90),
    };
    let journal_object = MapiObject::JournalEntry {
        folder_id: JOURNAL_FOLDER_ID,
        journal_entry_id: crate::mapi::identity::mapi_store_id(91),
    };
    let notes = snapshot.notes_for_folder(NOTES_FOLDER_ID);
    let journal_entries = snapshot.journal_entries_for_folder(JOURNAL_FOLDER_ID);

    assert_eq!(
        serialize_object_property(
            Some(&note_object),
            &principal,
            &[],
            &[],
            &snapshot,
            PID_TAG_MESSAGE_CLASS_W,
        ),
        serialize_note_row(
            &notes[0].note,
            crate::mapi::identity::mapi_store_id(90),
            NOTES_FOLDER_ID,
            &[PID_TAG_MESSAGE_CLASS_W],
        )
    );
    assert_eq!(
        serialize_object_property(
            Some(&journal_object),
            &principal,
            &[],
            &[],
            &snapshot,
            PID_TAG_SUBJECT_W,
        ),
        serialize_journal_entry_row(
            &journal_entries[0].entry,
            crate::mapi::identity::mapi_store_id(91),
            JOURNAL_FOLDER_ID,
            &[PID_TAG_SUBJECT_W],
        )
    );
}

#[test]
pub(in crate::mapi) fn reserved_rop_is_terminal_and_uses_common_unsupported_response() {
    let mut cursor = Cursor::new(&[0x28, 0x00, 0x03, 0xAA]);
    let request = read_rop_request(&mut cursor).unwrap();

    assert!(request.typed().unsupported_is_terminal());
    assert_eq!(request.input_handle_index(), Some(3));
    assert_eq!(cursor.remaining(), 0);
    assert!(serialize_rop_request(&request).is_err());
    assert_eq!(
        unsupported_rop_response(0x28, request.response_handle_index()),
        vec![0x28, 0x03, 0x02, 0x01, 0x04, 0x80]
    );
}
