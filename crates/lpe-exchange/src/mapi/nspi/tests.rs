use super::*;

#[test]
fn nspi_request_and_property_manifests_cover_implemented_static_values() {
    for request_type in NSPI_SUPPORTED_REQUEST_TYPES {
        assert!(
            request_type.requires_nspi_session()
                || matches!(
                    request_type,
                    MapiRequestType::Bind | MapiRequestType::DnToMid | MapiRequestType::Unbind
                )
        );
        assert_ne!(request_type.header_value(), "");
    }

    for tag in NSPI_BOOTSTRAP_PROPERTY_TAGS {
        assert!(nspi_property_tag_is_supported(*tag));
    }
    for tag in NSPI_ADDITIONAL_REQUESTED_PROPERTY_TAGS {
        assert!(nspi_property_tag_is_supported(*tag));
    }
    assert_eq!(nspi_known_unsupported_property_tag_name(0x3A06_001F), None);
    assert_eq!(nspi_known_unsupported_property_tag_name(0x0FF8_0102), None);
    assert_eq!(nspi_known_unsupported_property_tag_name(0x3A20_001F), None);
    assert_eq!(nspi_known_unsupported_property_tag_name(0x3A1B_101F), None);
}

#[tokio::test]
async fn get_hierarchy_info_returns_successful_address_book_hierarchy() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
        account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };
    let response = nspi_hierarchy_info_response(
        &principal,
        &NSPI_UNICODE_STRINGS_FLAG.to_le_bytes(),
        "test-request",
    );

    assert_eq!(
        response_header(&response, "x-requesttype").unwrap(),
        "GetHierarchyInfo"
    );
    assert_eq!(response_header(&response, "x-responsecode").unwrap(), "0");

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let global_address_list = "Global Address List"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    assert!(body
        .windows(global_address_list.len())
        .any(|window| window == global_address_list));
}

#[test]
fn microsoft_oxprops_contact_address_book_fields_project_to_nspi_rows() {
    let account_id = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let entry = ExchangeAddressBookEntry {
        id: Uuid::parse_str("26b8ebbd-63a5-4741-b8d6-d7eda9c31c3d").unwrap(),
        display_name: "Bob Contact".to_string(),
        email: "bob.contact@example.test".to_string(),
        entry_kind: ExchangeAddressBookEntryKind::Contact,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
        member_emails: Vec::new(),
        details: ExchangeAddressBookEntryDetails {
            given_name: "Bob".to_string(),
            surname: "Contact".to_string(),
            nickname: "Bobby".to_string(),
            primary_phone: "+41 22 555 0100".to_string(),
            mobile_phone: "+41 79 555 0100".to_string(),
            home_phone: "+41 22 555 0199".to_string(),
            business2_phones: vec!["+41 22 555 0101".to_string(), "+41 22 555 0102".to_string()],
            company_name: "Fabrikam".to_string(),
            title: "Architect".to_string(),
            department_name: "Engineering".to_string(),
            postal_address: "1 Example Way, Geneva 1201".to_string(),
            street_address: "1 Example Way".to_string(),
            locality: "Geneva".to_string(),
            state_or_province: "GE".to_string(),
            country: "Switzerland".to_string(),
            postal_code: "1201".to_string(),
            phonetic_given_name: "Bahb".to_string(),
            phonetic_surname: "Kontaakt".to_string(),
        },
    };

    for tag in [
        0x3A06_001F,
        0x3A0B_001F,
        0x3A4F_001F,
        0x3A08_001F,
        0x3A09_001F,
        0x3A1A_001F,
        0x3A1B_001F,
        0x3A1B_101F,
        0x3A1C_001F,
        0x3A16_001F,
        0x3A17_001F,
        0x3A18_001F,
        0x3A15_001F,
        0x3A26_001F,
        0x3A27_001F,
        0x3A28_001F,
        0x3A29_001F,
        0x3A2A_001F,
        0x3A8D_001F,
        0x3A8E_001F,
    ] {
        assert!(nspi_property_tag_is_supported(tag));
        assert_eq!(nspi_known_unsupported_property_tag_name(tag), None);
    }
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A06_001F)),
        "Bob"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A0B_001F)),
        "Contact"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A4F_001F)),
        "Bobby"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A08_001F)),
        "+41 22 555 0100"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A1A_001F)),
        "+41 22 555 0100"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A09_001F)),
        "+41 22 555 0199"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A1B_001F)),
        "+41 22 555 0101"
    );
    assert_eq!(
        nspi_multistring_value(nspi_entry_value(account_id, &entry, 0x3A1B_101F)),
        vec!["+41 22 555 0101".to_string(), "+41 22 555 0102".to_string()]
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A1C_001F)),
        "+41 79 555 0100"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A16_001F)),
        "Fabrikam"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A17_001F)),
        "Architect"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A18_001F)),
        "Engineering"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A15_001F)),
        "1 Example Way, Geneva 1201"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A26_001F)),
        "Switzerland"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A27_001F)),
        "Geneva"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A28_001F)),
        "GE"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A29_001F)),
        "1 Example Way"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A2A_001F)),
        "1201"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A8D_001F)),
        "Bahb"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A8E_001F)),
        "Kontaakt"
    );
}

#[test]
fn nspi_entry_required_address_book_properties_match_exchange_identity_contract() {
    let account_id = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let entry = ExchangeAddressBookEntry {
        id: Uuid::parse_str("26b8ebbd-63a5-4741-b8d6-d7eda9c31c3d").unwrap(),
        display_name: "Bob Contact".to_string(),
        email: "bob.contact@example.test".to_string(),
        entry_kind: ExchangeAddressBookEntryKind::Contact,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
        member_emails: Vec::new(),
        details: ExchangeAddressBookEntryDetails::default(),
    };
    let legacy_dn = nspi_entry_unprefixed_legacy_dn(&entry);
    let permanent_entry_id = nspi_entry_permanent_entry_id(&entry);

    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3002_001F)),
        "EX"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3003_001F)),
        legacy_dn
    );
    assert_eq!(
        nspi_binary_value(nspi_entry_value(account_id, &entry, 0x0FFF_0102)),
        permanent_entry_id
    );
    assert_eq!(
        nspi_binary_value(nspi_entry_value(account_id, &entry, 0x3902_0102)),
        permanent_entry_id
    );
    assert_eq!(
        nspi_binary_value(nspi_entry_value(account_id, &entry, 0x0FF9_0102)),
        permanent_entry_id
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x803C_001F)),
        legacy_dn
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A20_001F)),
        "Bob Contact"
    );
    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &entry, 0x39FF_001F)),
        "Bob Contact"
    );
    assert_eq!(
        nspi_u32_value(nspi_entry_value(account_id, &entry, 0x3F08_0003)),
        0
    );
    assert_eq!(
        nspi_u32_value(nspi_entry_value(account_id, &entry, 0xFFFD_0003)),
        0
    );
    assert_eq!(
        nspi_binary_value(nspi_entry_value(account_id, &entry, 0x300B_0102)),
        format!("EX:{}", legacy_dn.to_ascii_uppercase())
            .bytes()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>()
    );
}

#[test]
fn principal_lookup_accepts_autodiscover_and_connect_legacy_dn_aliases() {
    let principal = AccountPrincipal {
        tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
        account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
        email: "test@l-p-e.ch".to_string(),
        display_name: "test".to_string(),
        quota_mb: None,
        quota_used_octets: None,
    };

    assert!(nspi_lookup_matches_principal(
        "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=test-l-p-e-ch",
        &principal
    ));
    assert!(nspi_lookup_matches_principal(
        "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=acct-test-l-p-e-ch",
        &principal
    ));
    assert!(nspi_lookup_matches_principal(
        "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=test",
        &principal
    ));
}

#[test]
fn get_props_stat_current_rec_is_parsed_from_documented_stat_field() {
    let request = hex_bytes(
            "00000000ff0000000000000012000080000000000000000000000000b00400000904000009080000ff0100000002016d8c00000000",
        );

    assert_eq!(nspi_stat_current_rec(&request), Some(0x8000_0012));
}

#[test]
fn get_props_stat_words_are_not_entry_ids_when_current_rec_is_empty() {
    let request = hex_bytes(
            "00000000ff0000000000000000000000000000000000000000000000b00400000904000009080000ff0100000002016d8c00000000",
        );

    assert_eq!(nspi_stat_current_rec(&request), None);
    assert!(!nspi_request_has_entry_selector(&request));
}

#[test]
fn requested_entry_ids_ignore_misaligned_utf16_lookup_bytes() {
    let mut request = vec![0, 0, 0];
    request.extend("test@l-p-e.ch\0".encode_utf16().flat_map(u16::to_le_bytes));

    assert!(nspi_requested_entry_ids(&request).is_empty());
    assert_eq!(
        scan_address_book_lookup_values(&request),
        vec!["test@l-p-e.ch".to_string()]
    );
}

#[test]
fn query_rows_count_skips_explicit_table_before_count() {
    let mut request = Vec::new();
    request.extend_from_slice(&0u32.to_le_bytes());
    request.extend_from_slice(&[0; 36]);
    request.extend_from_slice(&2u32.to_le_bytes());
    request.extend_from_slice(&0x8000_0034u32.to_le_bytes());
    request.extend_from_slice(&0x4000_0001u32.to_le_bytes());
    request.extend_from_slice(&7u32.to_le_bytes());

    assert_eq!(nspi_query_rows_count("QueryRows", &request), Some(7));
}

#[test]
fn query_rows_count_parses_outlook_explicit_table_body() {
    let request = hex_bytes(
            "00000000ff0000000000000000000000000000000000000000000000e40400000904000009080000010000003400008001000000ff0b0000000201ff0f1f0001300300fe0f030000391f00203a1f0003301f0002300b00403a1f00ff391f00",
        );

    assert_eq!(nspi_query_rows_count("QueryRows", &request), Some(1));
    assert_eq!(
        nspi_query_rows_explicit_entry_ids("QueryRows", &request),
        vec![0x8000_0034]
    );
}

#[test]
fn query_rows_parser_falls_back_to_body_shape_for_logged_outlook_body() {
    let request = hex_bytes(
            "00000000ff0000000000000000000000000000000000000000000000e40400000904000009080000010000003400008001000000ff0b0000000201ff0f1f0001300300fe0f030000391f00203a1f0003301f0002300b00403a1f00ff391f00",
        );

    assert_eq!(nspi_query_rows_count("", &request), Some(1));
    assert_eq!(
        nspi_query_rows_explicit_entry_ids("", &request),
        vec![0x8000_0034]
    );
}

#[test]
fn query_rows_parser_handles_shifted_outlook_stat_boundary() {
    let request = hex_bytes(
            "00000000ff000000000000000000000000000000000000000000000000e40400000904000009080000010000003400008001000000ff0b0000000201ff0f1f0001300300fe0f030000391f00203a1f0003301f0002300b00403a1f00ff391f00",
        );

    assert_eq!(nspi_query_rows_count("QueryRows", &request), Some(1));
    assert_eq!(
        nspi_query_rows_explicit_entry_ids("QueryRows", &request),
        vec![0x8000_0034]
    );
}

#[test]
fn query_rows_explicit_table_filters_rows_by_requested_mid() {
    let account_id = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let contact = ExchangeAddressBookEntry {
        id: Uuid::from_bytes([0x37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
        display_name: "Denis Ducret".to_string(),
        email: "denis.ducret@sdic.ch".to_string(),
        entry_kind: ExchangeAddressBookEntryKind::Contact,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
        member_emails: Vec::new(),
        details: ExchangeAddressBookEntryDetails::default(),
    };
    let account = ExchangeAddressBookEntry {
        id: Uuid::from_bytes([0x34, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
        display_name: "test".to_string(),
        email: "test@l-p-e.ch".to_string(),
        entry_kind: ExchangeAddressBookEntryKind::Account,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
        member_emails: Vec::new(),
        details: ExchangeAddressBookEntryDetails::default(),
    };

    let filtered =
        nspi_filter_explicit_table_entries(account_id, vec![contact, account], &[0x8000_0034]);

    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].email, "test@l-p-e.ch");
}

#[test]
fn lookup_scanner_ignores_binary_words_that_only_contain_at_sign() {
    assert!(scan_address_book_lookup_values(b"@\x3a\x1f\0").is_empty());
    assert_eq!(
        scan_address_book_lookup_values(b"SMTP:alice@example.test\0"),
        vec!["alice@example.test".to_string()]
    );
}

#[test]
fn nspi_entry_debug_summary_includes_mid_kind_email_and_name() {
    let account_id = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let entry = ExchangeAddressBookEntry {
        id: Uuid::parse_str("26b8ebbd-63a5-4741-b8d6-d7eda9c31c3d").unwrap(),
        display_name: "Bob Contact".to_string(),
        email: "bob.contact@example.test".to_string(),
        entry_kind: ExchangeAddressBookEntryKind::Contact,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
        member_emails: Vec::new(),
        details: ExchangeAddressBookEntryDetails::default(),
    };

    let summary = format_nspi_entry_summaries_for_debug(account_id, &[entry]);

    assert!(summary.contains(":contact:bob.contact@example.test:Bob Contact"));
}

#[test]
fn nspi_duplicate_debug_groups_rows_by_kind_email_and_name() {
    let entries = vec![
        ExchangeAddressBookEntry {
            id: Uuid::parse_str("26b8ebbd-63a5-4741-b8d6-d7eda9c31c3d").unwrap(),
            display_name: "Bob Contact".to_string(),
            email: "bob.contact@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Contact,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
            details: ExchangeAddressBookEntryDetails::default(),
        },
        ExchangeAddressBookEntry {
            id: Uuid::parse_str("9bd2958d-9858-4fe3-8e6b-4ddd9dcc6bc6").unwrap(),
            display_name: " bob contact ".to_string(),
            email: "BOB.CONTACT@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Contact,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
            details: ExchangeAddressBookEntryDetails::default(),
        },
    ];

    let (count, keys) = format_nspi_duplicate_entry_keys_for_debug(&entries);

    assert_eq!(count, 1);
    assert_eq!(keys, "contact:bob.contact@example.test:bob contactx2");
}

#[test]
fn nspi_duplicate_contacts_have_distinct_outlook_identity_fields() {
    let account_id = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
    let first = ExchangeAddressBookEntry {
        id: Uuid::parse_str("26b8ebbd-63a5-4741-b8d6-d7eda9c31c3d").unwrap(),
        display_name: "Bob Contact".to_string(),
        email: "bob.contact@example.test".to_string(),
        entry_kind: ExchangeAddressBookEntryKind::Contact,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
        member_emails: Vec::new(),
        details: ExchangeAddressBookEntryDetails::default(),
    };
    let second = ExchangeAddressBookEntry {
        id: Uuid::parse_str("9bd2958d-9858-4fe3-8e6b-4ddd9dcc6bc6").unwrap(),
        display_name: "Bob Contact".to_string(),
        email: "bob.contact@example.test".to_string(),
        entry_kind: ExchangeAddressBookEntryKind::Contact,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
        member_emails: Vec::new(),
        details: ExchangeAddressBookEntryDetails::default(),
    };

    assert_eq!(
        nspi_string_value(nspi_entry_value(account_id, &first, 0x39FE_001F)),
        nspi_string_value(nspi_entry_value(account_id, &second, 0x39FE_001F))
    );
    assert_ne!(
        nspi_u32_value(nspi_entry_value(account_id, &first, 0x3000_0003)),
        nspi_u32_value(nspi_entry_value(account_id, &second, 0x3000_0003))
    );
    assert_ne!(nspi_entry_legacy_dn(&first), nspi_entry_legacy_dn(&second));
    assert_ne!(
        nspi_binary_value(nspi_entry_value(account_id, &first, 0x0FF6_0102)),
        nspi_binary_value(nspi_entry_value(account_id, &second, 0x0FF6_0102))
    );
    assert_ne!(
        nspi_binary_value(nspi_entry_value(account_id, &first, 0x0FF9_0102)),
        nspi_binary_value(nspi_entry_value(account_id, &second, 0x0FF9_0102))
    );
    assert_ne!(
        nspi_binary_value(nspi_entry_value(account_id, &first, 0x0FFF_0102)),
        nspi_binary_value(nspi_entry_value(account_id, &second, 0x0FFF_0102))
    );
    assert_ne!(
        nspi_binary_value(nspi_entry_value(account_id, &first, 0x300B_0102)),
        nspi_binary_value(nspi_entry_value(account_id, &second, 0x300B_0102))
    );
}

fn nspi_binary_value(value: NspiValue<'_>) -> Vec<u8> {
    match value {
        NspiValue::OwnedBinary(value) => value,
        _ => panic!("expected binary NSPI value"),
    }
}

fn nspi_u32_value(value: NspiValue<'_>) -> u32 {
    match value {
        NspiValue::U32(value) => value,
        _ => panic!("expected u32 NSPI value"),
    }
}

fn nspi_string_value(value: NspiValue<'_>) -> String {
    match value {
        NspiValue::String(value) => value.to_string(),
        NspiValue::OwnedString(value) => value,
        _ => panic!("expected string NSPI value"),
    }
}

fn nspi_multistring_value(value: NspiValue<'_>) -> Vec<String> {
    match value {
        NspiValue::MultiString(value) => value,
        _ => panic!("expected multistring NSPI value"),
    }
}

fn hex_bytes(hex: &str) -> Vec<u8> {
    let compact = hex
        .as_bytes()
        .iter()
        .copied()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    compact
        .chunks_exact(2)
        .map(|chunk| {
            let high = hex_value(chunk[0]);
            let low = hex_value(chunk[1]);
            (high << 4) | low
        })
        .collect()
}

fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => panic!("invalid hex byte"),
    }
}
