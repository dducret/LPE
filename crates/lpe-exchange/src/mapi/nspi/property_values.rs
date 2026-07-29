use super::*;

pub(super) const NSPI_BOOTSTRAP_PROPERTY_TAGS: &[u32] = &[
    0x3001_001F, // PidTagDisplayName
    0x39FE_001F, // PidTagSmtpAddress
    0x3003_001F, // PidTagEmailAddress
    0x3A00_001F, // PidTagAccount
    0x0FFE_0003, // PidTagObjectType
    0x3000_0003, // PidTagRowId
    0x3004_001F, // PidTagComment
    0x3002_001F, // PidTagAddressType / legacy bootstrap metadata
];

pub(super) const NSPI_ADDITIONAL_REQUESTED_PROPERTY_TAGS: &[u32] = &[
    0x0FF6_0102, // PidTagInstanceKey
    0x0FF9_0102, // PidTagRecordKey
    0x0FFF_0102, // PidTagEntryId
    0x300B_0102, // PidTagSearchKey
    0x0FF8_0102, // PidTagMappingSignature
    0x3902_0102, // PidTagTemplateid
    0x39FF_001E, // PidTag7BitDisplayName string8
    0x39FF_001F, // PidTag7BitDisplayName
    0x3001_001E, // PidTagDisplayName string8
    0x39FE_001E, // PidTagSmtpAddress string8
    0x3003_001E, // PidTagEmailAddress string8
    0x3A00_001E, // PidTagAccount string8
    0x3004_001E, // PidTagComment string8
    0x3002_001E, // PidTagAddressType string8
    0x3005_001E, // PidTagAddressBookDisplayNamePrintable / legacy DN string8
    0x3005_001F, // PidTagAddressBookDisplayNamePrintable / legacy DN
    0x3A20_001E, // PidTagTransmittableDisplayName string8
    0x3A20_001F, // PidTagTransmittableDisplayName
    0x3A06_001E, // PidTagGivenName string8
    0x3A06_001F, // PidTagGivenName
    0x3A0F_001E, // PidTagMessageHandlingSystemCommonName string8
    0x3A0F_001F, // PidTagMessageHandlingSystemCommonName
    0x3A11_001E, // PidTagSurname string8
    0x3A11_001F, // PidTagSurname
    0x3A4F_001E, // PidTagNickname string8
    0x3A4F_001F, // PidTagNickname
    0x3A08_001E, // PidTagBusinessTelephoneNumber string8
    0x3A08_001F, // PidTagBusinessTelephoneNumber
    0x3A09_001E, // PidTagHomeTelephoneNumber string8
    0x3A09_001F, // PidTagHomeTelephoneNumber
    0x3A1A_001E, // PidTagPrimaryTelephoneNumber string8
    0x3A1A_001F, // PidTagPrimaryTelephoneNumber
    0x3A1B_001E, // PidTagBusiness2TelephoneNumber string8
    0x3A1B_001F, // PidTagBusiness2TelephoneNumber
    0x3A1B_101F, // PidTagBusiness2TelephoneNumbers
    0x3A1C_001E, // PidTagMobileTelephoneNumber string8
    0x3A1C_001F, // PidTagMobileTelephoneNumber
    0x3A16_001E, // PidTagCompanyName string8
    0x3A16_001F, // PidTagCompanyName
    0x3A17_001E, // PidTagTitle string8
    0x3A17_001F, // PidTagTitle
    0x3A18_001E, // PidTagDepartmentName string8
    0x3A18_001F, // PidTagDepartmentName
    0x3A15_001E, // PidTagPostalAddress string8
    0x3A15_001F, // PidTagPostalAddress
    0x3A26_001E, // PidTagCountry string8
    0x3A26_001F, // PidTagCountry
    0x3A27_001E, // PidTagLocality string8
    0x3A27_001F, // PidTagLocality
    0x3A28_001E, // PidTagStateOrProvince string8
    0x3A28_001F, // PidTagStateOrProvince
    0x3A29_001E, // PidTagStreetAddress string8
    0x3A29_001F, // PidTagStreetAddress
    0x3A2A_001E, // PidTagPostalCode string8
    0x3A2A_001F, // PidTagPostalCode
    0x3A8D_001E, // PidTagAddressBookPhoneticGivenName string8
    0x3A8D_001F, // PidTagAddressBookPhoneticGivenName
    0x3A8E_001E, // PidTagAddressBookPhoneticSurname string8
    0x3A8E_001F, // PidTagAddressBookPhoneticSurname
    0x3F08_0003, // PidTagInitialDetailsPane
    0x3900_0003, // PidTagDisplayType
    0x803C_001E, // PidTagAddressBookObjectDistinguishedName string8
    0x803C_001F, // PidTagAddressBookObjectDistinguishedName
    0x800F_101E, // PidTagAddressBookProxyAddresses string8
    0x800F_101F, // PidTagAddressBookProxyAddresses
    0x8009_000D, // PidTagAddressBookMember
    0x8CA8_001E, // Outlook address book string8 compatibility column
    0x8CE2_0003, // PidTagAddressBookDistributionListMemberCount
    0x8CE3_0003, // PidTagAddressBookDistributionListExternalMemberCount
    0x8C6D_0102, // PidTagAddressBookObjectGuid
    0x3905_0003, // PidTagDisplayTypeEx
    0x3E04_0003, // Outlook account-row compatibility column
    0x8888_0003, // Outlook account-row compatibility column
    0xFFFD_0003, // PidTagAddressBookContainerId
];

#[allow(dead_code)]
pub(super) const NSPI_SUPPORTED_REQUEST_TYPES: &[MapiRequestType] = &[
    MapiRequestType::Bind,
    MapiRequestType::Unbind,
    MapiRequestType::CompareMids,
    MapiRequestType::DnToEph,
    MapiRequestType::DnToMid,
    MapiRequestType::GetMatches,
    MapiRequestType::GetPropList,
    MapiRequestType::GetProps,
    MapiRequestType::GetHierarchyInfo,
    MapiRequestType::GetSpecialTable,
    MapiRequestType::GetTemplateInfo,
    MapiRequestType::ModLinkAtt,
    MapiRequestType::ModProps,
    MapiRequestType::GetAddressBookUrl,
    MapiRequestType::GetMailboxUrl,
    MapiRequestType::QueryColumns,
    MapiRequestType::QueryRows,
    MapiRequestType::ResolveNames,
    MapiRequestType::ResortRestriction,
    MapiRequestType::SeekEntries,
    MapiRequestType::UpdateStat,
];

const NSPI_KNOWN_UNSUPPORTED_PROPERTY_TAGS: &[(u32, &str)] = &[
    (0x3A19_001E, "PidTagOfficeLocation"),
    (0x3A19_001F, "PidTagOfficeLocation"),
    (0x3A71_001F, "PidTagSendRichInfo"),
    (0x3A8C_001E, "PidTagAddressBookPhoneticDisplayName"),
    (0x3A8C_001F, "PidTagAddressBookPhoneticDisplayName"),
    (0x3A8F_001E, "PidTagAddressBookPhoneticCompanyName"),
    (0x3A8F_001F, "PidTagAddressBookPhoneticCompanyName"),
    (0x3A4E_001E, "PidTagManagerName"),
    (0x3A4E_001F, "PidTagManagerName"),
    (0x3A73_001E, "PidTagHomeAddressStreet"),
    (0x3A73_001F, "PidTagHomeAddressStreet"),
    (0x3A74_001E, "PidTagHomeAddressCity"),
    (0x3A74_001F, "PidTagHomeAddressCity"),
    (0x3A75_001E, "PidTagHomeAddressStateOrProvince"),
    (0x3A75_001F, "PidTagHomeAddressStateOrProvince"),
    (0x3A76_001E, "PidTagHomeAddressPostalCode"),
    (0x3A76_001F, "PidTagHomeAddressPostalCode"),
    (0x3A77_001E, "PidTagHomeAddressCountry"),
    (0x3A77_001F, "PidTagHomeAddressCountry"),
];

#[allow(dead_code)]
pub(in crate::mapi) fn nspi_known_unsupported_property_tag_name(tag: u32) -> Option<&'static str> {
    NSPI_KNOWN_UNSUPPORTED_PROPERTY_TAGS
        .iter()
        .find_map(|(known_tag, name)| (*known_tag == tag).then_some(*name))
}

pub(in crate::mapi) fn nspi_property_tags_response(
    request_type: &str,
    request_id: &str,
) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(1);
    write_large_property_tag_array(&mut body, NSPI_BOOTSTRAP_PROPERTY_TAGS);
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

pub(in crate::mapi) fn nspi_requested_property_tags(request: &[u8]) -> Vec<u32> {
    let mut tags = Vec::new();
    let mut offset = 0usize;
    while offset + 4 <= request.len() {
        let tag = u32::from_le_bytes([
            request[offset],
            request[offset + 1],
            request[offset + 2],
            request[offset + 3],
        ]);
        if nspi_property_tag_is_supported(tag) && !tags.contains(&tag) {
            tags.push(tag);
        }
        offset += 4;
    }
    if tags.is_empty() {
        NSPI_BOOTSTRAP_PROPERTY_TAGS.to_vec()
    } else {
        tags
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct NspiGetPropsRequest {
    pub(super) flags: u32,
    pub(super) code_page: u32,
    pub(super) current_rec: Option<u32>,
    pub(super) property_tags: Option<Vec<u32>>,
}

// MS-OXCMAPIHTTP 2.2.5.7.1: Flags, optional 36-byte STAT, optional
// LargePropertyTagArray, then the auxiliary buffer.
pub(super) fn parse_nspi_get_props_request(request: &[u8]) -> Option<NspiGetPropsRequest> {
    let mut cursor = Cursor::new(request);
    let flags = cursor.read_u32().ok()?;
    let has_state = cursor.read_u8().ok()? != 0;
    let (code_page, current_rec) = if has_state {
        let _sort_type = cursor.read_u32().ok()?;
        let _container_id = cursor.read_u32().ok()?;
        let current_rec = cursor.read_u32().ok()?;
        let _delta = cursor.read_u32().ok()?;
        let _num_pos = cursor.read_u32().ok()?;
        let _total_recs = cursor.read_u32().ok()?;
        let code_page = cursor.read_u32().ok()?;
        let _template_locale = cursor.read_u32().ok()?;
        let _sort_locale = cursor.read_u32().ok()?;
        (code_page, Some(current_rec))
    } else {
        (NSPI_UNICODE_CODEPAGE, None)
    };
    let property_tags = if cursor.read_u8().ok()? != 0 {
        let count = cursor.read_u32().ok()? as usize;
        if count > cursor.remaining().saturating_sub(4) / 4 {
            return None;
        }
        let mut tags = Vec::with_capacity(count);
        for _ in 0..count {
            tags.push(cursor.read_u32().ok()?);
        }
        Some(tags)
    } else {
        None
    };
    let auxiliary_size = cursor.read_u32().ok()? as usize;
    cursor.read_bytes(auxiliary_size).ok()?;
    Some(NspiGetPropsRequest {
        flags,
        code_page,
        current_rec,
        property_tags,
    })
}

pub(super) fn nspi_get_props_property_tags(
    request: &[u8],
    entry: Option<&ExchangeAddressBookEntry>,
) -> Vec<u32> {
    if let Some(parsed) = parse_nspi_get_props_request(request) {
        return parsed.property_tags.unwrap_or_else(|| {
            entry
                .map(|entry| nspi_entry_available_property_tags(entry, parsed.flags))
                .unwrap_or_default()
        });
    }
    nspi_requested_property_tags(request)
}

pub(in crate::mapi) fn nspi_property_tag_is_supported(tag: u32) -> bool {
    NSPI_BOOTSTRAP_PROPERTY_TAGS.contains(&tag)
        || NSPI_ADDITIONAL_REQUESTED_PROPERTY_TAGS.contains(&tag)
}

// MS-OXNSPI 3.1.4.1.6 constraint 5 and 3.1.4.1.7 constraint 5 require
// GetPropList and NULL-tag GetProps to use the same entry-owned properties.
pub(in crate::mapi) fn nspi_entry_available_property_tags(
    entry: &ExchangeAddressBookEntry,
    flags: u32,
) -> Vec<u32> {
    let mut tags = vec![
        0x0FF6_0102, // PidTagInstanceKey
        0x0FF8_0102, // PidTagMappingSignature
        0x0FF9_0102, // PidTagRecordKey
        0x0FFE_0003, // PidTagObjectType
        0x0FFF_0102, // PidTagEntryId
        0x3001_001F, // PidTagDisplayName
        0x3002_001F, // PidTagAddressType
        0x3003_001F, // PidTagEmailAddress
        0x300B_0102, // PidTagSearchKey
        0x3900_0003, // PidTagDisplayType
        0x3902_0102, // PidTagTemplateid
        0x3905_0003, // PidTagDisplayTypeEx
        0x39FE_001F, // PidTagSmtpAddress
        0x39FF_001F, // PidTagAddressBookDisplayNamePrintable
        0x3A00_001F, // PidTagAccount
        0x3A0F_001F, // PidTagMessageHandlingSystemCommonName
        0x3A20_001F, // PidTagTransmittableDisplayName
        0x3F08_0003, // PidTagInitialDetailsPane
        0x800F_101F, // PidTagAddressBookProxyAddresses
        0x803C_001F, // PidTagAddressBookObjectDistinguishedName
        0x8C6D_0102, // PidTagAddressBookObjectGuid
        0xFFFD_0003, // PidTagAddressBookContainerId
    ];
    if !entry.details.given_name.is_empty() {
        tags.push(0x3A06_001F);
    }
    if !entry.details.surname.is_empty() {
        tags.push(0x3A11_001F);
    }
    if !entry.details.nickname.is_empty() {
        tags.push(0x3A4F_001F);
    }
    if !entry.details.primary_phone.is_empty() {
        tags.extend([0x3A08_001F, 0x3A1A_001F]);
    }
    if !entry.details.home_phone.is_empty() {
        tags.push(0x3A09_001F);
    }
    if !entry.details.business2_phones.is_empty() {
        tags.extend([0x3A1B_001F, 0x3A1B_101F]);
    }
    if !entry.details.mobile_phone.is_empty() {
        tags.push(0x3A1C_001F);
    }
    if !entry.details.company_name.is_empty() {
        tags.push(0x3A16_001F);
    }
    if !entry.details.title.is_empty() {
        tags.push(0x3A17_001F);
    }
    if !entry.details.department_name.is_empty() {
        tags.push(0x3A18_001F);
    }
    if !entry.details.postal_address.is_empty() {
        tags.push(0x3A15_001F);
    }
    if !entry.details.country.is_empty() {
        tags.push(0x3A26_001F);
    }
    if !entry.details.locality.is_empty() {
        tags.push(0x3A27_001F);
    }
    if !entry.details.state_or_province.is_empty() {
        tags.push(0x3A28_001F);
    }
    if !entry.details.street_address.is_empty() {
        tags.push(0x3A29_001F);
    }
    if !entry.details.postal_code.is_empty() {
        tags.push(0x3A2A_001F);
    }
    if !entry.details.phonetic_given_name.is_empty() {
        tags.push(0x3A8D_001F);
    }
    if !entry.details.phonetic_surname.is_empty() {
        tags.push(0x3A8E_001F);
    }
    if entry.entry_kind == ExchangeAddressBookEntryKind::DistributionList {
        if flags & 0x0000_0001 == 0 {
            tags.push(0x8009_000D);
        }
        tags.extend([0x8CE2_0003, 0x8CE3_0003]);
    }
    tags
}

pub(in crate::mapi) fn nspi_resolved_entry_row(
    account_id: Uuid,
    entry: &ExchangeAddressBookEntry,
    columns: &[u32],
    directory_entries: &[ExchangeAddressBookEntry],
) -> Vec<u8> {
    let mut row = Vec::new();
    row.push(0);
    for property_tag in columns {
        write_address_book_property_value(
            &mut row,
            *property_tag,
            &nspi_entry_value_with_directory(account_id, entry, *property_tag, directory_entries),
        );
    }
    row
}

pub(in crate::mapi) fn nspi_entry_property_value_list(
    account_id: Uuid,
    entry: &ExchangeAddressBookEntry,
    tags: &[u32],
    directory_entries: &[ExchangeAddressBookEntry],
) -> Vec<u8> {
    let mut values = Vec::new();
    write_u32(&mut values, tags.len() as u32);
    for property_tag in tags {
        write_address_book_tagged_property_value(
            &mut values,
            *property_tag,
            &nspi_entry_value_with_directory(account_id, entry, *property_tag, directory_entries),
        );
    }
    values
}

pub(super) fn nspi_get_props_property_value_list(
    account_id: Uuid,
    entry: &ExchangeAddressBookEntry,
    tags: &[u32],
    directory_entries: &[ExchangeAddressBookEntry],
) -> (Vec<u8>, bool) {
    // MS-OXNSPI 3.1.4.1.7 constraint 12 requires one order-preserving output
    // slot per requested tag and PtypErrorCode for a property without a value.
    let mut values = Vec::new();
    let mut errors_returned = false;
    write_u32(&mut values, tags.len() as u32);
    for property_tag in tags {
        if nspi_property_tag_is_supported(*property_tag) {
            write_address_book_tagged_property_value(
                &mut values,
                *property_tag,
                &nspi_entry_value_with_directory(
                    account_id,
                    entry,
                    *property_tag,
                    directory_entries,
                ),
            );
        } else {
            errors_returned = true;
            write_u32(&mut values, (*property_tag & 0xFFFF_0000) | 0x000A);
            write_u32(&mut values, 0x8004_010F);
        }
    }
    (values, errors_returned)
}

pub(super) fn nspi_get_props_missing_property_value_list(tags: &[u32]) -> Vec<u8> {
    let mut values = Vec::new();
    write_u32(&mut values, tags.len() as u32);
    for property_tag in tags {
        write_u32(&mut values, (*property_tag & 0xFFFF_0000) | 0x000A);
        write_u32(&mut values, 0x8004_010F);
    }
    values
}

pub(in crate::mapi) enum NspiValue<'a> {
    Null,
    String(&'a str),
    OwnedString(String),
    MultiString(Vec<String>),
    EmbeddedTable(Uuid, Vec<ExchangeAddressBookEntry>),
    OwnedBinary(Vec<u8>),
    U32(u32),
    Bool(bool),
}

#[cfg(test)]
pub(super) fn nspi_entry_value(
    account_id: Uuid,
    entry: &ExchangeAddressBookEntry,
    property_tag: u32,
) -> NspiValue<'_> {
    nspi_entry_value_with_directory(account_id, entry, property_tag, &[])
}

pub(in crate::mapi) fn nspi_entry_value_with_directory<'a>(
    account_id: Uuid,
    entry: &'a ExchangeAddressBookEntry,
    property_tag: u32,
    directory_entries: &'a [ExchangeAddressBookEntry],
) -> NspiValue<'a> {
    match property_tag {
        0x0000_0001 => NspiValue::Null,
        0x0FF8_0102 => NspiValue::OwnedBinary(mapi_mailstore::STORE_REPLICA_GUID.to_vec()),
        0x3902_0102 => NspiValue::OwnedBinary(nspi_entry_permanent_entry_id(entry)),
        0x39FF_001F | 0x39FF_001E => NspiValue::String(&entry.display_name),
        0x3001_001F | 0x3001_001E => NspiValue::String(&entry.display_name),
        0x39FE_001F | 0x39FE_001E => NspiValue::String(&entry.email),
        0x3003_001F | 0x3003_001E => NspiValue::OwnedString(nspi_entry_unprefixed_legacy_dn(entry)),
        0x3A00_001F | 0x3A00_001E => NspiValue::OwnedString(nspi_entry_alias(entry)),
        0x0FFE_0003 => NspiValue::U32(MAPI_MAILUSER_OBJECT_TYPE),
        0x3900_0003 => NspiValue::U32(nspi_entry_display_type(entry)),
        0x3905_0003 => NspiValue::U32(nspi_entry_display_type_ex(entry)),
        0x3000_0003 => NspiValue::U32(nspi_entry_id(account_id, entry)),
        0x0FF6_0102 => NspiValue::OwnedBinary(nspi_entry_instance_key(account_id, entry)),
        0x0FF9_0102 => NspiValue::OwnedBinary(nspi_entry_record_key(entry)),
        0x0FFF_0102 => NspiValue::OwnedBinary(nspi_entry_permanent_entry_id(entry)),
        0x300B_0102 => NspiValue::OwnedBinary(nspi_entry_search_key(entry)),
        0x3004_001F | 0x3004_001E => NspiValue::String(&entry.email),
        0x3002_001F | 0x3002_001E => NspiValue::String("EX"),
        0x3005_001F | 0x3005_001E => NspiValue::OwnedString(nspi_entry_legacy_dn(entry)),
        0x3A20_001F | 0x3A20_001E => NspiValue::String(&entry.display_name),
        0x3A06_001F | 0x3A06_001E => NspiValue::String(&entry.details.given_name),
        0x3A0F_001F | 0x3A0F_001E => NspiValue::String(&entry.display_name),
        0x3A11_001F | 0x3A11_001E => NspiValue::String(&entry.details.surname),
        0x3A4F_001F | 0x3A4F_001E => NspiValue::String(&entry.details.nickname),
        0x3A08_001F | 0x3A08_001E | 0x3A1A_001F | 0x3A1A_001E => {
            NspiValue::String(&entry.details.primary_phone)
        }
        0x3A09_001F | 0x3A09_001E => NspiValue::String(&entry.details.home_phone),
        0x3A1B_001F | 0x3A1B_001E => NspiValue::OwnedString(
            entry
                .details
                .business2_phones
                .first()
                .cloned()
                .unwrap_or_default(),
        ),
        0x3A1B_101F => NspiValue::MultiString(entry.details.business2_phones.clone()),
        0x3A1C_001F | 0x3A1C_001E => NspiValue::String(&entry.details.mobile_phone),
        0x3A16_001F | 0x3A16_001E => NspiValue::String(&entry.details.company_name),
        0x3A17_001F | 0x3A17_001E => NspiValue::String(&entry.details.title),
        0x3A18_001F | 0x3A18_001E => NspiValue::String(&entry.details.department_name),
        0x3A15_001F | 0x3A15_001E => NspiValue::String(&entry.details.postal_address),
        0x3A26_001F | 0x3A26_001E => NspiValue::String(&entry.details.country),
        0x3A27_001F | 0x3A27_001E => NspiValue::String(&entry.details.locality),
        0x3A28_001F | 0x3A28_001E => NspiValue::String(&entry.details.state_or_province),
        0x3A29_001F | 0x3A29_001E => NspiValue::String(&entry.details.street_address),
        0x3A2A_001F | 0x3A2A_001E => NspiValue::String(&entry.details.postal_code),
        0x3A8D_001F | 0x3A8D_001E => NspiValue::String(&entry.details.phonetic_given_name),
        0x3A8E_001F | 0x3A8E_001E => NspiValue::String(&entry.details.phonetic_surname),
        0x3F08_0003 => NspiValue::U32(0),
        0x803C_001F | 0x803C_001E => NspiValue::OwnedString(nspi_entry_unprefixed_legacy_dn(entry)),
        0x800F_101F | 0x800F_101E => NspiValue::MultiString(vec![format!("SMTP:{}", entry.email)]),
        0x8009_000D => NspiValue::EmbeddedTable(
            account_id,
            nspi_distribution_list_members(entry, directory_entries),
        ),
        0x8CE2_0003 => NspiValue::U32(
            nspi_distribution_list_members(entry, directory_entries)
                .len()
                .min(u32::MAX as usize) as u32,
        ),
        0x8CE3_0003 => NspiValue::U32(0),
        0x8C6D_0102 => NspiValue::OwnedBinary(entry.id.to_bytes_le().to_vec()),
        0xFFFD_0003 => NspiValue::U32(0),
        _ => match property_tag & 0xFFFF {
            0x001F | 0x001E => NspiValue::String(""),
            0x0003 => NspiValue::U32(0),
            _ => NspiValue::U32(0),
        },
    }
}

fn nspi_distribution_list_members(
    entry: &ExchangeAddressBookEntry,
    directory_entries: &[ExchangeAddressBookEntry],
) -> Vec<ExchangeAddressBookEntry> {
    if entry.entry_kind != ExchangeAddressBookEntryKind::DistributionList {
        return Vec::new();
    }
    entry
        .member_emails
        .iter()
        .filter_map(|email| {
            let normalized = email.trim().to_ascii_lowercase();
            directory_entries
                .iter()
                .find(|candidate| {
                    candidate.entry_kind != ExchangeAddressBookEntryKind::DistributionList
                        && candidate.email.trim().eq_ignore_ascii_case(&normalized)
                })
                .cloned()
        })
        .collect()
}

pub(in crate::mapi) async fn allocate_nspi_entry_identities<S>(
    store: &S,
    principal: &AccountPrincipal,
    entries: &[ExchangeAddressBookEntry],
) -> Result<()>
where
    S: ExchangeStore,
{
    let requests = entries
        .iter()
        .filter_map(nspi_identity_request)
        .collect::<Vec<_>>();
    remember_nspi_identity_records(store, principal, &requests).await
}

pub(in crate::mapi) async fn allocate_principal_nspi_identity<S>(
    store: &S,
    principal: &AccountPrincipal,
) -> Result<()>
where
    S: ExchangeStore,
{
    let entry = principal_address_book_entry(principal);
    let Some(request) = nspi_identity_request(&entry) else {
        return Ok(());
    };
    remember_nspi_identity_records(store, principal, &[request]).await
}

async fn remember_nspi_identity_records<S>(
    store: &S,
    principal: &AccountPrincipal,
    requests: &[MapiIdentityRequest],
) -> Result<()>
where
    S: ExchangeStore,
{
    if requests.is_empty() {
        return Ok(());
    }
    let records = store
        .fetch_or_allocate_mapi_identities(principal.account_id, requests)
        .await?;
    for (request, record) in requests.iter().zip(records.iter()) {
        if let Some(kind_key) = nspi_identity_kind_key_for_request(request.object_kind) {
            remember_nspi_identity(
                principal.account_id,
                kind_key,
                record.canonical_id,
                record.object_id,
            );
        }
    }
    Ok(())
}

fn nspi_identity_request(entry: &ExchangeAddressBookEntry) -> Option<MapiIdentityRequest> {
    let object_kind = match entry.entry_kind {
        ExchangeAddressBookEntryKind::Account => MapiIdentityObjectKind::Account,
        ExchangeAddressBookEntryKind::Contact => MapiIdentityObjectKind::Contact,
        ExchangeAddressBookEntryKind::DistributionList => return None,
    };
    Some(MapiIdentityRequest {
        object_kind,
        canonical_id: entry.id,
        reserved_global_counter: None,
        source_key: None,
    })
}

fn remember_nspi_identity(account_id: Uuid, kind_key: u8, canonical_id: Uuid, object_id: u64) {
    let mut ids = NSPI_OBJECT_IDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    ids.insert((account_id, kind_key, canonical_id), object_id);
}

fn mapped_nspi_object_id(account_id: Uuid, entry: &ExchangeAddressBookEntry) -> Option<u64> {
    NSPI_OBJECT_IDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&(
            account_id,
            nspi_identity_kind_key(entry.entry_kind),
            entry.id,
        ))
        .copied()
}

fn nspi_identity_kind_key(entry_kind: ExchangeAddressBookEntryKind) -> u8 {
    match entry_kind {
        ExchangeAddressBookEntryKind::Account => 1,
        ExchangeAddressBookEntryKind::Contact => 2,
        ExchangeAddressBookEntryKind::DistributionList => 3,
    }
}

fn nspi_identity_kind_key_for_request(object_kind: MapiIdentityObjectKind) -> Option<u8> {
    match object_kind {
        MapiIdentityObjectKind::Account => Some(1),
        MapiIdentityObjectKind::Contact => Some(2),
        _ => None,
    }
}

pub(in crate::mapi) fn nspi_entry_id(account_id: Uuid, entry: &ExchangeAddressBookEntry) -> u32 {
    mapped_nspi_object_id(account_id, entry)
        .and_then(|object_id| nspi_minimal_id_from_object_id(object_id, entry.entry_kind))
        .unwrap_or_else(|| legacy_nspi_entry_id(entry))
}

pub(in crate::mapi) fn nspi_minimal_id_from_object_id(
    object_id: u64,
    entry_kind: ExchangeAddressBookEntryKind,
) -> Option<u32> {
    let counter = identity::global_counter_from_store_id(object_id)? as u32;
    let value = (counter & 0x3FFF_FFFF)
        | match entry_kind {
            ExchangeAddressBookEntryKind::Account => 0x8000_0000,
            ExchangeAddressBookEntryKind::Contact
            | ExchangeAddressBookEntryKind::DistributionList => 0x4000_0000,
        };
    (value >= 2).then_some(value)
}

fn legacy_nspi_entry_id(entry: &ExchangeAddressBookEntry) -> u32 {
    let bytes = entry.id.as_bytes();
    let value = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    match entry.entry_kind {
        ExchangeAddressBookEntryKind::Account => value | 0x8000_0000,
        ExchangeAddressBookEntryKind::Contact | ExchangeAddressBookEntryKind::DistributionList => {
            value | 0x4000_0000
        }
    }
    .max(2)
}

pub(in crate::mapi) fn principal_minimal_entry_id(principal: &AccountPrincipal) -> u32 {
    nspi_entry_id(
        principal.account_id,
        &principal_address_book_entry(principal),
    )
}

pub(in crate::mapi) fn principal_address_book_entry(
    principal: &AccountPrincipal,
) -> ExchangeAddressBookEntry {
    ExchangeAddressBookEntry {
        id: principal.account_id,
        display_name: principal.display_name.clone(),
        email: principal.email.clone(),
        entry_kind: ExchangeAddressBookEntryKind::Account,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
        member_emails: Vec::new(),
        details: ExchangeAddressBookEntryDetails::default(),
    }
}

pub(in crate::mapi) fn nspi_entry_display_type(entry: &ExchangeAddressBookEntry) -> u32 {
    match (entry.entry_kind, entry.directory_kind) {
        (ExchangeAddressBookEntryKind::DistributionList, _) => 1,
        (ExchangeAddressBookEntryKind::Contact, _) => 6,
        (ExchangeAddressBookEntryKind::Account, ExchangeAddressBookDirectoryKind::Room) => 7,
        (ExchangeAddressBookEntryKind::Account, ExchangeAddressBookDirectoryKind::Equipment) => 8,
        (ExchangeAddressBookEntryKind::Account, ExchangeAddressBookDirectoryKind::Person) => 0,
    }
}

fn nspi_entry_display_type_ex(entry: &ExchangeAddressBookEntry) -> u32 {
    nspi_entry_display_type(entry)
        | if entry.entry_kind == ExchangeAddressBookEntryKind::Account {
            0x4000_0000
        } else {
            0
        }
}

pub(in crate::mapi) fn write_large_property_tag_array(body: &mut Vec<u8>, tags: &[u32]) {
    write_u32(body, tags.len() as u32);
    for tag in tags {
        write_u32(body, *tag);
    }
}

pub(in crate::mapi) fn write_address_book_tagged_property_value(
    body: &mut Vec<u8>,
    property_tag: u32,
    value: &NspiValue<'_>,
) {
    // MS-OXCMAPIHTTP 2.2.1.2 places the value directly after the 4-byte tag.
    write_u32(body, property_tag);
    write_address_book_property_value(body, property_tag, value);
}

pub(in crate::mapi) fn write_address_book_property_value(
    body: &mut Vec<u8>,
    property_tag: u32,
    value: &NspiValue<'_>,
) {
    match (property_tag & 0xFFFF, value) {
        (0x0001, NspiValue::Null) => {}
        (0x001E, NspiValue::String(value)) => {
            body.push(0xFF);
            write_ascii_z(body, value);
        }
        (0x001E, NspiValue::OwnedString(value)) => {
            body.push(0xFF);
            write_ascii_z(body, value);
        }
        (0x001F, NspiValue::String(value)) => {
            body.push(0xFF);
            write_utf16z(body, value);
        }
        (0x001F, NspiValue::OwnedString(value)) => {
            body.push(0xFF);
            write_utf16z(body, value);
        }
        (0x101E, NspiValue::MultiString(values)) => {
            body.push(0xFF);
            write_nspi_multi_string8(body, values);
        }
        (0x101F, NspiValue::MultiString(values)) => {
            body.push(0xFF);
            write_nspi_multi_string(body, values);
        }
        (0x000D, NspiValue::EmbeddedTable(account_id, entries)) => {
            write_embedded_address_book_table(body, *account_id, entries)
        }
        (0x0102, NspiValue::OwnedBinary(value)) => write_nspi_binary(body, value),
        (0x0003, NspiValue::U32(value)) => write_u32(body, *value),
        (0x0003, _) => write_u32(body, 0),
        (0x000B, NspiValue::Bool(value)) => body.push(u8::from(*value)),
        (0x000B, _) => body.push(0),
        (0x101E | 0x101F, _) => write_u32(body, 0),
        (_, NspiValue::U32(value)) => write_u32(body, *value),
        (_, NspiValue::Bool(value)) => body.push(u8::from(*value)),
        (_, NspiValue::OwnedBinary(value)) => write_nspi_binary(body, value),
        (_, NspiValue::MultiString(values)) => {
            body.push(0xFF);
            write_nspi_multi_string(body, values);
        }
        (_, NspiValue::EmbeddedTable(account_id, entries)) => {
            write_embedded_address_book_table(body, *account_id, entries)
        }
        (_, NspiValue::String(value)) => {
            body.push(0xFF);
            write_utf16z(body, value);
        }
        (_, NspiValue::OwnedString(value)) => {
            body.push(0xFF);
            write_utf16z(body, value);
        }
        (_, NspiValue::Null) => {}
    }
}

fn write_nspi_multi_string8(body: &mut Vec<u8>, values: &[String]) {
    write_u32(body, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        // MS-OXCMAPIHTTP 2.2.1.1 requires an element HasValue byte for
        // PtypMultipleString8 and PtypMultipleString values.
        body.push(0xFF);
        write_ascii_z(body, value);
    }
}

fn write_nspi_multi_string(body: &mut Vec<u8>, values: &[String]) {
    write_u32(body, values.len().min(u32::MAX as usize) as u32);
    for value in values.iter().take(u32::MAX as usize) {
        body.push(0xFF);
        write_utf16z(body, value);
    }
}

fn write_embedded_address_book_table(
    body: &mut Vec<u8>,
    account_id: Uuid,
    entries: &[ExchangeAddressBookEntry],
) {
    let columns = [0x3001_001F, 0x39FE_001F, 0x3003_001F, 0x3900_0003];
    write_large_property_tag_array(body, &columns);
    write_u32(body, entries.len().min(u32::MAX as usize) as u32);
    for entry in entries {
        body.extend_from_slice(&nspi_resolved_entry_row(
            account_id, entry, &columns, entries,
        ));
    }
}

fn write_nspi_binary(body: &mut Vec<u8>, value: &[u8]) {
    let len = value.len().min(u32::MAX as usize);
    // MS-OXCMAPIHTTP 2.2.1.1 requires HasValue for PtypBinary.
    body.push(0xFF);
    write_u32(body, len as u32);
    body.extend_from_slice(&value[..len]);
}
