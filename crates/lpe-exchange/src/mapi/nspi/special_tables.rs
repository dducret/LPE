use super::*;

const PID_TAG_ENTRY_ID: u32 = 0x0FFF_0102;
const PID_TAG_CONTAINER_FLAGS: u32 = 0x3600_0003;
const PID_TAG_DEPTH: u32 = 0x3005_0003;
const PID_TAG_ADDRESS_BOOK_CONTAINER_ID: u32 = 0xFFFD_0003;
const PID_TAG_ADDRESS_BOOK_IS_MASTER: u32 = 0xFFFB_000B;
const AB_RECIPIENTS: u32 = 0x0000_0001;
const AB_SUBCONTAINERS: u32 = 0x0000_0002;
const AB_UNMODIFIABLE: u32 = 0x0000_0008;
const DT_CONTAINER: u32 = 0x0000_0100;
const NSPI_ADDRESS_CREATION_TEMPLATES_FLAG: u32 = 0x0000_0002;
pub(in crate::mapi) const NSPI_UNICODE_STRINGS_FLAG: u32 = 0x0000_0004;

struct NspiSpecialTableContainer {
    display_name: &'static str,
    dn: &'static str,
    container_id: u32,
    depth: u32,
    flags: u32,
    is_master: bool,
}

// MS-OXOABK 2.2.1.1 requires "/" for the GAL and "/guid=<32 HEXDIG>" for
// every other address-list DN embedded in an MS-OXNSPI 3.1.4.1.3 hierarchy row.
const NSPI_SPECIAL_TABLE_CONTAINERS: &[NspiSpecialTableContainer] = &[
    NspiSpecialTableContainer {
        display_name: "Global Address List",
        dn: "/",
        container_id: 0,
        depth: 0,
        flags: AB_RECIPIENTS | AB_SUBCONTAINERS | AB_UNMODIFIABLE,
        is_master: false,
    },
    NspiSpecialTableContainer {
        display_name: "All Users",
        dn: "/guid=5f462d24409b4de39ac520f4bb7bf2a1",
        container_id: 2,
        depth: 1,
        flags: AB_RECIPIENTS | AB_UNMODIFIABLE,
        is_master: false,
    },
    NspiSpecialTableContainer {
        display_name: "All Groups",
        dn: "/guid=ca66e476bca14d44aa1012e422225805",
        container_id: 3,
        depth: 1,
        flags: AB_RECIPIENTS | AB_UNMODIFIABLE,
        is_master: false,
    },
    NspiSpecialTableContainer {
        display_name: "All Contacts",
        dn: "/guid=69f67788f05649cd862d51c09217eaa8",
        container_id: 4,
        depth: 1,
        flags: AB_RECIPIENTS | AB_UNMODIFIABLE,
        is_master: false,
    },
];

pub(in crate::mapi) fn nspi_special_table_response(
    principal: &AccountPrincipal,
    request: &[u8],
    request_id: &str,
) -> Response {
    nspi_hierarchy_table_response(
        principal,
        request,
        request_id,
        "GetSpecialTable",
        "special_table",
    )
}

pub(in crate::mapi) fn nspi_hierarchy_info_response(
    principal: &AccountPrincipal,
    request: &[u8],
    request_id: &str,
) -> Response {
    nspi_hierarchy_table_response(
        principal,
        request,
        request_id,
        "GetHierarchyInfo",
        "hierarchy_info",
    )
}

fn nspi_hierarchy_table_response(
    principal: &AccountPrincipal,
    request: &[u8],
    request_id: &str,
    request_type: &'static str,
    context_name: &'static str,
) -> Response {
    let flags = nspi_request_flags(request);
    let context = format!(
        "{context_name};request_flags={};unicode_strings={};address_creation_templates={}",
        flags
            .map(|value| format!("{value:#010x}"))
            .unwrap_or_else(|| "missing".to_string()),
        flags.is_some_and(|value| value & NSPI_UNICODE_STRINGS_FLAG != 0),
        flags.is_some_and(|value| value & NSPI_ADDRESS_CREATION_TEMPLATES_FLAG != 0)
    );
    // MS-OXNSPI 3.1.4.1.3 defines these six hierarchy columns as mandatory;
    // 4.1.4.3 shows PidTagAddressBookParentEntryId omitted from a valid response.
    let property_tags = [
        PID_TAG_ENTRY_ID,
        PID_TAG_CONTAINER_FLAGS,
        PID_TAG_DEPTH,
        PID_TAG_ADDRESS_BOOK_CONTAINER_ID,
        0x3001_001F,
        PID_TAG_ADDRESS_BOOK_IS_MASTER,
    ];
    let rows = NSPI_SPECIAL_TABLE_CONTAINERS
        .iter()
        .map(nspi_special_table_row)
        .collect::<Vec<_>>();

    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, NSPI_UNICODE_CODEPAGE);
    body.push(1);
    write_u32(&mut body, 1);
    body.push(1);
    write_u32(&mut body, rows.len().min(u32::MAX as usize) as u32);
    for row in &rows {
        body.extend_from_slice(row);
    }
    write_u32(&mut body, 0);
    let context = format!(
        "{context};containers={}",
        format_nspi_special_table_containers_for_debug(NSPI_SPECIAL_TABLE_CONTAINERS)
    );
    log_nspi_response_contract(
        principal,
        request_type,
        request_id,
        0,
        &body,
        true,
        rows.len(),
        &property_tags,
        &context,
    );
    mapi_response(request_type, request_id, 0, body, None)
}

fn nspi_request_flags(request: &[u8]) -> Option<u32> {
    request
        .get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
}

fn nspi_special_table_row(container: &NspiSpecialTableContainer) -> Vec<u8> {
    let mut table_row = Vec::new();
    // MS-OXCMAPIHTTP 2.2.5.8.2 encodes each row directly as an
    // AddressBookPropertyValueList (section 2.2.1.3), beginning with its count.
    write_u32(&mut table_row, 6);
    write_address_book_tagged_property_value(
        &mut table_row,
        PID_TAG_ENTRY_ID,
        &NspiValue::OwnedBinary(nspi_container_entry_id(container.dn)),
    );
    write_address_book_tagged_property_value(
        &mut table_row,
        PID_TAG_CONTAINER_FLAGS,
        &NspiValue::U32(container.flags),
    );
    write_address_book_tagged_property_value(
        &mut table_row,
        PID_TAG_DEPTH,
        &NspiValue::U32(container.depth),
    );
    write_address_book_tagged_property_value(
        &mut table_row,
        PID_TAG_ADDRESS_BOOK_CONTAINER_ID,
        &NspiValue::U32(container.container_id),
    );
    write_address_book_tagged_property_value(
        &mut table_row,
        0x3001_001F,
        &NspiValue::String(container.display_name),
    );
    write_address_book_tagged_property_value(
        &mut table_row,
        PID_TAG_ADDRESS_BOOK_IS_MASTER,
        &NspiValue::Bool(container.is_master),
    );
    table_row
}

fn nspi_container_entry_id(dn: &str) -> Vec<u8> {
    let mut value = Vec::with_capacity(28 + dn.len() + 1);
    value.extend_from_slice(&[0, 0, 0, 0]);
    value.extend_from_slice(&NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID);
    value.extend_from_slice(&1u32.to_le_bytes());
    value.extend_from_slice(&DT_CONTAINER.to_le_bytes());
    value.extend_from_slice(dn.as_bytes());
    value.push(0);
    value
}

fn format_nspi_special_table_containers_for_debug(
    containers: &[NspiSpecialTableContainer],
) -> String {
    containers
        .iter()
        .map(|container| {
            format!(
                "{}:depth={}:container_id={:#010x}:entryid_len={}:flags={:#010x}:display_type={:#010x}:selectable=true:browsable=true:is_master={}",
                container.display_name,
                container.depth,
                container.container_id,
                nspi_container_entry_id(container.dn).len(),
                container.flags,
                DT_CONTAINER,
                container.is_master
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hierarchy_permanent_entry_ids_use_address_list_dn_forms() {
        for container in NSPI_SPECIAL_TABLE_CONTAINERS {
            let entry_id = nspi_container_entry_id(container.dn);

            assert_eq!(&entry_id[..4], &[0, 0, 0, 0]);
            assert_eq!(&entry_id[4..20], &NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID);
            assert_eq!(u32::from_le_bytes(entry_id[20..24].try_into().unwrap()), 1);
            assert_eq!(
                u32::from_le_bytes(entry_id[24..28].try_into().unwrap()),
                DT_CONTAINER
            );
            assert_eq!(entry_id.last(), Some(&0));

            let dn = std::str::from_utf8(&entry_id[28..entry_id.len() - 1]).unwrap();
            if container.container_id == 0 {
                assert_eq!(dn, "/");
            } else {
                let guid = dn.strip_prefix("/guid=").expect("address-list DN");
                assert_eq!(guid.len(), 32);
                assert!(guid.bytes().all(|byte| byte.is_ascii_hexdigit()));
            }
        }
    }

    #[test]
    fn hierarchy_rows_begin_with_address_book_property_value_count() {
        for container in NSPI_SPECIAL_TABLE_CONTAINERS {
            let row = nspi_special_table_row(container);

            assert_eq!(u32::from_le_bytes(row[..4].try_into().unwrap()), 6);
            assert_eq!(
                u32::from_le_bytes(row[4..8].try_into().unwrap()),
                PID_TAG_ENTRY_ID
            );
        }
    }
}
