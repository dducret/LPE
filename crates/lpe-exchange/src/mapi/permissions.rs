use super::rop::*;
use super::tables::*;
use super::*;

pub(crate) const PID_TAG_MEMBER_ID: u32 = 0x6671_0014;
pub(crate) const PID_TAG_MEMBER_NAME_W: u32 = 0x6672_001F;
pub(crate) const PID_TAG_MEMBER_RIGHTS: u32 = 0x6673_0003;

pub(in crate::mapi) const MEMBER_ID_DEFAULT: u64 = 0;
pub(in crate::mapi) const MEMBER_ID_ANONYMOUS: u64 = u64::MAX;

const RIGHTS_READ_ANY: u32 = 0x0000_0001;
const RIGHTS_CREATE: u32 = 0x0000_0002;
const RIGHTS_EDIT_ANY: u32 = 0x0000_0020;
const RIGHTS_DELETE_ANY: u32 = 0x0000_0040;
const RIGHTS_CREATE_SUBFOLDER: u32 = 0x0000_0080;
const RIGHTS_OWNER: u32 = 0x0000_0100;
const RIGHTS_CONTACT_FOLDER: u32 = 0x0000_0200;
const RIGHTS_FOLDER_VISIBLE: u32 = 0x0000_0400;

pub(in crate::mapi) fn default_permission_columns() -> Vec<u32> {
    vec![
        PID_TAG_MEMBER_ID,
        PID_TAG_MEMBER_NAME_W,
        PID_TAG_MEMBER_RIGHTS,
    ]
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MapiFolderPermission {
    pub(crate) mailbox_id: Uuid,
    pub(crate) member_account_id: Option<Uuid>,
    pub(crate) member_name: String,
    pub(crate) rights: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MapiFolderAccess {
    pub(in crate::mapi) may_read: bool,
    pub(in crate::mapi) may_write: bool,
    pub(in crate::mapi) may_delete: bool,
}

pub(crate) fn owner_permission(
    mailbox_id: Uuid,
    principal: &AccountPrincipal,
) -> MapiFolderPermission {
    MapiFolderPermission {
        mailbox_id,
        member_account_id: Some(principal.account_id),
        member_name: principal.display_name.clone(),
        rights: owner_rights(),
    }
}

pub(crate) fn rights_from_grant(
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
) -> u32 {
    let mut rights = RIGHTS_FOLDER_VISIBLE;
    if may_read {
        rights |= RIGHTS_READ_ANY;
    }
    if may_write {
        rights |= RIGHTS_CREATE | RIGHTS_EDIT_ANY;
    }
    if may_delete {
        rights |= RIGHTS_DELETE_ANY;
    }
    if may_share {
        rights |= RIGHTS_CREATE_SUBFOLDER | RIGHTS_OWNER;
    }
    rights
}

pub(in crate::mapi) fn owner_rights() -> u32 {
    rights_from_grant(true, true, true, true) | RIGHTS_CONTACT_FOLDER
}

pub(crate) fn access_from_rights(rights: u32) -> MapiFolderAccess {
    MapiFolderAccess {
        may_read: rights & RIGHTS_READ_ANY != 0,
        may_write: rights & (RIGHTS_CREATE | RIGHTS_EDIT_ANY) != 0,
        may_delete: rights & RIGHTS_DELETE_ANY != 0,
    }
}

pub(in crate::mapi) fn rop_get_permissions_table_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x3E, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_modify_permissions_response(request: &RopRequest) -> Vec<u8> {
    let permission_count = request.modify_permissions_count();
    if permission_count == Some(0) {
        let mut response = vec![0x40, request.response_handle_index()];
        write_u32(&mut response, 0);
        return response;
    }
    unsupported_rop_response(0x40, request.response_handle_index())
}

pub(in crate::mapi) fn serialize_permission_row(
    permission: &MapiFolderPermission,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_MEMBER_ID => write_u64(&mut row, permission_member_id(permission)),
            PID_TAG_MEMBER_NAME_W => write_utf16z(&mut row, &permission.member_name),
            PID_TAG_MEMBER_RIGHTS => write_u32(&mut row, permission.rights),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(crate) fn reserved_permission_rows(mailbox_id: Uuid) -> Vec<MapiFolderPermission> {
    vec![
        MapiFolderPermission {
            mailbox_id,
            member_account_id: None,
            member_name: String::new(),
            rights: 0,
        },
        MapiFolderPermission {
            mailbox_id,
            member_account_id: None,
            member_name: "Anonymous".to_string(),
            rights: 0,
        },
    ]
}

fn permission_member_id(permission: &MapiFolderPermission) -> u64 {
    if permission.member_account_id.is_none() && permission.member_name.is_empty() {
        return MEMBER_ID_DEFAULT;
    }
    if permission.member_account_id.is_none() && permission.member_name == "Anonymous" {
        return MEMBER_ID_ANONYMOUS;
    }
    permission
        .member_account_id
        .and_then(|id| crate::mapi::identity::mapped_mapi_object_id(&id))
        .unwrap_or_else(|| stable_text_member_id(&permission.member_name))
}

fn stable_text_member_id(value: &str) -> u64 {
    value.bytes().fold(0xcbf2_9ce4_8422_2325u64, |acc, byte| {
        (acc ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}
