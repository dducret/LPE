use super::{rop_error_response, write_object_id, write_u32, RopRequest};
use crate::mapi::identity::{
    self, long_term_id_from_object_id, object_id_from_long_term_id_with_replica_guids,
};
use crate::mapi::tables::is_advertised_special_folder;
use crate::mapi::wire::RopId;

pub(in crate::mapi) fn rop_long_term_id_from_id_response(request: &RopRequest) -> Vec<u8> {
    let Some(object_id) = request.long_term_source_object_id() else {
        return rop_error_response(0x43, request.response_handle_index(), 0x8004_010F);
    };
    let Some(long_term_id) = long_term_id_from_object_id(object_id) else {
        return rop_error_response(0x43, request.response_handle_index(), 0x8004_010F);
    };
    let mut response = vec![0x43, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&long_term_id);
    response
}

pub(in crate::mapi) fn rop_id_from_long_term_id_response(
    request: &RopRequest,
    replica_guid_aliases: &[[u8; 16]],
) -> Vec<u8> {
    let Some(long_term_id) = request.long_term_id() else {
        return rop_error_response(0x44, request.response_handle_index(), 0x8004_0102);
    };
    let Some(object_id) =
        object_id_from_long_term_id_with_replica_guids(long_term_id, replica_guid_aliases)
            .or_else(|| stale_special_folder_object_id_from_long_term_id(long_term_id))
    else {
        return rop_error_response(0x44, request.response_handle_index(), 0x8004_010F);
    };
    let mut response = vec![0x44, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_object_id(&mut response, object_id);
    response
}

impl RopRequest {
    pub(in crate::mapi) fn long_term_source_object_id(&self) -> Option<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::LongTermIdFromId)) {
            return None;
        }
        let bytes = self.payload.get(..8)?;
        identity::object_id_from_wire_id(bytes)
            .or_else(|| identity::object_id_from_trailing_replid_wire_id(bytes))
            .or_else(|| stale_special_folder_object_id_from_short_id(bytes))
    }

    pub(in crate::mapi) fn long_term_source_id_bytes(&self) -> Option<&[u8]> {
        self.payload.get(..8)
    }
}

fn stale_special_folder_object_id_from_long_term_id(long_term_id: &[u8]) -> Option<u64> {
    if long_term_id.len() != 24 || long_term_id[22..24] != [0, 0] {
        return None;
    }
    let global_counter = identity::global_counter_from_globcnt(&long_term_id[16..22])?;
    let object_id = identity::mapi_store_id(global_counter);
    is_advertised_special_folder(object_id).then_some(object_id)
}

fn stale_special_folder_object_id_from_short_id(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    let leading_replid = u16::from_le_bytes(bytes[..2].try_into().ok()?);
    let trailing_replid = u16::from_le_bytes(bytes[6..8].try_into().ok()?);
    let candidates = [
        (
            leading_replid,
            identity::global_counter_from_globcnt(&bytes[2..8]),
        ),
        (
            trailing_replid,
            identity::global_counter_from_globcnt(&bytes[..6]),
        ),
        (
            leading_replid,
            global_counter_from_little_endian_globcnt(&bytes[2..8]),
        ),
        (
            trailing_replid,
            global_counter_from_little_endian_globcnt(&bytes[..6]),
        ),
    ];
    candidates
        .into_iter()
        .filter(|(replica_id, _)| *replica_id != 0)
        .filter_map(|(_, counter)| counter)
        .map(identity::mapi_store_id)
        .find(|object_id| is_advertised_special_folder(*object_id))
        .or_else(|| advertised_virtual_object_id_from_bare_little_endian_short_id(bytes))
        .or_else(|| dynamic_object_id_from_bare_little_endian_short_id(bytes))
}

fn advertised_virtual_object_id_from_bare_little_endian_short_id(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 || bytes[6..8] != [0, 0] {
        return None;
    }
    let counter = global_counter_from_little_endian_globcnt(&bytes[..6])?;
    let object_id = identity::mapi_store_id(counter);
    (counter >= identity::SYNC_ISSUES_FOLDER_COUNTER && is_advertised_special_folder(object_id))
        .then_some(object_id)
}

fn dynamic_object_id_from_bare_little_endian_short_id(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 || bytes[6..8] != [0, 0] {
        return None;
    }
    let counter = global_counter_from_little_endian_globcnt(&bytes[..6])?;
    (counter >= identity::FIRST_DYNAMIC_GLOBAL_COUNTER).then(|| identity::mapi_store_id(counter))
}

fn global_counter_from_little_endian_globcnt(bytes: &[u8]) -> Option<u64> {
    let bytes: [u8; 6] = bytes.try_into().ok()?;
    let counter = u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], 0, 0,
    ]);
    (counter != 0).then_some(counter)
}
