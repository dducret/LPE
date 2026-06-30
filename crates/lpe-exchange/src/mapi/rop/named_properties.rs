use super::{write_u32, RopRequest};
use crate::mapi::properties::write_named_property;
use crate::mapi::session::MapiSession;

pub(in crate::mapi) fn rop_get_property_ids_from_names_response(
    request: &RopRequest,
    property_ids: &[u16],
) -> Vec<u8> {
    let mut response = vec![0x56, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(property_ids.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for property_id in property_ids.iter().take(u16::MAX as usize) {
        response.extend_from_slice(&property_id.to_le_bytes());
    }
    response
}

pub(in crate::mapi) fn rop_get_names_from_property_ids_response(
    request: &RopRequest,
    session: &MapiSession,
) -> Vec<u8> {
    let property_ids = request.property_ids();
    let mut response = vec![0x55, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(property_ids.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for property_id in property_ids.iter().take(u16::MAX as usize) {
        write_named_property(&mut response, &session.property_name_for_id(*property_id));
    }
    response
}

pub(in crate::mapi) fn rop_query_named_properties_response(
    request: &RopRequest,
    session: &MapiSession,
) -> Vec<u8> {
    let properties = session.named_properties_for_query(request.named_property_query_guid());
    let mut response = vec![0x5F, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(properties.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for (property_id, _property) in properties.iter().take(u16::MAX as usize) {
        response.extend_from_slice(&property_id.to_le_bytes());
    }
    for (_property_id, property) in properties.iter().take(u16::MAX as usize) {
        write_named_property(&mut response, property);
    }
    response
}
