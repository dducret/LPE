use super::*;

pub(in crate::mapi) struct ModifyRulesRow {
    pub(in crate::mapi) flags: u8,
    pub(in crate::mapi) properties: HashMap<u32, MapiValue>,
}

#[allow(dead_code)]
pub(in crate::mapi) fn serialize_rop_request(request: &RopRequest) -> Result<Vec<u8>> {
    let mut buffer = vec![request.rop_id, 0];
    match request.typed() {
        TypedRopRequest::Release(request) => buffer.push(request.input_handle_index),
        TypedRopRequest::OpenFolder(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.output_handle_index);
            buffer.extend_from_slice(
                &crate::mapi::identity::wire_id_bytes_from_object_id(request.folder_id)
                    .ok_or_else(|| anyhow!("invalid OpenFolder folder id"))?,
            );
            buffer.push(request.open_mode_flags);
        }
        TypedRopRequest::OpenMessage(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.output_handle_index);
            write_u16(&mut buffer, 0);
            buffer.extend_from_slice(
                &crate::mapi::identity::wire_id_bytes_from_object_id(request.folder_id)
                    .ok_or_else(|| anyhow!("invalid OpenMessage folder id"))?,
            );
            buffer.push(request.open_mode_flags);
            buffer.extend_from_slice(
                &crate::mapi::identity::wire_id_bytes_from_object_id(request.message_id)
                    .ok_or_else(|| anyhow!("invalid OpenMessage message id"))?,
            );
        }
        TypedRopRequest::OpenTable(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.output_handle_index);
            buffer.push(request.table_flags);
        }
        TypedRopRequest::CreateMessage(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.output_handle_index);
            write_u16(&mut buffer, 0);
            buffer.extend_from_slice(
                &crate::mapi::identity::wire_id_bytes_from_object_id(request.folder_id)
                    .ok_or_else(|| anyhow!("invalid CreateMessage folder id"))?,
            );
            buffer.push(request.associated_flag);
        }
        TypedRopRequest::SaveChangesMessage(request) => {
            buffer.push(request.response_handle_index);
            buffer.push(request.input_handle_index);
            buffer.push(request.save_flags);
        }
        TypedRopRequest::OpenEmbeddedMessage(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.output_handle_index);
            write_u16(&mut buffer, request.code_page_id);
            buffer.push(request.open_mode_flags);
        }
        TypedRopRequest::SetColumns(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.flags);
            write_u16(
                &mut buffer,
                request.property_tags.len().min(u16::MAX as usize) as u16,
            );
            for property_tag in request.property_tags {
                write_u32(&mut buffer, property_tag);
            }
        }
        TypedRopRequest::Restrict(request) if request.rop_id == 0x14 => {
            buffer.push(request.input_handle_index);
            buffer.push(request.flags);
            write_u16(
                &mut buffer,
                request.restriction.len().min(u16::MAX as usize) as u16,
            );
            buffer.extend_from_slice(&request.restriction);
        }
        TypedRopRequest::QueryRows(request) => {
            buffer.push(request.input_handle_index);
            buffer.push(request.flags);
            buffer.push(request.forward_read as u8);
            write_u16(&mut buffer, request.row_count);
        }
        TypedRopRequest::Logon(request) => {
            buffer.push(request.output_handle_index);
            buffer.push(request.logon_flags);
            buffer.extend_from_slice(&request.prefix);
            write_u16(
                &mut buffer,
                request.essdn.len().min(u16::MAX as usize) as u16,
            );
            buffer.extend_from_slice(&request.essdn);
        }
        TypedRopRequest::SupportedRaw(request) => {
            return Err(anyhow!(
                "ROP 0x{:02X} request serialization is not typed yet",
                request.rop_id
            ));
        }
        TypedRopRequest::Restrict(request) => {
            return Err(anyhow!(
                "ROP 0x{:02X} request serialization is not typed yet",
                request.rop_id
            ));
        }
        TypedRopRequest::Unsupported(request) => {
            return Err(anyhow!(
                "unsupported ROP 0x{:02X} request serialization",
                request.rop_id
            ));
        }
    }
    Ok(buffer)
}
