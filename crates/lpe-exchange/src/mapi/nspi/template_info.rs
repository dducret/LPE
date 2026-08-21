use super::*;

/// Validates the complete MAPI/HTTP template probe before the bounded
/// principal projection is allocated. [MS-OXCMAPIHTTP] section 2.2.5.9.1
/// defines this field ordering; [MS-OXNSPI] section 3.1.4.1.18 and
/// [MS-OXOABKT] section 3.2.5.2 document that full display and
/// address-creation templates remain a separate contract.
pub(super) fn parse_nspi_template_info_request(request: &[u8]) -> Option<()> {
    let mut cursor = Cursor::new(request);
    let _flags = cursor.read_u32().ok()?;
    let _display_type = cursor.read_u32().ok()?;
    if cursor.read_u8().ok()? != 0 {
        let template_dn = cursor.read_ascii_z().ok()?;
        if !template_dn.is_ascii() {
            return None;
        }
    }
    let _code_page = cursor.read_u32().ok()?;
    let _locale_id = cursor.read_u32().ok()?;
    let auxiliary_size = cursor.read_u32().ok()? as usize;
    cursor.read_bytes(auxiliary_size).ok()?;
    (cursor.remaining() == 0).then_some(())
}
