use super::{parse_tagged_property, Cursor, ModifyRulesRow, RopRequest};
use anyhow::Result;
use std::collections::HashMap;

impl RopRequest {
    pub(in crate::mapi) fn modify_permissions_count(&self) -> Option<u16> {
        if self.rop_id != 0x40 {
            return None;
        }
        let bytes = self.payload.get(1..3)?;
        Some(u16::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn modify_rules_count(&self) -> Option<u16> {
        if self.rop_id != 0x41 {
            return None;
        }
        let bytes = self.payload.get(1..3)?;
        Some(u16::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn modify_rules_rows(&self) -> Result<Vec<ModifyRulesRow>> {
        if self.rop_id != 0x41 {
            return Ok(Vec::new());
        }
        let count = self.modify_rules_count().unwrap_or(0) as usize;
        parse_modify_rows(self.payload.get(3..).unwrap_or_default(), count)
    }

    pub(in crate::mapi) fn modify_permissions_rows(&self) -> Result<Vec<ModifyRulesRow>> {
        if self.rop_id != 0x40 {
            return Ok(Vec::new());
        }
        let count = self.modify_permissions_count().unwrap_or(0) as usize;
        parse_modify_rows(self.payload.get(3..).unwrap_or_default(), count)
    }
}

fn parse_modify_rows(payload: &[u8], count: usize) -> Result<Vec<ModifyRulesRow>> {
    let mut cursor = Cursor::new(payload);
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let flags = cursor.read_u8()?;
        let property_count = cursor.read_u16()? as usize;
        let mut properties = HashMap::new();
        for _ in 0..property_count {
            let (property_tag, value) = parse_tagged_property(&mut cursor)?;
            properties.insert(property_tag, value);
        }
        rows.push(ModifyRulesRow { flags, properties });
    }
    Ok(rows)
}
