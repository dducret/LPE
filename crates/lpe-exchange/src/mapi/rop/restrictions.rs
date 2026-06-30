use super::{
    parse_tagged_property, parse_tagged_property_value, Cursor, MapiRestriction,
    MapiRestrictionType,
};
use anyhow::{anyhow, Result};

pub(in crate::mapi) fn parse_mapi_restriction(bytes: &[u8]) -> Result<MapiRestriction> {
    let mut cursor = Cursor::new(bytes);
    let restriction = parse_mapi_restriction_from(&mut cursor)?;
    if cursor.remaining() != 0 {
        return Err(anyhow!("restriction data has trailing bytes"));
    }
    Ok(restriction)
}

pub(in crate::mapi) fn parse_mapi_restriction_from(
    cursor: &mut Cursor<'_>,
) -> Result<MapiRestriction> {
    let restriction_type = cursor.read_u8()?;
    match MapiRestrictionType::from_u8(restriction_type) {
        Some(MapiRestrictionType::And) => {
            let count = cursor.read_u16()? as usize;
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(parse_mapi_restriction_from(cursor)?);
            }
            Ok(MapiRestriction::And(children))
        }
        Some(MapiRestrictionType::Or) => {
            let count = cursor.read_u16()? as usize;
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(parse_mapi_restriction_from(cursor)?);
            }
            Ok(MapiRestriction::Or(children))
        }
        Some(MapiRestrictionType::Not) => Ok(MapiRestriction::Not(Box::new(
            parse_mapi_restriction_from(cursor)?,
        ))),
        Some(MapiRestrictionType::Content) => {
            let fuzzy_level_low = cursor.read_u16()?;
            let fuzzy_level_high = cursor.read_u16()?;
            let property_tag = cursor.read_u32()?;
            let value = parse_tagged_property_value(cursor)?
                .into_text()
                .ok_or_else(|| anyhow!("content restriction requires a text value"))?;
            Ok(MapiRestriction::Content {
                property_tag,
                value,
                fuzzy_level_low,
                fuzzy_level_high,
            })
        }
        Some(MapiRestrictionType::Property) => {
            let relop = cursor.read_u8()?;
            let property_tag = cursor.read_u32()?;
            let value = parse_tagged_property_value(cursor)?;
            Ok(MapiRestriction::Property {
                relop,
                property_tag,
                value,
            })
        }
        Some(MapiRestrictionType::CompareProperties) => {
            let relop = cursor.read_u8()?;
            let left_property_tag = cursor.read_u32()?;
            let right_property_tag = cursor.read_u32()?;
            Ok(MapiRestriction::CompareProperties {
                relop,
                left_property_tag,
                right_property_tag,
            })
        }
        Some(MapiRestrictionType::Bitmask) => {
            let rel_bmr = cursor.read_u8()?;
            let property_tag = cursor.read_u32()?;
            let mask = cursor.read_u32()?;
            Ok(MapiRestriction::Bitmask {
                property_tag,
                mask,
                must_be_nonzero: rel_bmr != 0,
            })
        }
        Some(MapiRestrictionType::Size) => {
            let relop = cursor.read_u8()?;
            let property_tag = cursor.read_u32()?;
            let size = cursor.read_u32()?;
            Ok(MapiRestriction::Size {
                relop,
                property_tag,
                size,
            })
        }
        Some(MapiRestrictionType::Exist) => {
            let property_tag = cursor.read_u32()?;
            Ok(MapiRestriction::Exist { property_tag })
        }
        Some(MapiRestrictionType::SubObject) => {
            let subobject = cursor.read_u32()?;
            let child = parse_mapi_restriction_from(cursor)?;
            Ok(MapiRestriction::SubObject {
                subobject,
                child: Box::new(child),
            })
        }
        Some(MapiRestrictionType::Comment) => {
            let count = cursor.read_u8()? as usize;
            for _ in 0..count {
                parse_tagged_property(cursor)?;
            }
            match cursor.read_u8()? {
                0x00 => Ok(MapiRestriction::And(Vec::new())),
                0x01 => parse_mapi_restriction_from(cursor),
                _ => Err(anyhow!("comment restriction has invalid present flag")),
            }
        }
        Some(MapiRestrictionType::Count) => {
            let count = cursor.read_u32()?;
            let child = parse_mapi_restriction_from(cursor)?;
            Ok(MapiRestriction::Count {
                count,
                child: Box::new(child),
            })
        }
        _ => {
            tracing::warn!(
                adapter = "mapi",
                enum_name = "MapiRestrictionType",
                raw_value = restriction_type,
                "unsupported MAPI restriction type rejected at parser boundary"
            );
            Err(anyhow!("unsupported MAPI restriction type"))
        }
    }
}
