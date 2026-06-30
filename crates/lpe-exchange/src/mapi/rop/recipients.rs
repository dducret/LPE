use super::{format_property_tags_for_debug, parse_property_value_for_tag, Cursor, RopRequest};
use crate::{
    mapi::{
        nspi::{
            normalize_nspi_lookup_value, nspi_entry_legacy_dn, nspi_entry_unprefixed_legacy_dn,
            principal_address_book_entry,
        },
        properties::{
            canonical_property_storage_tag, normalize_mapi_submit_address, MapiValue,
            PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W, PID_TAG_DISPLAY_NAME_W,
            PID_TAG_EMAIL_ADDRESS_W, PID_TAG_RECIPIENT_DISPLAY_NAME_W, PID_TAG_RECIPIENT_TYPE,
            PID_TAG_SMTP_ADDRESS_W,
        },
        session::PendingRecipient,
        session::PendingRecipientChange,
    },
    store::ExchangeAddressBookEntry,
};
use anyhow::{anyhow, Result};
use lpe_mail_auth::AccountPrincipal;
use std::collections::HashMap;

impl RopRequest {
    pub(in crate::mapi) fn modify_recipients(
        &self,
        principal: &AccountPrincipal,
        address_book_entries: &[ExchangeAddressBookEntry],
    ) -> Result<Vec<PendingRecipientChange>> {
        let Some(count_bytes) = self.payload.get(..2) else {
            return Ok(Vec::new());
        };
        let column_count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        let columns_end = 2 + column_count * 4;
        let columns = self
            .payload
            .get(2..columns_end)
            .ok_or_else(|| anyhow!("truncated recipient columns"))?
            .chunks_exact(4)
            .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect::<Vec<_>>();
        let row_count_bytes = self
            .payload
            .get(columns_end..columns_end + 2)
            .ok_or_else(|| anyhow!("missing recipient row count"))?;
        let row_count = u16::from_le_bytes([row_count_bytes[0], row_count_bytes[1]]) as usize;
        let mut cursor = Cursor::new(
            self.payload
                .get(columns_end + 2..)
                .ok_or_else(|| anyhow!("missing recipient rows"))?,
        );
        let mut changes = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let row_id = cursor.read_u32()?;
            let recipient_type = cursor.read_u8()?;
            let row_size = cursor.read_u16()? as usize;
            if row_size == 0 {
                changes.push(PendingRecipientChange::Delete(row_id));
                continue;
            }
            let row = cursor.read_bytes(row_size)?;
            changes.push(PendingRecipientChange::Upsert(parse_pending_recipient_row(
                row_id,
                recipient_type,
                &columns,
                row,
                principal,
                address_book_entries,
            )?));
        }
        Ok(changes)
    }
}

pub(in crate::mapi) fn parse_pending_recipient_row(
    row_id: u32,
    fallback_recipient_type: u8,
    columns: &[u32],
    row: &[u8],
    principal: &AccountPrincipal,
    address_book_entries: &[ExchangeAddressBookEntry],
) -> Result<PendingRecipient> {
    if let Ok(recipient) = parse_wrapped_pending_recipient_row(
        row_id,
        fallback_recipient_type,
        columns,
        row,
        principal,
        address_book_entries,
    ) {
        return Ok(recipient);
    }

    parse_simple_pending_recipient_row(row_id, fallback_recipient_type, columns, row)
}

fn parse_simple_pending_recipient_row(
    row_id: u32,
    fallback_recipient_type: u8,
    columns: &[u32],
    row: &[u8],
) -> Result<PendingRecipient> {
    let mut cursor = Cursor::new(row);
    let mut values = HashMap::new();
    for column in columns {
        values.insert(
            canonical_property_storage_tag(*column),
            parse_property_value_for_tag(&mut cursor, *column)?,
        );
    }
    let recipient_type = values
        .get(&PID_TAG_RECIPIENT_TYPE)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u8::try_from(value).ok())
        .unwrap_or(fallback_recipient_type);
    let recipient_type = normalize_recipient_type(recipient_type)?;
    let address =
        optional_mapi_value_text(&values, &[PID_TAG_SMTP_ADDRESS_W, PID_TAG_EMAIL_ADDRESS_W])
            .and_then(normalize_mapi_submit_address)
            .ok_or_else(|| {
                anyhow!(
                    "recipient address is required;row_format=simple;columns={}",
                    format_property_tags_for_debug(columns)
                )
            })?;
    let display_name = recipient_display_name_from_values(&values)
        .filter(|value| !value.eq_ignore_ascii_case(&address));

    Ok(PendingRecipient {
        row_id,
        recipient_type,
        address,
        display_name,
    })
}

fn parse_wrapped_pending_recipient_row(
    row_id: u32,
    fallback_recipient_type: u8,
    columns: &[u32],
    row: &[u8],
    principal: &AccountPrincipal,
    address_book_entries: &[ExchangeAddressBookEntry],
) -> Result<PendingRecipient> {
    let mut cursor = Cursor::new(row);
    let recipient_flags = cursor.read_u16()?;
    let address_type = recipient_flags & 0x0007;
    let unicode_strings = recipient_flags & 0x0200 != 0;

    let x500_dn = if address_type == 0x01 {
        let _address_prefix_used = cursor.read_u8()?;
        let _display_type = cursor.read_u8()?;
        Some(cursor.read_ascii_z()?).filter(|value| !value.is_empty())
    } else if matches!(address_type, 0x06 | 0x07) {
        let entry_id_size = cursor.read_u16()? as usize;
        let _entry_id = cursor.read_bytes(entry_id_size)?;
        let search_key_size = cursor.read_u16()? as usize;
        let _search_key = cursor.read_bytes(search_key_size)?;
        None
    } else {
        None
    };

    if address_type == 0x00 && recipient_flags & 0x8000 != 0 {
        let _address_type = cursor.read_ascii_z()?;
    }

    let email_address = if recipient_flags & 0x0008 != 0 {
        Some(read_recipient_string(&mut cursor, unicode_strings)?)
    } else {
        None
    };
    let display_name = if recipient_flags & 0x0010 != 0 {
        Some(read_recipient_string(&mut cursor, unicode_strings)?)
    } else {
        None
    };
    if recipient_flags & 0x0400 != 0 {
        let _simple_display_name = read_recipient_string(&mut cursor, unicode_strings)?;
    }
    if recipient_flags & 0x0020 != 0 {
        let _transmittable_display_name = read_recipient_string(&mut cursor, unicode_strings)?;
    }

    let recipient_column_count = cursor.read_u16()? as usize;
    if recipient_column_count > columns.len() {
        return Err(anyhow!(
            "recipient column count exceeds request column count"
        ));
    }
    let row_kind = cursor.read_u8()?;
    if row_kind != 0 {
        return Err(anyhow!("unsupported flagged recipient property row"));
    }

    let mut values = HashMap::new();
    for column in columns.iter().take(recipient_column_count) {
        values.insert(
            canonical_property_storage_tag(*column),
            parse_property_value_for_tag(&mut cursor, *column)?,
        );
    }

    let recipient_type = values
        .get(&PID_TAG_RECIPIENT_TYPE)
        .and_then(MapiValue::as_i64)
        .and_then(|value| u8::try_from(value).ok())
        .unwrap_or(fallback_recipient_type);
    let recipient_type = normalize_recipient_type(recipient_type)?;
    let address =
        optional_mapi_value_text(&values, &[PID_TAG_SMTP_ADDRESS_W, PID_TAG_EMAIL_ADDRESS_W])
            .or(email_address)
            .and_then(normalize_mapi_submit_address)
            .or_else(|| {
                x500_dn
                    .as_deref()
                    .and_then(|dn| legacy_dn_recipient_address(dn, principal, address_book_entries))
            })
            .ok_or_else(|| {
                anyhow!(
                    "recipient address is required;row_format=wrapped;recipient_flags={recipient_flags:#06x};address_type={address_type:#04x};recipient_column_count={recipient_column_count};columns={}",
                    format_property_tags_for_debug(columns)
                )
            })?;
    let display_name = recipient_display_name_from_values(&values)
        .or(display_name)
        .filter(|value| !value.eq_ignore_ascii_case(&address));

    Ok(PendingRecipient {
        row_id,
        recipient_type,
        address,
        display_name,
    })
}

fn recipient_display_name_from_values(values: &HashMap<u32, MapiValue>) -> Option<String> {
    optional_mapi_value_text(
        values,
        &[
            PID_TAG_DISPLAY_NAME_W,
            PID_TAG_RECIPIENT_DISPLAY_NAME_W,
            PID_TAG_ADDRESS_BOOK_DISPLAY_NAME_PRINTABLE_W,
        ],
    )
}

fn optional_mapi_value_text(values: &HashMap<u32, MapiValue>, tags: &[u32]) -> Option<String> {
    tags.iter()
        .find_map(|tag| values.get(tag).and_then(|value| value.clone().into_text()))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn normalize_recipient_type(recipient_type: u8) -> Result<u8> {
    let base_type = recipient_type & 0x0F;
    let flags = recipient_type & !0x0F;
    if matches!(base_type, 0x01..=0x03) && flags & !0x90 == 0 {
        Ok(base_type)
    } else {
        Err(anyhow!("invalid recipient type {recipient_type:#04x}"))
    }
}

fn legacy_dn_recipient_address(
    legacy_dn: &str,
    principal: &AccountPrincipal,
    address_book_entries: &[ExchangeAddressBookEntry],
) -> Option<String> {
    let legacy_dn = normalize_nspi_lookup_value(legacy_dn);
    let principal_entry = principal_address_book_entry(principal);
    std::iter::once(&principal_entry)
        .chain(address_book_entries.iter())
        .find(|entry| {
            legacy_dn_matches_entry(&legacy_dn, &nspi_entry_legacy_dn(entry))
                || legacy_dn_matches_entry(&legacy_dn, &nspi_entry_unprefixed_legacy_dn(entry))
        })
        .map(|entry| lpe_storage::normalize_mailbox_email(&entry.email))
        .filter(|address| !address.is_empty())
}

fn legacy_dn_matches_entry(actual: &str, expected: &str) -> bool {
    let expected = expected.to_ascii_lowercase();
    actual == expected || actual == expected.trim_start_matches('/')
}

fn read_recipient_string(cursor: &mut Cursor<'_>, unicode: bool) -> Result<String> {
    if unicode {
        cursor.read_utf16z()
    } else {
        cursor.read_ascii_z()
    }
}
