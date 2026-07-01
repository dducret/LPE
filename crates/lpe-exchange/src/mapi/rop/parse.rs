use super::{
    parse_mapi_restriction, read_u16_prefixed_string, rop_id_is_reserved, typed_requests::*, Cursor,
};
use crate::mapi::properties::{
    canonical_property_storage_tag, parse_mapi_property_value, MapiNamedProperty,
    MapiNamedPropertyKind, MapiRestriction, MapiSortOrder, MapiValue, PID_TAG_SOURCE_KEY,
};
use crate::mapi::wire::RopId;
use anyhow::{anyhow, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopRequest {
    pub(in crate::mapi) rop_id: u8,
    pub(in crate::mapi) input_handle_index: Option<u8>,
    pub(in crate::mapi) output_handle_index: Option<u8>,
    pub(in crate::mapi) payload: Vec<u8>,
}

impl RopRequest {
    pub(in crate::mapi) fn input_handle_index(&self) -> Option<u8> {
        self.input_handle_index
    }

    pub(in crate::mapi) fn output_handle_index(&self) -> Option<u8> {
        self.output_handle_index
    }

    pub(in crate::mapi) fn response_handle_index(&self) -> u8 {
        if matches!(
            self.rop_id,
            0x02 | 0x03
                | 0x04
                | 0x05
                | 0x06
                | 0x0C
                | 0x11
                | 0x1C
                | 0x21
                | 0x22
                | 0x2B
                | 0x25
                | 0x29
                | 0x3B
                | 0x3E
                | 0x3F
                | 0x46
                | 0x4B
                | 0x4C
                | 0x4D
                | 0x53
                | 0x69
                | 0x70
                | 0x72
                | 0x7E
                | 0x82
        ) {
            return self.output_handle_index.unwrap_or(0);
        }
        self.input_handle_index
            .unwrap_or(self.output_handle_index.unwrap_or(0))
    }

    pub(in crate::mapi) fn folder_id(&self) -> Option<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::OpenFolder | RopId::OpenMessage | RopId::CreateMessage)
        ) {
            return None;
        }
        let bytes = self.payload.get(..8)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn create_message_associated(&self) -> bool {
        matches!(RopId::from_u8(self.rop_id), Some(RopId::CreateMessage))
            && self.payload.get(8).is_some_and(|flag| *flag != 0)
    }

    pub(in crate::mapi) fn abort_submit_folder_id(&self) -> Option<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::AbortSubmit)) {
            return None;
        }
        let bytes = self.payload.get(..8)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn abort_submit_message_id(&self) -> Option<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::AbortSubmit)) {
            return None;
        }
        let bytes = self.payload.get(8..16)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
            .or_else(|| bytes.try_into().ok().map(u64::from_le_bytes))
    }

    pub(in crate::mapi) fn public_folder_probe_object_id(&self) -> Option<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::GetOwningServers | RopId::PublicFolderIsGhosted)
        ) {
            return None;
        }
        let bytes = self.payload.get(..8)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
            .or_else(|| crate::mapi::identity::object_id_from_trailing_replid_wire_id(bytes))
    }

    pub(in crate::mapi) fn notification_types(&self) -> Option<u16> {
        if self.rop_id != 0x29 {
            return None;
        }
        let bytes = self.payload.get(..2)?;
        Some(u16::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn notification_want_whole_store(&self) -> Option<bool> {
        if self.rop_id != 0x29 {
            return None;
        }
        let offset = if self.notification_types()? & 0x0400 != 0 {
            3
        } else {
            2
        };
        Some(self.payload.get(offset).copied()? != 0)
    }

    pub(in crate::mapi) fn notification_folder_id(&self) -> Option<u64> {
        if self.rop_id != 0x29 || self.notification_want_whole_store()? {
            return None;
        }
        let offset = if self.notification_types()? & 0x0400 != 0 {
            4
        } else {
            3
        };
        let bytes = self.payload.get(offset..offset + 8)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn message_id(&self) -> Option<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::OpenMessage)) {
            return None;
        }
        let bytes = self.payload.get(9..17)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn row_id(&self) -> Option<u32> {
        let bytes = self.payload.get(..4)?;
        Some(u32::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn read_recipients_reserved(&self) -> Option<u16> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::ReadRecipients)) {
            return None;
        }
        let bytes = self.payload.get(4..6)?;
        Some(u16::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn attach_num(&self) -> Option<u32> {
        let bytes = if self.rop_id == 0x24 {
            self.payload.get(..4)?
        } else {
            self.payload.get(1..5)?
        };
        Some(u32::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn stream_property_tag(&self) -> Option<u32> {
        let bytes = self.payload.get(..4)?;
        Some(u32::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn stream_open_mode(&self) -> Option<u8> {
        self.payload.get(4).copied()
    }

    pub(in crate::mapi) fn read_byte_count(&self) -> Option<usize> {
        let bytes = self.payload.get(..2)?;
        let byte_count = u16::from_le_bytes(bytes.try_into().ok()?);
        if byte_count == 0xBABE {
            let maximum = self.payload.get(2..6)?;
            let maximum = u32::from_le_bytes(maximum.try_into().ok()?);
            return Some((maximum as usize).min(u16::MAX as usize));
        }
        Some(usize::from(byte_count))
    }

    pub(in crate::mapi) fn stream_write_data(&self) -> &[u8] {
        let size = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
            .unwrap_or(0);
        self.payload.get(2..2 + size).unwrap_or_default()
    }

    pub(in crate::mapi) fn stream_seek_origin(&self) -> Option<u8> {
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn stream_seek_offset(&self) -> Option<i64> {
        let bytes = self.payload.get(1..9)?;
        Some(i64::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn stream_size(&self) -> Option<u64> {
        let bytes = self.payload.get(..8)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn read_flags(&self) -> Option<u8> {
        match self.rop_id {
            0x11 => self.payload.first().copied(),
            0x66 => self.payload.get(1).copied(),
            _ => None,
        }
    }

    pub(in crate::mapi) fn want_asynchronous(&self) -> Option<u8> {
        if matches!(RopId::from_u8(self.rop_id), Some(RopId::SetReadFlags)) {
            self.payload.first().copied()
        } else {
            None
        }
    }

    pub(in crate::mapi) fn sync_type(&self) -> u8 {
        self.payload.first().copied().unwrap_or(0)
    }

    pub(in crate::mapi) fn sync_send_options(&self) -> u8 {
        self.payload.get(1).copied().unwrap_or(0)
    }

    pub(in crate::mapi) fn sync_flags(&self) -> u16 {
        self.payload
            .get(2..4)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn sync_extra_flags(&self) -> u32 {
        let restriction_size = self
            .payload
            .get(4..6)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload
            .get(6 + restriction_size..10 + restriction_size)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn sync_property_tags(&self) -> Vec<u32> {
        let restriction_size = self
            .payload
            .get(4..6)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        let count_offset = 10 + restriction_size;
        let count = self
            .payload
            .get(count_offset..count_offset + 2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload
            .get(count_offset + 2..)
            .unwrap_or_default()
            .chunks_exact(4)
            .take(count)
            .map(|bytes| u32::from_le_bytes(bytes.try_into().unwrap_or_default()))
            .collect()
    }

    pub(in crate::mapi) fn fast_transfer_buffer_size(&self) -> usize {
        let requested = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(u16::MAX);
        if requested == 0xBABE {
            return self
                .payload
                .get(2..4)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u16::from_le_bytes)
                .map(usize::from)
                .unwrap_or(u16::MAX as usize);
        }
        usize::from(requested)
    }

    pub(in crate::mapi) fn stream_data(&self) -> &[u8] {
        let Some(size_bytes) = self.payload.get(..4) else {
            return &[];
        };
        let size = u32::from_le_bytes([size_bytes[0], size_bytes[1], size_bytes[2], size_bytes[3]])
            as usize;
        self.payload.get(4..4 + size).unwrap_or_default()
    }

    pub(in crate::mapi) fn fast_transfer_upload_data(&self) -> &[u8] {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(
                RopId::FastTransferDestinationPutBuffer
                    | RopId::FastTransferDestinationPutBufferExtended
            )
        ) {
            return &[];
        }
        let Some(size_bytes) = self.payload.get(..2) else {
            return &[];
        };
        let size = u16::from_le_bytes([size_bytes[0], size_bytes[1]]) as usize;
        self.payload.get(2..2 + size).unwrap_or_default()
    }

    pub(in crate::mapi) fn upload_state_property_tag(&self) -> Option<u32> {
        self.payload
            .get(..4)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
    }

    pub(in crate::mapi) fn upload_state_transfer_size(&self) -> Option<u32> {
        self.payload
            .get(4..8)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
    }

    pub(in crate::mapi) fn import_message_id(&self) -> Option<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::SynchronizationImportMessageChange)
        ) {
            return None;
        }
        self.import_property_values()
            .ok()?
            .into_iter()
            .find_map(|(tag, value)| match (tag, value) {
                (PID_TAG_SOURCE_KEY, MapiValue::Binary(bytes)) => {
                    crate::mapi::identity::object_id_from_source_key(&bytes)
                }
                _ => None,
            })
    }

    pub(in crate::mapi) fn import_flag(&self) -> Option<u8> {
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn import_property_values(&self) -> Result<Vec<(u32, MapiValue)>> {
        let property_payload = self
            .payload
            .get(1..)
            .ok_or_else(|| anyhow!("missing import property payload"))?;
        let mut cursor = Cursor::new(property_payload);
        let property_value_count = cursor.read_u16()? as usize;
        let mut values = Vec::with_capacity(property_value_count);
        for _ in 0..property_value_count {
            values.push(parse_tagged_property(&mut cursor)?);
        }
        Ok(values)
    }

    pub(in crate::mapi) fn import_hierarchy_values(
        &self,
    ) -> Result<(Vec<(u32, MapiValue)>, Vec<(u32, MapiValue)>)> {
        let mut cursor = Cursor::new(self.payload.as_slice());
        let hierarchy_count = cursor.read_u16()? as usize;
        let mut hierarchy_values = Vec::with_capacity(hierarchy_count);
        for _ in 0..hierarchy_count {
            hierarchy_values.push(parse_tagged_property(&mut cursor)?);
        }
        let property_count = cursor.read_u16()? as usize;
        let mut property_values = Vec::with_capacity(property_count);
        for _ in 0..property_count {
            property_values.push(parse_tagged_property(&mut cursor)?);
        }
        Ok((hierarchy_values, property_values))
    }

    pub(in crate::mapi) fn import_delete_message_ids(&self) -> Vec<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::SynchronizationImportDeletes)
        ) {
            return Vec::new();
        }
        let count = self
            .payload
            .get(1..3)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload
            .get(3..)
            .unwrap_or_default()
            .chunks_exact(8)
            .take(count)
            .filter_map(crate::mapi::identity::object_id_from_wire_id)
            .collect()
    }

    pub(in crate::mapi) fn import_delete_hard_delete(&self) -> bool {
        self.payload.first().is_some_and(|flags| flags & 0x02 != 0)
    }

    pub(in crate::mapi) fn fast_transfer_message_ids(&self) -> Vec<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::FastTransferSourceCopyMessages)
        ) {
            return Vec::new();
        }
        let count = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
            .unwrap_or(0);
        self.payload
            .get(2..)
            .unwrap_or_default()
            .chunks_exact(8)
            .take(count)
            .filter_map(crate::mapi::identity::object_id_from_wire_id)
            .collect()
    }

    pub(in crate::mapi) fn import_move(&self) -> Option<(u64, u64)> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::SynchronizationImportMessageMove)
        ) {
            return None;
        }
        let mut cursor = Cursor::new(&self.payload);
        let source_folder_id_size = cursor.read_u32().ok()? as usize;
        let source_folder_id = cursor.read_bytes(source_folder_id_size).ok()?;
        let source_folder_id = crate::mapi::identity::object_id_from_wire_id(source_folder_id)?;
        let source_message_id_size = cursor.read_u32().ok()? as usize;
        let source_message_id = cursor.read_bytes(source_message_id_size).ok()?;
        let source_message_id = crate::mapi::identity::object_id_from_wire_id(source_message_id)?;
        Some((source_folder_id, source_message_id))
    }

    pub(in crate::mapi) fn import_read_state_changes(&self) -> Vec<(u64, bool)> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::SynchronizationImportReadStateChanges)
        ) {
            return Vec::new();
        }
        let size = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        if self.payload.len() == 2 + size.saturating_mul(9) {
            return self.payload[2..]
                .chunks_exact(9)
                .filter_map(|chunk| {
                    crate::mapi::identity::object_id_from_wire_id(&chunk[..8])
                        .map(|message_id| (message_id, chunk[8] == 0))
                })
                .collect();
        }
        let mut cursor = Cursor::new(self.payload.get(2..2 + size).unwrap_or_default());
        let mut changes = Vec::new();
        while cursor.remaining() >= 3 {
            let Ok(message_id_size) = cursor.read_u16().map(usize::from) else {
                break;
            };
            let Ok(message_id_bytes) = cursor.read_bytes(message_id_size) else {
                break;
            };
            let Ok(mark_as_read) = cursor.read_u8() else {
                break;
            };
            if let Some(message_id) =
                crate::mapi::identity::object_id_from_wire_id(message_id_bytes)
                    .or_else(|| crate::mapi::identity::object_id_from_source_key(message_id_bytes))
            {
                changes.push((message_id, mark_as_read == 0));
            }
        }
        changes
    }

    pub(in crate::mapi) fn local_replica_midset_deleted(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub(in crate::mapi) fn search_criteria_restriction_bytes(&self) -> Option<&[u8]> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::SetSearchCriteria)) {
            return None;
        }
        let size = u16::from_le_bytes(self.payload.get(..2)?.try_into().ok()?) as usize;
        self.payload.get(2..2 + size)
    }

    pub(in crate::mapi) fn search_criteria_folder_ids(&self) -> Option<Vec<u64>> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::SetSearchCriteria)) {
            return None;
        }
        let size = u16::from_le_bytes(self.payload.get(..2)?.try_into().ok()?) as usize;
        let count_offset = 2 + size;
        let count = u16::from_le_bytes(
            self.payload
                .get(count_offset..count_offset + 2)?
                .try_into()
                .ok()?,
        ) as usize;
        let ids_offset = count_offset + 2;
        self.payload
            .get(ids_offset..ids_offset + count * 8)?
            .chunks_exact(8)
            .map(crate::mapi::identity::object_id_from_wire_id)
            .collect()
    }

    pub(in crate::mapi) fn search_criteria_flags(&self) -> Option<u32> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::SetSearchCriteria)) {
            return None;
        }
        let size = u16::from_le_bytes(self.payload.get(..2)?.try_into().ok()?) as usize;
        let count_offset = 2 + size;
        let count = u16::from_le_bytes(
            self.payload
                .get(count_offset..count_offset + 2)?
                .try_into()
                .ok()?,
        ) as usize;
        let flags_offset = count_offset + 2 + count * 8;
        Some(u32::from_le_bytes(
            self.payload
                .get(flags_offset..flags_offset + 4)?
                .try_into()
                .ok()?,
        ))
    }

    pub(in crate::mapi) fn get_search_criteria_include_restriction(&self) -> bool {
        matches!(RopId::from_u8(self.rop_id), Some(RopId::GetSearchCriteria))
            && self.payload.get(1).copied().unwrap_or(0) != 0
    }

    pub(in crate::mapi) fn get_search_criteria_use_unicode(&self) -> bool {
        matches!(RopId::from_u8(self.rop_id), Some(RopId::GetSearchCriteria))
            && self.payload.first().copied().unwrap_or(0) != 0
    }

    pub(in crate::mapi) fn get_search_criteria_include_folders(&self) -> bool {
        matches!(RopId::from_u8(self.rop_id), Some(RopId::GetSearchCriteria))
            && self.payload.get(2).copied().unwrap_or(0) != 0
    }

    pub(in crate::mapi) fn receive_folder_message_class(&self) -> Option<&str> {
        let bytes = self.payload.strip_suffix(&[0])?;
        std::str::from_utf8(bytes).ok()
    }

    pub(in crate::mapi) fn set_receive_folder_id(&self) -> Option<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::SetReceiveFolder)) {
            return None;
        }
        crate::mapi::identity::object_id_from_wire_id(self.payload.get(..8)?)
    }

    pub(in crate::mapi) fn set_receive_folder_message_class(&self) -> Option<&str> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::SetReceiveFolder)) {
            return None;
        }
        let bytes = self.payload.get(8..)?.strip_suffix(&[0])?;
        std::str::from_utf8(bytes).ok()
    }

    pub(in crate::mapi) fn local_replica_id_count(&self) -> u32 {
        self.payload
            .get(..4)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(1)
    }

    pub(in crate::mapi) fn long_term_id(&self) -> Option<&[u8]> {
        self.payload.get(..24)
    }

    pub(in crate::mapi) fn per_user_folder_object_id(&self) -> Option<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::ReadPerUserInformation | RopId::WritePerUserInformation)
        ) {
            return None;
        }
        crate::mapi::identity::object_id_from_long_term_id(self.payload.get(..24)?)
    }

    pub(in crate::mapi) fn per_user_data_offset(&self) -> u32 {
        self.payload
            .get(25..29)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn per_user_max_data_size(&self) -> u16 {
        self.payload
            .get(29..31)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn per_user_has_finished(&self) -> bool {
        self.payload.get(24).copied().unwrap_or(0) != 0
    }

    pub(in crate::mapi) fn per_user_write_data(&self) -> &[u8] {
        let size = self.per_user_max_data_size() as usize;
        self.payload.get(31..31 + size).unwrap_or_default()
    }

    pub(in crate::mapi) fn message_ids(&self) -> Vec<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::DeleteMessages | RopId::HardDeleteMessages | RopId::SetReadFlags)
        ) {
            return Vec::new();
        }
        let (count_offset, ids_offset) = (2, 4);
        let Some(count_bytes) = self.payload.get(count_offset..count_offset + 2) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload[ids_offset..]
            .chunks_exact(8)
            .take(count)
            .filter_map(crate::mapi::identity::object_id_from_wire_id)
            .collect()
    }

    pub(in crate::mapi) fn delete_messages_want_asynchronous(&self) -> Option<u8> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::DeleteMessages | RopId::HardDeleteMessages)
        ) {
            return None;
        }
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn delete_messages_notify_non_read(&self) -> Option<u8> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::DeleteMessages | RopId::HardDeleteMessages)
        ) {
            return None;
        }
        self.payload.get(1).copied()
    }

    pub(in crate::mapi) fn status_message_id(&self) -> Option<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::GetMessageStatus | RopId::SetMessageStatus)
        ) {
            return None;
        }
        let bytes = self.payload.get(..8)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn message_status_flags(&self) -> u32 {
        self.payload
            .get(8..12)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn message_status_mask(&self) -> u32 {
        self.payload
            .get(12..16)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn reload_cached_information_reserved(&self) -> Option<u16> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::ReloadCachedInformation)
        ) {
            return None;
        }
        let bytes = self.payload.get(..2)?;
        Some(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub(in crate::mapi) fn create_folder_type(&self) -> u8 {
        self.payload.first().copied().unwrap_or(0)
    }

    pub(in crate::mapi) fn create_folder_open_existing(&self) -> bool {
        self.payload
            .get(2)
            .is_some_and(|open_existing| *open_existing != 0)
    }

    pub(in crate::mapi) fn create_folder_reserved(&self) -> u8 {
        self.payload.get(3).copied().unwrap_or(0)
    }

    pub(in crate::mapi) fn create_folder_display_name(&self) -> String {
        read_u16_prefixed_string(&self.payload, 4).unwrap_or_default()
    }

    pub(in crate::mapi) fn delete_folder_flags(&self) -> Option<u8> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::DeleteFolder)) {
            return None;
        }
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn delete_folder_id(&self) -> Option<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::DeleteFolder)) {
            return None;
        }
        let bytes = self.payload.get(1..9)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn move_copy_message_ids(&self) -> Vec<u64> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::MoveCopyMessages)) {
            return Vec::new();
        }
        let Some(count_bytes) = self.payload.get(..2) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload[2..]
            .chunks_exact(8)
            .take(count)
            .filter_map(crate::mapi::identity::object_id_from_wire_id)
            .collect()
    }

    pub(in crate::mapi) fn move_copy_want_copy(&self) -> bool {
        let count = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload
            .get(2 + count * 8 + 1)
            .is_some_and(|want_copy| *want_copy != 0)
    }

    pub(in crate::mapi) fn move_copy_want_asynchronous(&self) -> Option<u8> {
        let count = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload.get(2 + count * 8).copied()
    }

    pub(in crate::mapi) fn move_copy_want_copy_raw(&self) -> Option<u8> {
        let count = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload.get(2 + count * 8 + 1).copied()
    }

    pub(in crate::mapi) fn folder_move_copy_folder_id(&self) -> Option<u64> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::MoveFolder | RopId::CopyFolder)
        ) {
            return None;
        }
        let offset = if matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyFolder)) {
            3
        } else {
            2
        };
        let bytes = self.payload.get(offset..offset + 8)?;
        crate::mapi::identity::object_id_from_wire_id(bytes)
    }

    pub(in crate::mapi) fn folder_move_copy_want_asynchronous(&self) -> Option<u8> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::MoveFolder | RopId::CopyFolder)
        ) {
            return None;
        }
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn folder_move_copy_want_recursive(&self) -> Option<u8> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyFolder)) {
            return None;
        }
        self.payload.get(1).copied()
    }

    pub(in crate::mapi) fn folder_move_copy_use_unicode(&self) -> Option<u8> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::MoveFolder | RopId::CopyFolder)
        ) {
            return None;
        }
        let offset = if matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyFolder)) {
            2
        } else {
            1
        };
        self.payload.get(offset).copied()
    }

    pub(in crate::mapi) fn folder_move_copy_display_name(&self) -> String {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::MoveFolder | RopId::CopyFolder)
        ) {
            return String::new();
        }
        let unicode_offset = if matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyFolder)) {
            2
        } else {
            1
        };
        let Some(use_unicode) = self.payload.get(unicode_offset) else {
            return String::new();
        };
        let name_offset = unicode_offset + 1 + 8;
        let Some(name_bytes) = self.payload.get(name_offset..) else {
            return String::new();
        };
        if *use_unicode == 0 {
            let end = name_bytes
                .iter()
                .position(|byte| *byte == 0)
                .unwrap_or(name_bytes.len());
            String::from_utf8_lossy(&name_bytes[..end]).into_owned()
        } else {
            let mut units = Vec::new();
            for bytes in name_bytes.chunks_exact(2) {
                let unit = u16::from_le_bytes([bytes[0], bytes[1]]);
                if unit == 0 {
                    break;
                }
                units.push(unit);
            }
            String::from_utf16_lossy(&units)
        }
    }

    pub(in crate::mapi) fn empty_folder_want_asynchronous(&self) -> Option<u8> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::EmptyFolder | RopId::HardDeleteMessagesAndSubfolders)
        ) {
            return None;
        }
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn empty_folder_want_delete_associated(&self) -> Option<u8> {
        if !matches!(
            RopId::from_u8(self.rop_id),
            Some(RopId::EmptyFolder | RopId::HardDeleteMessagesAndSubfolders)
        ) {
            return None;
        }
        self.payload.get(1).copied()
    }

    pub(in crate::mapi) fn move_copy_target_handle(&self, input_handles: &[u32]) -> Option<u32> {
        input_handles
            .get(self.output_handle_index? as usize)
            .copied()
            .filter(|handle| *handle != u32::MAX)
    }

    pub(in crate::mapi) fn copy_to_want_asynchronous(&self) -> Option<u8> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyTo)) {
            return None;
        }
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn copy_to_want_subobjects(&self) -> Option<u8> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyTo)) {
            return None;
        }
        self.payload.get(1).copied()
    }

    pub(in crate::mapi) fn copy_to_excluded_property_tags(&self) -> Vec<u32> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyTo)) {
            return Vec::new();
        }
        let Some(count_bytes) = self.payload.get(3..5) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload
            .get(5..)
            .unwrap_or_default()
            .chunks_exact(4)
            .take(count)
            .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect()
    }

    pub(in crate::mapi) fn copy_properties_want_asynchronous(&self) -> Option<u8> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyProperties)) {
            return None;
        }
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn copy_properties_property_tags(&self) -> Vec<u32> {
        if !matches!(RopId::from_u8(self.rop_id), Some(RopId::CopyProperties)) {
            return Vec::new();
        }
        let Some(count_bytes) = self.payload.get(2..4) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload
            .get(4..)
            .unwrap_or_default()
            .chunks_exact(4)
            .take(count)
            .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect()
    }

    pub(in crate::mapi) fn query_row_count(&self) -> Option<usize> {
        let bytes = self.payload.get(2..4)?;
        Some(u16::from_le_bytes(bytes.try_into().ok()?) as usize)
    }

    pub(in crate::mapi) fn query_no_advance(&self) -> bool {
        self.payload.first().is_some_and(|flags| flags & 0x01 != 0)
    }

    pub(in crate::mapi) fn query_forward_read(&self) -> bool {
        self.payload
            .get(1)
            .map(|forward| *forward != 0)
            .unwrap_or(true)
    }

    pub(in crate::mapi) fn restriction(&self) -> Result<Option<MapiRestriction>> {
        let Some(size_bytes) = self.payload.get(1..3) else {
            return Ok(None);
        };
        let size = u16::from_le_bytes([size_bytes[0], size_bytes[1]]) as usize;
        if size == 0 {
            return Ok(None);
        }
        let bytes = self
            .payload
            .get(3..3 + size)
            .ok_or_else(|| anyhow!("restriction data is truncated"))?;
        parse_mapi_restriction(bytes).map(Some)
    }

    pub(in crate::mapi) fn find_origin(&self) -> Option<u8> {
        let size = u16::from_le_bytes(self.payload.get(1..3)?.try_into().ok()?) as usize;
        self.payload.get(3 + size).copied()
    }

    pub(in crate::mapi) fn find_backward(&self) -> bool {
        self.payload.first().is_some_and(|flags| flags & 0x01 != 0)
    }

    pub(in crate::mapi) fn bookmark(&self) -> &[u8] {
        let size = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0) as usize;
        self.payload.get(2..2 + size).unwrap_or_default()
    }

    pub(in crate::mapi) fn bookmark_row_count(&self) -> Option<i32> {
        let size = u16::from_le_bytes(self.payload.get(..2)?.try_into().ok()?) as usize;
        let bytes = self.payload.get(2 + size..6 + size)?;
        Some(i32::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn bookmark_want_row_moved_count(&self) -> bool {
        let Some(size) = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
        else {
            return false;
        };
        self.payload.get(6 + size).is_some_and(|want| *want != 0)
    }

    pub(in crate::mapi) fn seek_origin(&self) -> Option<u8> {
        self.payload.first().copied()
    }

    pub(in crate::mapi) fn seek_row_count(&self) -> Option<i32> {
        let bytes = self.payload.get(1..5)?;
        Some(i32::from_le_bytes(bytes.try_into().ok()?))
    }

    pub(in crate::mapi) fn want_row_moved_count(&self) -> bool {
        self.payload.get(5).is_some_and(|want| *want != 0)
    }

    pub(in crate::mapi) fn fractional_position(&self) -> Option<(u32, u32)> {
        let numerator = u32::from_le_bytes(self.payload.get(..4)?.try_into().ok()?);
        let denominator = u32::from_le_bytes(self.payload.get(4..8)?.try_into().ok()?);
        Some((numerator, denominator))
    }

    pub(in crate::mapi) fn sort_orders(&self) -> Vec<MapiSortOrder> {
        let Some(count_bytes) = self.payload.get(1..3) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload
            .get(7..)
            .unwrap_or_default()
            .chunks_exact(5)
            .take(count)
            .map(|bytes| MapiSortOrder {
                property_tag: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                order: bytes[4],
            })
            .collect()
    }

    pub(in crate::mapi) fn sort_category_count(&self) -> u16 {
        self.payload
            .get(3..5)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn sort_expanded_count(&self) -> u16 {
        self.payload
            .get(5..7)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn category_id(&self) -> Option<u64> {
        let offset = match RopId::from_u8(self.rop_id) {
            Some(RopId::ExpandRow) => 2,
            Some(RopId::CollapseRow) => 0,
            _ => return None,
        };
        self.payload
            .get(offset..offset + 8)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_le_bytes)
    }

    pub(in crate::mapi) fn expand_max_row_count(&self) -> usize {
        self.payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn collapse_state(&self) -> &[u8] {
        let size = self
            .payload
            .get(..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_le_bytes)
            .map(usize::from)
            .unwrap_or(0);
        self.payload.get(2..2 + size).unwrap_or_default()
    }

    pub(in crate::mapi) fn collapse_state_row_id(&self) -> Option<u64> {
        if self.rop_id != 0x6B {
            return None;
        }
        self.payload
            .get(..8)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_le_bytes)
    }

    pub(in crate::mapi) fn collapse_state_row_instance_number(&self) -> u32 {
        self.payload
            .get(8..12)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(0)
    }

    pub(in crate::mapi) fn property_tags(&self) -> Vec<u32> {
        let start = match self.rop_id {
            0x07 => 4,
            0x0B | 0x0E | 0x7A => 2,
            _ => 3,
        };
        if self.payload.len() < start {
            return Vec::new();
        }
        let count_offset = start - 2;
        let count = u16::from_le_bytes([self.payload[count_offset], self.payload[count_offset + 1]])
            as usize;
        self.payload[start..]
            .chunks_exact(4)
            .take(count)
            .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect()
    }

    pub(in crate::mapi) fn property_ids(&self) -> Vec<u16> {
        let Some(count_bytes) = self.payload.get(..2) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload
            .get(2..)
            .unwrap_or_default()
            .chunks_exact(2)
            .take(count)
            .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
            .collect()
    }

    pub(in crate::mapi) fn named_property_create(&self) -> bool {
        self.payload.first().is_some_and(|flags| *flags == 0x02)
    }

    pub(in crate::mapi) fn named_property_names(&self) -> Result<Vec<MapiNamedProperty>> {
        let Some(count_bytes) = self.payload.get(1..3) else {
            return Ok(Vec::new());
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        let mut cursor = Cursor::new(
            self.payload
                .get(3..)
                .ok_or_else(|| anyhow!("missing named property payload"))?,
        );
        let mut properties = Vec::with_capacity(count);
        for _ in 0..count {
            properties.push(parse_named_property(&mut cursor)?);
        }
        Ok(properties)
    }

    pub(in crate::mapi) fn named_property_query_guid(&self) -> Option<[u8; 16]> {
        if self.payload.get(1).copied().unwrap_or_default() == 0 {
            return None;
        }
        self.payload.get(2..18)?.try_into().ok()
    }

    pub(in crate::mapi) fn property_values(&self) -> Result<Vec<(u32, MapiValue)>> {
        let Some(size_bytes) = self.payload.get(..2) else {
            return Ok(Vec::new());
        };
        let property_value_size = u16::from_le_bytes([size_bytes[0], size_bytes[1]]) as usize;
        if property_value_size < 2 {
            return Err(anyhow!("invalid property value size"));
        }
        let Some(count_bytes) = self.payload.get(2..4) else {
            return Err(anyhow!("missing property value count"));
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        let value_bytes = self
            .payload
            .get(4..4 + property_value_size - 2)
            .ok_or_else(|| anyhow!("truncated property values"))?;
        let mut cursor = Cursor::new(value_bytes);
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(parse_tagged_property(&mut cursor)?);
        }
        Ok(values)
    }
}
impl TypedRopRequest {
    pub(in crate::mapi) fn rop_id(&self) -> u8 {
        match self {
            Self::Release(request) => request.rop_id,
            Self::OpenFolder(_) => 0x02,
            Self::OpenMessage(_) => 0x03,
            Self::OpenTable(request) => request.rop_id,
            Self::CreateMessage(_) => 0x06,
            Self::SaveChangesMessage(_) => 0x0C,
            Self::OpenEmbeddedMessage(_) => 0x46,
            Self::SetColumns(_) => 0x12,
            Self::Restrict(request) => request.rop_id,
            Self::QueryRows(_) => 0x15,
            Self::Logon(_) => 0xFE,
            Self::SupportedRaw(request) => request.rop_id,
            Self::Unsupported(request) => request.rop_id,
        }
    }

    pub(in crate::mapi) fn unsupported_is_terminal(&self) -> bool {
        matches!(self, Self::Unsupported(_))
    }
}

impl RopRequest {
    pub(in crate::mapi) fn typed(&self) -> TypedRopRequest {
        match RopId::from_u8(self.rop_id) {
            Some(RopId::Release) => TypedRopRequest::Release(RopInputOnlyRequest {
                rop_id: self.rop_id,
                input_handle_index: self.input_handle_index.unwrap_or(0),
            }),
            Some(RopId::OpenFolder) => TypedRopRequest::OpenFolder(RopOpenFolderRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                output_handle_index: self.output_handle_index.unwrap_or(0),
                folder_id: self.folder_id().unwrap_or(0),
                open_mode_flags: self.payload.get(8).copied().unwrap_or(0),
            }),
            Some(RopId::OpenMessage) => TypedRopRequest::OpenMessage(RopOpenMessageRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                output_handle_index: self.output_handle_index.unwrap_or(0),
                folder_id: self.folder_id().unwrap_or(0),
                open_mode_flags: self.payload.get(8).copied().unwrap_or(0),
                message_id: self.message_id().unwrap_or(0),
            }),
            Some(
                RopId::GetHierarchyTable | RopId::GetContentsTable | RopId::GetAttachmentTable,
            ) => TypedRopRequest::OpenTable(RopOpenTableRequest {
                rop_id: self.rop_id,
                input_handle_index: self.input_handle_index.unwrap_or(0),
                output_handle_index: self.output_handle_index.unwrap_or(0),
                table_flags: self.payload.first().copied().unwrap_or(0),
            }),
            Some(RopId::CreateMessage) => TypedRopRequest::CreateMessage(RopCreateMessageRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                output_handle_index: self.output_handle_index.unwrap_or(0),
                folder_id: self.folder_id().unwrap_or(0),
                associated_flag: self.payload.get(8).copied().unwrap_or(0),
            }),
            Some(RopId::SaveChangesMessage) => {
                TypedRopRequest::SaveChangesMessage(RopSaveChangesMessageRequest {
                    response_handle_index: self.output_handle_index.unwrap_or(0),
                    input_handle_index: self.input_handle_index.unwrap_or(0),
                    save_flags: self.payload.first().copied().unwrap_or(0),
                })
            }
            Some(RopId::OpenEmbeddedMessage) => {
                TypedRopRequest::OpenEmbeddedMessage(RopOpenEmbeddedMessageRequest {
                    input_handle_index: self.input_handle_index.unwrap_or(0),
                    output_handle_index: self.output_handle_index.unwrap_or(0),
                    code_page_id: self
                        .payload
                        .get(..2)
                        .and_then(|bytes| bytes.try_into().ok())
                        .map(u16::from_le_bytes)
                        .unwrap_or(0),
                    open_mode_flags: self.payload.get(2).copied().unwrap_or(0),
                })
            }
            Some(RopId::SetColumns) => TypedRopRequest::SetColumns(RopSetColumnsRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                flags: self.payload.first().copied().unwrap_or(0),
                property_tags: self.property_tags(),
            }),
            Some(RopId::Restrict | RopId::FindRow) => {
                let size = self
                    .payload
                    .get(1..3)
                    .and_then(|bytes| bytes.try_into().ok())
                    .map(u16::from_le_bytes)
                    .map(usize::from)
                    .unwrap_or(0);
                TypedRopRequest::Restrict(RopRestrictionRequest {
                    rop_id: self.rop_id,
                    input_handle_index: self.input_handle_index.unwrap_or(0),
                    flags: self.payload.first().copied().unwrap_or(0),
                    restriction: self.payload.get(3..3 + size).unwrap_or_default().to_vec(),
                })
            }
            Some(RopId::QueryRows) => TypedRopRequest::QueryRows(RopQueryRowsRequest {
                input_handle_index: self.input_handle_index.unwrap_or(0),
                flags: self.payload.first().copied().unwrap_or(0),
                forward_read: self.query_forward_read(),
                row_count: self.query_row_count().unwrap_or(0).min(u16::MAX as usize) as u16,
            }),
            Some(RopId::Logon) => {
                let essdn_size = self
                    .payload
                    .get(9..11)
                    .and_then(|bytes| bytes.try_into().ok())
                    .map(u16::from_le_bytes)
                    .map(usize::from)
                    .unwrap_or(0);
                TypedRopRequest::Logon(RopLogonRequest {
                    output_handle_index: self.output_handle_index.unwrap_or(0),
                    logon_flags: self.payload.first().copied().unwrap_or(0),
                    prefix: self.payload.get(1..9).unwrap_or_default().to_vec(),
                    essdn: self
                        .payload
                        .get(11..11 + essdn_size)
                        .unwrap_or_default()
                        .to_vec(),
                })
            }
            Some(rop_id) if rop_id.is_supported_by_dispatch() => {
                TypedRopRequest::SupportedRaw(RopSupportedRawRequest {
                    rop_id: self.rop_id,
                    input_handle_index: self.input_handle_index,
                    output_handle_index: self.output_handle_index,
                })
            }
            _ => TypedRopRequest::Unsupported(RopUnsupportedRequest {
                rop_id: self.rop_id,
                input_handle_index: self.input_handle_index,
                reserved: rop_id_is_reserved(self.rop_id),
            }),
        }
    }
}

pub(in crate::mapi) fn parse_tagged_property_value(cursor: &mut Cursor<'_>) -> Result<MapiValue> {
    parse_tagged_property(cursor).map(|(_property_tag, value)| value)
}

pub(in crate::mapi) fn parse_tagged_property(cursor: &mut Cursor<'_>) -> Result<(u32, MapiValue)> {
    let property_tag = cursor.read_u32()?;
    let value = parse_property_value_for_tag(cursor, property_tag)?;
    Ok((canonical_property_storage_tag(property_tag), value))
}

pub(in crate::mapi) fn parse_named_property(cursor: &mut Cursor<'_>) -> Result<MapiNamedProperty> {
    let kind = cursor.read_u8()?;
    let guid: [u8; 16] = cursor
        .read_bytes(16)?
        .try_into()
        .map_err(|_| anyhow!("invalid named property GUID"))?;
    let kind = match kind {
        0x00 => MapiNamedPropertyKind::Lid(cursor.read_u32()?),
        0x01 => {
            let name_size = cursor.read_u8()? as usize;
            let name_bytes = cursor.read_bytes(name_size)?;
            MapiNamedPropertyKind::Name(decode_utf16z_bytes(name_bytes))
        }
        _ => return Err(anyhow!("unsupported named property kind")),
    };
    Ok(MapiNamedProperty { guid, kind })
}

pub(in crate::mapi) fn decode_utf16z_bytes(bytes: &[u8]) -> String {
    String::from_utf16_lossy(
        &bytes
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .take_while(|unit| *unit != 0)
            .collect::<Vec<_>>(),
    )
}

pub(in crate::mapi) fn parse_property_value_for_tag(
    cursor: &mut Cursor<'_>,
    property_tag: u32,
) -> Result<MapiValue> {
    parse_mapi_property_value(cursor, property_tag)
}
