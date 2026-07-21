use super::RopRequest;
use crate::mapi::wire::{MapiSyncType, RopId};

impl RopRequest {
    pub(in crate::mapi) fn sync_type(&self) -> u8 {
        self.payload.first().copied().unwrap_or(0)
    }

    pub(in crate::mapi) fn collector_sync_type(&self) -> u8 {
        // [MS-OXCFXICS] section 2.2.3.2.4.1.1: unlike
        // RopSynchronizationConfigure, OpenCollector carries the Boolean
        // IsContentsCollector rather than a SynchronizationType value.
        if self.payload.first().copied().unwrap_or(0) == 0 {
            MapiSyncType::Hierarchy.as_u8()
        } else {
            MapiSyncType::Contents.as_u8()
        }
    }

    pub(in crate::mapi) fn sync_send_options(&self) -> u8 {
        self.payload.get(1).copied().unwrap_or(0)
    }

    pub(in crate::mapi) fn fast_transfer_source_send_options(&self) -> Option<u8> {
        match RopId::from_u8(self.rop_id) {
            Some(RopId::FastTransferSourceCopyFolder) => self.payload.get(1).copied(),
            Some(RopId::FastTransferSourceCopyTo) => self.payload.get(5).copied(),
            Some(RopId::FastTransferSourceCopyProperties) => self.payload.get(2).copied(),
            _ => None,
        }
    }

    pub(in crate::mapi) fn fast_transfer_source_level(&self) -> Option<u8> {
        match RopId::from_u8(self.rop_id) {
            Some(RopId::FastTransferSourceCopyTo | RopId::FastTransferSourceCopyProperties) => {
                self.payload.first().copied()
            }
            _ => None,
        }
    }

    pub(in crate::mapi) fn fast_transfer_source_property_tags(&self) -> Vec<u32> {
        let (count_offset, tags_offset) = match RopId::from_u8(self.rop_id) {
            Some(RopId::FastTransferSourceCopyTo) => (6, 8),
            Some(RopId::FastTransferSourceCopyProperties) => (3, 5),
            _ => return Vec::new(),
        };
        let Some(count_bytes) = self.payload.get(count_offset..count_offset + 2) else {
            return Vec::new();
        };
        let count = u16::from_le_bytes([count_bytes[0], count_bytes[1]]) as usize;
        self.payload
            .get(tags_offset..)
            .unwrap_or_default()
            .chunks_exact(4)
            .take(count)
            .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect()
    }
}
