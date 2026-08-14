use super::*;

pub(super) const PID_TAG_ADDITIONAL_REN_ENTRY_IDS: u32 = 0x36D8_1102;

pub(super) const OWNER_INBOX_SPECIAL_FOLDER_ENTRY_IDS: [(u32, u64); 7] = [
    (0x36D0_0102, crate::mapi::identity::CALENDAR_FOLDER_ID),
    (0x36D1_0102, crate::mapi::identity::CONTACTS_FOLDER_ID),
    (0x36D2_0102, crate::mapi::identity::JOURNAL_FOLDER_ID),
    (0x36D3_0102, crate::mapi::identity::NOTES_FOLDER_ID),
    (0x36D4_0102, crate::mapi::identity::TASKS_FOLDER_ID),
    (0x36D5_0102, crate::mapi::identity::REMINDERS_FOLDER_ID),
    (0x36D7_0102, crate::mapi::identity::DRAFTS_FOLDER_ID),
];

// [MS-OXCFXICS] sections 2.2.4.1 and 2.2.4.1.1 serialize a
// PtypMultipleBinary as a 32-bit value count followed by a 32-bit length and
// payload for each value. This is deliberately distinct from the ROP property
// encoding persisted in the folder profile, whose individual lengths are u16.
pub(super) fn write_multi_binary_property(output: &mut Vec<u8>, tag: u32, values: &[Vec<u8>]) {
    write_u32(output, tag);
    write_u32(output, values.len() as u32);
    for value in values {
        write_u32(output, value.len() as u32);
        output.extend_from_slice(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_binary_fast_transfer_uses_u32_counts_and_lengths() {
        let values = vec![vec![0x11, 0x22], vec![0x33, 0x44, 0x55]];
        let mut output = Vec::new();

        write_multi_binary_property(&mut output, PID_TAG_ADDITIONAL_REN_ENTRY_IDS, &values);

        assert_eq!(
            output,
            vec![
                0x02, 0x11, 0xD8, 0x36, // property tag
                0x02, 0x00, 0x00, 0x00, // value count
                0x02, 0x00, 0x00, 0x00, 0x11, 0x22, // first value
                0x03, 0x00, 0x00, 0x00, 0x33, 0x44, 0x55, // second value
            ]
        );
    }
}
