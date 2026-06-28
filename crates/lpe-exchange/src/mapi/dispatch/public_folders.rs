const PUBLIC_FOLDER_PER_USER_STREAM_MAGIC: &[u8; 8] = b"LPEPFU1\0";

pub(super) fn public_folder_per_user_stream(
    states: &[lpe_storage::PublicFolderPerUserState],
) -> Vec<u8> {
    let mut states = states.to_vec();
    states.sort_by(|left, right| left.item_id.cmp(&right.item_id));
    let mut stream = Vec::new();
    stream.extend_from_slice(PUBLIC_FOLDER_PER_USER_STREAM_MAGIC);
    stream.extend_from_slice(&(states.len().min(u16::MAX as usize) as u16).to_le_bytes());
    for state in states.into_iter().take(u16::MAX as usize) {
        stream.extend_from_slice(state.item_id.as_bytes());
        stream.push(state.is_read as u8);
        stream.extend_from_slice(&state.last_seen_change.to_le_bytes());
    }
    stream
}

pub(super) fn public_folder_per_user_patches(
    data: &[u8],
) -> Option<Vec<lpe_storage::PublicFolderPerUserStatePatch>> {
    if data.is_empty() {
        return Some(Vec::new());
    }
    if data.len() < PUBLIC_FOLDER_PER_USER_STREAM_MAGIC.len() + 2
        || &data[..PUBLIC_FOLDER_PER_USER_STREAM_MAGIC.len()]
            != PUBLIC_FOLDER_PER_USER_STREAM_MAGIC.as_slice()
    {
        return None;
    }
    let mut offset = PUBLIC_FOLDER_PER_USER_STREAM_MAGIC.len();
    let count = u16::from_le_bytes(data.get(offset..offset + 2)?.try_into().ok()?) as usize;
    offset += 2;
    let mut patches = Vec::with_capacity(count);
    for _ in 0..count {
        let item_id = uuid::Uuid::from_slice(data.get(offset..offset + 16)?).ok()?;
        offset += 16;
        let is_read = *data.get(offset)? != 0;
        offset += 1;
        let last_seen_change = i64::from_le_bytes(data.get(offset..offset + 8)?.try_into().ok()?);
        offset += 8;
        patches.push(lpe_storage::PublicFolderPerUserStatePatch {
            item_id,
            is_read,
            last_seen_change: Some(last_seen_change),
            private_json: None,
        });
    }
    (offset == data.len()).then_some(patches)
}
