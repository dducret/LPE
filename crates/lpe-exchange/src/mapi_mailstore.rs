use lpe_storage::{JmapEmail, JmapMailbox};
use uuid::Uuid;

pub(crate) const STORE_REPLICA_GUID: [u8; 16] = [
    0x4c, 0x50, 0x45, 0x00, 0x45, 0x4d, 0x53, 0x4d, 0x44, 0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
];

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

pub(crate) fn canonical_folder_change_number(mailbox: &JmapMailbox) -> u64 {
    stable_hash64([
        mailbox.id.as_bytes().as_slice(),
        mailbox.role.as_bytes(),
        mailbox.name.as_bytes(),
        &mailbox.sort_order.to_le_bytes(),
        &mailbox.total_emails.to_le_bytes(),
        &mailbox.unread_emails.to_le_bytes(),
    ])
}

pub(crate) fn canonical_message_change_number(email: &JmapEmail) -> u64 {
    let mut hash = FNV_OFFSET;
    hash = hash_bytes(hash, email.id.as_bytes());
    hash = hash_bytes(hash, email.thread_id.as_bytes());
    hash = hash_bytes(hash, email.mailbox_id.as_bytes());
    hash = hash_bytes(hash, email.mailbox_role.as_bytes());
    hash = hash_bytes(hash, email.mailbox_name.as_bytes());
    hash = hash_bytes(hash, email.received_at.as_bytes());
    hash = hash_bytes(
        hash,
        email.sent_at.as_deref().unwrap_or_default().as_bytes(),
    );
    hash = hash_bytes(hash, email.from_address.as_bytes());
    hash = hash_bytes(
        hash,
        email.from_display.as_deref().unwrap_or_default().as_bytes(),
    );
    hash = hash_bytes(hash, email.subject.as_bytes());
    hash = hash_bytes(hash, email.preview.as_bytes());
    hash = hash_bytes(hash, email.body_text.as_bytes());
    hash = hash_bytes(
        hash,
        &[
            email.unread as u8,
            email.flagged as u8,
            email.has_attachments as u8,
        ],
    );
    hash = hash_bytes(hash, &email.size_octets.to_le_bytes());
    hash = hash_bytes(
        hash,
        email
            .internet_message_id
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    );
    for recipient in email.to.iter().chain(email.cc.iter()) {
        hash = hash_bytes(hash, recipient.address.as_bytes());
        hash = hash_bytes(
            hash,
            recipient
                .display_name
                .as_deref()
                .unwrap_or_default()
                .as_bytes(),
        );
    }
    hash.max(1)
}

pub(crate) fn source_key_for_uuid(id: &Uuid) -> Vec<u8> {
    let mut key = STORE_REPLICA_GUID.to_vec();
    key.extend_from_slice(&uuid_global_counter(id).to_le_bytes());
    key
}

pub(crate) fn source_key_for_store_id(store_id: u64) -> Vec<u8> {
    let mut key = STORE_REPLICA_GUID.to_vec();
    key.extend_from_slice(&store_id.to_le_bytes());
    key
}

pub(crate) fn change_key_for_change_number(change_number: u64) -> Vec<u8> {
    let mut key = STORE_REPLICA_GUID.to_vec();
    key.extend_from_slice(&change_number.max(1).to_le_bytes());
    key
}

pub(crate) fn predecessor_change_list(change_number: u64) -> Vec<u8> {
    let change_number = change_number.max(1);
    let mut list = Vec::with_capacity(16);
    list.extend_from_slice(&(change_number.saturating_sub(1)).to_le_bytes());
    list.extend_from_slice(&change_number.to_le_bytes());
    list
}

pub(crate) fn sync_state_token(mailboxes: &[JmapMailbox], emails: &[JmapEmail]) -> Vec<u8> {
    let max_change = mailboxes
        .iter()
        .map(canonical_folder_change_number)
        .chain(emails.iter().map(canonical_message_change_number))
        .max()
        .unwrap_or(1);
    let mut token = b"LPE-MAPI-SYNC-STATE\0".to_vec();
    token.extend_from_slice(&(mailboxes.len().min(u32::MAX as usize) as u32).to_le_bytes());
    token.extend_from_slice(&(emails.len().min(u32::MAX as usize) as u32).to_le_bytes());
    token.extend_from_slice(&max_change.to_le_bytes());
    token
}

pub(crate) fn sync_manifest_buffer(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Vec<u8> {
    let mut buffer = b"LPE-MAPI-SYNC\0".to_vec();
    buffer.extend_from_slice(&folder_id.to_le_bytes());
    buffer.extend_from_slice(&(mailboxes.len().min(u32::MAX as usize) as u32).to_le_bytes());
    buffer.extend_from_slice(&(emails.len().min(u32::MAX as usize) as u32).to_le_bytes());

    let mut folders = mailboxes.iter().collect::<Vec<_>>();
    folders.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
    for mailbox in folders {
        let change_number = canonical_folder_change_number(mailbox);
        let source_key = source_key_for_uuid(&mailbox.id);
        write_prefixed_bytes(&mut buffer, &source_key);
        buffer.extend_from_slice(&change_number.to_le_bytes());
    }

    let mut messages = emails.iter().collect::<Vec<_>>();
    messages.sort_by(|left, right| {
        left.received_at
            .cmp(&right.received_at)
            .then(left.subject.cmp(&right.subject))
            .then(left.id.cmp(&right.id))
    });
    for email in messages {
        let change_number = canonical_message_change_number(email);
        let source_key = source_key_for_uuid(&email.id);
        write_prefixed_bytes(&mut buffer, &source_key);
        buffer.extend_from_slice(&change_number.to_le_bytes());
        write_prefixed_bytes(&mut buffer, email.subject.as_bytes());
    }
    buffer
}

pub(crate) fn local_replica_id_range(
    account_id: Uuid,
    requested: u32,
    sequence: u64,
) -> (u64, u32) {
    let count = requested.clamp(1, 1_024);
    let seed = stable_hash64([account_id.as_bytes().as_slice(), &sequence.to_le_bytes()]);
    ((seed & 0x0000_FFFF_FFFF_FFFF).max(0x100), count)
}

fn stable_hash64<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> u64 {
    let mut hash = FNV_OFFSET;
    for part in parts {
        hash = hash_bytes(hash, part);
        hash = hash_bytes(hash, &[0]);
    }
    hash.max(1)
}

fn hash_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn write_prefixed_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) {
    buffer.extend_from_slice(&(bytes.len().min(u16::MAX as usize) as u16).to_le_bytes());
    buffer.extend_from_slice(&bytes[..bytes.len().min(u16::MAX as usize)]);
}

fn uuid_global_counter(id: &Uuid) -> u64 {
    let bytes = id.as_bytes();
    let value = u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]) & 0x0000_FFFF_FFFF_FFFF;
    value.max(0x100)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lpe_storage::JmapEmailAddress;

    #[test]
    fn message_change_number_excludes_bcc_recipients() {
        let mut email = test_email();
        let baseline = canonical_message_change_number(&email);
        email.bcc.push(JmapEmailAddress {
            address: "secret@example.test".to_string(),
            display_name: Some("Secret".to_string()),
        });

        assert_eq!(canonical_message_change_number(&email), baseline);

        email.cc.push(JmapEmailAddress {
            address: "visible@example.test".to_string(),
            display_name: None,
        });
        assert_ne!(canonical_message_change_number(&email), baseline);
    }

    #[test]
    fn source_and_change_keys_are_stable_replica_scoped_values() {
        let id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let source_key = source_key_for_uuid(&id);
        let change_key = change_key_for_change_number(42);

        assert!(source_key.starts_with(&STORE_REPLICA_GUID));
        assert!(change_key.starts_with(&STORE_REPLICA_GUID));
        assert_eq!(source_key, source_key_for_uuid(&id));
    }

    fn test_email() -> JmapEmail {
        JmapEmail {
            id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            thread_id: Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
            mailbox_id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
            mailbox_role: "inbox".to_string(),
            mailbox_name: "Inbox".to_string(),
            received_at: "2026-05-06T12:00:00Z".to_string(),
            sent_at: None,
            from_address: "alice@example.test".to_string(),
            from_display: Some("Alice".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: Uuid::nil(),
            to: vec![JmapEmailAddress {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Hello".to_string(),
            preview: "Hello".to_string(),
            body_text: "Hello body".to_string(),
            body_html_sanitized: None,
            unread: true,
            flagged: false,
            has_attachments: false,
            size_octets: 42,
            internet_message_id: Some("<message@example.test>".to_string()),
            mime_blob_ref: None,
            delivery_status: "stored".to_string(),
        }
    }
}
