use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ActiveSyncQuery {
    #[serde(rename = "Cmd")]
    pub(crate) cmd: Option<String>,
    #[serde(rename = "User")]
    pub(crate) user: Option<String>,
    #[serde(rename = "DeviceId")]
    pub(crate) device_id: Option<String>,
    #[serde(rename = "DeviceType")]
    pub(crate) _device_type: Option<String>,
}

#[derive(Debug, Default)]
pub(crate) struct ParsedActiveSyncQuery {
    pub(crate) query: ActiveSyncQuery,
    pub(crate) protocol_version: Option<String>,
    pub(crate) _collection_id: Option<String>,
    pub(crate) _item_id: Option<String>,
    pub(crate) _attachment_name: Option<String>,
    pub(crate) _long_id: Option<String>,
    pub(crate) _occurrence: Option<String>,
    pub(crate) _policy_key: Option<u32>,
    pub(crate) _options: Option<u8>,
}

impl ParsedActiveSyncQuery {
    pub(crate) fn from_raw_query(raw_query: Option<&str>) -> Result<Self> {
        let Some(raw_query) = raw_query.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(Self::default());
        };
        if looks_like_plain_query(raw_query) {
            parse_plain_query(raw_query)
        } else {
            parse_base64_query(raw_query)
        }
    }
}

fn looks_like_plain_query(raw_query: &str) -> bool {
    raw_query
        .split('&')
        .filter_map(|part| part.split_once('='))
        .any(|(key, _)| {
            matches!(
                key,
                "Cmd"
                    | "User"
                    | "DeviceId"
                    | "DeviceType"
                    | "AttachmentName"
                    | "CollectionId"
                    | "ItemId"
                    | "LongId"
                    | "Occurrence"
                    | "SaveInSent"
            )
        })
}

fn parse_plain_query(raw_query: &str) -> Result<ParsedActiveSyncQuery> {
    let mut parsed = ParsedActiveSyncQuery::default();
    for part in raw_query.split('&') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        let value = decode_query_component(value, true)?;
        match key {
            "Cmd" => parsed.query.cmd = Some(value),
            "User" => parsed.query.user = Some(value),
            "DeviceId" => parsed.query.device_id = Some(value),
            "DeviceType" => parsed.query._device_type = Some(value),
            "AttachmentName" => parsed._attachment_name = Some(value),
            "CollectionId" => parsed._collection_id = Some(value),
            "ItemId" => parsed._item_id = Some(value),
            "LongId" => parsed._long_id = Some(value),
            "Occurrence" => parsed._occurrence = Some(value),
            "SaveInSent" => parsed._options = save_in_sent_options(&value),
            _ => {}
        }
    }
    Ok(parsed)
}

fn parse_base64_query(raw_query: &str) -> Result<ParsedActiveSyncQuery> {
    let encoded = decode_query_component(raw_query, false)?;
    let bytes = BASE64
        .decode(encoded.as_bytes())
        .map_err(|_| anyhow!("malformed ActiveSync base64 query"))?;
    let mut cursor = ByteCursor::new(&bytes);
    let protocol_version = decode_protocol_version(cursor.take_u8()?)?;
    let command = decode_command(cursor.take_u8()?)?.to_string();
    cursor.take_exact(2)?;
    let device_id_len = cursor.take_u8()? as usize;
    let device_id = cursor.take_string(device_id_len)?;
    if device_id.is_empty() {
        bail!("malformed ActiveSync base64 query");
    }
    let policy_key_len = cursor.take_u8()?;
    let policy_key = match policy_key_len {
        0 => None,
        4 => Some(u32::from_le_bytes(cursor.take_array()?)),
        _ => bail!("malformed ActiveSync base64 query"),
    };
    let device_type_len = cursor.take_u8()? as usize;
    let device_type = cursor.take_string(device_type_len)?;
    let mut parsed = ParsedActiveSyncQuery {
        query: ActiveSyncQuery {
            cmd: Some(command),
            user: None,
            device_id: Some(device_id),
            _device_type: Some(device_type),
        },
        protocol_version: Some(protocol_version.to_string()),
        _collection_id: None,
        _item_id: None,
        _attachment_name: None,
        _long_id: None,
        _occurrence: None,
        _policy_key: policy_key,
        _options: None,
    };
    while cursor.has_remaining() {
        let tag = cursor.take_u8()?;
        let len = cursor.take_u8()? as usize;
        let value = cursor.take_exact(len)?;
        match tag {
            0 => parsed._attachment_name = Some(decode_parameter_value(value)?),
            1 => parsed._collection_id = Some(decode_parameter_value(value)?),
            3 => parsed._item_id = Some(decode_parameter_value(value)?),
            4 => parsed._long_id = Some(decode_parameter_value(value)?),
            6 => parsed._occurrence = Some(decode_parameter_value(value)?),
            7 => {
                parsed._options = value.first().copied();
                if value.len() > 1 {
                    bail!("malformed ActiveSync base64 query");
                }
            }
            8 => parsed.query.user = Some(decode_parameter_value(value)?),
            _ => {}
        }
    }
    Ok(parsed)
}

fn decode_parameter_value(value: &[u8]) -> Result<String> {
    String::from_utf8(value.to_vec()).map_err(|_| anyhow!("malformed ActiveSync base64 query"))
}

fn decode_protocol_version(value: u8) -> Result<&'static str> {
    match value {
        121 => Ok("12.1"),
        140 => Ok("14.0"),
        141 => Ok("14.1"),
        160 => Ok("16.0"),
        161 => Ok("16.1"),
        _ => bail!("unsupported ActiveSync protocol version"),
    }
}

fn decode_command(value: u8) -> Result<&'static str> {
    match value {
        0 => Ok("Sync"),
        1 => Ok("SendMail"),
        2 => Ok("SmartForward"),
        3 => Ok("SmartReply"),
        4 => Ok("GetAttachment"),
        9 => Ok("FolderSync"),
        10 => Ok("FolderCreate"),
        11 => Ok("FolderDelete"),
        12 => Ok("FolderUpdate"),
        13 => Ok("MoveItems"),
        14 => Ok("GetItemEstimate"),
        15 => Ok("MeetingResponse"),
        16 => Ok("Search"),
        17 => Ok("Settings"),
        18 => Ok("Ping"),
        19 => Ok("ItemOperations"),
        20 => Ok("Provision"),
        21 => Ok("ResolveRecipients"),
        22 => Ok("ValidateCert"),
        23 => Ok("Find"),
        _ => bail!("unsupported ActiveSync command code"),
    }
}

fn decode_query_component(value: &str, plus_as_space: bool) -> Result<String> {
    let mut decoded = Vec::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                if index + 2 >= bytes.len() {
                    bail!("malformed ActiveSync query");
                }
                let high = hex_value(bytes[index + 1])?;
                let low = hex_value(bytes[index + 2])?;
                decoded.push((high << 4) | low);
                index += 3;
            }
            b'+' if plus_as_space => {
                decoded.push(b' ');
                index += 1;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).map_err(|_| anyhow!("malformed ActiveSync query"))
}

fn hex_value(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("malformed ActiveSync query"),
    }
}

fn save_in_sent_options(value: &str) -> Option<u8> {
    match value {
        "T" | "t" => Some(0x01),
        "F" | "f" => Some(0x00),
        _ => None,
    }
}

struct ByteCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> ByteCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn has_remaining(&self) -> bool {
        self.position < self.bytes.len()
    }

    fn take_u8(&mut self) -> Result<u8> {
        self.take_exact(1).map(|bytes| bytes[0])
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take_exact(N)?
            .try_into()
            .map_err(|_| anyhow!("malformed ActiveSync base64 query"))
    }

    fn take_string(&mut self, len: usize) -> Result<String> {
        decode_parameter_value(self.take_exact(len)?)
    }

    fn take_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.position + len > self.bytes.len() {
            bail!("malformed ActiveSync base64 query");
        }
        let start = self.position;
        self.position += len;
        Ok(&self.bytes[start..self.position])
    }
}

pub(crate) type AuthenticatedPrincipal = lpe_mail_auth::AccountPrincipal;

#[derive(Debug, Clone)]
pub(crate) struct CollectionDefinition {
    pub(crate) id: String,
    pub(crate) parent_id: Option<String>,
    pub(crate) account_id: Uuid,
    pub(crate) class_name: String,
    pub(crate) display_name: String,
    pub(crate) folder_type: String,
    pub(crate) mailbox_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub(crate) struct SnapshotEntry {
    pub(crate) server_id: String,
    pub(crate) fingerprint: String,
    pub(crate) data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotChange {
    pub(crate) kind: String,
    pub(crate) server_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CollectionStateEntry {
    pub(crate) server_id: String,
    pub(crate) fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct StoredSyncState {
    #[serde(default)]
    pub(crate) hierarchy_generation: Option<String>,
    pub(crate) collection_state: Vec<CollectionStateEntry>,
    pub(crate) pending_changes: Vec<SnapshotChange>,
    pub(crate) next_offset: usize,
}
