use serde_json::Value;
use serde::{Deserialize, Serialize};
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

pub(crate) type AuthenticatedPrincipal = lpe_mail_auth::AccountPrincipal;

#[derive(Debug, Clone)]
pub(crate) struct CollectionDefinition {
    pub(crate) id: String,
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
    pub(crate) collection_state: Vec<CollectionStateEntry>,
    pub(crate) pending_changes: Vec<SnapshotChange>,
    pub(crate) next_offset: usize,
}
