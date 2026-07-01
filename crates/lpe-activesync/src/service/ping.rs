use anyhow::{bail, Result};
use axum::response::Response;
use lpe_storage::{CanonicalChangeCategory, CanonicalChangeListener};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};
use uuid::Uuid;

use crate::{
    constants::{CALENDAR_CLASS, CONTACTS_CLASS, PING_SETTINGS_COLLECTION_ID},
    response::wbxml_response,
    snapshot::{diff_collection_states, mail_collection},
    store::ActiveSyncStore,
    types::{AuthenticatedPrincipal, CollectionDefinition, StoredSyncState},
    wbxml::{encode_wbxml, WbxmlNode},
};

use super::{decode_sync_state, ActiveSyncService};
const PING_SETTINGS_SYNC_KEY: &str = "current";
const PING_MIN_HEARTBEAT_SECONDS: u32 = 60;
const PING_MAX_HEARTBEAT_SECONDS: u32 = 3540;
const PING_MAX_FOLDERS: usize = 200;
const PING_FALLBACK_POLL_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PingSettings {
    heartbeat_interval: u32,
    folders: Vec<PingFolder>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PingFolder {
    id: String,
    class_name: String,
}

enum PingResolution {
    Ready(Vec<(CollectionDefinition, StoredSyncState)>),
    MissingParameters,
    FolderSyncRequired,
}

impl<S: ActiveSyncStore> ActiveSyncService<S> {
    pub(super) async fn handle_ping(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "Ping" {
            bail!("invalid Ping payload");
        }

        let cached_settings = self
            .load_ping_settings(principal.account_id, device_id)
            .await?;
        let Some(settings) = self.ping_settings_from_request(request, cached_settings.as_ref())
        else {
            return self.ping_status_response(protocol_version, "3", None, None, &[]);
        };

        if settings.heartbeat_interval < PING_MIN_HEARTBEAT_SECONDS {
            return self.ping_status_response(
                protocol_version,
                "5",
                Some(PING_MIN_HEARTBEAT_SECONDS),
                None,
                &[],
            );
        }
        if settings.heartbeat_interval > PING_MAX_HEARTBEAT_SECONDS {
            return self.ping_status_response(
                protocol_version,
                "5",
                Some(PING_MAX_HEARTBEAT_SECONDS),
                None,
                &[],
            );
        }
        if settings.folders.len() > PING_MAX_FOLDERS {
            return self.ping_status_response(
                protocol_version,
                "6",
                None,
                Some(PING_MAX_FOLDERS),
                &[],
            );
        }

        let mut change_listener = self
            .store
            .create_canonical_change_listener(principal.account_id)
            .await?;
        let collections = self.folder_collections(principal.account_id).await?;
        let monitored = match self
            .resolve_ping_collections(principal, device_id, &collections, &settings.folders)
            .await?
        {
            PingResolution::Ready(monitored) => monitored,
            PingResolution::MissingParameters => {
                return self.ping_status_response(protocol_version, "3", None, None, &[]);
            }
            PingResolution::FolderSyncRequired => {
                return self.ping_status_response(protocol_version, "7", None, None, &[]);
            }
        };
        self.store_ping_settings(principal.account_id, device_id, &settings)
            .await?;

        let categories = ping_change_categories(&monitored);
        let deadline = ping_deadline(settings.heartbeat_interval);
        let changed = loop {
            if self
                .ping_requires_folder_sync(principal.account_id, device_id, &monitored)
                .await?
            {
                return self.ping_status_response(protocol_version, "7", None, None, &[]);
            };

            let changed = self
                .changed_ping_collections(principal.account_id, &monitored)
                .await?;
            if !changed.is_empty() {
                break changed;
            }

            let now = Instant::now();
            if now >= deadline {
                break Vec::new();
            }
            self.wait_for_ping_change(&mut change_listener, &categories, deadline - now)
                .await?;
        };

        self.ping_status_response(
            protocol_version,
            if changed.is_empty() { "1" } else { "2" },
            None,
            None,
            &changed,
        )
    }

    async fn ping_requires_folder_sync(
        &self,
        account_id: Uuid,
        device_id: &str,
        monitored: &[(CollectionDefinition, StoredSyncState)],
    ) -> Result<bool> {
        let current_hierarchy_generation = self.current_hierarchy_generation(account_id).await?;
        if monitored.iter().any(|(_, previous_state)| {
            previous_state
                .hierarchy_generation
                .as_deref()
                .is_some_and(|generation| generation != current_hierarchy_generation)
        }) {
            return Ok(!self
                .device_hierarchy_is_current(account_id, device_id, &current_hierarchy_generation)
                .await?);
        }

        Ok(false)
    }

    async fn changed_ping_collections(
        &self,
        account_id: Uuid,
        monitored: &[(CollectionDefinition, StoredSyncState)],
    ) -> Result<Vec<String>> {
        let mut changed = Vec::new();
        for (collection, previous_state) in monitored {
            let current_state = self.collection_state(account_id, collection).await?;
            if diff_collection_states(&previous_state.collection_state, &current_state)
                .iter()
                .any(|change| change.kind == "Add")
            {
                changed.push(collection.id.clone());
            }
        }
        Ok(changed)
    }

    async fn wait_for_ping_change(
        &self,
        change_listener: &mut Option<CanonicalChangeListener>,
        categories: &[CanonicalChangeCategory],
        remaining: Duration,
    ) -> Result<()> {
        if remaining.is_zero() {
            return Ok(());
        }

        if let Some(listener) = change_listener {
            if let Ok(result) = timeout(remaining, listener.wait_for_change(categories)).await {
                result?;
            }
            return Ok(());
        }

        sleep(remaining.min(PING_FALLBACK_POLL_INTERVAL)).await;
        Ok(())
    }

    fn ping_status_response(
        &self,
        protocol_version: &str,
        status: &str,
        heartbeat_interval: Option<u32>,
        max_folders: Option<usize>,
        changed: &[String],
    ) -> Result<Response> {
        let mut response = WbxmlNode::new(13, "Ping");
        response.push(WbxmlNode::with_text(13, "Status", status));
        if let Some(heartbeat_interval) = heartbeat_interval {
            response.push(WbxmlNode::with_text(
                13,
                "HeartbeatInterval",
                heartbeat_interval.to_string(),
            ));
        }
        if let Some(max_folders) = max_folders {
            response.push(WbxmlNode::with_text(
                13,
                "MaxFolders",
                max_folders.to_string(),
            ));
        }
        if !changed.is_empty() {
            let mut folders_node = WbxmlNode::new(13, "Folders");
            for collection_id in changed {
                folders_node.push(WbxmlNode::with_text(13, "Folder", collection_id));
            }
            response.push(folders_node);
        }

        wbxml_response(protocol_version, encode_wbxml(&response))
    }

    fn ping_settings_from_request(
        &self,
        request: &WbxmlNode,
        cached: Option<&PingSettings>,
    ) -> Option<PingSettings> {
        let heartbeat_interval = match request.child("HeartbeatInterval") {
            Some(node) => node.text_value().trim().parse::<u32>().ok()?,
            None => cached?.heartbeat_interval,
        };

        let folders = match request.child("Folders") {
            Some(folders) => {
                let mut parsed = Vec::new();
                for folder in folders.children_named("Folder") {
                    let id = folder
                        .child("Id")
                        .map(|node| node.text_value().trim().to_string())
                        .filter(|value| !value.is_empty())?;
                    let class_name = folder
                        .child("Class")
                        .map(|node| node.text_value().trim().to_string())
                        .filter(|value| !value.is_empty())?;
                    parsed.push(PingFolder { id, class_name });
                }
                if parsed.is_empty() {
                    return None;
                }
                parsed
            }
            None => cached?.folders.clone(),
        };

        Some(PingSettings {
            heartbeat_interval,
            folders,
        })
    }

    async fn load_ping_settings(
        &self,
        account_id: Uuid,
        device_id: &str,
    ) -> Result<Option<PingSettings>> {
        self.store
            .fetch_latest_activesync_sync_state(account_id, device_id, PING_SETTINGS_COLLECTION_ID)
            .await?
            .map(|state| serde_json::from_str(&state.snapshot_json).map_err(Into::into))
            .transpose()
    }

    async fn store_ping_settings(
        &self,
        account_id: Uuid,
        device_id: &str,
        settings: &PingSettings,
    ) -> Result<()> {
        self.store
            .store_activesync_sync_state(
                account_id,
                device_id,
                PING_SETTINGS_COLLECTION_ID,
                PING_SETTINGS_SYNC_KEY,
                serde_json::to_string(settings)?,
            )
            .await
    }

    async fn resolve_ping_collections(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        collections: &[CollectionDefinition],
        folders: &[PingFolder],
    ) -> Result<PingResolution> {
        let mut monitored = Vec::new();
        for folder in folders {
            let Some(collection) = collections.iter().find(|entry| entry.id == folder.id) else {
                return Ok(PingResolution::FolderSyncRequired);
            };
            if collection.class_name != folder.class_name {
                return Ok(PingResolution::FolderSyncRequired);
            }
            let Some(state) = self
                .store
                .fetch_latest_activesync_sync_state(principal.account_id, device_id, &collection.id)
                .await?
            else {
                return Ok(PingResolution::MissingParameters);
            };
            monitored.push((collection.clone(), decode_sync_state(&state.snapshot_json)?));
        }

        Ok(PingResolution::Ready(monitored))
    }
}

fn ping_change_categories(
    monitored: &[(CollectionDefinition, StoredSyncState)],
) -> Vec<CanonicalChangeCategory> {
    let mut categories = Vec::new();
    for (collection, _) in monitored {
        let category = if mail_collection(collection) {
            Some(CanonicalChangeCategory::Mail)
        } else if collection.class_name == CONTACTS_CLASS {
            Some(CanonicalChangeCategory::Contacts)
        } else if collection.class_name == CALENDAR_CLASS {
            Some(CanonicalChangeCategory::Calendar)
        } else {
            None
        };
        if let Some(category) = category {
            if !categories.contains(&category) {
                categories.push(category);
            }
        }
    }
    categories
}

fn ping_deadline(heartbeat_interval: u32) -> Instant {
    Instant::now() + ping_heartbeat_duration(heartbeat_interval)
}

#[cfg(not(test))]
fn ping_heartbeat_duration(heartbeat_interval: u32) -> Duration {
    Duration::from_secs(u64::from(heartbeat_interval))
}

#[cfg(test)]
fn ping_heartbeat_duration(heartbeat_interval: u32) -> Duration {
    Duration::from_millis(u64::from(heartbeat_interval))
}
