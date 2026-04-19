use anyhow::{anyhow, bail, Result};
use axum::{http::HeaderMap, response::Response};
use lpe_storage::{AuditEntryInput, ClientContact, ClientEvent, JmapEmail, SubmitMessageInput};
use serde_json::json;
use uuid::Uuid;

use crate::{
    auth::{
        basic_credentials, bearer_token, normalize_login_name, protocol_version, verify_password,
    },
    constants::{
        CALENDAR_CLASS, CONTACTS_CLASS, FOLDER_SYNC_COLLECTION_ID, MAIL_CLASS, ROOT_FOLDER_ID,
    },
    message::{draft_input_from_application_data, merged_draft_input, parse_mime_message},
    response::{empty_response, is_message_rfc822, policy_key, sync_status_node, wbxml_response},
    snapshot::{
        collection_entries, collection_window_size, diff_snapshots, drafts_collection,
        mail_collection, parse_collection_mailbox_id, require_collection_id,
        require_sync_collections, snapshot_map, snapshot_to_value,
    },
    store::ActiveSyncStore,
    types::{ActiveSyncQuery, AuthenticatedPrincipal, CollectionDefinition, SnapshotEntry},
    wbxml::{decode_wbxml, encode_wbxml, WbxmlNode},
};

#[derive(Clone)]
pub struct ActiveSyncService<S> {
    store: S,
}

impl<S> ActiveSyncService<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

impl<S: ActiveSyncStore> ActiveSyncService<S> {
    pub(crate) async fn handle_request(
        &self,
        query: ActiveSyncQuery,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let command = query
            .cmd
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("missing ActiveSync command"))?;
        let device_id = query
            .device_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("missing DeviceId"))?;
        let protocol_version = protocol_version(headers);
        let principal = self.authenticate(query.user.as_deref(), headers).await?;

        match command {
            "Provision" => {
                let request = decode_wbxml(body)?;
                self.handle_provision(&principal, device_id, &protocol_version, &request)
                    .await
            }
            "FolderSync" => {
                let request = decode_wbxml(body)?;
                self.handle_folder_sync(&principal, device_id, &protocol_version, &request)
                    .await
            }
            "Sync" => {
                let request = decode_wbxml(body)?;
                self.handle_sync(&principal, device_id, &protocol_version, &request)
                    .await
            }
            "SendMail" => {
                self.handle_send_mail(&principal, &protocol_version, headers, body)
                    .await
            }
            other => bail!("unsupported ActiveSync command: {other}"),
        }
    }

    async fn authenticate(
        &self,
        hinted_user: Option<&str>,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedPrincipal> {
        if let Some(token) = bearer_token(headers) {
            if let Some(account) = self.store.fetch_account_session(&token).await? {
                return Ok(AuthenticatedPrincipal {
                    account_id: account.account_id,
                    email: account.email,
                    display_name: account.display_name,
                });
            }
        }

        if let Some((username, password)) = basic_credentials(headers)? {
            let login = self
                .store
                .fetch_account_login(&normalize_login_name(&username, hinted_user))
                .await?
                .ok_or_else(|| anyhow!("invalid credentials"))?;
            if login.status != "active" || !verify_password(&login.password_hash, &password) {
                bail!("invalid credentials");
            }
            return Ok(AuthenticatedPrincipal {
                account_id: login.account_id,
                email: login.email,
                display_name: login.display_name,
            });
        }

        bail!("missing account authentication");
    }

    async fn handle_provision(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "Provision" {
            bail!("invalid Provision payload");
        }

        let requested_key = request
            .child("Policies")
            .and_then(|policies| policies.child("Policy"))
            .and_then(|policy| policy.child("PolicyKey"))
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let client_status = request
            .child("Policies")
            .and_then(|policies| policies.child("Policy"))
            .and_then(|policy| policy.child("Status"))
            .map(|node| node.text_value().trim().to_string());

        let current_policy_key = policy_key(principal.account_id, device_id);
        let mut response = WbxmlNode::new(14, "Provision");

        if request
            .child("DeviceInformation")
            .or_else(|| {
                request
                    .children
                    .iter()
                    .find(|child| child.page == 18 && child.name == "DeviceInformation")
            })
            .is_some()
        {
            let mut device_information = WbxmlNode::new(18, "DeviceInformation");
            device_information.push(WbxmlNode::with_text(18, "Status", "1"));
            response.push(device_information);
        }

        response.push(WbxmlNode::with_text(14, "Status", "1"));
        let mut policies = WbxmlNode::new(14, "Policies");
        let mut policy = WbxmlNode::new(14, "Policy");
        policy.push(WbxmlNode::with_text(
            14,
            "PolicyType",
            "MS-EAS-Provisioning-WBXML",
        ));
        policy.push(WbxmlNode::with_text(14, "Status", "1"));
        policy.push(WbxmlNode::with_text(14, "PolicyKey", &current_policy_key));

        if client_status.as_deref() != Some("1") || requested_key != current_policy_key {
            let mut data = WbxmlNode::new(14, "Data");
            let mut document = WbxmlNode::new(14, "EASProvisionDoc");
            for (name, value) in [
                ("DevicePasswordEnabled", "0"),
                ("AlphanumericDevicePasswordRequired", "0"),
                ("AttachmentsEnabled", "1"),
                ("MinDevicePasswordLength", "0"),
                ("AllowSimpleDevicePassword", "1"),
                ("AllowStorageCard", "1"),
                ("AllowCamera", "1"),
                ("RequireDeviceEncryption", "0"),
                ("AllowWiFi", "1"),
                ("AllowTextMessaging", "1"),
                ("AllowPOPIMAPEmail", "1"),
                ("AllowBrowser", "1"),
                ("AllowConsumerEmail", "1"),
            ] {
                document.push(WbxmlNode::with_text(14, name, value));
            }
            data.push(document);
            policy.push(data);
        }

        policies.push(policy);
        response.push(policies);
        wbxml_response(protocol_version, encode_wbxml(&response))
    }

    async fn handle_folder_sync(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "FolderSync" {
            bail!("invalid FolderSync payload");
        }

        let requested_key = request
            .child("SyncKey")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let collections = self.folder_collections(principal.account_id).await?;
        let snapshot = snapshot_to_value(
            &collections
                .iter()
                .map(|collection| SnapshotEntry {
                    server_id: collection.id.clone(),
                    fingerprint: format!(
                        "{}:{}:{}",
                        collection.class_name, collection.display_name, collection.folder_type
                    ),
                    data: json!({
                        "page": 7,
                        "name": "Folder",
                        "children": [],
                    }),
                })
                .collect::<Vec<_>>(),
        );

        let old_snapshot = if requested_key == "0" || requested_key.is_empty() {
            None
        } else {
            self.store
                .fetch_activesync_sync_state(
                    principal.account_id,
                    device_id,
                    FOLDER_SYNC_COLLECTION_ID,
                    &requested_key,
                )
                .await?
                .map(|state| state.snapshot)
        };

        if !requested_key.is_empty() && requested_key != "0" && old_snapshot.is_none() {
            let mut response = WbxmlNode::new(7, "FolderSync");
            response.push(WbxmlNode::with_text(7, "Status", "9"));
            return wbxml_response(protocol_version, encode_wbxml(&response));
        }

        let new_key = Uuid::new_v4().to_string();
        self.store
            .store_activesync_sync_state(
                principal.account_id,
                device_id,
                FOLDER_SYNC_COLLECTION_ID,
                &new_key,
                snapshot.clone(),
            )
            .await?;

        let mut response = WbxmlNode::new(7, "FolderSync");
        response.push(WbxmlNode::with_text(7, "Status", "1"));
        response.push(WbxmlNode::with_text(7, "SyncKey", new_key));

        let changes = diff_snapshots(old_snapshot.as_ref(), &snapshot);
        let mut changes_node = WbxmlNode::new(7, "Changes");
        changes_node.push(WbxmlNode::with_text(7, "Count", changes.len().to_string()));
        for change in changes {
            match change.kind.as_str() {
                "Add" => {
                    if let Some(collection) = collections
                        .iter()
                        .find(|collection| collection.id == change.server_id)
                    {
                        let mut node = WbxmlNode::new(7, "Add");
                        node.push(WbxmlNode::with_text(7, "ServerId", &collection.id));
                        node.push(WbxmlNode::with_text(7, "ParentId", ROOT_FOLDER_ID));
                        node.push(WbxmlNode::with_text(
                            7,
                            "DisplayName",
                            &collection.display_name,
                        ));
                        node.push(WbxmlNode::with_text(7, "Type", &collection.folder_type));
                        changes_node.push(node);
                    }
                }
                "Update" => {
                    if let Some(collection) = collections
                        .iter()
                        .find(|collection| collection.id == change.server_id)
                    {
                        let mut node = WbxmlNode::new(7, "Update");
                        node.push(WbxmlNode::with_text(7, "ServerId", &collection.id));
                        node.push(WbxmlNode::with_text(7, "ParentId", ROOT_FOLDER_ID));
                        node.push(WbxmlNode::with_text(
                            7,
                            "DisplayName",
                            &collection.display_name,
                        ));
                        node.push(WbxmlNode::with_text(7, "Type", &collection.folder_type));
                        changes_node.push(node);
                    }
                }
                "Delete" => {
                    let mut node = WbxmlNode::new(7, "Delete");
                    node.push(WbxmlNode::with_text(7, "ServerId", &change.server_id));
                    changes_node.push(node);
                }
                _ => {}
            }
        }
        response.push(changes_node);
        wbxml_response(protocol_version, encode_wbxml(&response))
    }

    async fn handle_sync(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        let collection_nodes = require_sync_collections(request)?;
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections_node = WbxmlNode::new(0, "Collections");

        for collection_node in &collection_nodes {
            let response_collection = self
                .sync_collection(principal, device_id, request, collection_node)
                .await?;
            collections_node.push(response_collection);
        }

        sync.push(collections_node);
        wbxml_response(protocol_version, encode_wbxml(&sync))
    }

    async fn sync_collection(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        request: &WbxmlNode,
        collection_node: &WbxmlNode,
    ) -> Result<WbxmlNode> {
        let collection_id = require_collection_id(collection_node)?;
        let sync_key = collection_node
            .child("SyncKey")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let window_size = collection_window_size(request, collection_node);

        let Some(collection) = self
            .resolve_collection(principal.account_id, &collection_id)
            .await?
        else {
            return Ok(sync_status_node(&collection_id, "9"));
        };

        let previous_snapshot = if sync_key == "0" || sync_key.is_empty() {
            None
        } else {
            self.store
                .fetch_activesync_sync_state(
                    principal.account_id,
                    device_id,
                    &collection.id,
                    &sync_key,
                )
                .await?
                .map(|state| state.snapshot)
        };

        if !sync_key.is_empty() && sync_key != "0" && previous_snapshot.is_none() {
            return Ok(sync_status_node(&collection.id, "9"));
        }

        let client_responses = if drafts_collection(&collection) {
            self.apply_draft_sync_commands(principal, collection_node)
                .await?
        } else {
            Vec::new()
        };

        let final_snapshot = self
            .collection_snapshot(principal.account_id, &collection, window_size)
            .await?;
        let next_key = Uuid::new_v4().to_string();
        self.store
            .store_activesync_sync_state(
                principal.account_id,
                device_id,
                &collection.id,
                &next_key,
                final_snapshot.clone(),
            )
            .await?;

        let changed_items = diff_snapshots(previous_snapshot.as_ref(), &final_snapshot);
        let current_items = snapshot_map(&final_snapshot);
        let mut response_collection = WbxmlNode::new(0, "Collection");
        response_collection.push(WbxmlNode::with_text(0, "Class", &collection.class_name));
        response_collection.push(WbxmlNode::with_text(0, "SyncKey", next_key));
        response_collection.push(WbxmlNode::with_text(0, "CollectionId", &collection.id));
        response_collection.push(WbxmlNode::with_text(0, "Status", "1"));

        if !client_responses.is_empty() {
            let mut responses = WbxmlNode::new(0, "Responses");
            for client_response in client_responses {
                responses.push(client_response);
            }
            response_collection.push(responses);
        }

        if !changed_items.is_empty() {
            let mut commands = WbxmlNode::new(0, "Commands");
            for change in changed_items {
                match change.kind.as_str() {
                    "Add" | "Update" => {
                        if let Some(item) = current_items.get(&change.server_id) {
                            let mut node = WbxmlNode::new(
                                0,
                                if change.kind == "Add" {
                                    "Add"
                                } else {
                                    "Change"
                                },
                            );
                            node.push(WbxmlNode::with_text(0, "ServerId", &change.server_id));
                            node.push(item.clone());
                            commands.push(node);
                        }
                    }
                    "Delete" => {
                        let mut node = WbxmlNode::new(0, "Delete");
                        node.push(WbxmlNode::with_text(0, "ServerId", &change.server_id));
                        commands.push(node);
                    }
                    _ => {}
                }
            }
            response_collection.push(commands);
        }

        Ok(response_collection)
    }

    async fn apply_draft_sync_commands(
        &self,
        principal: &AuthenticatedPrincipal,
        collection_node: &WbxmlNode,
    ) -> Result<Vec<WbxmlNode>> {
        let mut responses = Vec::new();
        let Some(commands) = collection_node.child("Commands") else {
            return Ok(responses);
        };

        for command in &commands.children {
            match command.name.as_str() {
                "Add" => {
                    let client_id = command
                        .child("ClientId")
                        .map(|node| node.text_value().trim().to_string())
                        .unwrap_or_else(|| Uuid::new_v4().to_string());
                    let application_data = command
                        .child("ApplicationData")
                        .ok_or_else(|| anyhow!("draft add command is missing ApplicationData"))?;
                    let input = draft_input_from_application_data(
                        principal,
                        None,
                        application_data,
                        "activesync-sync-add",
                    );
                    let saved = self
                        .store
                        .save_draft_message(
                            input,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "activesync-save-draft".to_string(),
                                subject: client_id.clone(),
                            },
                        )
                        .await?;
                    let mut add = WbxmlNode::new(0, "Add");
                    add.push(WbxmlNode::with_text(0, "ClientId", client_id));
                    add.push(WbxmlNode::with_text(
                        0,
                        "ServerId",
                        saved.message_id.to_string(),
                    ));
                    add.push(WbxmlNode::with_text(0, "Status", "1"));
                    responses.push(add);
                }
                "Change" => {
                    let server_id = command
                        .child("ServerId")
                        .map(|node| node.text_value().trim().to_string())
                        .ok_or_else(|| anyhow!("draft change command is missing ServerId"))?;
                    let draft_id = Uuid::parse_str(&server_id)?;
                    let existing = self
                        .store
                        .fetch_jmap_draft(principal.account_id, draft_id)
                        .await?
                        .ok_or_else(|| anyhow!("draft not found"))?;
                    let application_data = command.child("ApplicationData").ok_or_else(|| {
                        anyhow!("draft change command is missing ApplicationData")
                    })?;
                    let input =
                        merged_draft_input(principal, draft_id, &existing, application_data);
                    self.store
                        .save_draft_message(
                            input,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "activesync-update-draft".to_string(),
                                subject: server_id.clone(),
                            },
                        )
                        .await?;
                    let mut change = WbxmlNode::new(0, "Change");
                    change.push(WbxmlNode::with_text(0, "ServerId", server_id));
                    change.push(WbxmlNode::with_text(0, "Status", "1"));
                    responses.push(change);
                }
                "Delete" => {
                    let server_id = command
                        .child("ServerId")
                        .map(|node| node.text_value().trim().to_string())
                        .ok_or_else(|| anyhow!("draft delete command is missing ServerId"))?;
                    let draft_id = Uuid::parse_str(&server_id)?;
                    self.store
                        .delete_draft_message(
                            principal.account_id,
                            draft_id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "activesync-delete-draft".to_string(),
                                subject: server_id.clone(),
                            },
                        )
                        .await?;
                    let mut delete = WbxmlNode::new(0, "Delete");
                    delete.push(WbxmlNode::with_text(0, "ServerId", server_id));
                    delete.push(WbxmlNode::with_text(0, "Status", "1"));
                    responses.push(delete);
                }
                "Fetch" => {}
                _ => {}
            }
        }

        Ok(responses)
    }

    async fn handle_send_mail(
        &self,
        principal: &AuthenticatedPrincipal,
        protocol_version: &str,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let mime_payload = if is_message_rfc822(headers) {
            body.to_vec()
        } else {
            let request = decode_wbxml(body)?;
            if request.name != "SendMail" {
                bail!("invalid SendMail payload");
            }
            request
                .child("Mime")
                .map(|node| node.text_value().as_bytes().to_vec())
                .ok_or_else(|| anyhow!("SendMail is missing MIME content"))?
        };

        let parsed = parse_mime_message(&mime_payload)?;
        let from_display = parsed
            .from
            .as_ref()
            .and_then(|mailbox| mailbox.display_name.clone())
            .or_else(|| Some(principal.display_name.clone()));
        let from_address = parsed
            .from
            .map(|mailbox| mailbox.address)
            .unwrap_or_else(|| principal.email.clone());
        self.store
            .submit_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id: principal.account_id,
                    source: "activesync-sendmail".to_string(),
                    from_display,
                    from_address,
                    to: parsed.to,
                    cc: parsed.cc,
                    bcc: parsed.bcc,
                    subject: parsed.subject,
                    body_text: parsed.body_text,
                    body_html_sanitized: None,
                    internet_message_id: parsed.internet_message_id,
                    mime_blob_ref: Some(format!("activesync-mime:{}", Uuid::new_v4())),
                    size_octets: mime_payload.len() as i64,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "activesync-sendmail".to_string(),
                    subject: "native client message submission".to_string(),
                },
            )
            .await?;

        if is_message_rfc822(headers) {
            Ok(empty_response())
        } else {
            wbxml_response(protocol_version, Vec::new())
        }
    }

    async fn folder_collections(&self, account_id: Uuid) -> Result<Vec<CollectionDefinition>> {
        let mailboxes = self.store.fetch_jmap_mailboxes(account_id).await?;
        let mut collections = mailboxes
            .into_iter()
            .filter_map(|mailbox| match mailbox.role.as_str() {
                "inbox" => Some(CollectionDefinition {
                    id: mailbox.id.to_string(),
                    class_name: MAIL_CLASS.to_string(),
                    display_name: "Inbox".to_string(),
                    folder_type: "2".to_string(),
                    mailbox_id: Some(mailbox.id),
                }),
                "sent" => Some(CollectionDefinition {
                    id: mailbox.id.to_string(),
                    class_name: MAIL_CLASS.to_string(),
                    display_name: "Sent".to_string(),
                    folder_type: "5".to_string(),
                    mailbox_id: Some(mailbox.id),
                }),
                "drafts" => Some(CollectionDefinition {
                    id: mailbox.id.to_string(),
                    class_name: MAIL_CLASS.to_string(),
                    display_name: "Drafts".to_string(),
                    folder_type: "3".to_string(),
                    mailbox_id: Some(mailbox.id),
                }),
                _ => None,
            })
            .collect::<Vec<_>>();

        collections.push(CollectionDefinition {
            id: "contacts".to_string(),
            class_name: CONTACTS_CLASS.to_string(),
            display_name: "Contacts".to_string(),
            folder_type: "9".to_string(),
            mailbox_id: None,
        });
        collections.push(CollectionDefinition {
            id: "calendar".to_string(),
            class_name: CALENDAR_CLASS.to_string(),
            display_name: "Calendar".to_string(),
            folder_type: "8".to_string(),
            mailbox_id: None,
        });
        Ok(collections)
    }

    async fn resolve_collection(
        &self,
        account_id: Uuid,
        collection_id: &str,
    ) -> Result<Option<CollectionDefinition>> {
        Ok(self
            .folder_collections(account_id)
            .await?
            .into_iter()
            .find(|collection| collection.id == collection_id))
    }

    async fn collection_snapshot(
        &self,
        account_id: Uuid,
        collection: &CollectionDefinition,
        window_size: u64,
    ) -> Result<serde_json::Value> {
        let emails = if mail_collection(collection) {
            let mailbox_id = parse_collection_mailbox_id(collection)?;
            let ids = self
                .store
                .query_jmap_email_ids(account_id, Some(mailbox_id), 0, window_size)
                .await?
                .ids;
            self.store.fetch_jmap_emails(account_id, &ids).await?
        } else {
            Vec::<JmapEmail>::new()
        };
        let contacts = if collection.class_name == CONTACTS_CLASS {
            self.store.fetch_client_contacts(account_id).await?
        } else {
            Vec::<ClientContact>::new()
        };
        let events = if collection.class_name == CALENDAR_CLASS {
            self.store.fetch_client_events(account_id).await?
        } else {
            Vec::<ClientEvent>::new()
        };
        collection_entries(collection, emails, contacts, events)
    }
}
