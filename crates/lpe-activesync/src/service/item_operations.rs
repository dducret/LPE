use anyhow::{anyhow, Result};
use axum::response::Response;
use uuid::Uuid;

use crate::{
    protocol::{ActiveSyncStatus, BodyPreferenceType},
    response::wbxml_response,
    snapshot::email_application_data,
    store::ActiveSyncStore,
    types::AuthenticatedPrincipal,
    wbxml::{encode_wbxml, WbxmlNode},
};

use super::{command_status_response, fetch_body_preference, value_to_wbxml, ActiveSyncService};
impl<S: ActiveSyncStore> ActiveSyncService<S> {
    pub(super) async fn handle_item_operations(
        &self,
        principal: &AuthenticatedPrincipal,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "ItemOperations" {
            return command_status_response(protocol_version, 20, "ItemOperations", "2");
        }

        let mut root = WbxmlNode::new(20, "ItemOperations");
        let mut response = WbxmlNode::new(20, "Response");
        let mut unsupported_child = false;

        for child in &request.children {
            if child.name == "Fetch" {
                response.push(self.handle_item_operations_fetch(principal, child).await?);
            } else {
                unsupported_child = true;
            }
        }

        if unsupported_child || response.children.is_empty() {
            root.push(WbxmlNode::with_text(20, "Status", "2"));
        } else {
            root.push(WbxmlNode::with_text(
                20,
                "Status",
                ActiveSyncStatus::Success.as_str(),
            ));
        }

        if !response.children.is_empty() {
            root.push(response);
        }

        wbxml_response(protocol_version, encode_wbxml(&root))
    }

    pub(super) async fn handle_item_operations_fetch(
        &self,
        principal: &AuthenticatedPrincipal,
        fetch: &WbxmlNode,
    ) -> Result<WbxmlNode> {
        let mut node = WbxmlNode::new(20, "Fetch");
        if let Some(file_reference) = fetch
            .child("FileReference")
            .map(|value| value.text_value().trim())
            .filter(|value| !value.is_empty())
        {
            let mut attachment = None;
            for access in self.mailbox_accesses(principal).await? {
                attachment = self
                    .store
                    .fetch_activesync_attachment_content(access.account_id, file_reference)
                    .await?;
                if attachment.is_some() {
                    break;
                }
            }
            if let Some(attachment) = attachment {
                node.push(WbxmlNode::with_text(
                    20,
                    "Status",
                    ActiveSyncStatus::Success.as_str(),
                ));
                node.push(WbxmlNode::with_text(
                    17,
                    "FileReference",
                    &attachment.file_reference,
                ));
                let mut properties = WbxmlNode::new(20, "Properties");
                properties.push(WbxmlNode::with_text(
                    17,
                    "ContentType",
                    &attachment.media_type,
                ));
                properties.push(WbxmlNode::with_opaque(20, "Data", attachment.blob_bytes));
                node.push(properties);
            } else {
                node.push(WbxmlNode::with_text(20, "Status", "15"));
            }
            return Ok(node);
        }

        let Some(server_id) = fetch
            .child("ServerId")
            .map(|value| value.text_value().trim())
        else {
            node.push(WbxmlNode::with_text(20, "Status", "15"));
            return Ok(node);
        };
        let message_id = match Uuid::parse_str(server_id) {
            Ok(message_id) => message_id,
            Err(_) => {
                node.push(WbxmlNode::with_text(20, "Status", "6"));
                return Ok(node);
            }
        };
        let mut resolved = None;
        if let Some(collection_id) = fetch
            .child("CollectionId")
            .map(|node| node.text_value().trim().to_string())
            .filter(|value| !value.is_empty())
        {
            let account_id = self
                .resolve_collection(principal.account_id, &collection_id)
                .await?
                .map(|collection| collection.account_id)
                .ok_or_else(|| anyhow!("collection not found"));
            let Ok(account_id) = account_id else {
                node.push(WbxmlNode::with_text(20, "Status", "6"));
                return Ok(node);
            };
            if let Some(email) = self
                .store
                .fetch_jmap_emails(account_id, &[message_id])
                .await?
                .into_iter()
                .next()
            {
                resolved = Some((account_id, email));
            }
        } else {
            for access in self.mailbox_accesses(principal).await? {
                if let Some(email) = self
                    .store
                    .fetch_jmap_emails(access.account_id, &[message_id])
                    .await?
                    .into_iter()
                    .next()
                {
                    resolved = Some((access.account_id, email));
                    break;
                }
            }
        }
        if let Some((account_id, email)) = resolved {
            let attachments = self
                .store
                .fetch_activesync_message_attachments(account_id, email.id)
                .await?;
            let body_preference = fetch_body_preference(fetch);
            let mime_blob = if body_preference.body_type == BodyPreferenceType::Mime {
                self.store
                    .fetch_jmap_message_blob(account_id, email.id)
                    .await?
            } else {
                None
            };
            node.push(WbxmlNode::with_text(
                20,
                "Status",
                ActiveSyncStatus::Success.as_str(),
            ));
            if let Some(collection_id) = fetch.child("CollectionId") {
                node.push(WbxmlNode::with_text(
                    0,
                    "CollectionId",
                    collection_id.text_value(),
                ));
            }
            node.push(WbxmlNode::with_text(0, "ServerId", email.id.to_string()));
            let mut properties = WbxmlNode::new(20, "Properties");
            properties.push(value_to_wbxml(email_application_data(
                &email,
                &attachments,
                &body_preference,
                mime_blob.as_ref(),
            )));
            node.push(properties);
        } else {
            node.push(WbxmlNode::with_text(20, "Status", "6"));
        }

        Ok(node)
    }
}
