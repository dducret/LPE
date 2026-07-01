use anyhow::Result;
use axum::response::Response;
use lpe_storage::AuditEntryInput;
use uuid::Uuid;

use crate::{
    protocol::ActiveSyncStatus,
    response::wbxml_response,
    snapshot::{mail_collection, parse_collection_mailbox_id},
    store::ActiveSyncStore,
    types::AuthenticatedPrincipal,
    wbxml::{encode_wbxml, WbxmlNode},
};

use super::{command_status_response, ActiveSyncService};
impl<S: ActiveSyncStore> ActiveSyncService<S> {
    pub(super) async fn handle_move_items(
        &self,
        principal: &AuthenticatedPrincipal,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "MoveItems" {
            return command_status_response(protocol_version, 5, "MoveItems", "5");
        }

        let mut root = WbxmlNode::new(5, "MoveItems");
        let mut unsupported_child = false;
        for child in &request.children {
            if child.name == "Move" {
                root.push(self.handle_move_item(principal, child).await?);
            } else {
                unsupported_child = true;
            }
        }
        if unsupported_child || root.children.is_empty() {
            root.push(WbxmlNode::with_text(5, "Status", "5"));
        }
        wbxml_response(protocol_version, encode_wbxml(&root))
    }

    async fn handle_move_item(
        &self,
        principal: &AuthenticatedPrincipal,
        move_node: &WbxmlNode,
    ) -> Result<WbxmlNode> {
        let src_msg_id = move_node
            .child("SrcMsgId")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let src_fld_id = move_node
            .child("SrcFldId")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let dst_fld_id = move_node
            .child("DstFldId")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();
        let mut response = WbxmlNode::new(5, "Response");
        if !src_msg_id.is_empty() {
            response.push(WbxmlNode::with_text(5, "SrcMsgId", &src_msg_id));
        }

        let source = self
            .resolve_collection(principal.account_id, &src_fld_id)
            .await?;
        let target = self
            .resolve_collection(principal.account_id, &dst_fld_id)
            .await?;
        let Some(source) = source else {
            response.push(WbxmlNode::with_text(
                5,
                "Status",
                ActiveSyncStatus::Success.as_str(),
            ));
            return Ok(response);
        };
        let Some(target) = target else {
            response.push(WbxmlNode::with_text(5, "Status", "2"));
            return Ok(response);
        };
        if source.id == target.id {
            response.push(WbxmlNode::with_text(5, "Status", "4"));
            return Ok(response);
        }
        if !mail_collection(&source)
            || !mail_collection(&target)
            || source.account_id != target.account_id
        {
            response.push(WbxmlNode::with_text(
                5,
                "Status",
                ActiveSyncStatus::Success.as_str(),
            ));
            return Ok(response);
        }

        let message_id = match Uuid::parse_str(&src_msg_id) {
            Ok(message_id) => message_id,
            Err(_) => {
                response.push(WbxmlNode::with_text(
                    5,
                    "Status",
                    ActiveSyncStatus::Success.as_str(),
                ));
                return Ok(response);
            }
        };
        let moved = self
            .store
            .move_jmap_email_from_mailbox(
                source.account_id,
                parse_collection_mailbox_id(&source)?,
                message_id,
                parse_collection_mailbox_id(&target)?,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "activesync-move-message".to_string(),
                    subject: format!("message:{src_msg_id}->{dst_fld_id}"),
                },
            )
            .await;

        match moved {
            Ok(email) => {
                response.push(WbxmlNode::with_text(5, "Status", "3"));
                response.push(WbxmlNode::with_text(5, "DstMsgId", email.id.to_string()));
            }
            Err(_) => response.push(WbxmlNode::with_text(
                5,
                "Status",
                ActiveSyncStatus::Success.as_str(),
            )),
        }
        Ok(response)
    }
}
