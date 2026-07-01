use anyhow::{bail, Result};
use axum::response::Response;

use crate::{
    protocol::ActiveSyncStatus,
    response::wbxml_response,
    snapshot::diff_collection_states,
    store::ActiveSyncStore,
    types::AuthenticatedPrincipal,
    wbxml::{encode_wbxml, WbxmlNode},
};

use super::{decode_sync_state, ActiveSyncService};
impl<S: ActiveSyncStore> ActiveSyncService<S> {
    pub(super) async fn handle_get_item_estimate(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "GetItemEstimate" {
            bail!("invalid GetItemEstimate payload");
        }

        let mut response = WbxmlNode::new(6, "GetItemEstimate");

        let Some(collections) = request.child("Collections") else {
            response.push(WbxmlNode::with_text(6, "Status", "2"));
            return wbxml_response(protocol_version, encode_wbxml(&response));
        };

        response.push(WbxmlNode::with_text(
            6,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        for collection_request in collections.children_named("Collection") {
            response.push(
                self.get_item_estimate_response(principal, device_id, collection_request)
                    .await?,
            );
        }

        wbxml_response(protocol_version, encode_wbxml(&response))
    }

    async fn get_item_estimate_response(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        collection_request: &WbxmlNode,
    ) -> Result<WbxmlNode> {
        let collection_id = collection_request
            .child("CollectionId")
            .map(|node| node.text_value().trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_default();
        let sync_key = collection_request
            .child("SyncKey")
            .map(|node| node.text_value().trim().to_string())
            .unwrap_or_default();

        let mut response = WbxmlNode::new(6, "Response");
        let Some(collection) = self
            .resolve_collection(principal.account_id, &collection_id)
            .await?
        else {
            response.push(WbxmlNode::with_text(6, "Status", "2"));
            return Ok(response);
        };

        if sync_key.is_empty() || sync_key == "0" {
            response.push(WbxmlNode::with_text(6, "Status", "3"));
            return Ok(response);
        }

        let Some(sync_state) = self
            .load_requested_sync_state(principal.account_id, device_id, &collection.id, &sync_key)
            .await?
        else {
            response.push(WbxmlNode::with_text(6, "Status", "4"));
            return Ok(response);
        };

        let previous_state = decode_sync_state(&sync_state.snapshot_json)?;
        let current_state = self
            .collection_state(principal.account_id, &collection)
            .await?;
        let estimate = if previous_state.next_offset < previous_state.pending_changes.len() {
            previous_state.pending_changes.len() - previous_state.next_offset
        } else {
            diff_collection_states(&previous_state.collection_state, &current_state).len()
        };

        response.push(WbxmlNode::with_text(
            6,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        let mut response_collection = WbxmlNode::new(6, "Collection");
        response_collection.push(WbxmlNode::with_text(6, "CollectionId", collection.id));
        response_collection.push(WbxmlNode::with_text(6, "Estimate", estimate.to_string()));
        response.push(response_collection);
        Ok(response)
    }
}
