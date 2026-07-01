use anyhow::{bail, Result};
use axum::response::Response;

use crate::{
    message::field_text,
    protocol::ActiveSyncStatus,
    response::wbxml_response,
    snapshot::{email_application_data, BodyPreference},
    store::ActiveSyncStore,
    types::AuthenticatedPrincipal,
    wbxml::{encode_wbxml, WbxmlNode},
};

use super::{search_status_response, value_to_wbxml, ActiveSyncService};
impl<S: ActiveSyncStore> ActiveSyncService<S> {
    pub(super) async fn handle_search(
        &self,
        principal: &AuthenticatedPrincipal,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "Search" {
            return search_status_response(protocol_version, "3", None);
        }

        let Some(store) = request.child("Store") else {
            return search_status_response(protocol_version, "3", None);
        };
        let query_text = search_query_text(store);
        let range = parse_range(
            store
                .child("Options")
                .and_then(|options| options.child("Range"))
                .or_else(|| store.child("Range"))
                .map(|node| node.text_value()),
        );
        let (start, end) = match range {
            Ok(range) => range,
            Err(_) => return search_status_response(protocol_version, "1", Some("2")),
        };
        let limit = end.saturating_sub(start) + 1;
        let query = self
            .store
            .query_jmap_email_ids(
                principal.account_id,
                None,
                query_text.as_deref(),
                start,
                limit,
            )
            .await?;
        let emails = self
            .store
            .fetch_jmap_emails(principal.account_id, &query.ids)
            .await?;

        let mut response = WbxmlNode::new(15, "Search");
        response.push(WbxmlNode::with_text(
            15,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        let mut response_store = WbxmlNode::new(15, "Store");
        response_store.push(WbxmlNode::with_text(
            15,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        response_store.push(WbxmlNode::with_text(15, "Name", "Mailbox"));
        response_store.push(WbxmlNode::with_text(15, "Total", query.total.to_string()));
        if !emails.is_empty() {
            response_store.push(WbxmlNode::with_text(
                15,
                "Range",
                format!("{}-{}", start, start + emails.len() as u64 - 1),
            ));
        }
        for email in emails {
            let attachments = self
                .store
                .fetch_activesync_message_attachments(principal.account_id, email.id)
                .await?;
            let mut result = WbxmlNode::new(15, "Result");
            result.push(WbxmlNode::with_text(15, "LongId", email.id.to_string()));
            let mut properties = WbxmlNode::new(15, "Properties");
            properties.push(WbxmlNode::with_text(
                0,
                "CollectionId",
                email.mailbox_id.to_string(),
            ));
            properties.push(WbxmlNode::with_text(0, "ServerId", email.id.to_string()));
            properties.push(value_to_wbxml(email_application_data(
                &email,
                &attachments,
                &BodyPreference::default(),
                None,
            )));
            properties.push(WbxmlNode::with_text(
                17,
                "Preview",
                trim_preview(&email.body_text),
            ));
            result.push(properties);
            response_store.push(result);
        }
        let mut response_node = WbxmlNode::new(15, "Response");
        response_node.push(response_store);
        response.push(response_node);

        wbxml_response(protocol_version, encode_wbxml(&response))
    }
}

fn search_query_text(store: &WbxmlNode) -> Option<String> {
    store
        .child("Query")
        .and_then(|query| {
            field_text(query, "FreeText").or_else(|| {
                let parts = query
                    .children_named("Value")
                    .into_iter()
                    .map(|node| node.text_value().trim())
                    .filter(|value| !value.is_empty())
                    .collect::<Vec<_>>();
                (!parts.is_empty()).then(|| parts.join(" "))
            })
        })
        .or_else(|| field_text(store, "Query"))
}

fn parse_range(value: Option<&str>) -> Result<(u64, u64)> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok((0, 49));
    };
    let Some((start, end)) = value.split_once('-') else {
        bail!("invalid Search range");
    };
    let start = start.trim().parse::<u64>()?;
    let end = end.trim().parse::<u64>()?;
    if end < start {
        bail!("invalid Search range");
    }
    Ok((start, end))
}

fn trim_preview(value: &str) -> String {
    value
        .split_whitespace()
        .take(24)
        .collect::<Vec<_>>()
        .join(" ")
}
