use anyhow::Result;
use axum::{http::HeaderMap, response::Response};
use uuid::Uuid;

use crate::{
    protocol::{ActiveSyncCommand, ActiveSyncStatus},
    response::{policy_key, wbxml_response},
    store::ActiveSyncStore,
    types::AuthenticatedPrincipal,
    wbxml::{encode_wbxml, WbxmlNode},
};

use super::{command_status_response, ActiveSyncService};

impl<S: ActiveSyncStore> ActiveSyncService<S> {
    pub(super) async fn handle_provision(
        &self,
        principal: &AuthenticatedPrincipal,
        device_id: &str,
        device_type: &str,
        protocol_version: &str,
        request: &WbxmlNode,
    ) -> Result<Response> {
        if request.name != "Provision" {
            return command_status_response(protocol_version, 14, "Provision", "2");
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
        if client_status.as_deref() == Some("1") && requested_key == current_policy_key {
            self.store
                .acknowledge_activesync_device_policy(
                    principal.account_id,
                    device_id,
                    device_type,
                    &current_policy_key,
                )
                .await?;
        } else {
            self.store
                .store_activesync_device_pending_policy(
                    principal.account_id,
                    device_id,
                    device_type,
                    &current_policy_key,
                )
                .await?;
        }
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
            device_information.push(WbxmlNode::with_text(
                18,
                "Status",
                ActiveSyncStatus::Success.as_str(),
            ));
            response.push(device_information);
        }

        response.push(WbxmlNode::with_text(
            14,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        let mut policies = WbxmlNode::new(14, "Policies");
        let mut policy = WbxmlNode::new(14, "Policy");
        policy.push(WbxmlNode::with_text(
            14,
            "PolicyType",
            "MS-EAS-Provisioning-WBXML",
        ));
        policy.push(WbxmlNode::with_text(
            14,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
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

    pub(super) async fn policy_key_is_current(
        &self,
        account_id: Uuid,
        device_id: &str,
        request_policy_key: Option<&str>,
    ) -> Result<bool> {
        let Some(request_policy_key) = request_policy_key
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Ok(false);
        };
        let Some(device) = self
            .store
            .fetch_activesync_device(account_id, device_id)
            .await?
        else {
            return Ok(false);
        };
        Ok(device.provision_status == "active"
            && device
                .policy_key
                .as_deref()
                .map(|policy_key| policy_key == request_policy_key)
                .unwrap_or(false))
    }
}

pub(super) fn header_policy_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-ms-policykey")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(super) fn policy_required_response(
    command: ActiveSyncCommand,
    protocol_version: &str,
) -> Result<Response> {
    let (page, root) = match command {
        ActiveSyncCommand::FolderSync => (7, "FolderSync"),
        ActiveSyncCommand::GetItemEstimate => (6, "GetItemEstimate"),
        ActiveSyncCommand::ItemOperations => (20, "ItemOperations"),
        ActiveSyncCommand::MoveItems => (5, "MoveItems"),
        ActiveSyncCommand::Search => (15, "Search"),
        ActiveSyncCommand::SendMail => (21, "SendMail"),
        ActiveSyncCommand::SmartForward => (21, "SmartForward"),
        ActiveSyncCommand::SmartReply => (21, "SmartReply"),
        ActiveSyncCommand::Sync => (0, "Sync"),
        _ => (0, "Sync"),
    };
    let mut response = WbxmlNode::new(page, root);
    response.push(WbxmlNode::with_text(
        page,
        "Status",
        ActiveSyncStatus::PolicyRequired.as_str(),
    ));
    wbxml_response(protocol_version, encode_wbxml(&response))
}
