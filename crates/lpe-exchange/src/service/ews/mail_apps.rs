use super::super::*;
use sha2::{Digest, Sha256};

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn get_app_manifests(
        &self,
        principal: &AccountPrincipal,
    ) -> Result<String> {
        let manifests = self.store.fetch_ews_mail_app_manifests(principal).await?;
        Ok(get_app_manifests_response(&manifests))
    }

    pub(in crate::service) async fn get_app_marketplace_url(
        &self,
        principal: &AccountPrincipal,
    ) -> Result<String> {
        let policy = self
            .store
            .fetch_ews_app_marketplace_policy(principal)
            .await?;
        if !policy.enabled {
            return Ok(operation_error_response(
                "GetAppMarketplaceUrl",
                "ErrorInvalidOperation",
                "Exchange marketplace federation is not enabled for this tenant.",
            ));
        }
        let Some(url) = policy.url.filter(|url| !url.trim().is_empty()) else {
            return Ok(operation_error_response(
                "GetAppMarketplaceUrl",
                "ErrorInvalidOperation",
                "No canonical mail app marketplace URL is configured.",
            ));
        };
        Ok(get_app_marketplace_url_response(&url))
    }

    pub(in crate::service) async fn install_app(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let Some(app_id) = mail_app_id_from_request(request) else {
            return Ok(operation_error_response(
                "InstallApp",
                "ErrorInvalidOperation",
                "InstallApp requires an AppId value.",
            ));
        };
        match self
            .store
            .install_ews_mail_app(
                principal,
                &app_id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-install-mail-app".to_string(),
                    subject: app_id.clone(),
                },
            )
            .await
        {
            Ok(install) => Ok(mail_app_state_response(
                "InstallApp",
                &install.app_id,
                &install.status,
            )),
            Err(error) => Ok(mail_app_operation_error_response("InstallApp", &error)),
        }
    }

    pub(in crate::service) async fn disable_app(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let Some(app_id) = mail_app_id_from_request(request) else {
            return Ok(operation_error_response(
                "DisableApp",
                "ErrorInvalidOperation",
                "DisableApp requires an AppId value.",
            ));
        };
        match self
            .store
            .disable_ews_mail_app(
                principal,
                &app_id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-disable-mail-app".to_string(),
                    subject: app_id.clone(),
                },
            )
            .await
        {
            Ok(install) => Ok(mail_app_state_response(
                "DisableApp",
                &install.app_id,
                &install.status,
            )),
            Err(error) => Ok(mail_app_operation_error_response("DisableApp", &error)),
        }
    }

    pub(in crate::service) async fn uninstall_app(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let Some(app_id) = mail_app_id_from_request(request) else {
            return Ok(operation_error_response(
                "UninstallApp",
                "ErrorInvalidOperation",
                "UninstallApp requires an AppId value.",
            ));
        };
        match self
            .store
            .uninstall_ews_mail_app(
                principal,
                &app_id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-uninstall-mail-app".to_string(),
                    subject: app_id.clone(),
                },
            )
            .await
        {
            Ok(install) => Ok(mail_app_state_response(
                "UninstallApp",
                &install.app_id,
                &install.status,
            )),
            Err(error) => Ok(mail_app_operation_error_response("UninstallApp", &error)),
        }
    }

    pub(in crate::service) async fn get_client_access_token(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let Some(app_id) = mail_app_id_from_request(request) else {
            return Ok(operation_error_response(
                "GetClientAccessToken",
                "ErrorInvalidOperation",
                "GetClientAccessToken requires an AppId value.",
            ));
        };
        let scopes = requested_mail_app_token_scopes(request);
        let token = format!("ews-app-token:{}", Uuid::new_v4());
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        let token_hash = format!("{:x}", hasher.finalize());
        match self
            .store
            .issue_ews_mail_app_token(
                principal,
                &app_id,
                &token_hash,
                &scopes,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-issue-mail-app-token".to_string(),
                    subject: format!("{}:{}", app_id, scopes.join(",")),
                },
            )
            .await
        {
            Ok(event) => Ok(get_client_access_token_response(&event, &token, &scopes)),
            Err(error) => Ok(mail_app_operation_error_response(
                "GetClientAccessToken",
                &error,
            )),
        }
    }
}

pub(in crate::service) fn mail_app_id_from_request(request: &str) -> Option<String> {
    element_text(request, "AppId")
        .or_else(|| element_text(request, "Id"))
        .or_else(|| element_text(request, "ID"))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(in crate::service) fn requested_mail_app_token_scopes(request: &str) -> Vec<String> {
    let mut scopes = element_contents(request, "TokenScope")
        .into_iter()
        .chain(element_contents(request, "Scope"))
        .map(xml_text)
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if scopes.is_empty() {
        scopes.push("ews".to_string());
    }
    scopes.sort();
    scopes.dedup();
    scopes
}

pub(in crate::service) fn get_app_manifests_response(manifests: &[EwsMailAppManifest]) -> String {
    let manifests_xml = manifests
        .iter()
        .map(|manifest| {
            format!(
                concat!(
                    "<t:AppManifest>",
                    "<t:AppId>{app_id}</t:AppId>",
                    "<t:DisplayName>{display_name}</t:DisplayName>",
                    "<t:ProviderName>{provider_name}</t:ProviderName>",
                    "<t:Version>{version}</t:Version>",
                    "<t:Status>{status}</t:Status>",
                    "<t:ManifestXml>{manifest_xml}</t:ManifestXml>",
                    "</t:AppManifest>"
                ),
                app_id = escape_xml(&manifest.app_id),
                display_name = escape_xml(&manifest.display_name),
                provider_name = escape_xml(&manifest.provider_name),
                version = escape_xml(&manifest.version),
                status = escape_xml(
                    manifest
                        .installation_status
                        .as_deref()
                        .unwrap_or("available")
                ),
                manifest_xml = escape_xml(&manifest.manifest_xml),
            )
        })
        .collect::<String>();
    format!(
        concat!(
            "<m:GetAppManifestsResponse>",
            "<m:ResponseMessages>",
            "<m:GetAppManifestsResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Manifests>{manifests_xml}</m:Manifests>",
            "</m:GetAppManifestsResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetAppManifestsResponse>"
        ),
        manifests_xml = manifests_xml,
    )
}

pub(in crate::service) fn get_app_marketplace_url_response(url: &str) -> String {
    format!(
        concat!(
            "<m:GetAppMarketplaceUrlResponse>",
            "<m:ResponseMessages>",
            "<m:GetAppMarketplaceUrlResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:AppMarketplaceUrl>{url}</m:AppMarketplaceUrl>",
            "</m:GetAppMarketplaceUrlResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetAppMarketplaceUrlResponse>"
        ),
        url = escape_xml(url),
    )
}

pub(in crate::service) fn mail_app_state_response(
    operation: &str,
    app_id: &str,
    status: &str,
) -> String {
    format!(
        concat!(
            "<m:{operation}Response>",
            "<m:ResponseMessages>",
            "<m:{operation}ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:AppId>{app_id}</m:AppId>",
            "<m:Status>{status}</m:Status>",
            "</m:{operation}ResponseMessage>",
            "</m:ResponseMessages>",
            "</m:{operation}Response>"
        ),
        operation = operation,
        app_id = escape_xml(app_id),
        status = escape_xml(status),
    )
}

pub(in crate::service) fn get_client_access_token_response(
    event: &EwsMailAppTokenEvent,
    token: &str,
    scopes: &[String],
) -> String {
    let scopes_xml = scopes
        .iter()
        .map(|scope| format!("<t:Scope>{}</t:Scope>", escape_xml(scope)))
        .collect::<String>();
    format!(
        concat!(
            "<m:GetClientAccessTokenResponse>",
            "<m:ResponseMessages>",
            "<m:GetClientAccessTokenResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Token>",
            "<t:TokenId>{token_id}</t:TokenId>",
            "<t:AppId>{app_id}</t:AppId>",
            "<t:TokenValue>{token}</t:TokenValue>",
            "<t:IssuedAt>{issued_at}</t:IssuedAt>",
            "<t:ExpiresAt>{expires_at}</t:ExpiresAt>",
            "<t:Scopes>{scopes_xml}</t:Scopes>",
            "</m:Token>",
            "</m:GetClientAccessTokenResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetClientAccessTokenResponse>"
        ),
        token_id = event.id,
        app_id = escape_xml(&event.app_id),
        token = escape_xml(token),
        issued_at = escape_xml(&event.issued_at),
        expires_at = escape_xml(&event.expires_at),
        scopes_xml = scopes_xml,
    )
}
