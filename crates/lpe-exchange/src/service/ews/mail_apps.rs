use super::super::*;

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
