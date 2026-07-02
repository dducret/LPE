use axum::{
    body::Bytes,
    extract::{Path, Query},
    http::{header::CONTENT_TYPE, header::LOCATION, HeaderMap, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use lpe_core::outlook_trace::{write_outlook_trace, OutlookTraceDirection, OutlookTraceEvent};
use lpe_storage::Storage;
use serde::Deserialize;
use serde_json::json;
use std::env;
use tracing::{info, warn};

const MAPI_HTTP_AUTODISCOVER_VERSION: u32 = 1;

pub fn router() -> Router<Storage> {
    Router::new()
        .route(
            "/autoconfig/mail/config-v1.1.xml",
            get(thunderbird_autoconfig),
        )
        .route(
            "/.well-known/autoconfig/mail/config-v1.1.xml",
            get(thunderbird_autoconfig),
        )
        .route("/.well-known/jmap", get(jmap_well_known))
        .route(
            "/autodiscover",
            get(outlook_autodiscover_get).post(outlook_autodiscover_post),
        )
        .route(
            "/Autodiscover",
            get(outlook_autodiscover_get).post(outlook_autodiscover_post),
        )
        .route(
            "/autodiscover/autodiscover.xml",
            get(outlook_autodiscover_get).post(outlook_autodiscover_post),
        )
        .route(
            "/Autodiscover/Autodiscover.xml",
            get(outlook_autodiscover_get).post(outlook_autodiscover_post),
        )
        .route(
            "/autodiscover/autodiscover.json/v1.0/{email}",
            get(outlook_autodiscover_json),
        )
        .route(
            "/Autodiscover/Autodiscover.json/v1.0/{email}",
            get(outlook_autodiscover_json),
        )
}

async fn thunderbird_autoconfig(headers: HeaderMap) -> Response {
    xml_response(render_thunderbird_autoconfig(
        &PublishedEndpoints::from_headers(&headers, None),
    ))
}

async fn jmap_well_known(headers: HeaderMap) -> Response {
    (
        StatusCode::TEMPORARY_REDIRECT,
        [(LOCATION, jmap_well_known_location(&headers))],
    )
        .into_response()
}

fn jmap_well_known_location(headers: &HeaderMap) -> String {
    PublishedEndpoints::from_headers(headers, None).jmap_session_url
}

async fn outlook_autodiscover_get(uri: Uri, headers: HeaderMap) -> Response {
    let endpoints = PublishedEndpoints::from_headers(&headers, None);
    let response_body = render_outlook_autodiscover(&endpoints, None);
    let response = xml_response(response_body.clone());
    log_autodiscover_connection(
        "GET",
        &uri,
        &headers,
        &endpoints,
        None,
        "pox",
        0,
        &response,
        None,
        Some(&response_body),
        None,
    );
    response
}

async fn outlook_autodiscover_post(uri: Uri, headers: HeaderMap, body: Bytes) -> Response {
    let email = parse_autodiscover_email(body.as_ref());
    let endpoints = PublishedEndpoints::from_headers(&headers, email.as_deref());
    let response_kind = if requested_soap_user_settings(body.as_ref()) {
        "soap_user_settings"
    } else if requested_mobilesync_schema(body.as_ref()) {
        "mobilesync"
    } else {
        "pox"
    };
    let response_body = if response_kind == "soap_user_settings" {
        match render_soap_user_settings_response(&endpoints, email.as_deref()) {
            Some(response) => response,
            None => {
                let response = (
                    StatusCode::NOT_FOUND,
                    "SOAP Autodiscover is not published for the default Outlook IMAP profile.\n",
                )
                    .into_response();
                log_autodiscover_connection(
                    "POST",
                    &uri,
                    &headers,
                    &endpoints,
                    email.as_deref(),
                    response_kind,
                    body.len(),
                    &response,
                    Some(body.as_ref()),
                    Some("SOAP Autodiscover is not published for the default Outlook IMAP profile.\n"),
                    Some("SOAP Exchange autodiscover is not published"),
                );
                return response;
            }
        }
    } else if response_kind == "mobilesync" {
        render_mobilesync_autodiscover(&endpoints, email.as_deref())
    } else {
        render_outlook_autodiscover(&endpoints, email.as_deref())
    };
    let response = xml_response(response_body.clone());
    log_autodiscover_connection(
        "POST",
        &uri,
        &headers,
        &endpoints,
        email.as_deref(),
        response_kind,
        body.len(),
        &response,
        Some(body.as_ref()),
        Some(&response_body),
        None,
    );
    response
}

#[derive(Debug, Deserialize)]
struct AutodiscoverJsonQuery {
    #[serde(rename = "Protocol", alias = "protocol")]
    protocol: Option<String>,
}

async fn outlook_autodiscover_json(
    uri: Uri,
    headers: HeaderMap,
    Path(email): Path<String>,
    Query(query): Query<AutodiscoverJsonQuery>,
) -> Response {
    let endpoints = PublishedEndpoints::from_headers(&headers, Some(&email));
    let response = match render_autodiscover_json(&endpoints, query.protocol.as_deref()) {
        Some(response) => response,
        None => autodiscover_json_invalid_protocol_response(&endpoints, query.protocol.as_deref()),
    };
    log_autodiscover_connection(
        "GET",
        &uri,
        &headers,
        &endpoints,
        Some(&email),
        query.protocol.as_deref().unwrap_or("AutoDiscoverV1"),
        0,
        &response,
        None,
        None,
        None,
    );
    response
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublishedEndpoints {
    display_domain: String,
    imap_host: Option<String>,
    imap_port: Option<u16>,
    smtp_host: Option<String>,
    smtp_port: Option<u16>,
    smtp_socket_type: Option<String>,
    ews_enabled: bool,
    ews_url: String,
    mapi_enabled: bool,
    outlook_interop_gate_passed: bool,
    mapi_http_requested: bool,
    legacy_exch_autodiscover_enabled: bool,
    legacy_expr_autodiscover_enabled: bool,
    rpc_proxy_enabled: bool,
    soap_exchange_autodiscover_enabled: bool,
    mapi_emsmdb_url: String,
    mapi_nspi_url: String,
    activesync_url: String,
    jmap_session_url: String,
    autodiscover_xml_url: String,
}

impl PublishedEndpoints {
    fn from_headers(headers: &HeaderMap, email_hint: Option<&str>) -> Self {
        let public_host = public_host(headers);
        let public_host_name = host_without_port(&public_host);
        let public_scheme = public_scheme(headers);
        let display_domain = email_hint
            .and_then(email_domain)
            .unwrap_or_else(|| public_host_name.clone());

        let imap_host = env::var("LPE_AUTOCONFIG_IMAP_HOST")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let imap_port = imap_host
            .as_ref()
            .map(|_| read_u16_env("LPE_AUTOCONFIG_IMAP_PORT", 993));

        let smtp_host = env::var("LPE_AUTOCONFIG_SMTP_HOST")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let smtp_port = smtp_host
            .as_ref()
            .map(|_| read_u16_env("LPE_AUTOCONFIG_SMTP_PORT", 465));
        let smtp_socket_type = smtp_host.as_ref().map(|_| {
            env::var("LPE_AUTOCONFIG_SMTP_SOCKET_TYPE").unwrap_or_else(|_| "SSL".to_string())
        });

        let jmap_session_url = env::var("LPE_AUTOCONFIG_JMAP_SESSION_URL")
            .unwrap_or_else(|_| format!("{public_scheme}://{public_host}/api/jmap/session"));
        let autodiscover_xml_url =
            format!("{public_scheme}://{public_host}/autodiscover/autodiscover.xml");
        let ews_enabled = env_flag("LPE_AUTOCONFIG_EWS_ENABLED");
        let ews_url = env::var("LPE_AUTOCONFIG_EWS_URL")
            .unwrap_or_else(|_| format!("{public_scheme}://{public_host}/EWS/Exchange.asmx"));
        let mapi_enabled = env_flag("LPE_AUTOCONFIG_MAPI_ENABLED");
        let outlook_interop_gate_passed = env_flag("LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED");
        let mapi_http_requested = headers
            .get("x-mapihttpcapability")
            .and_then(|value| value.to_str().ok())
            .and_then(valid_mapi_http_capability);
        let legacy_exch_autodiscover_enabled = env_flag("LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED");
        let legacy_expr_autodiscover_enabled = env_flag("LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED");
        let rpc_proxy_enabled = env_flag("LPE_AUTOCONFIG_RPC_PROXY_ENABLED");
        let soap_exchange_autodiscover_enabled =
            env_flag("LPE_AUTOCONFIG_SOAP_EXCHANGE_AUTODISCOVER_ENABLED");
        let mapi_mailbox_id = email_hint.unwrap_or_default();
        let mapi_emsmdb_url = env::var("LPE_AUTOCONFIG_MAPI_EMSMDB_URL").unwrap_or_else(|_| {
            format!("{public_scheme}://{public_host}/mapi/emsmdb/?MailboxId={mapi_mailbox_id}")
        });
        let mapi_nspi_url = env::var("LPE_AUTOCONFIG_MAPI_NSPI_URL").unwrap_or_else(|_| {
            format!("{public_scheme}://{public_host}/mapi/nspi/?MailboxId={mapi_mailbox_id}")
        });
        let activesync_url = env::var("LPE_AUTOCONFIG_ACTIVESYNC_URL").unwrap_or_else(|_| {
            format!("{public_scheme}://{public_host}/Microsoft-Server-ActiveSync")
        });

        Self {
            display_domain,
            imap_host,
            imap_port,
            smtp_host,
            smtp_port,
            smtp_socket_type,
            ews_enabled,
            ews_url,
            mapi_enabled,
            outlook_interop_gate_passed,
            mapi_http_requested: mapi_http_requested.is_some(),
            legacy_exch_autodiscover_enabled,
            legacy_expr_autodiscover_enabled,
            rpc_proxy_enabled,
            soap_exchange_autodiscover_enabled,
            mapi_emsmdb_url,
            mapi_nspi_url,
            activesync_url,
            jmap_session_url,
            autodiscover_xml_url,
        }
    }

    fn exchange_autodiscover_enabled(&self) -> bool {
        self.ews_enabled || self.mapi_autodiscover_enabled()
    }

    fn mapi_autodiscover_enabled(&self) -> bool {
        self.mapi_enabled && self.outlook_interop_gate_passed
    }

    fn mapi_http_autodiscover_selected(&self) -> bool {
        self.mapi_http_requested && self.mapi_autodiscover_enabled()
    }

    fn exch_autodiscover_enabled(&self) -> bool {
        self.legacy_exch_autodiscover_enabled && self.exchange_autodiscover_enabled()
    }

    fn expr_autodiscover_enabled(&self) -> bool {
        self.legacy_expr_autodiscover_enabled
            && self.rpc_proxy_enabled
            && self.outlook_interop_gate_passed
            && self.exchange_autodiscover_enabled()
    }

    fn soap_exchange_autodiscover_enabled(&self) -> bool {
        self.soap_exchange_autodiscover_enabled && self.exchange_autodiscover_enabled()
    }
}

fn render_autodiscover_json(
    config: &PublishedEndpoints,
    protocol: Option<&str>,
) -> Option<Response> {
    let requested = protocol
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("AutodiscoverV1");
    let (protocol, url) = match requested.to_ascii_lowercase().as_str() {
        "autodiscoverv1" | "autodiscover" => {
            ("AutoDiscoverV1", config.autodiscover_xml_url.as_str())
        }
        "ews" if config.ews_enabled => ("EWS", config.ews_url.as_str()),
        "activesync" | "mobilesync" => ("ActiveSync", config.activesync_url.as_str()),
        "mapihttp" if config.mapi_autodiscover_enabled() => {
            ("MapiHttp", config.mapi_emsmdb_url.as_str())
        }
        _ => return None,
    };

    Some(
        Json(json!({
            "Protocol": protocol,
            "Url": url,
        }))
        .into_response(),
    )
}

fn supported_autodiscover_json_protocols(config: &PublishedEndpoints) -> String {
    let mut protocols = vec!["ActiveSync", "AutoDiscoverV1", "MobileSync"];
    if config.ews_enabled {
        protocols.push("EWS");
    }
    if config.mapi_autodiscover_enabled() {
        protocols.push("MapiHttp");
    }
    protocols.join(",")
}

fn autodiscover_json_invalid_protocol_response(
    config: &PublishedEndpoints,
    protocol: Option<&str>,
) -> Response {
    let requested = protocol
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("");
    let supported = supported_autodiscover_json_protocols(config);

    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "ErrorCode": "InvalidProtocol",
            "ErrorMessage": format!(
                "The given protocol value '{}' is invalid. Supported values are '{}'",
                requested,
                supported
            )
        })),
    )
        .into_response()
}

fn valid_mapi_http_capability(value: &str) -> Option<u32> {
    let capability = value.trim().parse::<u32>().ok()?;
    (capability >= MAPI_HTTP_AUTODISCOVER_VERSION).then_some(capability)
}

fn render_thunderbird_autoconfig(config: &PublishedEndpoints) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<clientConfig version="1.1">
"#,
    );
    xml.push_str(&format!(
        "  <emailProvider id=\"{}\">\n",
        escape_xml(&config.display_domain)
    ));
    xml.push_str(&format!(
        "    <domain>{}</domain>\n",
        escape_xml(&config.display_domain)
    ));
    xml.push_str("    <displayName>LPE Mail</displayName>\n");
    xml.push_str("    <displayShortName>LPE</displayShortName>\n");
    if let (Some(imap_host), Some(imap_port)) = (config.imap_host.as_deref(), config.imap_port) {
        xml.push_str("    <incomingServer type=\"imap\">\n");
        xml.push_str(&format!(
            "      <hostname>{}</hostname>\n",
            escape_xml(imap_host)
        ));
        xml.push_str(&format!("      <port>{}</port>\n", imap_port));
        xml.push_str("      <socketType>SSL</socketType>\n");
        xml.push_str("      <authentication>password-cleartext</authentication>\n");
        xml.push_str("      <username>%EMAILADDRESS%</username>\n");
        xml.push_str("    </incomingServer>\n");
    }
    if let (Some(smtp_host), Some(smtp_port), Some(smtp_socket_type)) = (
        config.smtp_host.as_deref(),
        config.smtp_port,
        config.smtp_socket_type.as_deref(),
    ) {
        xml.push_str("    <outgoingServer type=\"smtp\">\n");
        xml.push_str(&format!(
            "      <hostname>{}</hostname>\n",
            escape_xml(smtp_host)
        ));
        xml.push_str(&format!("      <port>{}</port>\n", smtp_port));
        xml.push_str(&format!(
            "      <socketType>{}</socketType>\n",
            escape_xml(smtp_socket_type)
        ));
        xml.push_str("      <authentication>password-cleartext</authentication>\n");
        xml.push_str("      <username>%EMAILADDRESS%</username>\n");
        xml.push_str("    </outgoingServer>\n");
    }
    xml.push_str(&format!(
        "    <documentation url=\"{}\">\n",
        escape_xml(&config.jmap_session_url)
    ));
    xml.push_str("      <descr lang=\"en\">JMAP session endpoint published by LPE.</descr>\n");
    xml.push_str("    </documentation>\n");
    xml.push_str("  </emailProvider>\n");
    xml.push_str("</clientConfig>\n");
    xml
}

fn render_outlook_autodiscover(config: &PublishedEndpoints, email: Option<&str>) -> String {
    let email = email.unwrap_or_default();
    let mut xml = format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n",
            "<Autodiscover xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"http://schemas.microsoft.com/exchange/autodiscover/responseschema/2006\">\n",
            "  <Response xmlns=\"http://schemas.microsoft.com/exchange/autodiscover/outlook/responseschema/2006a\">\n",
            "    <User>\n",
            "      <DisplayName>{display_domain}</DisplayName>\n",
            "      <LegacyDN>/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={legacy_user}</LegacyDN>\n",
            "      <AutoDiscoverSMTPAddress>{email}</AutoDiscoverSMTPAddress>\n",
            "      <DeploymentId>{deployment_id}</DeploymentId>\n",
            "    </User>\n",
            "    <Account>\n",
            "      <AccountType>email</AccountType>\n",
            "      <Action>settings</Action>\n",
            "      <MicrosoftOnline>False</MicrosoftOnline>\n"
        ),
        display_domain = escape_xml(&config.display_domain),
        email = escape_xml(email),
        legacy_user = escape_xml(&legacy_user(email, &config.display_domain)),
        deployment_id = escape_xml(&deployment_id(&config.display_domain)),
    );

    if let (Some(imap_host), Some(imap_port)) = (config.imap_host.as_deref(), config.imap_port) {
        xml.push_str(&format!(
            concat!(
                "      <Protocol>\n",
                "        <Type>IMAP</Type>\n",
                "        <Server>{imap_host}</Server>\n",
                "        <Port>{imap_port}</Port>\n",
                "        <DomainRequired>off</DomainRequired>\n",
                "        <LoginName>{email}</LoginName>\n",
                "        <SPA>off</SPA>\n",
                "        <SSL>on</SSL>\n",
                "        <AuthRequired>on</AuthRequired>\n",
                "      </Protocol>\n"
            ),
            imap_host = escape_xml(imap_host),
            imap_port = imap_port,
            email = escape_xml(email),
        ));
    }

    if let (Some(smtp_host), Some(smtp_port)) = (config.smtp_host.as_deref(), config.smtp_port) {
        xml.push_str(&format!(
            concat!(
                "      <Protocol>\n",
                "        <Type>SMTP</Type>\n",
                "        <Server>{smtp_host}</Server>\n",
                "        <Port>{smtp_port}</Port>\n",
                "        <DomainRequired>off</DomainRequired>\n",
                "        <LoginName>{email}</LoginName>\n",
                "        <SPA>off</SPA>\n",
                "        <SSL>on</SSL>\n",
                "        <AuthRequired>on</AuthRequired>\n",
                "        <UsePOPAuth>off</UsePOPAuth>\n",
                "        <SMTPLast>off</SMTPLast>\n",
                "      </Protocol>\n"
            ),
            smtp_host = escape_xml(smtp_host),
            smtp_port = smtp_port,
            email = escape_xml(email),
        ));
    }

    if config.ews_enabled {
        xml.push_str(&render_ews_web_autodiscover_protocol(config, email));
    }
    if (config.exch_autodiscover_enabled() || config.expr_autodiscover_enabled())
        && !config.mapi_http_autodiscover_selected()
    {
        xml.push_str(&render_exchange_provider_autodiscover_protocols(
            config, email,
        ));
    }
    if config.mapi_http_autodiscover_selected() {
        xml.push_str(&render_mapi_http_autodiscover_protocol(config));
    }

    xml.push_str(concat!(
        "    </Account>\n",
        "  </Response>\n",
        "</Autodiscover>\n"
    ));
    xml
}

fn render_exchange_provider_autodiscover_protocols(
    config: &PublishedEndpoints,
    email: &str,
) -> String {
    let mailbox_server = mailbox_server_name(config);
    let server_dn = format!(
        "/o=LPE/ou=Exchange Administrative Group/cn=Configuration/cn=Servers/cn={mailbox_server}"
    );
    let mdb_dn = format!("{server_dn}/cn=LPE Private MDB");
    let cert_principal = format!("msstd:{mailbox_server}");
    let mut xml = String::new();
    if config.exch_autodiscover_enabled() {
        xml.push_str(&format!(
            concat!(
                "      <Protocol>\n",
                "        <Type>EXCH</Type>\n",
                "        <Server>{mailbox_server}</Server>\n",
                "        <ServerDN>{server_dn}</ServerDN>\n",
                "        <MdbDN>{mdb_dn}</MdbDN>\n",
                "{ews_url_fields}",
                "        <Port>0</Port>\n",
                "        <DirectoryPort>0</DirectoryPort>\n",
                "        <ReferralPort>0</ReferralPort>\n",
                "        <AD>{mailbox_server}</AD>\n",
                "        <PublicFolderServer>{mailbox_server}</PublicFolderServer>\n",
                "        <ServerExclusiveConnect>off</ServerExclusiveConnect>\n",
                "      </Protocol>\n",
            ),
            mailbox_server = escape_xml(mailbox_server),
            server_dn = escape_xml(&server_dn),
            mdb_dn = escape_xml(&mdb_dn),
            ews_url_fields = exchange_provider_ews_url_fields(config),
        ));
    }
    if config.expr_autodiscover_enabled() {
        xml.push_str(&format!(
            concat!(
                "      <Protocol>\n",
                "        <Type>EXPR</Type>\n",
                "        <Server>{mailbox_server}</Server>\n",
                "        <ServerDN>{server_dn}</ServerDN>\n",
                "        <MdbDN>{mdb_dn}</MdbDN>\n",
                "{ews_url_fields}",
                "        <Port>0</Port>\n",
                "        <DirectoryPort>0</DirectoryPort>\n",
                "        <ReferralPort>0</ReferralPort>\n",
                "        <SSL>On</SSL>\n",
                "        <AuthPackage>Basic</AuthPackage>\n",
                "        <CertPrincipalName>{cert_principal}</CertPrincipalName>\n",
                "        <LoginName>{email}</LoginName>\n",
                "      </Protocol>\n",
            ),
            mailbox_server = escape_xml(mailbox_server),
            server_dn = escape_xml(&server_dn),
            mdb_dn = escape_xml(&mdb_dn),
            cert_principal = escape_xml(&cert_principal),
            ews_url_fields = exchange_provider_ews_url_fields(config),
            email = escape_xml(email),
        ));
    }
    xml
}

fn mailbox_server_name(config: &PublishedEndpoints) -> &str {
    ews_host(&config.ews_url).unwrap_or_else(|| fallback_host(config))
}

fn fallback_host(config: &PublishedEndpoints) -> &str {
    config
        .imap_host
        .as_deref()
        .unwrap_or(&config.display_domain)
}

fn exchange_provider_ews_url_fields(config: &PublishedEndpoints) -> String {
    if !config.ews_enabled {
        return String::new();
    }
    format!(
        concat!(
            "        <ASUrl>{ews_url}</ASUrl>\n",
            "        <OOFUrl>{ews_url}</OOFUrl>\n",
            "        <EwsUrl>{ews_url}</EwsUrl>\n",
        ),
        ews_url = escape_xml(&config.ews_url),
    )
}

fn render_mapi_http_autodiscover_protocol(config: &PublishedEndpoints) -> String {
    format!(
        concat!(
            "      <Protocol Type=\"mapiHttp\" Version=\"1\">\n",
            "        <MailStore>\n",
            "          <InternalUrl>{emsmdb_url}</InternalUrl>\n",
            "          <ExternalUrl>{emsmdb_url}</ExternalUrl>\n",
            "        </MailStore>\n",
            "        <AddressBook>\n",
            "          <InternalUrl>{nspi_url}</InternalUrl>\n",
            "          <ExternalUrl>{nspi_url}</ExternalUrl>\n",
            "        </AddressBook>\n",
            "      </Protocol>\n"
        ),
        emsmdb_url = escape_xml(&config.mapi_emsmdb_url),
        nspi_url = escape_xml(&config.mapi_nspi_url),
    )
}

fn render_ews_web_autodiscover_protocol(config: &PublishedEndpoints, email: &str) -> String {
    format!(
        concat!(
            "      <Protocol>\n",
            "        <Type>WEB</Type>\n",
            "        <Server>{public_host}</Server>\n",
            "        <LoginName>{email}</LoginName>\n",
            "        <SSL>on</SSL>\n",
            "        <AuthPackage>Basic</AuthPackage>\n",
            "        <External>\n",
            "          <OWAUrl AuthenticationMethod=\"Basic\">{ews_url}</OWAUrl>\n",
            "          <ASUrl>{ews_url}</ASUrl>\n",
            "        </External>\n",
            "      </Protocol>\n"
        ),
        public_host =
            escape_xml(&ews_host(&config.ews_url).unwrap_or_else(|| fallback_host(config))),
        email = escape_xml(email),
        ews_url = escape_xml(&config.ews_url),
    )
}

fn render_mobilesync_autodiscover(config: &PublishedEndpoints, email: Option<&str>) -> String {
    let email = email.unwrap_or_default();
    format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n",
            "<Autodiscover xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"http://schemas.microsoft.com/exchange/autodiscover/responseschema/2006\">\n",
            "  <Response xmlns=\"http://schemas.microsoft.com/exchange/autodiscover/mobilesync/responseschema/2006\">\n",
            "    <Culture>en:us</Culture>\n",
            "    <User>\n",
            "      <DisplayName>{display_domain}</DisplayName>\n",
            "      <EMailAddress>{email}</EMailAddress>\n",
            "    </User>\n",
            "    <Action>\n",
            "      <Settings>\n",
            "        <Server>\n",
            "          <Type>MobileSync</Type>\n",
            "          <Url>{activesync_url}</Url>\n",
            "          <Name>{activesync_url}</Name>\n",
            "        </Server>\n",
            "      </Settings>\n",
            "    </Action>\n",
            "  </Response>\n",
            "</Autodiscover>\n"
        ),
        display_domain = escape_xml(&config.display_domain),
        email = escape_xml(email),
        activesync_url = escape_xml(&config.activesync_url),
    )
}

fn render_soap_user_settings_autodiscover(
    config: &PublishedEndpoints,
    email: Option<&str>,
) -> String {
    let email = email.unwrap_or_default();
    let ews_url = if config.ews_enabled {
        config.ews_url.as_str()
    } else {
        ""
    };
    let mailbox_server = ews_host(ews_url).unwrap_or_else(|| fallback_host(config));
    let legacy_dn = format!(
        "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={}",
        legacy_user(email, &config.display_domain)
    );
    let mailbox_dn = format!(
        "/o=LPE/ou=Exchange Administrative Group/cn=Configuration/cn=Servers/cn={}/cn=LPE Private MDB",
        mailbox_server
    );
    format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n",
            "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" ",
            "xmlns:a=\"http://schemas.microsoft.com/exchange/2010/Autodiscover\" ",
            "xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">\n",
            "  <s:Header>\n",
            "    <a:ServerVersionInfo MajorVersion=\"15\" MinorVersion=\"0\" MajorBuildNumber=\"0\" MinorBuildNumber=\"0\" Version=\"Exchange2013\"/>\n",
            "  </s:Header>\n",
            "  <s:Body>\n",
            "    <a:GetUserSettingsResponseMessage>\n",
            "      <a:Response>\n",
            "        <a:ErrorCode>NoError</a:ErrorCode>\n",
            "        <a:ErrorMessage>No error.</a:ErrorMessage>\n",
            "        <a:UserResponses>\n",
            "          <a:UserResponse>\n",
            "            <a:ErrorCode>NoError</a:ErrorCode>\n",
            "            <a:ErrorMessage>No error.</a:ErrorMessage>\n",
            "            <a:RedirectTarget/>\n",
            "            <a:UserSettingErrors/>\n",
            "            <a:UserSettings>\n",
            "{settings}",
            "            </a:UserSettings>\n",
            "          </a:UserResponse>\n",
            "        </a:UserResponses>\n",
            "      </a:Response>\n",
            "    </a:GetUserSettingsResponseMessage>\n",
            "  </s:Body>\n",
            "</s:Envelope>\n"
        ),
        settings = [
            soap_string_user_setting("UserDisplayName", &config.display_domain),
            soap_string_user_setting("UserDN", &legacy_dn),
            soap_string_user_setting("UserDeploymentId", &deployment_id(&config.display_domain)),
            soap_string_user_setting("AutoDiscoverSMTPAddress", email),
            soap_string_user_setting("ExternalMailboxServer", mailbox_server),
            soap_string_user_setting("InternalMailboxServer", mailbox_server),
            soap_string_user_setting("InternalRpcClientServer", mailbox_server),
            soap_string_user_setting("MailboxDN", &mailbox_dn),
            soap_string_user_setting("ActiveDirectoryServer", mailbox_server),
            soap_string_user_setting("PublicFolderServer", mailbox_server),
            soap_string_user_setting("ExternalMailboxServerRequiresSSL", "On"),
            soap_string_user_setting("ExternalServerExclusiveConnect", "On"),
            soap_string_user_setting("CasVersion", "15.00.0000.000"),
            soap_string_user_setting("GroupingInformation", &deployment_id(&config.display_domain)),
            soap_string_user_setting("UserMSOnline", "False"),
            soap_string_user_setting(
                "MapiHttpEnabled",
                if config.mapi_autodiscover_enabled() { "True" } else { "False" },
            ),
            soap_string_list_user_setting("ExternalMailboxServerAuthenticationMethods", &["Basic"]),
            soap_string_user_setting("ExternalEwsUrl", ews_url),
            soap_string_user_setting("InternalEwsUrl", ews_url),
            soap_string_list_user_setting(
                "EwsSupportedSchemas",
                &[
                    "Exchange2007",
                    "Exchange2007_SP1",
                    "Exchange2010",
                    "Exchange2010_SP1",
                    "Exchange2010_SP2",
                    "Exchange2013",
                ],
            ),
        ]
        .join("")
    )
}

fn render_soap_user_settings_response(
    config: &PublishedEndpoints,
    email: Option<&str>,
) -> Option<String> {
    config
        .soap_exchange_autodiscover_enabled()
        .then(|| render_soap_user_settings_autodiscover(config, email))
}

fn soap_string_user_setting(name: &str, value: &str) -> String {
    format!(
        concat!(
            "              <a:UserSetting i:type=\"a:StringSetting\">\n",
            "                <a:Name>{name}</a:Name>\n",
            "                <a:Value>{value}</a:Value>\n",
            "              </a:UserSetting>\n",
        ),
        name = escape_xml(name),
        value = escape_xml(value),
    )
}

fn soap_string_list_user_setting(name: &str, values: &[&str]) -> String {
    let values = values
        .iter()
        .map(|value| {
            format!(
                "                  <a:Value>{}</a:Value>\n",
                escape_xml(value)
            )
        })
        .collect::<String>();
    format!(
        concat!(
            "              <a:UserSetting i:type=\"a:StringListSetting\">\n",
            "                <a:Name>{name}</a:Name>\n",
            "                <a:Values>\n",
            "{values}",
            "                </a:Values>\n",
            "              </a:UserSetting>\n",
        ),
        name = escape_xml(name),
        values = values,
    )
}

fn parse_autodiscover_email(body: &[u8]) -> Option<String> {
    let body = String::from_utf8_lossy(body);
    xml_tag_value(&body, "EMailAddress")
        .or_else(|| xml_tag_value(&body, "Mailbox"))
        .or_else(|| xml_tag_value(&body, "EMail"))
        .filter(|value| value.contains('@'))
}

fn requested_soap_user_settings(body: &[u8]) -> bool {
    let body = String::from_utf8_lossy(body).to_ascii_lowercase();
    body.contains("getusersettingsrequestmessage") || body.contains("getusersettings")
}

fn requested_mobilesync_schema(body: &[u8]) -> bool {
    String::from_utf8_lossy(body)
        .to_ascii_lowercase()
        .contains("autodiscover/mobilesync/responseschema/2006")
}

fn xml_tag_value(body: &str, tag: &str) -> Option<String> {
    let lower_body = body.to_ascii_lowercase();
    let tag = tag.to_ascii_lowercase();
    let mut search_start = 0;

    while let Some(relative_open) = lower_body[search_start..].find('<') {
        let open = search_start + relative_open;
        if lower_body[open + 1..].starts_with('/') {
            search_start = open + 1;
            continue;
        }
        let Some(relative_close) = lower_body[open..].find('>') else {
            return None;
        };
        let open_end = open + relative_close;
        let element_name = lower_body[open + 1..open_end]
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .rsplit(':')
            .next()
            .unwrap_or_default();
        if element_name != tag {
            search_start = open_end + 1;
            continue;
        }

        let close_marker = format!("</");
        let mut close_search_start = open_end + 1;
        while let Some(relative_end_open) = lower_body[close_search_start..].find(&close_marker) {
            let end_open = close_search_start + relative_end_open;
            let Some(relative_end_close) = lower_body[end_open..].find('>') else {
                return None;
            };
            let end_close = end_open + relative_end_close;
            let closing_name = lower_body[end_open + 2..end_close]
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .rsplit(':')
                .next()
                .unwrap_or_default();
            if closing_name == tag {
                return Some(body[open_end + 1..end_open].trim().to_string());
            }
            close_search_start = end_close + 1;
        }
        return None;
    }

    None
}

fn public_host(headers: &HeaderMap) -> String {
    env::var("LPE_PUBLIC_HOSTNAME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| header_value(headers, "x-forwarded-host"))
        .or_else(|| header_value(headers, "host"))
        .unwrap_or_else(|| "localhost".to_string())
}

fn public_scheme(headers: &HeaderMap) -> String {
    env::var("LPE_PUBLIC_SCHEME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| header_value(headers, "x-forwarded-proto"))
        .and_then(|value| {
            value
                .split(',')
                .next()
                .map(|entry| entry.trim().to_string())
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https".to_string())
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.split(',').next().unwrap_or(value).trim().to_string())
}

fn safe_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(240).collect())
}

fn log_autodiscover_connection(
    method: &str,
    uri: &Uri,
    headers: &HeaderMap,
    endpoints: &PublishedEndpoints,
    email: Option<&str>,
    response_kind: &str,
    request_body_bytes: usize,
    response: &Response,
    request_body: Option<&[u8]>,
    response_body: Option<&str>,
    error: Option<&str>,
) {
    let status = response.status().as_u16();
    let trace_id = safe_header(headers, "x-trace-id").unwrap_or_default();
    let user_agent = safe_header(headers, "user-agent").unwrap_or_default();
    let x_request_id = safe_header(headers, "x-requestid").unwrap_or_default();
    let client_request_id = safe_header(headers, "client-request-id").unwrap_or_default();
    let x_mapi_http_capability = safe_header(headers, "x-mapihttpcapability").unwrap_or_default();
    let message = "rca debug autodiscover connection";
    trace_autodiscover_connection(
        method,
        uri,
        headers,
        email,
        response_kind,
        request_body,
        response,
        response_body,
    );

    if status < 400 {
        info!(
            rca_debug = true,
            adapter = "autodiscover",
            method = %method,
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            mailbox = %email.unwrap_or_default(),
            response_kind = %response_kind,
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            x_mapi_http_capability = %x_mapi_http_capability,
            mapi_enabled = endpoints.mapi_enabled,
            outlook_interop_gate_passed = endpoints.outlook_interop_gate_passed,
            mapi_http_requested = endpoints.mapi_http_requested,
            mapi_autodiscover_enabled = endpoints.mapi_autodiscover_enabled(),
            mapi_http_selected = endpoints.mapi_http_autodiscover_selected(),
            legacy_exch_autodiscover_enabled = endpoints.exch_autodiscover_enabled(),
            legacy_expr_autodiscover_enabled = endpoints.expr_autodiscover_enabled(),
            soap_exchange_autodiscover_enabled = endpoints.soap_exchange_autodiscover_enabled(),
            mapi_emsmdb_url = %endpoints.mapi_emsmdb_url,
            mapi_nspi_url = %endpoints.mapi_nspi_url,
            http_status = status,
            request_body_bytes,
            response_body = %response_body.unwrap_or_default(),
            user_agent = %user_agent,
            "{message}"
        );
    } else {
        warn!(
            rca_debug = true,
            adapter = "autodiscover",
            method = %method,
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            mailbox = %email.unwrap_or_default(),
            response_kind = %response_kind,
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            x_mapi_http_capability = %x_mapi_http_capability,
            mapi_enabled = endpoints.mapi_enabled,
            outlook_interop_gate_passed = endpoints.outlook_interop_gate_passed,
            mapi_http_requested = endpoints.mapi_http_requested,
            mapi_autodiscover_enabled = endpoints.mapi_autodiscover_enabled(),
            mapi_http_selected = endpoints.mapi_http_autodiscover_selected(),
            legacy_exch_autodiscover_enabled = endpoints.exch_autodiscover_enabled(),
            legacy_expr_autodiscover_enabled = endpoints.expr_autodiscover_enabled(),
            soap_exchange_autodiscover_enabled = endpoints.soap_exchange_autodiscover_enabled(),
            mapi_emsmdb_url = %endpoints.mapi_emsmdb_url,
            mapi_nspi_url = %endpoints.mapi_nspi_url,
            http_status = status,
            request_body_bytes,
            response_body = %response_body.unwrap_or_default(),
            user_agent = %user_agent,
            error = %error.unwrap_or_default(),
            "{message}"
        );
    }
}

fn trace_autodiscover_connection(
    method: &str,
    uri: &Uri,
    headers: &HeaderMap,
    email: Option<&str>,
    response_kind: &str,
    request_body: Option<&[u8]>,
    response: &Response,
    response_body: Option<&str>,
) {
    let session_key = safe_header(headers, "client-request-id")
        .or_else(|| safe_header(headers, "x-requestid"))
        .or_else(|| safe_header(headers, "x-trace-id"))
        .unwrap_or_else(|| {
            format!(
                "autodiscover:{}:{}:{}",
                method,
                uri.path(),
                email.unwrap_or_default()
            )
        });
    let remote_peer = safe_header(headers, "x-forwarded-for")
        .and_then(|value| value.split(',').next().map(|part| part.trim().to_string()))
        .filter(|value| !value.is_empty())
        .or_else(|| safe_header(headers, "x-real-ip"));
    let trace_id = safe_header(headers, "x-trace-id").unwrap_or_default();
    let client_request_id = safe_header(headers, "client-request-id").unwrap_or_default();
    let x_request_id = safe_header(headers, "x-requestid").unwrap_or_default();
    let user_agent = safe_header(headers, "user-agent").unwrap_or_default();
    let x_mapi_http_capability = safe_header(headers, "x-mapihttpcapability").unwrap_or_default();

    write_outlook_trace(&OutlookTraceEvent {
        component: "autodiscover",
        endpoint: "autodiscover",
        session_key: &session_key,
        direction: OutlookTraceDirection::Inbound,
        phase: response_kind,
        remote_peer: remote_peer.as_deref(),
        tenant_id: None,
        account: email,
        status: None,
        metadata: vec![
            ("method", method.to_string()),
            ("path", uri.path().to_string()),
            ("query", uri.query().unwrap_or_default().to_string()),
            ("trace_id", trace_id.clone()),
            ("client_request_id", client_request_id.clone()),
            ("x_request_id", x_request_id.clone()),
            ("x_mapi_http_capability", x_mapi_http_capability.clone()),
            ("user_agent", user_agent.clone()),
        ],
        payload: request_body,
    });
    write_outlook_trace(&OutlookTraceEvent {
        component: "autodiscover",
        endpoint: "autodiscover",
        session_key: &session_key,
        direction: OutlookTraceDirection::Outbound,
        phase: response_kind,
        remote_peer: remote_peer.as_deref(),
        tenant_id: None,
        account: email,
        status: Some(response.status().as_u16()),
        metadata: vec![
            ("method", method.to_string()),
            ("path", uri.path().to_string()),
            ("trace_id", trace_id),
            ("client_request_id", client_request_id),
            ("x_request_id", x_request_id),
            ("x_mapi_http_capability", x_mapi_http_capability),
            ("user_agent", user_agent),
        ],
        payload: response_body.map(str::as_bytes),
    });
}

fn host_without_port(value: &str) -> String {
    if let Some(inner) = value
        .strip_prefix('[')
        .and_then(|rest| rest.split_once(']'))
    {
        return inner.0.to_string();
    }

    match value.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') && port.parse::<u16>().is_ok() => {
            host.to_string()
        }
        _ => value.to_string(),
    }
}

fn read_u16_env(name: &str, default: u16) -> u16 {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u16>().ok())
        .unwrap_or(default)
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn email_domain(email: &str) -> Option<String> {
    email
        .rsplit_once('@')
        .map(|(_, domain)| domain.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn ews_host(ews_url: &str) -> Option<&str> {
    let after_scheme = ews_url.split_once("://").map(|(_, rest)| rest)?;
    after_scheme
        .split('/')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn legacy_user(email: &str, display_domain: &str) -> String {
    let source = if email.trim().is_empty() {
        display_domain
    } else {
        email
    };
    source
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

fn deployment_id(display_domain: &str) -> String {
    format!("lpe-{}", legacy_user(display_domain, display_domain))
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn xml_response(body: String) -> Response {
    let mut response = (StatusCode::OK, body).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/xml; charset=utf-8"),
    );
    response
}

#[cfg(test)]
mod tests;
