use axum::{
    body::Bytes,
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use lpe_storage::Storage;
use std::env;

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
        .route(
            "/autodiscover/autodiscover.xml",
            get(outlook_autodiscover_get).post(outlook_autodiscover_post),
        )
        .route(
            "/Autodiscover/Autodiscover.xml",
            get(outlook_autodiscover_get).post(outlook_autodiscover_post),
        )
}

async fn thunderbird_autoconfig(headers: HeaderMap) -> Response {
    xml_response(render_thunderbird_autoconfig(
        &PublishedEndpoints::from_headers(&headers, None),
    ))
}

async fn outlook_autodiscover_get(headers: HeaderMap) -> Response {
    xml_response(render_outlook_autodiscover(
        &PublishedEndpoints::from_headers(&headers, None),
        None,
    ))
}

async fn outlook_autodiscover_post(headers: HeaderMap, body: Bytes) -> Response {
    let email = parse_autodiscover_email(body.as_ref());
    let endpoints = PublishedEndpoints::from_headers(&headers, email.as_deref());
    let response = if requested_soap_user_settings(body.as_ref()) {
        match render_soap_user_settings_response(&endpoints, email.as_deref()) {
            Some(response) => response,
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    "SOAP Autodiscover is not published for the default Outlook IMAP profile.\n",
                )
                    .into_response();
            }
        }
    } else if requested_mobilesync_schema(body.as_ref()) {
        render_mobilesync_autodiscover(&endpoints, email.as_deref())
    } else {
        render_outlook_autodiscover(&endpoints, email.as_deref())
    };
    xml_response(response)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublishedEndpoints {
    display_domain: String,
    imap_host: String,
    imap_port: u16,
    smtp_host: Option<String>,
    smtp_port: Option<u16>,
    smtp_socket_type: Option<String>,
    ews_enabled: bool,
    ews_url: String,
    mapi_enabled: bool,
    mapi_http_requested: bool,
    legacy_exchange_autodiscover_enabled: bool,
    soap_exchange_autodiscover_enabled: bool,
    mapi_emsmdb_url: String,
    mapi_nspi_url: String,
    activesync_url: String,
    jmap_session_url: String,
}

impl PublishedEndpoints {
    fn from_headers(headers: &HeaderMap, email_hint: Option<&str>) -> Self {
        let public_host = public_host(headers);
        let public_host_name = host_without_port(&public_host);
        let public_scheme = public_scheme(headers);
        let display_domain = email_hint
            .and_then(email_domain)
            .unwrap_or_else(|| public_host_name.clone());

        let imap_host =
            env::var("LPE_AUTOCONFIG_IMAP_HOST").unwrap_or_else(|_| public_host_name.clone());
        let imap_port = read_u16_env("LPE_AUTOCONFIG_IMAP_PORT", 993);

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
        let ews_enabled = env_flag("LPE_AUTOCONFIG_EWS_ENABLED");
        let ews_url = env::var("LPE_AUTOCONFIG_EWS_URL")
            .unwrap_or_else(|_| format!("{public_scheme}://{public_host}/EWS/Exchange.asmx"));
        let mapi_enabled = env_flag("LPE_AUTOCONFIG_MAPI_ENABLED");
        let mapi_http_requested = headers
            .get("x-mapihttpcapability")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| !value.trim().is_empty());
        let legacy_exchange_autodiscover_enabled =
            env_flag("LPE_AUTOCONFIG_LEGACY_EXCHANGE_AUTODISCOVER_ENABLED");
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
            mapi_http_requested,
            legacy_exchange_autodiscover_enabled,
            soap_exchange_autodiscover_enabled,
            mapi_emsmdb_url,
            mapi_nspi_url,
            activesync_url,
            jmap_session_url,
        }
    }

    fn exchange_autodiscover_enabled(&self) -> bool {
        self.ews_enabled || self.mapi_enabled
    }

    fn soap_exchange_autodiscover_enabled(&self) -> bool {
        self.soap_exchange_autodiscover_enabled && self.exchange_autodiscover_enabled()
    }
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
    xml.push_str("    <incomingServer type=\"imap\">\n");
    xml.push_str(&format!(
        "      <hostname>{}</hostname>\n",
        escape_xml(&config.imap_host)
    ));
    xml.push_str(&format!("      <port>{}</port>\n", config.imap_port));
    xml.push_str("      <socketType>SSL</socketType>\n");
    xml.push_str("      <authentication>password-cleartext</authentication>\n");
    xml.push_str("      <username>%EMAILADDRESS%</username>\n");
    xml.push_str("    </incomingServer>\n");
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
            "      <MicrosoftOnline>False</MicrosoftOnline>\n",
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
        display_domain = escape_xml(&config.display_domain),
        email = escape_xml(email),
        legacy_user = escape_xml(&legacy_user(email, &config.display_domain)),
        deployment_id = escape_xml(&deployment_id(&config.display_domain)),
        imap_host = escape_xml(&config.imap_host),
        imap_port = config.imap_port,
    );

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

    if config.mapi_enabled
        && config.legacy_exchange_autodiscover_enabled
        && !config.mapi_http_requested
    {
        xml.push_str(&render_exchange_provider_autodiscover_protocols(
            config, email,
        ));
    }
    if config.ews_enabled && !config.mapi_enabled {
        xml.push_str(&render_ews_web_autodiscover_protocol(config, email));
    }
    if config.mapi_enabled && config.mapi_http_requested {
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
    let mailbox_server = ews_host(&config.ews_url).unwrap_or(&config.imap_host);
    let server_dn = format!(
        "/o=LPE/ou=Exchange Administrative Group/cn=Configuration/cn=Servers/cn={mailbox_server}"
    );
    let mdb_dn = format!("{server_dn}/cn=LPE Private MDB");
    let cert_principal = format!("msstd:{mailbox_server}");
    format!(
        concat!(
            "      <Protocol>\n",
            "        <Type>EXCH</Type>\n",
            "        <Server>{mailbox_server}</Server>\n",
            "        <ServerDN>{server_dn}</ServerDN>\n",
            "        <MdbDN>{mdb_dn}</MdbDN>\n",
            "        <ASUrl>{ews_url}</ASUrl>\n",
            "        <OOFUrl>{ews_url}</OOFUrl>\n",
            "        <EwsUrl>{ews_url}</EwsUrl>\n",
            "        <Port>0</Port>\n",
            "        <DirectoryPort>0</DirectoryPort>\n",
            "        <ReferralPort>0</ReferralPort>\n",
            "        <AD>{mailbox_server}</AD>\n",
            "        <PublicFolderServer>{mailbox_server}</PublicFolderServer>\n",
            "        <ServerExclusiveConnect>off</ServerExclusiveConnect>\n",
            "      </Protocol>\n",
            "      <Protocol>\n",
            "        <Type>EXPR</Type>\n",
            "        <Server>{mailbox_server}</Server>\n",
            "        <ServerDN>{server_dn}</ServerDN>\n",
            "        <MdbDN>{mdb_dn}</MdbDN>\n",
            "        <ASUrl>{ews_url}</ASUrl>\n",
            "        <OOFUrl>{ews_url}</OOFUrl>\n",
            "        <EwsUrl>{ews_url}</EwsUrl>\n",
            "        <Port>0</Port>\n",
            "        <DirectoryPort>0</DirectoryPort>\n",
            "        <ReferralPort>0</ReferralPort>\n",
            "        <SSL>On</SSL>\n",
            "        <AuthPackage>Basic</AuthPackage>\n",
            "        <CertPrincipalName>{cert_principal}</CertPrincipalName>\n",
            "        <LoginName>{email}</LoginName>\n",
            "      </Protocol>\n",
            "      <Protocol>\n",
            "        <Type>WEB</Type>\n",
            "        <Server>{mailbox_server}</Server>\n",
            "        <LoginName>{email}</LoginName>\n",
            "        <SSL>on</SSL>\n",
            "        <AuthPackage>Basic</AuthPackage>\n",
            "        <External>\n",
            "          <OWAUrl AuthenticationMethod=\"Basic\">{ews_url}</OWAUrl>\n",
            "          <Protocol>\n",
            "            <Type>EXPR</Type>\n",
            "            <ASUrl>{ews_url}</ASUrl>\n",
            "          </Protocol>\n",
            "        </External>\n",
            "        <Internal>\n",
            "          <OWAUrl AuthenticationMethod=\"Basic\">{ews_url}</OWAUrl>\n",
            "          <Protocol>\n",
            "            <Type>EXCH</Type>\n",
            "            <ASUrl>{ews_url}</ASUrl>\n",
            "          </Protocol>\n",
            "        </Internal>\n",
            "      </Protocol>\n"
        ),
        mailbox_server = escape_xml(mailbox_server),
        server_dn = escape_xml(&server_dn),
        mdb_dn = escape_xml(&mdb_dn),
        ews_url = escape_xml(&config.ews_url),
        cert_principal = escape_xml(&cert_principal),
        email = escape_xml(email),
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
            "          <Protocol>\n",
            "            <Type>EXPR</Type>\n",
            "            <ASUrl>{ews_url}</ASUrl>\n",
            "          </Protocol>\n",
            "        </External>\n",
            "      </Protocol>\n"
        ),
        public_host = escape_xml(&ews_host(&config.ews_url).unwrap_or(&config.imap_host)),
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
    let mailbox_server = ews_host(ews_url).unwrap_or(&config.imap_host);
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
                if config.mapi_enabled { "True" } else { "False" },
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
mod tests {
    use super::{
        parse_autodiscover_email, render_mobilesync_autodiscover, render_outlook_autodiscover,
        render_soap_user_settings_autodiscover, render_soap_user_settings_response,
        render_thunderbird_autoconfig, requested_mobilesync_schema, requested_soap_user_settings,
        PublishedEndpoints,
    };
    use axum::http::HeaderMap;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn sample_config() -> PublishedEndpoints {
        PublishedEndpoints {
            display_domain: "example.test".to_string(),
            imap_host: "mail.example.test".to_string(),
            imap_port: 993,
            smtp_host: None,
            smtp_port: None,
            smtp_socket_type: None,
            ews_enabled: false,
            ews_url: "https://mail.example.test/EWS/Exchange.asmx".to_string(),
            mapi_enabled: false,
            mapi_http_requested: false,
            legacy_exchange_autodiscover_enabled: false,
            soap_exchange_autodiscover_enabled: false,
            mapi_emsmdb_url: "https://mail.example.test/mapi/emsmdb/?MailboxId=alice@example.test"
                .to_string(),
            mapi_nspi_url: "https://mail.example.test/mapi/nspi/?MailboxId=alice@example.test"
                .to_string(),
            activesync_url: "https://mail.example.test/Microsoft-Server-ActiveSync".to_string(),
            jmap_session_url: "https://mail.example.test/api/jmap/session".to_string(),
        }
    }

    #[test]
    fn thunderbird_autoconfig_defaults_to_imap_only() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("LPE_AUTOCONFIG_SMTP_HOST");
        let xml = render_thunderbird_autoconfig(&sample_config());

        assert!(xml.contains("<incomingServer type=\"imap\">"));
        assert!(!xml.contains("<outgoingServer type=\"smtp\">"));
        assert!(xml.contains("https://mail.example.test/api/jmap/session"));
    }

    #[test]
    fn thunderbird_autoconfig_can_publish_explicit_submission_endpoint() {
        let config = PublishedEndpoints {
            smtp_host: Some("submit.example.test".to_string()),
            smtp_port: Some(465),
            smtp_socket_type: Some("SSL".to_string()),
            ..sample_config()
        };

        let xml = render_thunderbird_autoconfig(&config);

        assert!(xml.contains("<outgoingServer type=\"smtp\">"));
        assert!(xml.contains("<hostname>submit.example.test</hostname>"));
        assert!(xml.contains("<port>465</port>"));
    }

    #[test]
    fn outlook_autodiscover_publishes_imap_without_forcing_exchange_activesync() {
        let xml = render_outlook_autodiscover(&sample_config(), Some("alice@example.test"));

        assert!(xml.contains("<Type>IMAP</Type>"));
        assert!(xml.contains("<Server>mail.example.test</Server>"));
        assert!(xml.contains("<Port>993</Port>"));
        assert!(xml.contains("<MicrosoftOnline>False</MicrosoftOnline>"));
        assert!(!xml.contains("<Type>MobileSync</Type>"));
        assert!(!xml.contains("<ASUrl>"));
        assert!(!xml.contains("Type=\"mapiHttp\""));
        assert!(!xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
        assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
        assert!(!xml.contains("<EwsUrl>"));
        assert!(!xml.contains("<EMailAddress>alice@example.test</EMailAddress>"));
    }

    #[test]
    fn outlook_autodiscover_includes_smtp_only_when_explicitly_configured() {
        let config = PublishedEndpoints {
            smtp_host: Some("submit.example.test".to_string()),
            smtp_port: Some(465),
            smtp_socket_type: Some("SSL".to_string()),
            ..sample_config()
        };

        let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

        assert!(xml.contains("<Type>IMAP</Type>"));
        assert!(xml.contains("<Type>SMTP</Type>"));
        assert!(xml.contains("<Server>submit.example.test</Server>"));
        assert!(xml.contains("<Port>465</Port>"));
        assert!(xml.contains("<UsePOPAuth>off</UsePOPAuth>"));
        assert!(xml.contains("<SMTPLast>off</SMTPLast>"));
    }

    #[test]
    fn outlook_autodiscover_can_publish_explicit_ews_web_endpoint() {
        let config = PublishedEndpoints {
            ews_enabled: true,
            ews_url: "https://mail.example.test/EWS/Exchange.asmx".to_string(),
            ..sample_config()
        };

        let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

        assert!(xml.contains("<Type>WEB</Type>"));
        assert!(xml.contains("<OWAUrl AuthenticationMethod=\"Basic\">https://mail.example.test/EWS/Exchange.asmx</OWAUrl>"));
        assert!(xml.contains("<Type>EXPR</Type>"));
        assert!(xml.contains("<ASUrl>https://mail.example.test/EWS/Exchange.asmx</ASUrl>"));
        assert!(xml.contains("<Server>mail.example.test</Server>"));
        assert!(!xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
        assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
        assert!(!xml.contains("<Type>MobileSync</Type>"));
        assert!(!xml.contains("<Type>MAPI</Type>"));
    }

    #[test]
    fn outlook_autodiscover_can_publish_explicit_mapi_http_protocol() {
        let config = PublishedEndpoints {
            mapi_enabled: true,
            mapi_http_requested: true,
            mapi_emsmdb_url: "https://mail.example.test/mapi/emsmdb/?MailboxId=alice@example.test"
                .to_string(),
            mapi_nspi_url: "https://mail.example.test/mapi/nspi/?MailboxId=alice@example.test"
                .to_string(),
            ..sample_config()
        };

        let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

        assert!(xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));
        assert!(xml.contains("<MailStore>"));
        assert!(xml.contains("<InternalUrl>https://mail.example.test/mapi/emsmdb/?MailboxId=alice@example.test</InternalUrl>"));
        assert!(xml.contains("<ExternalUrl>https://mail.example.test/mapi/emsmdb/?MailboxId=alice@example.test</ExternalUrl>"));
        assert!(xml.contains("<AddressBook>"));
        assert!(xml.contains("<InternalUrl>https://mail.example.test/mapi/nspi/?MailboxId=alice@example.test</InternalUrl>"));
        assert!(xml.contains("<ExternalUrl>https://mail.example.test/mapi/nspi/?MailboxId=alice@example.test</ExternalUrl>"));
        assert!(!xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
        assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
    }

    #[test]
    fn outlook_autodiscover_can_publish_exchange_providers_for_legacy_mapi_probe() {
        let config = PublishedEndpoints {
            mapi_enabled: true,
            mapi_http_requested: false,
            legacy_exchange_autodiscover_enabled: true,
            ..sample_config()
        };

        let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

        assert!(xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
        assert!(xml.contains("<ServerDN>/o=LPE/ou=Exchange Administrative Group/cn=Configuration/cn=Servers/cn=mail.example.test</ServerDN>"));
        assert!(xml.contains("<MdbDN>/o=LPE/ou=Exchange Administrative Group/cn=Configuration/cn=Servers/cn=mail.example.test/cn=LPE Private MDB</MdbDN>"));
        assert!(xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
        assert!(xml.contains("<AuthPackage>Basic</AuthPackage>"));
        assert!(xml.contains("<CertPrincipalName>msstd:mail.example.test</CertPrincipalName>"));
        assert!(xml.contains("<EwsUrl>https://mail.example.test/EWS/Exchange.asmx</EwsUrl>"));
        assert!(!xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));
    }

    #[test]
    fn mapi_enabled_does_not_hijack_default_outlook_imap_profile() {
        let config = PublishedEndpoints {
            mapi_enabled: true,
            mapi_http_requested: false,
            legacy_exchange_autodiscover_enabled: false,
            ..sample_config()
        };

        let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

        assert!(xml.contains("<Type>IMAP</Type>"));
        assert!(!xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
        assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
        assert!(!xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));
        assert!(!xml.contains("<Type>WEB</Type>"));
    }

    #[test]
    fn mapi_autodiscover_publication_is_env_opt_in() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("LPE_AUTOCONFIG_MAPI_ENABLED", "true");
        std::env::remove_var("LPE_AUTOCONFIG_LEGACY_EXCHANGE_AUTODISCOVER_ENABLED");
        std::env::remove_var("LPE_AUTOCONFIG_MAPI_EMSMDB_URL");
        std::env::remove_var("LPE_AUTOCONFIG_MAPI_NSPI_URL");
        std::env::remove_var("LPE_PUBLIC_HOSTNAME");
        std::env::remove_var("LPE_PUBLIC_SCHEME");

        let mut headers = HeaderMap::new();
        headers.insert("host", "mail.example.test".parse().unwrap());
        headers.insert("x-mapihttpcapability", "1".parse().unwrap());
        let config = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
        let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

        assert!(config.mapi_enabled);
        assert!(config.mapi_http_requested);
        assert!(!config.legacy_exchange_autodiscover_enabled);
        assert_eq!(
            config.mapi_emsmdb_url,
            "https://mail.example.test/mapi/emsmdb/?MailboxId=alice@example.test"
        );
        assert_eq!(
            config.mapi_nspi_url,
            "https://mail.example.test/mapi/nspi/?MailboxId=alice@example.test"
        );
        assert!(xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));

        std::env::remove_var("LPE_AUTOCONFIG_MAPI_ENABLED");
    }

    #[test]
    fn legacy_exchange_autodiscover_publication_has_separate_opt_in() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("LPE_AUTOCONFIG_MAPI_ENABLED", "true");
        std::env::set_var(
            "LPE_AUTOCONFIG_LEGACY_EXCHANGE_AUTODISCOVER_ENABLED",
            "true",
        );
        std::env::remove_var("LPE_PUBLIC_HOSTNAME");
        std::env::remove_var("LPE_PUBLIC_SCHEME");

        let mut headers = HeaderMap::new();
        headers.insert("host", "mail.example.test".parse().unwrap());
        let config = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
        let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

        assert!(config.mapi_enabled);
        assert!(config.legacy_exchange_autodiscover_enabled);
        assert!(xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
        assert!(xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
        assert!(!xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));

        std::env::remove_var("LPE_AUTOCONFIG_MAPI_ENABLED");
        std::env::remove_var("LPE_AUTOCONFIG_LEGACY_EXCHANGE_AUTODISCOVER_ENABLED");
    }

    #[test]
    fn outlook_autodiscover_ews_publication_is_env_opt_in() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("LPE_AUTOCONFIG_EWS_ENABLED", "true");
        std::env::remove_var("LPE_AUTOCONFIG_EWS_URL");
        std::env::remove_var("LPE_PUBLIC_HOSTNAME");
        std::env::remove_var("LPE_PUBLIC_SCHEME");

        let mut headers = HeaderMap::new();
        headers.insert("host", "mail.example.test".parse().unwrap());
        let config = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
        let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

        assert!(config.ews_enabled);
        assert_eq!(
            config.ews_url,
            "https://mail.example.test/EWS/Exchange.asmx"
        );
        assert!(xml.contains("<Type>WEB</Type>"));
        assert!(xml.contains("<Type>EXPR</Type>"));
        assert!(xml.contains("<ASUrl>https://mail.example.test/EWS/Exchange.asmx</ASUrl>"));
        assert!(!xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
        assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));

        std::env::remove_var("LPE_AUTOCONFIG_EWS_ENABLED");
    }

    #[test]
    fn autodiscover_request_parser_extracts_email_address() {
        let email = parse_autodiscover_email(
            br#"<?xml version="1.0"?><Autodiscover><Request><EMailAddress>alice@example.test</EMailAddress></Request></Autodiscover>"#,
        );

        assert_eq!(email.as_deref(), Some("alice@example.test"));
    }

    #[test]
    fn autodiscover_request_parser_extracts_namespaced_email_address() {
        let email = parse_autodiscover_email(
            br#"<?xml version="1.0" encoding="utf-8"?>
<a:Autodiscover xmlns:a="http://schemas.microsoft.com/exchange/autodiscover/outlook/requestschema/2006">
  <a:Request>
    <a:EMailAddress>test@l-p-e.ch</a:EMailAddress>
    <a:AcceptableResponseSchema>http://schemas.microsoft.com/exchange/autodiscover/outlook/responseschema/2006a</a:AcceptableResponseSchema>
  </a:Request>
</a:Autodiscover>"#,
        );

        assert_eq!(email.as_deref(), Some("test@l-p-e.ch"));
    }

    #[test]
    fn autodiscover_request_parser_extracts_soap_mailbox() {
        let email = parse_autodiscover_email(
            br#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body><a:GetUserSettingsRequestMessage><a:Request><a:Users><a:User><a:Mailbox>test@l-p-e.ch</a:Mailbox></a:User></a:Users></a:Request></a:GetUserSettingsRequestMessage></s:Body>
</s:Envelope>"#,
        );

        assert_eq!(email.as_deref(), Some("test@l-p-e.ch"));
    }

    #[test]
    fn autodiscover_detects_mobilesync_response_schema_request() {
        assert!(requested_mobilesync_schema(
            br#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover xmlns="http://schemas.microsoft.com/exchange/autodiscover/mobilesync/requestschema/2006">
  <Request>
    <EMailAddress>test@l-p-e.ch</EMailAddress>
    <AcceptableResponseSchema>http://schemas.microsoft.com/exchange/autodiscover/mobilesync/responseschema/2006</AcceptableResponseSchema>
  </Request>
</Autodiscover>"#
        ));
    }

    #[test]
    fn mobilesync_autodiscover_publishes_activesync_endpoint() {
        let xml = render_mobilesync_autodiscover(&sample_config(), Some("alice@example.test"));

        assert!(xml.contains(
            "<Response xmlns=\"http://schemas.microsoft.com/exchange/autodiscover/mobilesync/responseschema/2006\">"
        ));
        assert!(xml.contains("<Type>MobileSync</Type>"));
        assert!(xml.contains("<Url>https://mail.example.test/Microsoft-Server-ActiveSync</Url>"));
        assert!(xml.contains("<EMailAddress>alice@example.test</EMailAddress>"));
        assert!(!xml.contains("<Type>IMAP</Type>"));
    }

    #[test]
    fn soap_autodiscover_publishes_ews_user_settings_when_enabled() {
        let config = PublishedEndpoints {
            ews_enabled: true,
            ews_url: "https://mail.example.test/EWS/Exchange.asmx".to_string(),
            ..sample_config()
        };

        let xml = render_soap_user_settings_autodiscover(&config, Some("alice@example.test"));

        assert!(xml.contains("<s:Envelope"));
        assert!(xml.contains("<a:GetUserSettingsResponseMessage>"));
        assert!(xml.contains("<a:Name>ExternalEwsUrl</a:Name>"));
        assert!(xml.contains("<a:Value>https://mail.example.test/EWS/Exchange.asmx</a:Value>"));
        assert!(xml.contains("<a:Name>InternalEwsUrl</a:Name>"));
        assert!(xml.contains("<a:Name>MailboxDN</a:Name>"));
        assert!(xml.contains("<a:Name>ExternalMailboxServerRequiresSSL</a:Name>"));
        assert!(xml.contains("<a:Name>ExternalMailboxServerAuthenticationMethods</a:Name>"));
        assert!(xml.contains("<a:Name>MapiHttpEnabled</a:Name>"));
        assert!(xml.contains("<a:Value>False</a:Value>"));
        assert!(xml.contains("<a:Name>EwsSupportedSchemas</a:Name>"));
        assert!(xml.contains("<a:Value>Exchange2013</a:Value>"));
        assert!(!xml.contains("<Type>MAPI</Type>"));
    }

    #[test]
    fn soap_autodiscover_is_not_published_for_default_outlook_imap_profile() {
        let xml = render_soap_user_settings_response(&sample_config(), Some("alice@example.test"));

        assert!(xml.is_none());
    }

    #[test]
    fn soap_autodiscover_requires_separate_publication_opt_in() {
        let ews_config = PublishedEndpoints {
            ews_enabled: true,
            ..sample_config()
        };
        let mapi_config = PublishedEndpoints {
            mapi_enabled: true,
            ..sample_config()
        };

        assert!(
            render_soap_user_settings_response(&ews_config, Some("alice@example.test")).is_none()
        );
        assert!(
            render_soap_user_settings_response(&mapi_config, Some("alice@example.test")).is_none()
        );
    }

    #[test]
    fn soap_exchange_autodiscover_publication_is_env_opt_in() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("LPE_AUTOCONFIG_EWS_ENABLED", "true");
        std::env::set_var("LPE_AUTOCONFIG_SOAP_EXCHANGE_AUTODISCOVER_ENABLED", "true");
        std::env::remove_var("LPE_AUTOCONFIG_EWS_URL");
        std::env::remove_var("LPE_PUBLIC_HOSTNAME");
        std::env::remove_var("LPE_PUBLIC_SCHEME");

        let mut headers = HeaderMap::new();
        headers.insert("host", "mail.example.test".parse().unwrap());
        let config = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
        let xml = render_soap_user_settings_response(&config, Some("alice@example.test"))
            .expect("explicit SOAP Exchange Autodiscover opt-in should publish user settings");

        assert!(config.ews_enabled);
        assert!(config.soap_exchange_autodiscover_enabled);
        assert!(xml.contains("<a:Name>ExternalEwsUrl</a:Name>"));
        assert!(xml.contains("<a:Value>https://mail.example.test/EWS/Exchange.asmx</a:Value>"));

        std::env::remove_var("LPE_AUTOCONFIG_EWS_ENABLED");
        std::env::remove_var("LPE_AUTOCONFIG_SOAP_EXCHANGE_AUTODISCOVER_ENABLED");
    }

    #[test]
    fn soap_autodiscover_reports_mapi_http_enabled_when_opted_in() {
        let config = PublishedEndpoints {
            mapi_enabled: true,
            soap_exchange_autodiscover_enabled: true,
            ..sample_config()
        };

        let xml = render_soap_user_settings_response(&config, Some("alice@example.test"))
            .expect("MAPI opt-in should publish SOAP Autodiscover");

        assert!(xml.contains("<a:Name>MapiHttpEnabled</a:Name>"));
        assert!(xml.contains("<a:Value>True</a:Value>"));
    }

    #[test]
    fn autodiscover_detects_soap_get_user_settings_request() {
        assert!(requested_soap_user_settings(
            br#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body><a:GetUserSettingsRequestMessage><a:Request><a:Users><a:User><a:Mailbox>test@l-p-e.ch</a:Mailbox></a:User></a:Users></a:Request></a:GetUserSettingsRequestMessage></s:Body>
</s:Envelope>"#
        ));
    }

    #[test]
    fn outlook_autodiscover_includes_required_pox_user_fields() {
        let xml = render_outlook_autodiscover(&sample_config(), Some("alice@example.test"));

        assert!(xml.contains("<LegacyDN>/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice-example-test</LegacyDN>"));
        assert!(
            xml.contains("<AutoDiscoverSMTPAddress>alice@example.test</AutoDiscoverSMTPAddress>")
        );
        assert!(xml.contains("<DeploymentId>lpe-example-test</DeploymentId>"));
        assert!(!xml.contains("<EMailAddress>alice@example.test</EMailAddress>"));
    }
}
