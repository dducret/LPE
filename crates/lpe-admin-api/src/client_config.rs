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
    xml_response(render_outlook_autodiscover(
        &PublishedEndpoints::from_headers(&headers, email.as_deref()),
        email.as_deref(),
    ))
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

        Self {
            display_domain,
            imap_host,
            imap_port,
            smtp_host,
            smtp_port,
            smtp_socket_type,
            ews_enabled,
            ews_url,
            jmap_session_url,
        }
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
            "<Autodiscover xmlns=\"http://schemas.microsoft.com/exchange/autodiscover/responseschema/2006\">\n",
            "  <Response xmlns=\"http://schemas.microsoft.com/exchange/autodiscover/outlook/responseschema/2006a\">\n",
            "    <User>\n",
            "      <DisplayName>{display_domain}</DisplayName>\n",
            "      <EMailAddress>{email}</EMailAddress>\n",
            "    </User>\n",
            "    <Account>\n",
            "      <AccountType>email</AccountType>\n",
            "      <Action>settings</Action>\n",
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
                "      </Protocol>\n"
            ),
            smtp_host = escape_xml(smtp_host),
            smtp_port = smtp_port,
            email = escape_xml(email),
        ));
    }

    if config.ews_enabled {
        xml.push_str(&format!(
            concat!(
                "      <Protocol>\n",
                "        <Type>EXCH</Type>\n",
                "        <Server>{imap_host}</Server>\n",
                "        <LoginName>{email}</LoginName>\n",
                "        <SSL>on</SSL>\n",
                "        <AuthPackage>Basic</AuthPackage>\n",
                "        <EwsUrl>{ews_url}</EwsUrl>\n",
                "      </Protocol>\n"
            ),
            imap_host = escape_xml(&config.imap_host),
            email = escape_xml(email),
            ews_url = escape_xml(&config.ews_url),
        ));
    }

    xml.push_str(concat!(
        "    </Account>\n",
        "  </Response>\n",
        "</Autodiscover>\n"
    ));
    xml
}

fn parse_autodiscover_email(body: &[u8]) -> Option<String> {
    let body = String::from_utf8_lossy(body);
    xml_tag_value(&body, "EMailAddress").filter(|value| value.contains('@'))
}

fn xml_tag_value(body: &str, tag: &str) -> Option<String> {
    let lower_body = body.to_ascii_lowercase();
    let open_tag = format!("<{}>", tag.to_ascii_lowercase());
    let close_tag = format!("</{}>", tag.to_ascii_lowercase());
    let start = lower_body.find(&open_tag)? + open_tag.len();
    let end = lower_body[start..].find(&close_tag)? + start;
    Some(body[start..end].trim().to_string())
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
        HeaderValue::from_static("application/xml; charset=utf-8"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::{
        parse_autodiscover_email, render_outlook_autodiscover, render_thunderbird_autoconfig,
        PublishedEndpoints,
    };
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
        assert!(!xml.contains("<Type>MobileSync</Type>"));
        assert!(!xml.contains("<ASUrl>"));
        assert!(!xml.contains("<Type>EXCH</Type>"));
        assert!(!xml.contains("<EwsUrl>"));
        assert!(xml.contains("<EMailAddress>alice@example.test</EMailAddress>"));
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
    }

    #[test]
    fn outlook_autodiscover_can_publish_explicit_ews_endpoint() {
        let config = PublishedEndpoints {
            ews_enabled: true,
            ews_url: "https://mail.example.test/EWS/Exchange.asmx".to_string(),
            ..sample_config()
        };

        let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

        assert!(xml.contains("<Type>EXCH</Type>"));
        assert!(xml.contains("<EwsUrl>https://mail.example.test/EWS/Exchange.asmx</EwsUrl>"));
        assert!(!xml.contains("<Type>MobileSync</Type>"));
        assert!(!xml.contains("<Type>MAPI</Type>"));
    }

    #[test]
    fn autodiscover_request_parser_extracts_email_address() {
        let email = parse_autodiscover_email(
            br#"<?xml version="1.0"?><Autodiscover><Request><EMailAddress>alice@example.test</EMailAddress></Request></Autodiscover>"#,
        );

        assert_eq!(email.as_deref(), Some("alice@example.test"));
    }
}
