use super::{
    autodiscover_json_invalid_protocol_response, jmap_well_known_location,
    outlook_autodiscover_json, parse_autodiscover_email, render_autodiscover_json,
    render_mobilesync_autodiscover, render_outlook_autodiscover,
    render_soap_user_settings_autodiscover, render_soap_user_settings_response,
    render_thunderbird_autoconfig, requested_mobilesync_schema, requested_soap_user_settings,
    AutodiscoverJsonQuery, PublishedEndpoints,
};
use axum::{body, extract::Path, extract::Query, http::HeaderMap, http::Uri};
use quick_xml::{events::Event, Reader};
use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone)]
struct XmlNode {
    name: String,
    text: String,
    children: Vec<XmlNode>,
}

impl XmlNode {
    fn child(&self, name: &str) -> Option<&XmlNode> {
        self.children.iter().find(|child| child.name == name)
    }

    fn child_text(&self, name: &str) -> Option<&str> {
        self.child(name)
            .map(|child| child.text.trim())
            .filter(|text| !text.is_empty())
    }

    fn children_named<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a XmlNode> + 'a {
        self.children.iter().filter(move |child| child.name == name)
    }
}

fn parse_xml(xml: &str) -> XmlNode {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut stack = vec![XmlNode {
        name: String::new(),
        text: String::new(),
        children: Vec::new(),
    }];
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf).unwrap() {
            Event::Start(start) => stack.push(XmlNode {
                name: String::from_utf8_lossy(local_name(start.name().as_ref())).to_string(),
                text: String::new(),
                children: Vec::new(),
            }),
            Event::Empty(empty) => {
                let node = XmlNode {
                    name: String::from_utf8_lossy(local_name(empty.name().as_ref())).to_string(),
                    text: String::new(),
                    children: Vec::new(),
                };
                stack.last_mut().unwrap().children.push(node);
            }
            Event::Text(text) => {
                stack
                    .last_mut()
                    .unwrap()
                    .text
                    .push_str(&text.xml_content().unwrap());
            }
            Event::End(_) => {
                let node = stack.pop().unwrap();
                stack.last_mut().unwrap().children.push(node);
            }
            Event::Eof => break,
            _ => {}
        }

        buf.clear();
    }

    stack
        .pop()
        .unwrap()
        .children
        .pop()
        .expect("POX XML should have a document root")
}

fn local_name(name: &[u8]) -> &[u8] {
    name.rsplit(|byte| *byte == b':').next().unwrap_or(name)
}

fn outlook_account(xml: &str) -> XmlNode {
    parse_xml(xml)
        .child("Response")
        .and_then(|response| response.child("Account"))
        .cloned()
        .expect("POX Autodiscover response should contain Account")
}

fn web_protocol(account: &XmlNode) -> &XmlNode {
    account
        .children_named("Protocol")
        .find(|protocol| protocol.child_text("Type") == Some("WEB"))
        .expect("POX Autodiscover response should contain WEB protocol")
}

fn sample_config() -> PublishedEndpoints {
    PublishedEndpoints {
        display_domain: "example.test".to_string(),
        imap_host: Some("mail.example.test".to_string()),
        imap_port: Some(993),
        smtp_host: None,
        smtp_port: None,
        smtp_socket_type: None,
        ews_enabled: false,
        ews_url: "https://mail.example.test/EWS/Exchange.asmx".to_string(),
        mapi_enabled: false,
        outlook_interop_gate_passed: false,
        mapi_http_requested: false,
        legacy_exch_autodiscover_enabled: false,
        legacy_expr_autodiscover_enabled: false,
        rpc_proxy_enabled: false,
        soap_exchange_autodiscover_enabled: false,
        mapi_emsmdb_url: "https://mail.example.test/mapi/emsmdb/?MailboxId=alice@example.test"
            .to_string(),
        mapi_nspi_url: "https://mail.example.test/mapi/nspi/?MailboxId=alice@example.test"
            .to_string(),
        activesync_url: "https://mail.example.test/Microsoft-Server-ActiveSync".to_string(),
        webmail_url: "https://mail.example.test/mail/".to_string(),
        jmap_session_url: "https://mail.example.test/api/jmap/session".to_string(),
        autodiscover_xml_url: "https://mail.example.test/autodiscover/autodiscover.xml".to_string(),
    }
}

#[test]
fn thunderbird_autoconfig_publishes_imap_only_when_edge_imaps_is_configured() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("LPE_AUTOCONFIG_IMAP_HOST");
    std::env::remove_var("LPE_AUTOCONFIG_IMAP_PORT");
    std::env::remove_var("LPE_AUTOCONFIG_SMTP_HOST");
    let mut headers = HeaderMap::new();
    headers.insert("host", "core.example.test".parse().unwrap());
    let unpublished = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
    let unpublished_xml = render_thunderbird_autoconfig(&unpublished);

    assert!(unpublished.imap_host.is_none());
    assert!(!unpublished_xml.contains("<incomingServer type=\"imap\">"));
    assert!(!unpublished_xml.contains("<outgoingServer type=\"smtp\">"));

    std::env::set_var("LPE_AUTOCONFIG_IMAP_HOST", "imap.edge.example.test");
    std::env::set_var("LPE_AUTOCONFIG_IMAP_PORT", "993");
    let published = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
    let published_xml = render_thunderbird_autoconfig(&published);

    assert_eq!(
        published.imap_host.as_deref(),
        Some("imap.edge.example.test")
    );
    assert!(published_xml.contains("<incomingServer type=\"imap\">"));
    assert!(published_xml.contains("<hostname>imap.edge.example.test</hostname>"));
    assert!(published_xml.contains("<port>993</port>"));
    assert!(!published_xml.contains("<outgoingServer type=\"smtp\">"));
    assert!(published_xml.contains("https://core.example.test/api/jmap/session"));

    std::env::remove_var("LPE_AUTOCONFIG_IMAP_HOST");
    std::env::remove_var("LPE_AUTOCONFIG_IMAP_PORT");
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
fn outlook_autodiscover_does_not_publish_imap_or_smtp_without_explicit_edge_configuration() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("LPE_AUTOCONFIG_IMAP_HOST");
    std::env::remove_var("LPE_AUTOCONFIG_IMAP_PORT");
    std::env::remove_var("LPE_AUTOCONFIG_SMTP_HOST");
    std::env::remove_var("LPE_AUTOCONFIG_SMTP_PORT");
    let mut headers = HeaderMap::new();
    headers.insert("host", "core.example.test".parse().unwrap());

    let config = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(config.imap_host.is_none());
    assert!(!xml.contains("<Type>IMAP</Type>"));
    assert!(!xml.contains("<Type>SMTP</Type>"));
    assert!(xml.contains("<AutoDiscoverSMTPAddress>alice@example.test</AutoDiscoverSMTPAddress>"));
}

#[test]
fn jmap_well_known_redirects_to_public_session_url() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("LPE_AUTOCONFIG_JMAP_SESSION_URL");
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-proto", "https".parse().unwrap());
    headers.insert("x-forwarded-host", "mail.example.test".parse().unwrap());

    assert_eq!(
        jmap_well_known_location(&headers),
        "https://mail.example.test/api/jmap/session"
    );

    std::env::set_var(
        "LPE_AUTOCONFIG_JMAP_SESSION_URL",
        "https://jmap.example.test/.well-known/jmap-session",
    );
    assert_eq!(
        jmap_well_known_location(&headers),
        "https://jmap.example.test/.well-known/jmap-session"
    );
    std::env::remove_var("LPE_AUTOCONFIG_JMAP_SESSION_URL");
}

#[tokio::test]
async fn autodiscover_json_defaults_to_pox_endpoint() {
    let response = render_autodiscover_json(&sample_config(), None)
        .expect("default AutodiscoverV1 response should be published");

    assert_eq!(response.status(), 200);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["Protocol"], "AutoDiscoverV1");
    assert_eq!(
        payload["Url"],
        "https://mail.example.test/autodiscover/autodiscover.xml"
    );
}

#[tokio::test]
async fn autodiscover_json_autodiscover_v1_returns_pox_endpoint() {
    let response = render_autodiscover_json(&sample_config(), Some("AutoDiscoverV1"))
        .expect("AutoDiscoverV1 JSON discovery should point to POX autodiscover");

    assert_eq!(response.status(), 200);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["Protocol"], "AutoDiscoverV1");
    assert_eq!(
        payload["Url"],
        "https://mail.example.test/autodiscover/autodiscover.xml"
    );
}

#[tokio::test]
async fn autodiscover_json_supported_protocol_returns_protocol_and_url() {
    let response = render_autodiscover_json(&sample_config(), Some("ActiveSync"))
        .expect("ActiveSync JSON discovery should be available for mobile probes");

    assert_eq!(response.status(), 200);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["Protocol"], "ActiveSync");
    assert_eq!(
        payload["Url"],
        "https://mail.example.test/Microsoft-Server-ActiveSync"
    );
    assert!(payload.get("ErrorCode").is_none());
}

#[tokio::test]
async fn autodiscover_json_accepts_outlook_redirect_count_request() {
    let uri: Uri =
            "/autodiscover/autodiscover.json/v1.0/alice@example.test?Protocol=ActiveSync&RedirectCount=1"
                .parse()
                .unwrap();
    let query = Query::<AutodiscoverJsonQuery>::try_from_uri(&uri).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("host", "mail.example.test".parse().unwrap());
    let response =
        outlook_autodiscover_json(uri, headers, Path("alice@example.test".to_string()), query)
            .await;

    assert_eq!(response.status(), 200);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["Protocol"], "ActiveSync");
    assert_eq!(
        payload["Url"],
        "https://mail.example.test/Microsoft-Server-ActiveSync"
    );
}

#[tokio::test]
async fn autodiscover_json_rejects_rest_without_fake_endpoint() {
    assert!(render_autodiscover_json(&sample_config(), Some("REST")).is_none());

    let response = autodiscover_json_invalid_protocol_response(&sample_config(), Some("REST"));

    assert_eq!(response.status(), 400);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["ErrorCode"], "InvalidProtocol");
    assert!(payload["ErrorMessage"]
        .as_str()
        .unwrap()
        .contains("The given protocol value 'REST' is invalid."));
    assert!(!payload["ErrorMessage"].as_str().unwrap().contains("/api"));
    assert!(payload.get("Url").is_none());
}

#[tokio::test]
async fn autodiscover_json_rejects_jmap_protocol() {
    assert!(render_autodiscover_json(&sample_config(), Some("JMAP")).is_none());

    let response = autodiscover_json_invalid_protocol_response(&sample_config(), Some("JMAP"));

    assert_eq!(response.status(), 400);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["ErrorCode"], "InvalidProtocol");
    assert!(payload.get("Url").is_none());
}

#[tokio::test]
async fn autodiscover_json_handler_rejects_rest_request_with_redirect_count() {
    let uri: Uri =
        "/autodiscover/autodiscover.json/v1.0/alice@example.test?Protocol=REST&RedirectCount=1"
            .parse()
            .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("host", "mail.example.test".parse().unwrap());
    let response = outlook_autodiscover_json(
        uri,
        headers,
        Path("alice@example.test".to_string()),
        Query(AutodiscoverJsonQuery {
            protocol: Some("REST".to_string()),
        }),
    )
    .await;

    assert_eq!(response.status(), 400);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["ErrorCode"], "InvalidProtocol");
    assert!(payload["ErrorMessage"]
        .as_str()
        .unwrap()
        .contains("The given protocol value 'REST' is invalid."));
    assert!(payload.get("Url").is_none());
}

#[tokio::test]
async fn autodiscover_json_unsupported_protocol_uses_microsoft_error_shape() {
    let response =
        autodiscover_json_invalid_protocol_response(&sample_config(), Some("UnknownProtocol"));

    assert_eq!(response.status(), 400);
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["ErrorCode"], "InvalidProtocol");
    assert!(payload["ErrorMessage"]
        .as_str()
        .unwrap()
        .contains("Supported values are 'ActiveSync,AutoDiscoverV1,MobileSync'"));
    assert!(payload.get("Protocol").is_none());
    assert!(payload.get("Url").is_none());
}

#[tokio::test]
async fn autodiscover_json_publishes_ews_only_when_enabled() {
    assert!(render_autodiscover_json(&sample_config(), Some("EWS")).is_none());

    let config = PublishedEndpoints {
        ews_enabled: true,
        ..sample_config()
    };
    let response = render_autodiscover_json(&config, Some("EWS"))
        .expect("EWS JSON discovery should be published when EWS is enabled");
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["Protocol"], "EWS");
    assert_eq!(
        payload["Url"],
        "https://mail.example.test/EWS/Exchange.asmx"
    );
}

#[tokio::test]
async fn autodiscover_json_publishes_mapi_when_enabled() {
    assert!(render_autodiscover_json(&sample_config(), Some("MapiHttp")).is_none());

    let config = PublishedEndpoints {
        mapi_enabled: true,
        ..sample_config()
    };
    let response = render_autodiscover_json(&config, Some("MapiHttp"))
        .expect("MAPI JSON discovery should be published when MAPI is enabled");
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["Protocol"], "MapiHttp");
    assert_eq!(
        payload["Url"],
        "https://mail.example.test/mapi/emsmdb/?MailboxId=alice@example.test"
    );
}

#[tokio::test]
async fn autodiscover_json_returns_activesync_only_for_mobile_protocol_probe() {
    let response = render_autodiscover_json(&sample_config(), Some("ActiveSync"))
        .expect("ActiveSync JSON discovery should be available for mobile probes");
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["Protocol"], "ActiveSync");
    assert_eq!(
        payload["Url"],
        "https://mail.example.test/Microsoft-Server-ActiveSync"
    );
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
    assert!(xml.contains(
        "<OWAUrl AuthenticationMethod=\"Basic\">https://mail.example.test/mail/</OWAUrl>"
    ));
    assert!(!xml.contains("<OWAUrl AuthenticationMethod=\"Basic\">https://mail.example.test/EWS/Exchange.asmx</OWAUrl>"));
    assert!(!xml.contains("<ASUrl>"));
    assert!(!xml.contains("<Type>EXPR</Type>"));
    assert!(xml.contains("<Server>mail.example.test</Server>"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
    assert!(!xml.contains("<Type>MobileSync</Type>"));
    assert!(!xml.contains("<Type>MAPI</Type>"));
}

#[test]
fn outlook_autodiscover_web_external_uses_ms_oxdscli_protocol_shape() {
    let config = PublishedEndpoints {
        ews_enabled: true,
        ..sample_config()
    };
    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));
    let account = outlook_account(&xml);
    let external = web_protocol(&account)
        .child("External")
        .expect("WEB protocol should contain External settings");
    let owa_url = external
        .child_text("OWAUrl")
        .expect("WEB External should contain OWAUrl");

    assert_eq!(owa_url, "https://mail.example.test/mail/");
    assert!(!owa_url.ends_with("/EWS/Exchange.asmx"));
    assert!(external.child("ASUrl").is_none());
    assert!(external.child("Protocol").is_none());

    let config = PublishedEndpoints {
        ews_enabled: true,
        legacy_exch_autodiscover_enabled: true,
        ..sample_config()
    };
    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));
    let account = outlook_account(&xml);
    let external = web_protocol(&account)
        .child("External")
        .expect("WEB protocol should contain External settings");
    let protocol = external
        .child("Protocol")
        .expect("WEB External should contain nested Protocol when EXCH is published");

    assert!(external.child("ASUrl").is_none());
    assert_eq!(protocol.child_text("Type"), Some("EXCH"));
    assert_eq!(
        protocol.child_text("ASUrl"),
        Some("https://mail.example.test/EWS/Exchange.asmx")
    );
}

#[test]
fn outlook_autodiscover_can_publish_explicit_mapi_http_protocol() {
    let config = PublishedEndpoints {
        mapi_enabled: true,
        outlook_interop_gate_passed: true,
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
fn outlook_autodiscover_mapi_probe_keeps_opt_in_ews_web_endpoint() {
    let config = PublishedEndpoints {
        ews_enabled: true,
        mapi_enabled: true,
        outlook_interop_gate_passed: true,
        mapi_http_requested: true,
        mapi_emsmdb_url: "https://mail.example.test/mapi/emsmdb/?MailboxId=alice@example.test"
            .to_string(),
        mapi_nspi_url: "https://mail.example.test/mapi/nspi/?MailboxId=alice@example.test"
            .to_string(),
        ..sample_config()
    };

    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));
    assert!(xml.contains("<Type>WEB</Type>"));
    assert!(xml.contains(
        "<OWAUrl AuthenticationMethod=\"Basic\">https://mail.example.test/mail/</OWAUrl>"
    ));
    assert!(!xml.contains("<OWAUrl AuthenticationMethod=\"Basic\">https://mail.example.test/EWS/Exchange.asmx</OWAUrl>"));
    assert!(!xml.contains("<ASUrl>"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
}

#[test]
fn outlook_autodiscover_mapi_http_capability_header_stays_env_gated() {
    let config = PublishedEndpoints {
        mapi_enabled: false,
        mapi_http_requested: true,
        legacy_exch_autodiscover_enabled: false,
        legacy_expr_autodiscover_enabled: false,
        ..sample_config()
    };

    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(xml.contains("<Type>IMAP</Type>"));
    assert!(!xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
    assert!(!xml.contains("<Type>WEB</Type>"));
}

#[test]
fn outlook_autodiscover_can_publish_exchange_provider_for_legacy_mapi_probe() {
    let config = PublishedEndpoints {
        mapi_enabled: true,
        outlook_interop_gate_passed: true,
        mapi_http_requested: false,
        legacy_exch_autodiscover_enabled: true,
        legacy_expr_autodiscover_enabled: true,
        rpc_proxy_enabled: true,
        ..sample_config()
    };

    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(xml.contains("<ServerDN>/o=LPE/ou=Exchange Administrative Group/cn=Configuration/cn=Servers/cn=mail.example.test</ServerDN>"));
    assert!(xml.contains("<MdbDN>/o=LPE/ou=Exchange Administrative Group/cn=Configuration/cn=Servers/cn=mail.example.test/cn=LPE Private MDB</MdbDN>"));
    assert!(xml.contains("<AuthPackage>Basic</AuthPackage>"));
    assert!(!xml.contains("<EwsUrl>"));
    assert!(!xml.contains("<ASUrl>"));
    assert!(xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
    assert!(xml.contains("<CertPrincipalName>msstd:mail.example.test</CertPrincipalName>"));
    assert!(!xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));
}

#[test]
fn outlook_autodiscover_can_publish_exchange_providers_for_legacy_ews_probe() {
    let config = PublishedEndpoints {
        ews_enabled: true,
        outlook_interop_gate_passed: true,
        legacy_exch_autodiscover_enabled: true,
        legacy_expr_autodiscover_enabled: true,
        rpc_proxy_enabled: true,
        ..sample_config()
    };

    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
    assert!(xml.contains("<EwsUrl>https://mail.example.test/EWS/Exchange.asmx</EwsUrl>"));
    assert!(xml.contains("<ASUrl>https://mail.example.test/EWS/Exchange.asmx</ASUrl>"));
    assert!(!xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));
}

#[test]
fn outlook_autodiscover_can_publish_legacy_exch_without_expr() {
    let config = PublishedEndpoints {
        ews_enabled: true,
        legacy_exch_autodiscover_enabled: true,
        legacy_expr_autodiscover_enabled: false,
        rpc_proxy_enabled: false,
        ..sample_config()
    };

    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
    assert!(xml.contains("<EwsUrl>https://mail.example.test/EWS/Exchange.asmx</EwsUrl>"));
}

#[test]
fn outlook_autodiscover_can_publish_legacy_expr_without_exch() {
    let config = PublishedEndpoints {
        ews_enabled: true,
        outlook_interop_gate_passed: true,
        legacy_exch_autodiscover_enabled: false,
        legacy_expr_autodiscover_enabled: true,
        rpc_proxy_enabled: true,
        ..sample_config()
    };

    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(!xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
    assert!(xml.contains("<AuthPackage>Basic</AuthPackage>"));
    assert!(xml.contains("<CertPrincipalName>msstd:mail.example.test</CertPrincipalName>"));
}

#[test]
fn outlook_autodiscover_expr_requires_rpc_proxy_publication() {
    let config = PublishedEndpoints {
        ews_enabled: true,
        outlook_interop_gate_passed: true,
        legacy_exch_autodiscover_enabled: false,
        legacy_expr_autodiscover_enabled: true,
        rpc_proxy_enabled: false,
        ..sample_config()
    };

    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(xml.contains("<Type>WEB</Type>"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
}

#[test]
fn outlook_autodiscover_expr_requires_final_outlook_gate() {
    let config = PublishedEndpoints {
        ews_enabled: true,
        legacy_exch_autodiscover_enabled: false,
        legacy_expr_autodiscover_enabled: true,
        rpc_proxy_enabled: true,
        ..sample_config()
    };

    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(xml.contains("<Type>WEB</Type>"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
}

#[test]
fn mapi_enabled_does_not_hijack_default_outlook_imap_profile() {
    let config = PublishedEndpoints {
        mapi_enabled: true,
        mapi_http_requested: false,
        legacy_exch_autodiscover_enabled: false,
        legacy_expr_autodiscover_enabled: false,
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
    std::env::set_var("LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED", "true");
    std::env::remove_var("LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_RPC_PROXY_ENABLED");
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
    assert!(config.outlook_interop_gate_passed);
    assert!(config.mapi_http_requested);
    assert!(!config.legacy_exch_autodiscover_enabled);
    assert!(!config.legacy_expr_autodiscover_enabled);
    assert!(!config.rpc_proxy_enabled);
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
    std::env::remove_var("LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED");
}

#[test]
fn invalid_mapi_http_capability_header_is_ignored() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("LPE_AUTOCONFIG_MAPI_ENABLED", "true");
    std::env::set_var("LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED", "true");
    std::env::set_var("LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED", "true");
    std::env::set_var("LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED", "true");
    std::env::set_var("LPE_AUTOCONFIG_RPC_PROXY_ENABLED", "true");
    std::env::remove_var("LPE_PUBLIC_HOSTNAME");
    std::env::remove_var("LPE_PUBLIC_SCHEME");

    for value in ["", "0", "not-a-version"] {
        let mut headers = HeaderMap::new();
        headers.insert("host", "mail.example.test".parse().unwrap());
        if !value.is_empty() {
            headers.insert("x-mapihttpcapability", value.parse().unwrap());
        }
        let config = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
        let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

        assert!(!config.mapi_http_requested);
        assert!(!xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));
        assert!(xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
        assert!(xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
    }

    std::env::remove_var("LPE_AUTOCONFIG_MAPI_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED");
    std::env::remove_var("LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_RPC_PROXY_ENABLED");
}

#[test]
fn mapi_http_capability_header_and_enable_flag_publish_mapi() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("LPE_AUTOCONFIG_MAPI_ENABLED", "true");
    std::env::remove_var("LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED");
    std::env::remove_var("LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_RPC_PROXY_ENABLED");
    std::env::remove_var("LPE_PUBLIC_HOSTNAME");
    std::env::remove_var("LPE_PUBLIC_SCHEME");

    let mut headers = HeaderMap::new();
    headers.insert("host", "mail.example.test".parse().unwrap());
    headers.insert("x-mapihttpcapability", "1".parse().unwrap());
    let config = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(config.mapi_enabled);
    assert!(!config.outlook_interop_gate_passed);
    assert!(config.mapi_http_requested);
    assert!(!config.legacy_exch_autodiscover_enabled);
    assert!(!config.legacy_expr_autodiscover_enabled);
    assert!(!config.rpc_proxy_enabled);
    assert!(xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));

    std::env::remove_var("LPE_AUTOCONFIG_MAPI_ENABLED");
}

#[test]
fn legacy_exchange_autodiscover_publication_has_separate_provider_opt_ins() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("LPE_AUTOCONFIG_MAPI_ENABLED", "true");
    std::env::remove_var("LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED");
    std::env::set_var("LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED", "true");
    std::env::remove_var("LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_RPC_PROXY_ENABLED");
    std::env::remove_var("LPE_PUBLIC_HOSTNAME");
    std::env::remove_var("LPE_PUBLIC_SCHEME");

    let mut headers = HeaderMap::new();
    headers.insert("host", "mail.example.test".parse().unwrap());
    let config = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(config.mapi_enabled);
    assert!(!config.outlook_interop_gate_passed);
    assert!(config.legacy_exch_autodiscover_enabled);
    assert!(!config.legacy_expr_autodiscover_enabled);
    assert!(xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
    assert!(!xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));

    std::env::set_var("LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED", "true");
    std::env::set_var("LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED", "true");
    let config = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(config.outlook_interop_gate_passed);
    assert!(xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
    assert!(!xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));

    std::env::set_var("LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED", "true");
    std::env::set_var("LPE_AUTOCONFIG_RPC_PROXY_ENABLED", "true");
    std::env::remove_var("LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED");
    let config = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(!config.legacy_exch_autodiscover_enabled);
    assert!(config.legacy_expr_autodiscover_enabled);
    assert!(config.rpc_proxy_enabled);
    assert!(!xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));

    std::env::remove_var("LPE_AUTOCONFIG_MAPI_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED");
    std::env::remove_var("LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_RPC_PROXY_ENABLED");
}

#[test]
fn legacy_exchange_autodiscover_publication_works_with_ews_provider_opt_ins() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("LPE_AUTOCONFIG_EWS_ENABLED", "true");
    std::env::remove_var("LPE_AUTOCONFIG_MAPI_ENABLED");
    std::env::set_var("LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED", "true");
    std::env::set_var("LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED", "true");
    std::env::set_var("LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED", "true");
    std::env::set_var("LPE_AUTOCONFIG_RPC_PROXY_ENABLED", "true");
    std::env::remove_var("LPE_PUBLIC_HOSTNAME");
    std::env::remove_var("LPE_PUBLIC_SCHEME");

    let mut headers = HeaderMap::new();
    headers.insert("host", "mail.example.test".parse().unwrap());
    let config = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(config.ews_enabled);
    assert!(!config.mapi_enabled);
    assert!(config.legacy_exch_autodiscover_enabled);
    assert!(config.legacy_expr_autodiscover_enabled);
    assert!(config.rpc_proxy_enabled);
    assert!(xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
    assert!(xml.contains("<EwsUrl>https://mail.example.test/EWS/Exchange.asmx</EwsUrl>"));
    assert!(!xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));

    std::env::remove_var("LPE_AUTOCONFIG_EWS_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED");
    std::env::remove_var("LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_RPC_PROXY_ENABLED");
}

#[test]
fn legacy_exchange_autodiscover_survives_mapi_capability_header_without_mapi_publication() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("LPE_AUTOCONFIG_EWS_ENABLED", "true");
    std::env::remove_var("LPE_AUTOCONFIG_MAPI_ENABLED");
    std::env::set_var("LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED", "true");
    std::env::set_var("LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED", "true");
    std::env::set_var("LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED", "true");
    std::env::set_var("LPE_AUTOCONFIG_RPC_PROXY_ENABLED", "true");
    std::env::remove_var("LPE_PUBLIC_HOSTNAME");
    std::env::remove_var("LPE_PUBLIC_SCHEME");

    let mut headers = HeaderMap::new();
    headers.insert("host", "mail.example.test".parse().unwrap());
    headers.insert("x-mapihttpcapability", "1".parse().unwrap());
    let config = PublishedEndpoints::from_headers(&headers, Some("alice@example.test"));
    let xml = render_outlook_autodiscover(&config, Some("alice@example.test"));

    assert!(config.ews_enabled);
    assert!(config.mapi_http_requested);
    assert!(!config.mapi_autodiscover_enabled());
    assert!(xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));
    assert!(xml.contains("<EwsUrl>https://mail.example.test/EWS/Exchange.asmx</EwsUrl>"));
    assert!(!xml.contains("<Protocol Type=\"mapiHttp\" Version=\"1\">"));

    std::env::remove_var("LPE_AUTOCONFIG_EWS_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED");
    std::env::remove_var("LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_RPC_PROXY_ENABLED");
}

#[test]
fn outlook_autodiscover_ews_publication_is_env_opt_in() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("LPE_AUTOCONFIG_EWS_ENABLED", "true");
    std::env::set_var(
        "LPE_AUTOCONFIG_WEBMAIL_URL",
        "https://webmail.example.test/mail/",
    );
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
    assert_eq!(config.webmail_url, "https://webmail.example.test/mail/");
    assert!(xml.contains("<Type>WEB</Type>"));
    assert!(xml.contains(
        "<OWAUrl AuthenticationMethod=\"Basic\">https://webmail.example.test/mail/</OWAUrl>"
    ));
    assert!(!xml.contains(
        "<OWAUrl AuthenticationMethod=\"Basic\">https://mail.example.test/EWS/Exchange.asmx</OWAUrl>"
    ));
    assert!(!xml.contains("<ASUrl>"));
    assert!(!xml.contains("<Type>EXPR</Type>"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXCH</Type>"));
    assert!(!xml.contains("      <Protocol>\n        <Type>EXPR</Type>"));

    std::env::remove_var("LPE_AUTOCONFIG_EWS_ENABLED");
    std::env::remove_var("LPE_AUTOCONFIG_WEBMAIL_URL");
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

    assert!(render_soap_user_settings_response(&ews_config, Some("alice@example.test")).is_none());
    assert!(render_soap_user_settings_response(&mapi_config, Some("alice@example.test")).is_none());
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
        outlook_interop_gate_passed: true,
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
    assert!(xml.contains("<AutoDiscoverSMTPAddress>alice@example.test</AutoDiscoverSMTPAddress>"));
    assert!(xml.contains("<DeploymentId>lpe-example-test</DeploymentId>"));
    assert!(!xml.contains("<EMailAddress>alice@example.test</EMailAddress>"));
}
