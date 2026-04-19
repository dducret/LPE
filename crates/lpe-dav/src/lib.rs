use anyhow::{anyhow, bail, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::Response,
    routing::any,
    Router,
};
use lpe_mail_auth::{authenticate_account, AccountAuthStore, AccountPrincipal};
use lpe_storage::{
    ClientContact, ClientEvent, Storage, UpsertClientContactInput, UpsertClientEventInput,
};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};
use uuid::Uuid;

const ROOT_PATH: &str = "/dav/";
const PRINCIPAL_PATH: &str = "/dav/principals/me/";
const ADDRESSBOOK_HOME_PATH: &str = "/dav/addressbooks/me/";
const ADDRESSBOOK_COLLECTION_PATH: &str = "/dav/addressbooks/me/default/";
const CALENDAR_HOME_PATH: &str = "/dav/calendars/me/";
const CALENDAR_COLLECTION_PATH: &str = "/dav/calendars/me/default/";

pub fn router() -> Router<Storage> {
    Router::new()
        .route("/dav", any(dav_handler))
        .route("/dav/{*path}", any(dav_handler))
        .route("/.well-known/carddav", any(carddav_redirect))
        .route("/.well-known/caldav", any(caldav_redirect))
}

pub trait DavStore: AccountAuthStore {
    fn fetch_client_contacts<'a>(
        &'a self,
        account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<ClientContact>>;
    fn fetch_client_events<'a>(
        &'a self,
        account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<ClientEvent>>;
    fn upsert_client_contact<'a>(
        &'a self,
        input: UpsertClientContactInput,
    ) -> lpe_mail_auth::StoreFuture<'a, ClientContact>;
    fn upsert_client_event<'a>(
        &'a self,
        input: UpsertClientEventInput,
    ) -> lpe_mail_auth::StoreFuture<'a, ClientEvent>;
    fn delete_client_contact<'a>(
        &'a self,
        account_id: Uuid,
        contact_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, ()>;
    fn delete_client_event<'a>(
        &'a self,
        account_id: Uuid,
        event_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, ()>;
}

impl DavStore for Storage {
    fn fetch_client_contacts<'a>(
        &'a self,
        account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<ClientContact>> {
        Box::pin(async move { self.fetch_client_contacts(account_id).await })
    }

    fn fetch_client_events<'a>(
        &'a self,
        account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<ClientEvent>> {
        Box::pin(async move { self.fetch_client_events(account_id).await })
    }

    fn upsert_client_contact<'a>(
        &'a self,
        input: UpsertClientContactInput,
    ) -> lpe_mail_auth::StoreFuture<'a, ClientContact> {
        Box::pin(async move { self.upsert_client_contact(input).await })
    }

    fn upsert_client_event<'a>(
        &'a self,
        input: UpsertClientEventInput,
    ) -> lpe_mail_auth::StoreFuture<'a, ClientEvent> {
        Box::pin(async move { self.upsert_client_event(input).await })
    }

    fn delete_client_contact<'a>(
        &'a self,
        account_id: Uuid,
        contact_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_client_contact(account_id, contact_id).await })
    }

    fn delete_client_event<'a>(
        &'a self,
        account_id: Uuid,
        event_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_client_event(account_id, event_id).await })
    }
}

#[derive(Clone)]
struct DavService<S> {
    store: S,
}

impl<S> DavService<S> {
    fn new(store: S) -> Self {
        Self { store }
    }
}

async fn carddav_redirect() -> Response {
    redirect_response(ADDRESSBOOK_COLLECTION_PATH)
}

async fn caldav_redirect() -> Response {
    redirect_response(CALENDAR_COLLECTION_PATH)
}

async fn dav_handler(
    State(storage): State<Storage>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let service = DavService::new(storage);
    match service.handle(&method, &uri, &headers, body.as_ref()).await {
        Ok(response) => response,
        Err(error) => error_response(error),
    }
}

impl<S: DavStore> DavService<S> {
    async fn handle(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let path = normalized_path(uri.path());
        if method == Method::OPTIONS {
            return Ok(options_response());
        }

        let principal = authenticate_account(&self.store, None, headers).await?;
        match method.as_str() {
            "PROPFIND" => self.handle_propfind(&principal, &path, headers).await,
            "REPORT" => self.handle_report(&principal, &path).await,
            "GET" => self.handle_get(&principal, &path).await,
            "PUT" => self.handle_put(&principal, &path, body).await,
            "DELETE" => self.handle_delete(&principal, &path).await,
            _ => bail!("method not allowed"),
        }
    }

    async fn handle_propfind(
        &self,
        principal: &AccountPrincipal,
        path: &str,
        headers: &HeaderMap,
    ) -> Result<Response> {
        let depth = headers
            .get("depth")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("0");
        let entries = match path {
            ROOT_PATH => vec![root_propfind_entry()],
            PRINCIPAL_PATH => vec![principal_propfind_entry()],
            ADDRESSBOOK_HOME_PATH => vec![collection_home_entry(
                ADDRESSBOOK_HOME_PATH,
                "Address Books",
                collection_resourcetype("collection"),
            )],
            CALENDAR_HOME_PATH => vec![collection_home_entry(
                CALENDAR_HOME_PATH,
                "Calendars",
                collection_resourcetype("collection"),
            )],
            ADDRESSBOOK_COLLECTION_PATH => {
                let mut entries = vec![addressbook_collection_entry()];
                if depth == "1" {
                    entries.extend(
                        self.store
                            .fetch_client_contacts(principal.account_id)
                            .await?
                            .into_iter()
                            .map(contact_resource_entry),
                    );
                }
                entries
            }
            CALENDAR_COLLECTION_PATH => {
                let mut entries = vec![calendar_collection_entry()];
                if depth == "1" {
                    entries.extend(
                        self.store
                            .fetch_client_events(principal.account_id)
                            .await?
                            .into_iter()
                            .map(event_resource_entry),
                    );
                }
                entries
            }
            _ => {
                if let Some(contact) = self.contact_for_path(principal.account_id, path).await? {
                    vec![contact_resource_entry(contact)]
                } else if let Some(event) = self.event_for_path(principal.account_id, path).await? {
                    vec![event_resource_entry(event)]
                } else {
                    bail!("not found");
                }
            }
        };
        Ok(multistatus_response(entries))
    }

    async fn handle_report(&self, principal: &AccountPrincipal, path: &str) -> Result<Response> {
        let entries = match path {
            ADDRESSBOOK_COLLECTION_PATH => self
                .store
                .fetch_client_contacts(principal.account_id)
                .await?
                .into_iter()
                .map(contact_report_entry)
                .collect(),
            CALENDAR_COLLECTION_PATH => self
                .store
                .fetch_client_events(principal.account_id)
                .await?
                .into_iter()
                .map(event_report_entry)
                .collect(),
            _ => bail!("not found"),
        };
        Ok(multistatus_response(entries))
    }

    async fn handle_get(&self, principal: &AccountPrincipal, path: &str) -> Result<Response> {
        if let Some(contact) = self.contact_for_path(principal.account_id, path).await? {
            return Ok(text_response(
                "text/vcard; charset=utf-8",
                serialize_vcard(&contact),
            ));
        }
        if let Some(event) = self.event_for_path(principal.account_id, path).await? {
            return Ok(text_response(
                "text/calendar; charset=utf-8",
                serialize_ical(&event),
            ));
        }
        bail!("not found")
    }

    async fn handle_put(
        &self,
        principal: &AccountPrincipal,
        path: &str,
        body: &[u8],
    ) -> Result<Response> {
        if let Some(resource_id) = resource_id_for_contact_path(path) {
            let existing = self.contact_for_path(principal.account_id, path).await?.is_some();
            let parsed = parse_vcard(resource_id, principal.account_id, body)?;
            self.store.upsert_client_contact(parsed).await?;
            return Ok(status_only(if existing { 204 } else { 201 }));
        }
        if let Some(resource_id) = resource_id_for_event_path(path) {
            let existing = self.event_for_path(principal.account_id, path).await?.is_some();
            let parsed = parse_ical(resource_id, principal.account_id, body)?;
            self.store.upsert_client_event(parsed).await?;
            return Ok(status_only(if existing { 204 } else { 201 }));
        }
        bail!("not found")
    }

    async fn handle_delete(&self, principal: &AccountPrincipal, path: &str) -> Result<Response> {
        if let Some(resource_id) = resource_id_for_contact_path(path) {
            self.store
                .delete_client_contact(principal.account_id, resource_id)
                .await?;
            return Ok(status_only(204));
        }
        if let Some(resource_id) = resource_id_for_event_path(path) {
            self.store
                .delete_client_event(principal.account_id, resource_id)
                .await?;
            return Ok(status_only(204));
        }
        bail!("not found")
    }

    async fn contact_for_path(&self, account_id: Uuid, path: &str) -> Result<Option<ClientContact>> {
        let Some(resource_id) = resource_id_for_contact_path(path) else {
            return Ok(None);
        };
        Ok(self
            .store
            .fetch_client_contacts(account_id)
            .await?
            .into_iter()
            .find(|contact| contact.id == resource_id))
    }

    async fn event_for_path(&self, account_id: Uuid, path: &str) -> Result<Option<ClientEvent>> {
        let Some(resource_id) = resource_id_for_event_path(path) else {
            return Ok(None);
        };
        Ok(self
            .store
            .fetch_client_events(account_id)
            .await?
            .into_iter()
            .find(|event| event.id == resource_id))
    }
}

fn normalized_path(path: &str) -> String {
    match path {
        "/dav" => ROOT_PATH.to_string(),
        other => other.to_string(),
    }
}

fn root_propfind_entry() -> String {
    response_entry(
        ROOT_PATH,
        collection_props(
            "DAV Root",
            "<d:collection/><d:principal/>",
            None,
            None,
            Some(format!(
                "<d:current-user-principal><d:href>{PRINCIPAL_PATH}</d:href></d:current-user-principal>"
            )),
        ),
    )
}

fn principal_propfind_entry() -> String {
    response_entry(
        PRINCIPAL_PATH,
        collection_props(
            "Current User",
            "<d:principal/>",
            None,
            None,
            Some(format!(
                "<card:addressbook-home-set><d:href>{ADDRESSBOOK_HOME_PATH}</d:href></card:addressbook-home-set>\
<cal:calendar-home-set><d:href>{CALENDAR_HOME_PATH}</d:href></cal:calendar-home-set>"
            )),
        ),
    )
}

fn addressbook_collection_entry() -> String {
    response_entry(
        ADDRESSBOOK_COLLECTION_PATH,
        collection_props(
            "Contacts",
            "<d:collection/><card:addressbook/>",
            None,
            None,
            None,
        ),
    )
}

fn calendar_collection_entry() -> String {
    response_entry(
        CALENDAR_COLLECTION_PATH,
        collection_props(
            "Calendar",
            "<d:collection/><cal:calendar/>",
            None,
            None,
            None,
        ),
    )
}

fn collection_home_entry(path: &str, display_name: &str, resource_type: String) -> String {
    response_entry(
        path,
        collection_props(display_name, &resource_type, None, None, None),
    )
}

fn contact_resource_entry(contact: ClientContact) -> String {
    let body = serialize_vcard(&contact);
    response_entry(
        &contact_href(contact.id),
        collection_props(
            &contact.name,
            "",
            Some("text/vcard; charset=utf-8"),
            Some(etag(&body)),
            None,
        ),
    )
}

fn event_resource_entry(event: ClientEvent) -> String {
    let body = serialize_ical(&event);
    response_entry(
        &event_href(event.id),
        collection_props(
            &event.title,
            "",
            Some("text/calendar; charset=utf-8"),
            Some(etag(&body)),
            None,
        ),
    )
}

fn contact_report_entry(contact: ClientContact) -> String {
    let body = serialize_vcard(&contact);
    response_entry(
        &contact_href(contact.id),
        format!(
            "<d:propstat><d:prop><d:getetag>{}</d:getetag><card:address-data>{}</card:address-data></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>",
            xml_escape(&etag(&body)),
            xml_escape(&body)
        ),
    )
}

fn event_report_entry(event: ClientEvent) -> String {
    let body = serialize_ical(&event);
    response_entry(
        &event_href(event.id),
        format!(
            "<d:propstat><d:prop><d:getetag>{}</d:getetag><cal:calendar-data>{}</cal:calendar-data></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>",
            xml_escape(&etag(&body)),
            xml_escape(&body)
        ),
    )
}

fn collection_props(
    display_name: &str,
    resource_type: &str,
    content_type: Option<&str>,
    etag: Option<String>,
    extra: Option<String>,
) -> String {
    let mut prop = format!(
        "<d:displayname>{}</d:displayname><d:resourcetype>{}</d:resourcetype>",
        xml_escape(display_name),
        resource_type
    );
    if let Some(content_type) = content_type {
        prop.push_str(&format!(
            "<d:getcontenttype>{}</d:getcontenttype>",
            xml_escape(content_type)
        ));
    }
    if let Some(etag) = etag {
        prop.push_str(&format!("<d:getetag>{}</d:getetag>", xml_escape(&etag)));
    }
    if let Some(extra) = extra {
        prop.push_str(&extra);
    }
    format!(
        "<d:propstat><d:prop>{prop}</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>"
    )
}

fn collection_resourcetype(kind: &str) -> String {
    match kind {
        "collection" => "<d:collection/>".to_string(),
        _ => String::new(),
    }
}

fn response_entry(href: &str, propstat: String) -> String {
    format!("<d:response><d:href>{href}</d:href>{propstat}</d:response>")
}

fn multistatus_response(entries: Vec<String>) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
<d:multistatus xmlns:d=\"DAV:\" xmlns:card=\"urn:ietf:params:xml:ns:carddav\" xmlns:cal=\"urn:ietf:params:xml:ns:caldav\">{}</d:multistatus>",
        entries.join("")
    );
    response_with_headers(
        207,
        "application/xml; charset=utf-8",
        body,
        &[("dav", "1, addressbook, calendar-access")],
    )
}

fn options_response() -> Response {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("allow", "OPTIONS, PROPFIND, REPORT, GET, PUT, DELETE")
        .header("dav", "1, addressbook, calendar-access")
        .header("ms-author-via", "DAV")
        .body(axum::body::Body::empty())
        .unwrap()
}

fn redirect_response(location: &str) -> Response {
    Response::builder()
        .status(StatusCode::TEMPORARY_REDIRECT)
        .header("location", location)
        .body(axum::body::Body::empty())
        .unwrap()
}

fn text_response(content_type: &str, body: String) -> Response {
    response_with_headers(200, content_type, body, &[("dav", "1, addressbook, calendar-access")])
}

fn response_with_headers(
    status: u16,
    content_type: &str,
    body: String,
    headers: &[(&str, &str)],
) -> Response {
    let mut builder = Response::builder()
        .status(StatusCode::from_u16(status).unwrap())
        .header("content-type", content_type);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder.body(axum::body::Body::from(body)).unwrap()
}

fn status_only(status: u16) -> Response {
    Response::builder()
        .status(StatusCode::from_u16(status).unwrap())
        .body(axum::body::Body::empty())
        .unwrap()
}

fn error_response(error: anyhow::Error) -> Response {
    let message = error.to_string();
    if message.contains("missing account authentication") || message.contains("invalid credentials") {
        return Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header("www-authenticate", "Basic realm=\"LPE DAV\"")
            .body(axum::body::Body::from(message))
            .unwrap();
    }
    if message.contains("not found") {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(axum::body::Body::from(message))
            .unwrap();
    }
    if message.contains("method not allowed") {
        return Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(axum::body::Body::from(message))
            .unwrap();
    }
    if message.contains("required") || message.contains("invalid") {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(axum::body::Body::from(message))
            .unwrap();
    }
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(axum::body::Body::from(message))
        .unwrap()
}

fn contact_href(id: Uuid) -> String {
    format!("{ADDRESSBOOK_COLLECTION_PATH}{id}.vcf")
}

fn event_href(id: Uuid) -> String {
    format!("{CALENDAR_COLLECTION_PATH}{id}.ics")
}

fn resource_id_for_contact_path(path: &str) -> Option<Uuid> {
    path.strip_prefix(ADDRESSBOOK_COLLECTION_PATH)
        .and_then(|value| value.strip_suffix(".vcf"))
        .and_then(parse_uuid_path_segment)
}

fn resource_id_for_event_path(path: &str) -> Option<Uuid> {
    path.strip_prefix(CALENDAR_COLLECTION_PATH)
        .and_then(|value| value.strip_suffix(".ics"))
        .and_then(parse_uuid_path_segment)
}

fn parse_uuid_path_segment(value: &str) -> Option<Uuid> {
    if value.contains('/') || value.is_empty() {
        return None;
    }
    Uuid::parse_str(value).ok()
}

fn etag(value: &str) -> String {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    format!("\"{:x}\"", hasher.finish())
}

fn serialize_vcard(contact: &ClientContact) -> String {
    let mut lines = vec![
        "BEGIN:VCARD".to_string(),
        "VERSION:3.0".to_string(),
        format!("UID:{}", contact.id),
        format!("FN:{}", text_escape(&contact.name)),
    ];
    push_line(&mut lines, "TITLE", &contact.role);
    push_line(&mut lines, "EMAIL;TYPE=INTERNET", &contact.email);
    push_line(&mut lines, "TEL", &contact.phone);
    push_line(&mut lines, "ORG", &contact.team);
    push_line(&mut lines, "NOTE", &contact.notes);
    lines.push("END:VCARD".to_string());
    lines.join("\r\n")
}

fn serialize_ical(event: &ClientEvent) -> String {
    let dtstart = format!(
        "{}T{}00",
        event.date.replace('-', ""),
        event.time.replace(':', "")
    );
    let mut lines = vec![
        "BEGIN:VCALENDAR".to_string(),
        "VERSION:2.0".to_string(),
        "PRODID:-//LPE//DAV MVP//EN".to_string(),
        "BEGIN:VEVENT".to_string(),
        format!("UID:{}", event.id),
        format!("DTSTART:{dtstart}"),
        format!("SUMMARY:{}", text_escape(&event.title)),
    ];
    push_line(&mut lines, "LOCATION", &event.location);
    push_line(&mut lines, "DESCRIPTION", &event.notes);
    push_line(&mut lines, "X-LPE-ATTENDEES", &event.attendees);
    lines.push("END:VEVENT".to_string());
    lines.push("END:VCALENDAR".to_string());
    lines.join("\r\n")
}

fn push_line(lines: &mut Vec<String>, name: &str, value: &str) {
    if !value.trim().is_empty() {
        lines.push(format!("{name}:{}", text_escape(value.trim())));
    }
}

fn parse_vcard(id: Uuid, account_id: Uuid, body: &[u8]) -> Result<UpsertClientContactInput> {
    let content = std::str::from_utf8(body)?;
    let mut name = String::new();
    let mut role = String::new();
    let mut email = String::new();
    let mut phone = String::new();
    let mut team = String::new();
    let mut notes = String::new();

    for line in unfolded_lines(content) {
        let Some((left, raw_value)) = line.split_once(':') else {
            continue;
        };
        let key = left.split(';').next().unwrap_or_default().to_ascii_uppercase();
        let value = text_unescape(raw_value.trim());
        match key.as_str() {
            "FN" => name = value,
            "TITLE" => role = value,
            "EMAIL" => email = value,
            "TEL" => phone = value,
            "ORG" => team = value,
            "NOTE" => notes = value,
            _ => {}
        }
    }

    if name.trim().is_empty() || email.trim().is_empty() {
        bail!("contact name and email are required");
    }

    Ok(UpsertClientContactInput {
        id: Some(id),
        account_id,
        name,
        role,
        email,
        phone,
        team,
        notes,
    })
}

fn parse_ical(id: Uuid, account_id: Uuid, body: &[u8]) -> Result<UpsertClientEventInput> {
    let content = std::str::from_utf8(body)?;
    let mut date = String::new();
    let mut time = String::new();
    let mut title = String::new();
    let mut location = String::new();
    let mut attendees = String::new();
    let mut notes = String::new();

    for line in unfolded_lines(content) {
        let Some((left, raw_value)) = line.split_once(':') else {
            continue;
        };
        let key = left.split(';').next().unwrap_or_default().to_ascii_uppercase();
        let value = text_unescape(raw_value.trim());
        match key.as_str() {
            "DTSTART" => {
                let (parsed_date, parsed_time) = parse_ical_datetime(&value)?;
                date = parsed_date;
                time = parsed_time;
            }
            "SUMMARY" => title = value,
            "LOCATION" => location = value,
            "DESCRIPTION" => notes = value,
            "X-LPE-ATTENDEES" => attendees = value,
            _ => {}
        }
    }

    if date.is_empty() || time.is_empty() || title.trim().is_empty() {
        bail!("event date, time, and title are required");
    }

    Ok(UpsertClientEventInput {
        id: Some(id),
        account_id,
        date,
        time,
        title,
        location,
        attendees,
        notes,
    })
}

fn parse_ical_datetime(value: &str) -> Result<(String, String)> {
    let compact = value.trim_end_matches('Z');
    let (date_part, time_part) = compact
        .split_once('T')
        .ok_or_else(|| anyhow!("invalid DTSTART"))?;
    if date_part.len() != 8 || time_part.len() < 4 {
        bail!("invalid DTSTART");
    }
    Ok((
        format!(
            "{}-{}-{}",
            &date_part[0..4],
            &date_part[4..6],
            &date_part[6..8]
        ),
        format!("{}:{}", &time_part[0..2], &time_part[2..4]),
    ))
}

fn unfolded_lines(content: &str) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    for raw in content.lines() {
        let line = raw.trim_end_matches('\r');
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(last) = lines.last_mut() {
                last.push_str(line.trim_start());
            }
        } else {
            lines.push(line.to_string());
        }
    }
    lines
}

fn text_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace(';', "\\;")
        .replace(',', "\\,")
}

fn text_unescape(value: &str) -> String {
    value
        .replace("\\n", "\n")
        .replace("\\N", "\n")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\\\", "\\")
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::HeaderValue;
    use lpe_mail_auth::AccountAuthStore;
    use lpe_storage::{AccountLogin, AuthenticatedAccount};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct FakeStore {
        session: Option<AuthenticatedAccount>,
        login: Option<AccountLogin>,
        contacts: Arc<Mutex<Vec<ClientContact>>>,
        events: Arc<Mutex<Vec<ClientEvent>>>,
    }

    impl FakeStore {
        fn account() -> AuthenticatedAccount {
            AuthenticatedAccount {
                account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                email: "alice@example.test".to_string(),
                display_name: "Alice".to_string(),
                expires_at: "2026-04-19T09:00:00Z".to_string(),
            }
        }
    }

    impl AccountAuthStore for FakeStore {
        fn fetch_account_session<'a>(
            &'a self,
            token: &'a str,
        ) -> lpe_mail_auth::StoreFuture<'a, Option<AuthenticatedAccount>> {
            let session = if token == "token" {
                self.session.clone()
            } else {
                None
            };
            Box::pin(async move { Ok(session) })
        }

        fn fetch_account_login<'a>(
            &'a self,
            _email: &'a str,
        ) -> lpe_mail_auth::StoreFuture<'a, Option<AccountLogin>> {
            let login = self.login.clone();
            Box::pin(async move { Ok(login) })
        }
    }

    impl DavStore for FakeStore {
        fn fetch_client_contacts<'a>(
            &'a self,
            _account_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, Vec<ClientContact>> {
            let contacts = self.contacts.lock().unwrap().clone();
            Box::pin(async move { Ok(contacts) })
        }

        fn fetch_client_events<'a>(
            &'a self,
            _account_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, Vec<ClientEvent>> {
            let events = self.events.lock().unwrap().clone();
            Box::pin(async move { Ok(events) })
        }

        fn upsert_client_contact<'a>(
            &'a self,
            input: UpsertClientContactInput,
        ) -> lpe_mail_auth::StoreFuture<'a, ClientContact> {
            let mut contacts = self.contacts.lock().unwrap();
            let contact = ClientContact {
                id: input.id.unwrap(),
                name: input.name,
                role: input.role,
                email: input.email,
                phone: input.phone,
                team: input.team,
                notes: input.notes,
            };
            contacts.retain(|entry| entry.id != contact.id);
            contacts.push(contact.clone());
            Box::pin(async move { Ok(contact) })
        }

        fn upsert_client_event<'a>(
            &'a self,
            input: UpsertClientEventInput,
        ) -> lpe_mail_auth::StoreFuture<'a, ClientEvent> {
            let mut events = self.events.lock().unwrap();
            let event = ClientEvent {
                id: input.id.unwrap(),
                date: input.date,
                time: input.time,
                title: input.title,
                location: input.location,
                attendees: input.attendees,
                notes: input.notes,
            };
            events.retain(|entry| entry.id != event.id);
            events.push(event.clone());
            Box::pin(async move { Ok(event) })
        }

        fn delete_client_contact<'a>(
            &'a self,
            _account_id: Uuid,
            contact_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, ()> {
            self.contacts.lock().unwrap().retain(|entry| entry.id != contact_id);
            Box::pin(async move { Ok(()) })
        }

        fn delete_client_event<'a>(
            &'a self,
            _account_id: Uuid,
            event_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, ()> {
            self.events.lock().unwrap().retain(|entry| entry.id != event_id);
            Box::pin(async move { Ok(()) })
        }
    }

    fn bearer_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer token"),
        );
        headers
    }

    async fn response_text(response: Response) -> String {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn propfind_lists_contact_resources() {
        let contact_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            contacts: Arc::new(Mutex::new(vec![ClientContact {
                id: contact_id,
                name: "Bob".to_string(),
                role: "Sales".to_string(),
                email: "bob@example.test".to_string(),
                phone: "+331234".to_string(),
                team: "North".to_string(),
                notes: "VIP".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store);
        let mut headers = bearer_headers();
        headers.insert("depth", HeaderValue::from_static("1"));

        let response = service
            .handle(
                &Method::from_bytes(b"PROPFIND").unwrap(),
                &Uri::from_static(ADDRESSBOOK_COLLECTION_PATH),
                &headers,
                &[],
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::from_u16(207).unwrap());
        let body = response_text(response).await;
        assert!(body.contains(&contact_href(contact_id)));
        assert!(body.contains("text/vcard"));
    }

    #[tokio::test]
    async fn get_returns_ical_for_existing_event() {
        let event_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            events: Arc::new(Mutex::new(vec![ClientEvent {
                id: event_id,
                date: "2026-04-20".to_string(),
                time: "09:30".to_string(),
                title: "Standup".to_string(),
                location: "Room A".to_string(),
                attendees: "alice@example.test".to_string(),
                notes: "Daily".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store);

        let response = service
            .handle(
                &Method::GET,
                &Uri::from_static("/dav/calendars/me/default/22222222-2222-2222-2222-222222222222.ics"),
                &bearer_headers(),
                &[],
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_text(response).await;
        assert!(body.contains("BEGIN:VEVENT"));
        assert!(body.contains("SUMMARY:Standup"));
    }

    #[tokio::test]
    async fn put_upserts_contact_from_vcard() {
        let contact_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = DavService::new(store.clone());

        let response = service
            .handle(
                &Method::PUT,
                &Uri::from_static("/dav/addressbooks/me/default/33333333-3333-3333-3333-333333333333.vcf"),
                &bearer_headers(),
                b"BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Carol\r\nEMAIL:carol@example.test\r\nTITLE:Ops\r\nEND:VCARD",
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let contacts = store.contacts.lock().unwrap();
        assert_eq!(contacts.len(), 1);
        assert_eq!(contacts[0].id, contact_id);
        assert_eq!(contacts[0].email, "carol@example.test");
    }

    #[tokio::test]
    async fn delete_removes_event() {
        let event_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            events: Arc::new(Mutex::new(vec![ClientEvent {
                id: event_id,
                date: "2026-04-21".to_string(),
                time: "11:00".to_string(),
                title: "Review".to_string(),
                location: "".to_string(),
                attendees: "".to_string(),
                notes: "".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store.clone());

        let response = service
            .handle(
                &Method::DELETE,
                &Uri::from_static("/dav/calendars/me/default/44444444-4444-4444-4444-444444444444.ics"),
                &bearer_headers(),
                &[],
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(store.events.lock().unwrap().is_empty());
    }
}
