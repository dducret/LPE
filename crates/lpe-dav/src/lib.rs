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
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DavAttendee {
    email: String,
    common_name: String,
    role: String,
    partstat: String,
    rsvp: bool,
}

#[derive(Debug, Default)]
struct ReportFilter {
    hrefs: Vec<String>,
    text_terms: Vec<String>,
    time_range_start: Option<String>,
    time_range_end: Option<String>,
}

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
            "REPORT" => self.handle_report(&principal, &path, body).await,
            "GET" => self.handle_get(&principal, &path, headers).await,
            "PUT" => self.handle_put(&principal, &path, headers, body).await,
            "DELETE" => self.handle_delete(&principal, &path, headers).await,
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

    async fn handle_report(
        &self,
        principal: &AccountPrincipal,
        path: &str,
        body: &[u8],
    ) -> Result<Response> {
        let filter = parse_report_filter(body)?;
        let entries = match path {
            ADDRESSBOOK_COLLECTION_PATH => self
                .store
                .fetch_client_contacts(principal.account_id)
                .await?
                .into_iter()
                .filter(|contact| contact_matches_report(contact, &filter))
                .map(contact_report_entry)
                .collect(),
            CALENDAR_COLLECTION_PATH => self
                .store
                .fetch_client_events(principal.account_id)
                .await?
                .into_iter()
                .filter(|event| event_matches_report(event, &filter))
                .map(event_report_entry)
                .collect(),
            _ => bail!("not found"),
        };
        Ok(multistatus_response(entries))
    }

    async fn handle_get(
        &self,
        principal: &AccountPrincipal,
        path: &str,
        headers: &HeaderMap,
    ) -> Result<Response> {
        if let Some(contact) = self.contact_for_path(principal.account_id, path).await? {
            let body = serialize_vcard(&contact);
            if precondition_not_modified(headers, &etag(&body)) {
                return Ok(status_only(304));
            }
            return Ok(text_response(
                "text/vcard; charset=utf-8",
                body,
                Some(etag_for_contact(&contact)),
            ));
        }
        if let Some(event) = self.event_for_path(principal.account_id, path).await? {
            let body = serialize_ical(&event);
            if precondition_not_modified(headers, &etag(&body)) {
                return Ok(status_only(304));
            }
            return Ok(text_response(
                "text/calendar; charset=utf-8",
                body,
                Some(etag_for_event(&event)),
            ));
        }
        bail!("not found")
    }

    async fn handle_put(
        &self,
        principal: &AccountPrincipal,
        path: &str,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        if let Some(resource_id) = resource_id_for_contact_path(path) {
            let existing = self.contact_for_path(principal.account_id, path).await?;
            check_write_preconditions(headers, existing.as_ref().map(etag_for_contact))?;
            let parsed = parse_vcard(resource_id, principal.account_id, body)?;
            let contact = self.store.upsert_client_contact(parsed).await?;
            return Ok(status_with_etag(
                if existing.is_some() { 204 } else { 201 },
                etag_for_contact(&contact),
            ));
        }
        if let Some(resource_id) = resource_id_for_event_path(path) {
            let existing = self.event_for_path(principal.account_id, path).await?;
            check_write_preconditions(headers, existing.as_ref().map(etag_for_event))?;
            let parsed = parse_ical(resource_id, principal.account_id, body)?;
            let event = self.store.upsert_client_event(parsed).await?;
            return Ok(status_with_etag(
                if existing.is_some() { 204 } else { 201 },
                etag_for_event(&event),
            ));
        }
        bail!("not found")
    }

    async fn handle_delete(
        &self,
        principal: &AccountPrincipal,
        path: &str,
        headers: &HeaderMap,
    ) -> Result<Response> {
        if let Some(resource_id) = resource_id_for_contact_path(path) {
            let existing = self.contact_for_path(principal.account_id, path).await?;
            check_delete_preconditions(headers, existing.as_ref().map(etag_for_contact))?;
            self.store
                .delete_client_contact(principal.account_id, resource_id)
                .await?;
            return Ok(status_only(204));
        }
        if let Some(resource_id) = resource_id_for_event_path(path) {
            let existing = self.event_for_path(principal.account_id, path).await?;
            check_delete_preconditions(headers, existing.as_ref().map(etag_for_event))?;
            self.store
                .delete_client_event(principal.account_id, resource_id)
                .await?;
            return Ok(status_only(204));
        }
        bail!("not found")
    }

    async fn contact_for_path(
        &self,
        account_id: Uuid,
        path: &str,
    ) -> Result<Option<ClientContact>> {
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
            Some(
                "<d:supported-report-set><d:supported-report><d:report><card:addressbook-query/></d:report></d:supported-report><d:supported-report><d:report><card:addressbook-multiget/></d:report></d:supported-report></d:supported-report-set>".to_string(),
            ),
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
            Some(
                "<d:supported-report-set><d:supported-report><d:report><cal:calendar-query/></d:report></d:supported-report><d:supported-report><d:report><cal:calendar-multiget/></d:report></d:supported-report></d:supported-report-set>".to_string(),
            ),
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
    format!("<d:propstat><d:prop>{prop}</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>")
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

fn text_response(content_type: &str, body: String, etag: Option<String>) -> Response {
    let mut headers = vec![("dav", "1, addressbook, calendar-access")];
    if let Some(ref value) = etag {
        headers.push(("etag", value.as_str()));
    }
    response_with_headers(200, content_type, body, &headers)
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

fn status_with_etag(status: u16, etag: String) -> Response {
    Response::builder()
        .status(StatusCode::from_u16(status).unwrap())
        .header("etag", etag)
        .body(axum::body::Body::empty())
        .unwrap()
}

fn error_response(error: anyhow::Error) -> Response {
    let message = error.to_string();
    if message.contains("missing account authentication") || message.contains("invalid credentials")
    {
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
    if message.contains("precondition failed") {
        return Response::builder()
            .status(StatusCode::PRECONDITION_FAILED)
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

fn etag_for_contact(contact: &ClientContact) -> String {
    etag(&serialize_vcard(contact))
}

fn etag_for_event(event: &ClientEvent) -> String {
    etag(&serialize_ical(event))
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
    let dtstart = format_ical_datetime(&event.date, &event.time);
    let attendees = attendees_for_event(event);
    let mut lines = vec![
        "BEGIN:VCALENDAR".to_string(),
        "VERSION:2.0".to_string(),
        "PRODID:-//LPE//DAV Adapter//EN".to_string(),
        "CALSCALE:GREGORIAN".to_string(),
        "BEGIN:VEVENT".to_string(),
        format!("UID:{}", event.id),
        format!(
            "{}:{dtstart}",
            property_name_with_tz("DTSTART", &event.time_zone)
        ),
        format!("DURATION:{}", format_duration(event.duration_minutes)),
        format!("SUMMARY:{}", text_escape(&event.title)),
    ];
    push_line(&mut lines, "LOCATION", &event.location);
    push_line(&mut lines, "DESCRIPTION", &event.notes);
    push_line(&mut lines, "RRULE", &event.recurrence_rule);
    for attendee in &attendees {
        lines.push(serialize_attendee(attendee));
    }
    if attendees.is_empty() {
        push_line(&mut lines, "X-LPE-ATTENDEES", &event.attendees);
    }
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
        let key = left
            .split(';')
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
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
    let mut time_zone = String::new();
    let mut duration_minutes = 0;
    let mut recurrence_rule = String::new();
    let mut title = String::new();
    let mut location = String::new();
    let mut attendees = String::new();
    let mut attendee_entries = Vec::new();
    let mut notes = String::new();

    for line in unfolded_lines(content) {
        let Some((left, raw_value)) = line.split_once(':') else {
            continue;
        };
        let key = left
            .split(';')
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        let value = text_unescape(raw_value.trim());
        match key.as_str() {
            "DTSTART" => {
                let (parsed_date, parsed_time) = parse_ical_datetime(&value)?;
                date = parsed_date;
                time = parsed_time;
                time_zone = property_parameter(left, "TZID").unwrap_or_default();
            }
            "DURATION" => duration_minutes = parse_ical_duration(&value)?,
            "RRULE" => recurrence_rule = value,
            "SUMMARY" => title = value,
            "LOCATION" => location = value,
            "DESCRIPTION" => notes = value,
            "X-LPE-ATTENDEES" => attendees = value,
            "ATTENDEE" => attendee_entries.push(parse_attendee(left, &value)?),
            _ => {}
        }
    }

    if date.is_empty() || time.is_empty() || title.trim().is_empty() {
        bail!("event date, time, and title are required");
    }

    if !attendee_entries.is_empty() {
        attendees = attendee_entries
            .iter()
            .map(attendee_label)
            .collect::<Vec<_>>()
            .join(", ");
    }

    Ok(UpsertClientEventInput {
        id: Some(id),
        account_id,
        date,
        time,
        time_zone,
        duration_minutes,
        recurrence_rule,
        title,
        location,
        attendees,
        attendees_json: serialize_attendees_json(&attendee_entries),
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

fn format_ical_datetime(date: &str, time: &str) -> String {
    format!("{}T{}00", date.replace('-', ""), time.replace(':', ""))
}

fn format_duration(minutes: i32) -> String {
    if minutes <= 0 {
        return "PT0S".to_string();
    }
    if minutes % 60 == 0 {
        return format!("PT{}H", minutes / 60);
    }
    format!("PT{}M", minutes)
}

fn parse_ical_duration(value: &str) -> Result<i32> {
    let value = value.trim();
    if value == "PT0S" {
        return Ok(0);
    }
    let Some(value) = value.strip_prefix("PT") else {
        bail!("invalid DURATION");
    };
    if let Some(hours) = value.strip_suffix('H') {
        return hours
            .parse::<i32>()
            .map(|value| value.max(0) * 60)
            .map_err(|_| anyhow!("invalid DURATION"));
    }
    if let Some(minutes) = value.strip_suffix('M') {
        return minutes
            .parse::<i32>()
            .map(|value| value.max(0))
            .map_err(|_| anyhow!("invalid DURATION"));
    }
    bail!("invalid DURATION")
}

fn property_name_with_tz(name: &str, time_zone: &str) -> String {
    let time_zone = time_zone.trim();
    if time_zone.is_empty() {
        return name.to_string();
    }
    format!("{name};TZID={time_zone}")
}

fn property_parameter(left: &str, name: &str) -> Option<String> {
    left.split(';').skip(1).find_map(|segment| {
        let (key, value) = segment.split_once('=')?;
        if key.eq_ignore_ascii_case(name) {
            Some(text_unescape(value.trim_matches('"')))
        } else {
            None
        }
    })
}

fn attendees_for_event(event: &ClientEvent) -> Vec<DavAttendee> {
    let parsed =
        serde_json::from_str::<Vec<DavAttendee>>(&event.attendees_json).unwrap_or_default();
    if !parsed.is_empty() {
        return parsed;
    }
    event
        .attendees
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| DavAttendee {
            email: value.to_string(),
            common_name: String::new(),
            role: "REQ-PARTICIPANT".to_string(),
            partstat: "NEEDS-ACTION".to_string(),
            rsvp: false,
        })
        .collect()
}

fn serialize_attendee(attendee: &DavAttendee) -> String {
    let mut property = "ATTENDEE".to_string();
    if !attendee.common_name.trim().is_empty() {
        property.push_str(&format!(";CN={}", param_escape(&attendee.common_name)));
    }
    if !attendee.role.trim().is_empty() {
        property.push_str(&format!(";ROLE={}", attendee.role.trim()));
    }
    if !attendee.partstat.trim().is_empty() {
        property.push_str(&format!(";PARTSTAT={}", attendee.partstat.trim()));
    }
    if attendee.rsvp {
        property.push_str(";RSVP=TRUE");
    }
    let value = if attendee.email.trim().is_empty() {
        "mailto:unknown@example.invalid".to_string()
    } else if attendee.email.to_ascii_lowercase().starts_with("mailto:") {
        attendee.email.clone()
    } else {
        format!("mailto:{}", attendee.email.trim())
    };
    format!("{property}:{value}")
}

fn parse_attendee(left: &str, value: &str) -> Result<DavAttendee> {
    let email = value
        .trim()
        .strip_prefix("mailto:")
        .unwrap_or(value.trim())
        .trim()
        .to_string();
    if email.is_empty() {
        bail!("ATTENDEE email is required");
    }
    Ok(DavAttendee {
        email,
        common_name: property_parameter(left, "CN").unwrap_or_default(),
        role: property_parameter(left, "ROLE").unwrap_or_else(|| "REQ-PARTICIPANT".to_string()),
        partstat: property_parameter(left, "PARTSTAT")
            .unwrap_or_else(|| "NEEDS-ACTION".to_string()),
        rsvp: property_parameter(left, "RSVP")
            .map(|value| value.eq_ignore_ascii_case("TRUE"))
            .unwrap_or(false),
    })
}

fn attendee_label(attendee: &DavAttendee) -> String {
    if !attendee.common_name.trim().is_empty() {
        attendee.common_name.trim().to_string()
    } else {
        attendee.email.trim().to_string()
    }
}

fn serialize_attendees_json(attendees: &[DavAttendee]) -> String {
    serde_json::to_string(attendees).unwrap_or_else(|_| "[]".to_string())
}

fn param_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "'")
        .replace(';', "\\;")
}

fn parse_report_filter(body: &[u8]) -> Result<ReportFilter> {
    if body.is_empty() {
        return Ok(ReportFilter::default());
    }
    let xml = std::str::from_utf8(body)?;
    Ok(ReportFilter {
        hrefs: xml_tag_values(xml, "href"),
        text_terms: xml_text_match_values(xml),
        time_range_start: xml_attribute_value(xml, "time-range", "start"),
        time_range_end: xml_attribute_value(xml, "time-range", "end"),
    })
}

fn xml_tag_values(xml: &str, local_name: &str) -> Vec<String> {
    let mut values = Vec::new();
    let needle = format!(":{local_name}>");
    let mut remaining = xml;
    while let Some(index) = remaining.find(&needle) {
        let value_start = index + needle.len();
        let rest = &remaining[value_start..];
        let Some(value_end) = rest.find('<') else {
            break;
        };
        let value = rest[..value_end].trim();
        if !value.is_empty() {
            values.push(value.to_string());
        }
        remaining = &rest[value_end..];
    }
    values
}

fn xml_text_match_values(xml: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut remaining = xml;
    while let Some(index) = remaining.find(":text-match") {
        let Some(open_end) = remaining[index..].find('>') else {
            break;
        };
        let rest = &remaining[index + open_end + 1..];
        let Some(close_index) = rest.find("</") else {
            break;
        };
        let value = rest[..close_index].trim();
        if !value.is_empty() {
            values.push(value.to_string());
        }
        remaining = &rest[close_index + 2..];
    }
    values
}

fn xml_attribute_value(xml: &str, element: &str, attribute: &str) -> Option<String> {
    let needle = format!(":{element}");
    let index = xml.find(&needle)?;
    let rest = &xml[index..];
    let open_end = rest.find('>')?;
    let element_text = &rest[..open_end];
    let attr = format!("{attribute}=\"");
    let attr_index = element_text.find(&attr)?;
    let value_start = attr_index + attr.len();
    let value = &element_text[value_start..];
    let value_end = value.find('"')?;
    Some(value[..value_end].to_string())
}

fn contact_matches_report(contact: &ClientContact, filter: &ReportFilter) -> bool {
    if !filter.hrefs.is_empty()
        && !filter
            .hrefs
            .iter()
            .any(|href| href == &contact_href(contact.id))
    {
        return false;
    }
    if filter.text_terms.is_empty() {
        return true;
    }
    let haystack = format!(
        "{} {} {} {} {} {}",
        contact.name, contact.email, contact.role, contact.phone, contact.team, contact.notes
    )
    .to_lowercase();
    filter
        .text_terms
        .iter()
        .all(|term| haystack.contains(&term.trim().to_lowercase()))
}

fn event_matches_report(event: &ClientEvent, filter: &ReportFilter) -> bool {
    if !filter.hrefs.is_empty()
        && !filter
            .hrefs
            .iter()
            .any(|href| href == &event_href(event.id))
    {
        return false;
    }
    if !filter.text_terms.is_empty() {
        let haystack = format!(
            "{} {} {} {}",
            event.title, event.location, event.attendees, event.notes
        )
        .to_lowercase();
        if !filter
            .text_terms
            .iter()
            .all(|term| haystack.contains(&term.trim().to_lowercase()))
        {
            return false;
        }
    }
    let start = format_ical_datetime(&event.date, &event.time);
    if let Some(range_start) = filter.time_range_start.as_deref() {
        if normalize_time_range_value(range_start).as_deref() > Some(start.as_str()) {
            return false;
        }
    }
    if let Some(range_end) = filter.time_range_end.as_deref() {
        if normalize_time_range_value(range_end).as_deref() <= Some(start.as_str()) {
            return false;
        }
    }
    true
}

fn normalize_time_range_value(value: &str) -> Option<String> {
    let value = value.trim_end_matches('Z');
    if value.len() < 15 {
        return None;
    }
    Some(value[..15].to_string())
}

fn precondition_not_modified(headers: &HeaderMap, current_etag: &str) -> bool {
    match_condition_header(headers.get("if-none-match"), current_etag)
}

fn check_write_preconditions(headers: &HeaderMap, current_etag: Option<String>) -> Result<()> {
    if let Some(if_match) = headers.get("if-match") {
        let Some(current_etag) = current_etag.as_deref() else {
            bail!("precondition failed");
        };
        if !match_condition_header(Some(if_match), current_etag) {
            bail!("precondition failed");
        }
    }
    if let Some(if_none_match) = headers.get("if-none-match") {
        if let Some(current_etag) = current_etag.as_deref() {
            if match_condition_header(Some(if_none_match), current_etag) {
                bail!("precondition failed");
            }
        }
    }
    Ok(())
}

fn check_delete_preconditions(headers: &HeaderMap, current_etag: Option<String>) -> Result<()> {
    let Some(current_etag) = current_etag else {
        bail!("not found");
    };
    if let Some(if_match) = headers.get("if-match") {
        if !match_condition_header(Some(if_match), &current_etag) {
            bail!("precondition failed");
        }
    }
    if let Some(if_none_match) = headers.get("if-none-match") {
        if match_condition_header(Some(if_none_match), &current_etag) {
            bail!("precondition failed");
        }
    }
    Ok(())
}

fn match_condition_header(
    header_value: Option<&axum::http::HeaderValue>,
    current_etag: &str,
) -> bool {
    let Some(header_value) = header_value.and_then(|value| value.to_str().ok()) else {
        return false;
    };
    header_value
        .split(',')
        .map(str::trim)
        .any(|candidate| candidate == "*" || candidate == current_etag)
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
                tenant_id: "tenant-a".to_string(),
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
                time_zone: input.time_zone,
                duration_minutes: input.duration_minutes,
                recurrence_rule: input.recurrence_rule,
                title: input.title,
                location: input.location,
                attendees: input.attendees,
                attendees_json: input.attendees_json,
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
            self.contacts
                .lock()
                .unwrap()
                .retain(|entry| entry.id != contact_id);
            Box::pin(async move { Ok(()) })
        }

        fn delete_client_event<'a>(
            &'a self,
            _account_id: Uuid,
            event_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, ()> {
            self.events
                .lock()
                .unwrap()
                .retain(|entry| entry.id != event_id);
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
                time_zone: "".to_string(),
                duration_minutes: 0,
                recurrence_rule: "".to_string(),
                title: "Standup".to_string(),
                location: "Room A".to_string(),
                attendees: "alice@example.test".to_string(),
                attendees_json: "[]".to_string(),
                notes: "Daily".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store);

        let response = service
            .handle(
                &Method::GET,
                &Uri::from_static(
                    "/dav/calendars/me/default/22222222-2222-2222-2222-222222222222.ics",
                ),
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
                time_zone: "".to_string(),
                duration_minutes: 0,
                recurrence_rule: "".to_string(),
                title: "Review".to_string(),
                location: "".to_string(),
                attendees: "".to_string(),
                attendees_json: "[]".to_string(),
                notes: "".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store.clone());

        let response = service
            .handle(
                &Method::DELETE,
                &Uri::from_static(
                    "/dav/calendars/me/default/44444444-4444-4444-4444-444444444444.ics",
                ),
                &bearer_headers(),
                &[],
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(store.events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_returns_not_modified_when_if_none_match_matches() {
        let event_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
        let event = ClientEvent {
            id: event_id,
            date: "2026-04-22".to_string(),
            time: "14:00".to_string(),
            time_zone: "Europe/Berlin".to_string(),
            duration_minutes: 45,
            recurrence_rule: "FREQ=WEEKLY;BYDAY=WE".to_string(),
            title: "Planning".to_string(),
            location: "Room B".to_string(),
            attendees: "Alice".to_string(),
            attendees_json: serialize_attendees_json(&[DavAttendee {
                email: "alice@example.test".to_string(),
                common_name: "Alice".to_string(),
                role: "REQ-PARTICIPANT".to_string(),
                partstat: "ACCEPTED".to_string(),
                rsvp: true,
            }]),
            notes: "Weekly planning".to_string(),
        };
        let store = FakeStore {
            session: Some(FakeStore::account()),
            events: Arc::new(Mutex::new(vec![event.clone()])),
            ..Default::default()
        };
        let service = DavService::new(store);
        let mut headers = bearer_headers();
        headers.insert(
            "if-none-match",
            HeaderValue::from_str(&etag_for_event(&event)).unwrap(),
        );

        let response = service
            .handle(
                &Method::GET,
                &Uri::from_static(
                    "/dav/calendars/me/default/55555555-5555-5555-5555-555555555555.ics",
                ),
                &headers,
                &[],
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    }

    #[tokio::test]
    async fn report_filters_collection_by_text_and_href() {
        let first_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
        let second_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            contacts: Arc::new(Mutex::new(vec![
                ClientContact {
                    id: first_id,
                    name: "Bob Example".to_string(),
                    role: "Sales".to_string(),
                    email: "bob@example.test".to_string(),
                    phone: "".to_string(),
                    team: "".to_string(),
                    notes: "".to_string(),
                },
                ClientContact {
                    id: second_id,
                    name: "Carol Example".to_string(),
                    role: "Ops".to_string(),
                    email: "carol@example.test".to_string(),
                    phone: "".to_string(),
                    team: "".to_string(),
                    notes: "".to_string(),
                },
            ])),
            ..Default::default()
        };
        let service = DavService::new(store);
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
<card:addressbook-query xmlns:d=\"DAV:\" xmlns:card=\"urn:ietf:params:xml:ns:carddav\">\
<d:prop><d:getetag/><card:address-data/></d:prop>\
<card:filter><card:prop-filter name=\"FN\"><card:text-match>bob</card:text-match></card:prop-filter></card:filter>\
<d:href>{}</d:href>\
</card:addressbook-query>",
            contact_href(first_id)
        );

        let response = service
            .handle(
                &Method::from_bytes(b"REPORT").unwrap(),
                &Uri::from_static(ADDRESSBOOK_COLLECTION_PATH),
                &bearer_headers(),
                body.as_bytes(),
            )
            .await
            .unwrap();

        let payload = response_text(response).await;
        assert!(payload.contains(&contact_href(first_id)));
        assert!(!payload.contains(&contact_href(second_id)));
    }

    #[tokio::test]
    async fn put_rejects_stale_if_match() {
        let contact_id = Uuid::parse_str("88888888-8888-8888-8888-888888888888").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            contacts: Arc::new(Mutex::new(vec![ClientContact {
                id: contact_id,
                name: "Dora".to_string(),
                role: "".to_string(),
                email: "dora@example.test".to_string(),
                phone: "".to_string(),
                team: "".to_string(),
                notes: "".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store);
        let mut headers = bearer_headers();
        headers.insert("if-match", HeaderValue::from_static("\"stale\""));

        let error = service
            .handle(
                &Method::PUT,
                &Uri::from_static("/dav/addressbooks/me/default/88888888-8888-8888-8888-888888888888.vcf"),
                &headers,
                b"BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Dora Updated\r\nEMAIL:dora@example.test\r\nEND:VCARD",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("precondition failed"));
    }

    #[tokio::test]
    async fn put_parses_structured_calendar_metadata() {
        let event_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = DavService::new(store.clone());

        let response = service
            .handle(
                &Method::PUT,
                &Uri::from_static("/dav/calendars/me/default/99999999-9999-9999-9999-999999999999.ics"),
                &bearer_headers(),
                b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:99999999-9999-9999-9999-999999999999\r\nDTSTART;TZID=Europe/Berlin:20260423T103000\r\nDURATION:PT45M\r\nRRULE:FREQ=WEEKLY;BYDAY=TH\r\nSUMMARY:Interop review\r\nATTENDEE;CN=Alice Example;ROLE=REQ-PARTICIPANT;PARTSTAT=ACCEPTED;RSVP=TRUE:mailto:alice@example.test\r\nDESCRIPTION:Calendar interop\r\nEND:VEVENT\r\nEND:VCALENDAR",
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let events = store.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, event_id);
        assert_eq!(events[0].time_zone, "Europe/Berlin");
        assert_eq!(events[0].duration_minutes, 45);
        assert_eq!(events[0].recurrence_rule, "FREQ=WEEKLY;BYDAY=TH");
        assert_eq!(events[0].attendees, "Alice Example");
        assert!(events[0].attendees_json.contains("alice@example.test"));
    }
}
