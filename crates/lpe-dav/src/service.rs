use anyhow::{anyhow, bail, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, Method, Uri},
    response::Response,
    routing::any,
    Router,
};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{AccessibleContact, AccessibleEvent, DavTask, Storage};
use uuid::Uuid;

use crate::{
    parse::{parse_ical, parse_vcard, parse_vtodo},
    paths::{
        collection_id_from_contact_path, collection_id_from_event_path, etag, etag_for_contact,
        etag_for_event, etag_for_task, normalized_path, resource_id_for_contact_path,
        resource_id_for_event_path, resource_id_for_task_path, task_collection_id_from_path,
        ADDRESSBOOK_COLLECTION_PATH, ADDRESSBOOK_HOME_PATH, CALENDAR_COLLECTION_PATH,
        CALENDAR_HOME_PATH, PRINCIPAL_PATH, ROOT_PATH,
    },
    preconditions::{
        check_delete_preconditions, check_write_preconditions, precondition_not_modified,
    },
    propfind::{
        addressbook_collection_entry, calendar_collection_entry, collection_home_entry,
        collection_resourcetype, contact_report_entry, contact_resource_entry, event_report_entry,
        event_resource_entry, principal_propfind_entry, root_propfind_entry, task_collection_entry,
        task_report_entry, task_resource_entry,
    },
    report::{
        contact_matches_report, event_matches_report, parse_report_filter, task_matches_report,
    },
    responses::{
        error_response, multistatus_response, options_response, redirect_response, status_only,
        status_with_etag, text_response,
    },
    serialize::{serialize_ical, serialize_vcard, serialize_vtodo},
    store::DavStore,
};

pub fn router() -> Router<Storage> {
    Router::new()
        .route("/dav", any(dav_handler))
        .route("/dav/{*path}", any(dav_handler))
        .route("/.well-known/carddav", any(carddav_redirect))
        .route("/.well-known/caldav", any(caldav_redirect))
}

#[derive(Clone)]
pub(crate) struct DavService<S> {
    store: S,
}

impl<S> DavService<S> {
    pub(crate) fn new(store: S) -> Self {
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
    pub(crate) async fn handle(
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

        let principal = authenticate_account(&self.store, None, headers, "dav").await?;
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
            ADDRESSBOOK_HOME_PATH => {
                let mut entries = vec![collection_home_entry(
                    ADDRESSBOOK_HOME_PATH,
                    "Address Books",
                    collection_resourcetype("collection"),
                )];
                if depth == "1" {
                    entries.extend(
                        self.store
                            .fetch_accessible_contact_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(addressbook_collection_entry),
                    );
                }
                entries
            }
            CALENDAR_HOME_PATH => {
                let mut entries = vec![collection_home_entry(
                    CALENDAR_HOME_PATH,
                    "Calendars",
                    collection_resourcetype("collection"),
                )];
                if depth == "1" {
                    entries.extend(
                        self.store
                            .fetch_accessible_calendar_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(calendar_collection_entry),
                    );
                    entries.extend(
                        self.store
                            .fetch_accessible_task_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(task_collection_entry),
                    );
                }
                entries
            }
            _ => {
                if let Some(contact) = self.contact_for_path(principal.account_id, path).await? {
                    vec![contact_resource_entry(contact)]
                } else if let Some(task) = self.task_for_path(principal.account_id, path).await? {
                    vec![task_resource_entry(task)]
                } else if let Some(event) = self.event_for_path(principal.account_id, path).await? {
                    vec![event_resource_entry(event)]
                } else if let Some(collection_id) = collection_id_from_contact_path(path) {
                    let collections = self
                        .store
                        .fetch_accessible_contact_collections(principal.account_id)
                        .await?;
                    let collection = collections
                        .into_iter()
                        .find(|entry| entry.id == collection_id)
                        .ok_or_else(|| anyhow!("not found"))?;
                    let mut entries = vec![addressbook_collection_entry(collection.clone())];
                    if depth == "1" {
                        entries.extend(
                            self.store
                                .fetch_accessible_contacts_in_collection(
                                    principal.account_id,
                                    &collection.id,
                                )
                                .await?
                                .into_iter()
                                .map(contact_resource_entry),
                        );
                    }
                    entries
                } else if let Some(collection_id) = task_collection_id_from_path(path) {
                    let collections = self
                        .store
                        .fetch_accessible_task_collections(principal.account_id)
                        .await?;
                    let collection = collections
                        .into_iter()
                        .find(|entry| entry.id == collection_id)
                        .ok_or_else(|| anyhow!("not found"))?;
                    let mut entries = vec![task_collection_entry(collection.clone())];
                    if depth == "1" {
                        entries.extend(
                            self.store
                                .fetch_dav_tasks(principal.account_id)
                                .await?
                                .into_iter()
                                .filter(|task| task.collection_id == collection.id)
                                .map(task_resource_entry),
                        );
                    }
                    entries
                } else if let Some(collection_id) = collection_id_from_event_path(path) {
                    let collections = self
                        .store
                        .fetch_accessible_calendar_collections(principal.account_id)
                        .await?;
                    let collection = collections
                        .into_iter()
                        .find(|entry| entry.id == collection_id)
                        .ok_or_else(|| anyhow!("not found"))?;
                    let mut entries = vec![calendar_collection_entry(collection.clone())];
                    if depth == "1" {
                        entries.extend(
                            self.store
                                .fetch_accessible_events_in_collection(
                                    principal.account_id,
                                    &collection.id,
                                )
                                .await?
                                .into_iter()
                                .map(event_resource_entry),
                        );
                    }
                    entries
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
        let entries = if let Some(collection_id) = collection_id_from_contact_path(path) {
            self.store
                .fetch_accessible_contacts_in_collection(principal.account_id, &collection_id)
                .await?
                .into_iter()
                .filter(|contact| contact_matches_report(contact, &filter))
                .map(contact_report_entry)
                .collect()
        } else if let Some(collection_id) = task_collection_id_from_path(path) {
            self.store
                .fetch_dav_tasks(principal.account_id)
                .await?
                .into_iter()
                .filter(|task| task.collection_id == collection_id)
                .filter(|task| task_matches_report(task, &filter))
                .map(task_report_entry)
                .collect()
        } else if let Some(collection_id) = collection_id_from_event_path(path) {
            self.store
                .fetch_accessible_events_in_collection(principal.account_id, &collection_id)
                .await?
                .into_iter()
                .filter(|event| event_matches_report(event, &filter))
                .map(event_report_entry)
                .collect()
        } else {
            bail!("not found")
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
        if let Some(task) = self.task_for_path(principal.account_id, path).await? {
            let body = serialize_vtodo(&task);
            if precondition_not_modified(headers, &etag(&body)) {
                return Ok(status_only(304));
            }
            return Ok(text_response(
                "text/calendar; charset=utf-8",
                body,
                Some(etag_for_task(&task)),
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
        if let Some((collection_id, resource_id)) = resource_id_for_contact_path(path) {
            let existing = self.contact_for_path(principal.account_id, path).await?;
            check_write_preconditions(headers, existing.as_ref().map(etag_for_contact))?;
            let parsed = parse_vcard(resource_id, principal.account_id, body)?;
            let contact = if existing.is_some() {
                self.store
                    .update_accessible_contact(principal.account_id, resource_id, parsed)
                    .await?
            } else {
                self.store
                    .create_accessible_contact(principal.account_id, Some(&collection_id), parsed)
                    .await?
            };
            return Ok(status_with_etag(
                if existing.is_some() { 204 } else { 201 },
                etag_for_contact(&contact),
            ));
        }
        if let Some((collection_id, resource_id)) = resource_id_for_task_path(path) {
            let existing = self.task_for_path(principal.account_id, path).await?;
            check_write_preconditions(headers, existing.as_ref().map(etag_for_task))?;
            let task = self
                .store
                .upsert_dav_task(parse_vtodo(
                    resource_id,
                    principal.account_id,
                    Some(&collection_id),
                    body,
                )?)
                .await?;
            return Ok(status_with_etag(
                if existing.is_some() { 204 } else { 201 },
                etag_for_task(&task),
            ));
        }
        if let Some((collection_id, resource_id)) = resource_id_for_event_path(path) {
            let existing = self.event_for_path(principal.account_id, path).await?;
            check_write_preconditions(headers, existing.as_ref().map(etag_for_event))?;
            let parsed = parse_ical(resource_id, principal.account_id, body)?;
            let event = if existing.is_some() {
                self.store
                    .update_accessible_event(principal.account_id, resource_id, parsed)
                    .await?
            } else {
                self.store
                    .create_accessible_event(principal.account_id, Some(&collection_id), parsed)
                    .await?
            };
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
        if let Some((_, resource_id)) = resource_id_for_contact_path(path) {
            let existing = self.contact_for_path(principal.account_id, path).await?;
            check_delete_preconditions(headers, existing.as_ref().map(etag_for_contact))?;
            self.store
                .delete_accessible_contact(principal.account_id, resource_id)
                .await?;
            return Ok(status_only(204));
        }
        if let Some((_, resource_id)) = resource_id_for_task_path(path) {
            let existing = self.task_for_path(principal.account_id, path).await?;
            check_delete_preconditions(headers, existing.as_ref().map(etag_for_task))?;
            self.store
                .delete_dav_task(principal.account_id, resource_id)
                .await?;
            return Ok(status_only(204));
        }
        if let Some((_, resource_id)) = resource_id_for_event_path(path) {
            let existing = self.event_for_path(principal.account_id, path).await?;
            check_delete_preconditions(headers, existing.as_ref().map(etag_for_event))?;
            self.store
                .delete_accessible_event(principal.account_id, resource_id)
                .await?;
            return Ok(status_only(204));
        }
        bail!("not found")
    }

    async fn contact_for_path(
        &self,
        account_id: Uuid,
        path: &str,
    ) -> Result<Option<AccessibleContact>> {
        let Some((collection_id, resource_id)) = resource_id_for_contact_path(path) else {
            return Ok(None);
        };
        Ok(self
            .store
            .fetch_accessible_contacts_in_collection(account_id, &collection_id)
            .await?
            .into_iter()
            .find(|contact| contact.id == resource_id))
    }

    async fn event_for_path(
        &self,
        account_id: Uuid,
        path: &str,
    ) -> Result<Option<AccessibleEvent>> {
        let Some((collection_id, resource_id)) = resource_id_for_event_path(path) else {
            return Ok(None);
        };
        Ok(self
            .store
            .fetch_accessible_events_in_collection(account_id, &collection_id)
            .await?
            .into_iter()
            .find(|event| event.id == resource_id))
    }

    async fn task_for_path(&self, account_id: Uuid, path: &str) -> Result<Option<DavTask>> {
        let Some((collection_id, resource_id)) = resource_id_for_task_path(path) else {
            return Ok(None);
        };
        Ok(self
            .store
            .fetch_dav_tasks_by_ids(account_id, &[resource_id])
            .await?
            .into_iter()
            .find(|task| task.collection_id == collection_id))
    }
}
