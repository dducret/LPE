use anyhow::{anyhow, bail, Result};
use axum::{
    body::Bytes,
    extract::State,
    http::{
        header::{CONTENT_TYPE, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
    routing::{on, MethodFilter},
    Router,
};
use lpe_mail_auth::{authenticate_account, AccountPrincipal};
use lpe_storage::{AccessibleContact, AccessibleEvent, CollaborationCollection, Storage};
use uuid::Uuid;

use crate::store::ExchangeStore;

const EWS_PATH: &str = "/EWS/Exchange.asmx";
const EWS_LOWER_PATH: &str = "/ews/exchange.asmx";
const CONTACTS_FOLDER_ID: &str = "contacts";
const CALENDAR_FOLDER_ID: &str = "calendar";
const DEFAULT_COLLECTION_ID: &str = "default";

pub fn router() -> Router<Storage> {
    Router::new()
        .route(
            EWS_PATH,
            on(MethodFilter::OPTIONS, options_handler).post(post_handler),
        )
        .route(
            EWS_LOWER_PATH,
            on(MethodFilter::OPTIONS, options_handler).post(post_handler),
        )
}

#[derive(Clone)]
pub(crate) struct ExchangeService<S> {
    store: S,
}

impl<S> ExchangeService<S> {
    pub(crate) fn new(store: S) -> Self {
        Self { store }
    }
}

async fn options_handler() -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .headers_mut()
        .insert("allow", HeaderValue::from_static("OPTIONS, POST"));
    response
}

async fn post_handler(State(storage): State<Storage>, headers: HeaderMap, body: Bytes) -> Response {
    let service = ExchangeService::new(storage);
    match service.handle(&headers, body.as_ref()).await {
        Ok(response) => response,
        Err(error) => error_response(&error),
    }
}

impl<S: ExchangeStore> ExchangeService<S> {
    pub(crate) async fn handle(&self, headers: &HeaderMap, body: &[u8]) -> Result<Response> {
        let principal = authenticate_account(&self.store, None, headers, "ews").await?;
        let body = std::str::from_utf8(body).map_err(|_| anyhow!("EWS request is not UTF-8"))?;
        let operation = operation_name(body).ok_or_else(|| anyhow!("unsupported EWS request"))?;

        let payload = match operation {
            "SyncFolderHierarchy" => self.sync_folder_hierarchy(&principal).await?,
            "FindFolder" => self.find_folder(&principal).await?,
            "GetFolder" => self.get_folder(&principal, body).await?,
            "FindItem" => self.find_item(&principal, body).await?,
            "GetItem" => self.get_item(&principal, body).await?,
            "SyncFolderItems" => self.sync_folder_items(&principal, body).await?,
            "GetServerTimeZones" => get_server_time_zones_response(),
            "ResolveNames" => resolve_names_no_results_response(),
            "GetUserAvailability" => get_user_availability_unavailable_response(),
            "CreateItem" => unsupported_operation_response("CreateItem"),
            "UpdateItem" => unsupported_operation_response("UpdateItem"),
            "DeleteItem" => unsupported_operation_response("DeleteItem"),
            _ => bail!("unsupported EWS operation {operation}"),
        };

        Ok(soap_response(payload))
    }

    async fn find_folder(&self, principal: &AccountPrincipal) -> Result<String> {
        let mut folders = String::new();
        for collection in self
            .store
            .fetch_accessible_contact_collections(principal.account_id)
            .await?
        {
            folders.push_str(&folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts"));
        }
        for collection in self
            .store
            .fetch_accessible_calendar_collections(principal.account_id)
            .await?
        {
            folders.push_str(&folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar"));
        }

        Ok(format!(
            concat!(
                "<m:FindFolderResponse>",
                "<m:ResponseMessages>",
                "<m:FindFolderResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:RootFolder TotalItemsInView=\"{count}\" IncludesLastItemInRange=\"true\">",
                "<t:Folders>{folders}</t:Folders>",
                "</m:RootFolder>",
                "</m:FindFolderResponseMessage>",
                "</m:ResponseMessages>",
                "</m:FindFolderResponse>"
            ),
            folders = folders,
            count = count_tag_occurrences(&folders, "<t:Folder>"),
        ))
    }

    async fn sync_folder_hierarchy(&self, principal: &AccountPrincipal) -> Result<String> {
        let mut changes = String::new();
        let mut count = 0;
        for collection in self
            .store
            .fetch_accessible_contact_collections(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts"));
            changes.push_str("</t:Create>");
            count += 1;
        }
        for collection in self
            .store
            .fetch_accessible_calendar_collections(principal.account_id)
            .await?
        {
            changes.push_str("<t:Create>");
            changes.push_str(&folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar"));
            changes.push_str("</t:Create>");
            count += 1;
        }
        let sync_state = format!("folder-hierarchy:{count}");

        Ok(format!(
            concat!(
                "<m:SyncFolderHierarchyResponse>",
                "<m:ResponseMessages>",
                "<m:SyncFolderHierarchyResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:SyncState>{sync_state}</m:SyncState>",
                "<m:IncludesLastFolderInRange>true</m:IncludesLastFolderInRange>",
                "<m:Changes>{changes}</m:Changes>",
                "</m:SyncFolderHierarchyResponseMessage>",
                "</m:ResponseMessages>",
                "</m:SyncFolderHierarchyResponse>"
            ),
            sync_state = escape_xml(&sync_state),
            changes = changes,
        ))
    }

    async fn get_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let requested = requested_folder_kinds(request);
        if requested.is_empty() && request_contains_folder_reference(request) {
            return Ok(get_folder_error_response(
                "ErrorFolderNotFound",
                "folder not found",
            ));
        }

        let mut folders = String::new();
        for kind in requested {
            match kind {
                FolderKind::Root => folders.push_str(&root_folder_xml()),
                FolderKind::Contacts => {
                    folders.push_str(
                        &self
                            .store
                            .fetch_accessible_contact_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(|collection| {
                                folder_xml(&collection, CONTACTS_FOLDER_ID, "Contacts")
                            })
                            .collect::<String>(),
                    );
                }
                FolderKind::Calendar => {
                    folders.push_str(
                        &self
                            .store
                            .fetch_accessible_calendar_collections(principal.account_id)
                            .await?
                            .into_iter()
                            .map(|collection| {
                                folder_xml(&collection, CALENDAR_FOLDER_ID, "Calendar")
                            })
                            .collect::<String>(),
                    );
                }
            }
        }

        if folders.is_empty() {
            bail!("folder not found");
        }

        Ok(format!(
            concat!(
                "<m:GetFolderResponse>",
                "<m:ResponseMessages>",
                "<m:GetFolderResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:Folders>{folders}</m:Folders>",
                "</m:GetFolderResponseMessage>",
                "</m:ResponseMessages>",
                "</m:GetFolderResponse>"
            ),
            folders = folders,
        ))
    }

    async fn find_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        match requested_folder_kind(request).unwrap_or(FolderKind::Contacts) {
            FolderKind::Root => Ok(find_item_response(String::new())),
            FolderKind::Contacts => {
                let collection_id = requested_collection_id(request).unwrap_or(CONTACTS_FOLDER_ID);
                let contacts = self
                    .store
                    .fetch_accessible_contacts_in_collection(principal.account_id, collection_id)
                    .await?;
                Ok(find_item_response(
                    contacts.iter().map(contact_summary_xml).collect(),
                ))
            }
            FolderKind::Calendar => {
                let collection_id = requested_collection_id(request).unwrap_or(CALENDAR_FOLDER_ID);
                let events = self
                    .store
                    .fetch_accessible_events_in_collection(principal.account_id, collection_id)
                    .await?;
                Ok(find_item_response(
                    events.iter().map(calendar_item_summary_xml).collect(),
                ))
            }
        }
    }

    async fn get_item(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let ids = requested_item_ids(request);
        let contact_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("contact:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();
        let event_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("event:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();

        let mut items = String::new();
        for contact in self
            .store
            .fetch_accessible_contacts_by_ids(principal.account_id, &contact_ids)
            .await?
        {
            items.push_str(&contact_item_xml(&contact));
        }
        for event in self
            .store
            .fetch_accessible_events_by_ids(principal.account_id, &event_ids)
            .await?
        {
            items.push_str(&calendar_item_xml(&event));
        }

        Ok(format!(
            concat!(
                "<m:GetItemResponse>",
                "<m:ResponseMessages>",
                "<m:GetItemResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:Items>{items}</m:Items>",
                "</m:GetItemResponseMessage>",
                "</m:ResponseMessages>",
                "</m:GetItemResponse>"
            ),
            items = items,
        ))
    }

    async fn sync_folder_items(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let mut changes = String::new();
        let sync_state = match requested_folder_kind(request).unwrap_or(FolderKind::Contacts) {
            FolderKind::Root => "root:0".to_string(),
            FolderKind::Contacts => {
                let collection_id = requested_collection_id(request).unwrap_or(CONTACTS_FOLDER_ID);
                let contacts = self
                    .store
                    .fetch_accessible_contacts_in_collection(principal.account_id, collection_id)
                    .await?;
                for contact in &contacts {
                    changes.push_str("<t:Create>");
                    changes.push_str(&contact_item_xml(contact));
                    changes.push_str("</t:Create>");
                }
                format!("contacts:{collection_id}:{}", contacts.len())
            }
            FolderKind::Calendar => {
                let collection_id = requested_collection_id(request).unwrap_or(CALENDAR_FOLDER_ID);
                let events = self
                    .store
                    .fetch_accessible_events_in_collection(principal.account_id, collection_id)
                    .await?;
                for event in &events {
                    changes.push_str("<t:Create>");
                    changes.push_str(&calendar_item_xml(event));
                    changes.push_str("</t:Create>");
                }
                format!("calendar:{collection_id}:{}", events.len())
            }
        };

        Ok(format!(
            concat!(
                "<m:SyncFolderItemsResponse>",
                "<m:ResponseMessages>",
                "<m:SyncFolderItemsResponseMessage ResponseClass=\"Success\">",
                "<m:ResponseCode>NoError</m:ResponseCode>",
                "<m:SyncState>{sync_state}</m:SyncState>",
                "<m:IncludesLastItemInRange>true</m:IncludesLastItemInRange>",
                "<m:Changes>{changes}</m:Changes>",
                "</m:SyncFolderItemsResponseMessage>",
                "</m:ResponseMessages>",
                "</m:SyncFolderItemsResponse>"
            ),
            sync_state = escape_xml(&sync_state),
            changes = changes,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FolderKind {
    Root,
    Contacts,
    Calendar,
}

fn operation_name(body: &str) -> Option<&'static str> {
    [
        "SyncFolderItems",
        "SyncFolderHierarchy",
        "GetServerTimeZones",
        "GetUserAvailability",
        "ResolveNames",
        "CreateItem",
        "UpdateItem",
        "DeleteItem",
        "FindFolder",
        "GetFolder",
        "FindItem",
        "GetItem",
    ]
    .into_iter()
    .find(|name| {
        body.contains(&format!("<m:{name}"))
            || body.contains(&format!("<{name}"))
            || body.contains(&format!(":{name}"))
    })
}

fn requested_folder_kind(request: &str) -> Option<FolderKind> {
    if request.contains("DistinguishedFolderId Id=\"msgfolderroot\"")
        || request.contains("DistinguishedFolderId Id='msgfolderroot'")
        || request.contains("DistinguishedFolderId Id=\"root\"")
        || request.contains("DistinguishedFolderId Id='root'")
        || request.contains("FolderId Id=\"msgfolderroot\"")
        || request.contains("FolderId Id='msgfolderroot'")
        || request.contains("FolderId Id=\"root\"")
        || request.contains("FolderId Id='root'")
    {
        return Some(FolderKind::Root);
    }
    if request.contains("DistinguishedFolderId Id=\"calendar\"")
        || request.contains("DistinguishedFolderId Id='calendar'")
        || request.contains("FolderId Id=\"calendar\"")
        || request.contains("FolderId Id='calendar'")
    {
        return Some(FolderKind::Calendar);
    }
    if request.contains("DistinguishedFolderId Id=\"contacts\"")
        || request.contains("DistinguishedFolderId Id='contacts'")
        || request.contains("FolderId Id=\"contacts\"")
        || request.contains("FolderId Id='contacts'")
    {
        return Some(FolderKind::Contacts);
    }
    requested_collection_id(request).and_then(|id| {
        if id.starts_with("shared-calendar-") {
            Some(FolderKind::Calendar)
        } else if id.starts_with("shared-contacts-") {
            Some(FolderKind::Contacts)
        } else if id == "msgfolderroot" || id == "root" {
            Some(FolderKind::Root)
        } else {
            None
        }
    })
}

fn requested_folder_kinds(request: &str) -> Vec<FolderKind> {
    let mut kinds = Vec::new();
    if request.contains("DistinguishedFolderId Id=\"msgfolderroot\"")
        || request.contains("DistinguishedFolderId Id='msgfolderroot'")
        || request.contains("DistinguishedFolderId Id=\"root\"")
        || request.contains("DistinguishedFolderId Id='root'")
        || request.contains("FolderId Id=\"msgfolderroot\"")
        || request.contains("FolderId Id='msgfolderroot'")
        || request.contains("FolderId Id=\"root\"")
        || request.contains("FolderId Id='root'")
    {
        kinds.push(FolderKind::Root);
    }
    if request.contains("DistinguishedFolderId Id=\"contacts\"")
        || request.contains("DistinguishedFolderId Id='contacts'")
        || request.contains("FolderId Id=\"contacts\"")
        || request.contains("FolderId Id='contacts'")
        || request.contains("shared-contacts-")
    {
        kinds.push(FolderKind::Contacts);
    }
    if request.contains("DistinguishedFolderId Id=\"calendar\"")
        || request.contains("DistinguishedFolderId Id='calendar'")
        || request.contains("FolderId Id=\"calendar\"")
        || request.contains("FolderId Id='calendar'")
        || request.contains("shared-calendar-")
    {
        kinds.push(FolderKind::Calendar);
    }
    kinds.dedup();
    kinds
}

fn request_contains_folder_reference(request: &str) -> bool {
    request.contains("FolderId") || request.contains("DistinguishedFolderId")
}

fn requested_collection_id(request: &str) -> Option<&str> {
    attribute_value_after(request, "FolderId", "Id")
        .or_else(|| attribute_value_after(request, "DistinguishedFolderId", "Id"))
        .map(|value| match value {
            "contacts" | "calendar" => DEFAULT_COLLECTION_ID,
            other => other,
        })
}

fn requested_item_ids(request: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut rest = request;
    while let Some(index) = rest.find("<t:ItemId").or_else(|| rest.find("<ItemId")) {
        rest = &rest[index..];
        if let Some(id) = attribute_value_after(rest, "ItemId", "Id") {
            ids.push(id.to_string());
        }
        rest = &rest[1..];
    }
    ids
}

fn attribute_value_after<'a>(body: &'a str, tag: &str, attr: &str) -> Option<&'a str> {
    let index = body.find(tag)?;
    let rest = &body[index..];
    let end = rest.find('>')?;
    let tag_text = &rest[..end];
    attribute_value(tag_text, attr)
}

fn attribute_value<'a>(tag_text: &'a str, attr: &str) -> Option<&'a str> {
    let pattern = format!("{attr}=");
    let start = tag_text.find(&pattern)? + pattern.len();
    let quote = tag_text[start..].chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let value_start = start + quote.len_utf8();
    let value_end = tag_text[value_start..].find(quote)? + value_start;
    Some(&tag_text[value_start..value_end])
}

fn find_item_response(items: String) -> String {
    format!(
        concat!(
            "<m:FindItemResponse>",
            "<m:ResponseMessages>",
            "<m:FindItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:RootFolder TotalItemsInView=\"{count}\" IncludesLastItemInRange=\"true\">",
            "<t:Items>{items}</t:Items>",
            "</m:RootFolder>",
            "</m:FindItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:FindItemResponse>"
        ),
        items = items,
        count = count_tag_occurrences(&items, "<t:ItemId")
    )
}

fn get_folder_error_response(code: &str, message: &str) -> String {
    format!(
        concat!(
            "<m:GetFolderResponse>",
            "<m:ResponseMessages>",
            "<m:GetFolderResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{message}</m:MessageText>",
            "<m:ResponseCode>{code}</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "<m:Folders/>",
            "</m:GetFolderResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetFolderResponse>"
        ),
        code = escape_xml(code),
        message = escape_xml(message),
    )
}

fn get_server_time_zones_response() -> String {
    concat!(
        "<m:GetServerTimeZonesResponse>",
        "<m:ResponseMessages>",
        "<m:GetServerTimeZonesResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "<m:TimeZoneDefinitions>",
        "<t:TimeZoneDefinition Id=\"UTC\" Name=\"(UTC) Coordinated Universal Time\"/>",
        "<t:TimeZoneDefinition Id=\"W. Europe Standard Time\" Name=\"(UTC+01:00) Amsterdam, Berlin, Bern, Rome, Stockholm, Vienna\"/>",
        "</m:TimeZoneDefinitions>",
        "</m:GetServerTimeZonesResponseMessage>",
        "</m:ResponseMessages>",
        "</m:GetServerTimeZonesResponse>"
    )
    .to_string()
}

fn resolve_names_no_results_response() -> String {
    concat!(
        "<m:ResolveNamesResponse>",
        "<m:ResponseMessages>",
        "<m:ResolveNamesResponseMessage ResponseClass=\"Error\">",
        "<m:MessageText>No results were found.</m:MessageText>",
        "<m:ResponseCode>ErrorNameResolutionNoResults</m:ResponseCode>",
        "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
        "</m:ResolveNamesResponseMessage>",
        "</m:ResponseMessages>",
        "</m:ResolveNamesResponse>"
    )
    .to_string()
}

fn get_user_availability_unavailable_response() -> String {
    concat!(
        "<m:GetUserAvailabilityResponse>",
        "<m:FreeBusyResponseArray>",
        "<m:FreeBusyResponse>",
        "<m:ResponseMessage ResponseClass=\"Error\">",
        "<m:MessageText>Free/busy is not implemented by the EWS MVP.</m:MessageText>",
        "<m:ResponseCode>ErrorFreeBusyGenerationFailed</m:ResponseCode>",
        "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
        "</m:ResponseMessage>",
        "</m:FreeBusyResponse>",
        "</m:FreeBusyResponseArray>",
        "</m:GetUserAvailabilityResponse>"
    )
    .to_string()
}

fn unsupported_operation_response(operation: &str) -> String {
    format!(
        concat!(
            "<m:{operation}Response>",
            "<m:ResponseMessages>",
            "<m:{operation}ResponseMessage ResponseClass=\"Error\">",
            "<m:MessageText>{operation} is not implemented by the EWS MVP.</m:MessageText>",
            "<m:ResponseCode>ErrorUnsupportedOperation</m:ResponseCode>",
            "<m:DescriptiveLinkKey>0</m:DescriptiveLinkKey>",
            "</m:{operation}ResponseMessage>",
            "</m:ResponseMessages>",
            "</m:{operation}Response>"
        ),
        operation = escape_xml(operation),
    )
}

fn root_folder_xml() -> String {
    concat!(
        "<t:Folder>",
        "<t:FolderId Id=\"msgfolderroot\"/>",
        "<t:DisplayName>Root</t:DisplayName>",
        "<t:FolderClass>IPF.Note</t:FolderClass>",
        "<t:DistinguishedFolderId Id=\"msgfolderroot\"/>",
        "</t:Folder>"
    )
    .to_string()
}

fn folder_xml(collection: &CollaborationCollection, distinguished_id: &str, class: &str) -> String {
    format!(
        concat!(
            "<t:Folder>",
            "<t:FolderId Id=\"{id}\"/>",
            "<t:DisplayName>{display}</t:DisplayName>",
            "<t:FolderClass>IPF.{class}</t:FolderClass>",
            "<t:DistinguishedFolderId Id=\"{distinguished_id}\"/>",
            "</t:Folder>"
        ),
        id = escape_xml(&collection.id),
        display = escape_xml(&collection.display_name),
        class = class,
        distinguished_id = distinguished_id,
    )
}

fn contact_summary_xml(contact: &AccessibleContact) -> String {
    format!(
        concat!(
            "<t:Contact>",
            "<t:ItemId Id=\"contact:{id}\"/>",
            "<t:Subject>{name}</t:Subject>",
            "<t:DisplayName>{name}</t:DisplayName>",
            "</t:Contact>"
        ),
        id = contact.id,
        name = escape_xml(&contact.name),
    )
}

fn contact_item_xml(contact: &AccessibleContact) -> String {
    format!(
        concat!(
            "<t:Contact>",
            "<t:ItemId Id=\"contact:{id}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{name}</t:Subject>",
            "<t:DisplayName>{name}</t:DisplayName>",
            "<t:GivenName>{given}</t:GivenName>",
            "<t:Surname>{surname}</t:Surname>",
            "<t:JobTitle>{role}</t:JobTitle>",
            "<t:CompanyName>{team}</t:CompanyName>",
            "<t:EmailAddresses><t:Entry Key=\"EmailAddress1\">{email}</t:Entry></t:EmailAddresses>",
            "<t:PhoneNumbers><t:Entry Key=\"MobilePhone\">{phone}</t:Entry></t:PhoneNumbers>",
            "<t:Body BodyType=\"Text\">{notes}</t:Body>",
            "</t:Contact>"
        ),
        id = contact.id,
        folder_id = escape_xml(&contact.collection_id),
        name = escape_xml(&contact.name),
        given = escape_xml(&first_name(&contact.name)),
        surname = escape_xml(&last_name(&contact.name)),
        role = escape_xml(&contact.role),
        team = escape_xml(&contact.team),
        email = escape_xml(&contact.email),
        phone = escape_xml(&contact.phone),
        notes = escape_xml(&contact.notes),
    )
}

fn calendar_item_summary_xml(event: &AccessibleEvent) -> String {
    format!(
        concat!(
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Start>{start}</t:Start>",
            "<t:End>{end}</t:End>",
            "</t:CalendarItem>"
        ),
        id = event.id,
        title = escape_xml(&event.title),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
    )
}

fn calendar_item_xml(event: &AccessibleEvent) -> String {
    format!(
        concat!(
            "<t:CalendarItem>",
            "<t:ItemId Id=\"event:{id}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{title}</t:Subject>",
            "<t:Location>{location}</t:Location>",
            "<t:Start>{start}</t:Start>",
            "<t:End>{end}</t:End>",
            "<t:LegacyFreeBusyStatus>Busy</t:LegacyFreeBusyStatus>",
            "<t:Body BodyType=\"Text\">{notes}</t:Body>",
            "</t:CalendarItem>"
        ),
        id = event.id,
        folder_id = escape_xml(&event.collection_id),
        title = escape_xml(&event.title),
        location = escape_xml(&event.location),
        start = escape_xml(&ews_datetime(&event.date, &event.time)),
        end = escape_xml(&event_end_datetime(event)),
        notes = escape_xml(&event.notes),
    )
}

fn ews_datetime(date: &str, time: &str) -> String {
    format!("{}T{}:00Z", date.trim(), time.trim())
}

fn event_end_datetime(event: &AccessibleEvent) -> String {
    let (hour, minute) = event
        .time
        .split_once(':')
        .and_then(|(hour, minute)| Some((hour.parse::<i32>().ok()?, minute.parse::<i32>().ok()?)))
        .unwrap_or((0, 0));
    let total = hour
        .saturating_mul(60)
        .saturating_add(minute)
        .saturating_add(event.duration_minutes.max(0));
    let end_hour = (total / 60).min(23);
    let end_minute = total % 60;
    ews_datetime(&event.date, &format!("{end_hour:02}:{end_minute:02}"))
}

fn first_name(name: &str) -> String {
    name.split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string()
}

fn last_name(name: &str) -> String {
    name.split_whitespace()
        .skip(1)
        .collect::<Vec<_>>()
        .join(" ")
}

fn count_tag_occurrences(value: &str, needle: &str) -> usize {
    value.match_indices(needle).count()
}

fn soap_response(body: String) -> Response {
    let envelope = format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
            "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" ",
            "xmlns:m=\"http://schemas.microsoft.com/exchange/services/2006/messages\" ",
            "xmlns:t=\"http://schemas.microsoft.com/exchange/services/2006/types\">",
            "<s:Header><t:ServerVersionInfo MajorVersion=\"15\" MinorVersion=\"0\" MajorBuildNumber=\"0\" MinorBuildNumber=\"0\" Version=\"Exchange2013\"/></s:Header>",
            "<s:Body>{body}</s:Body>",
            "</s:Envelope>"
        ),
        body = body,
    );
    xml_response(StatusCode::OK, envelope)
}

pub(crate) fn error_response(error: &anyhow::Error) -> Response {
    let message = error.to_string();
    if is_authentication_error(&message) {
        return soap_auth_challenge(&message);
    }
    soap_error(StatusCode::BAD_REQUEST, &message)
}

fn is_authentication_error(message: &str) -> bool {
    matches!(
        message,
        "missing account authentication" | "invalid credentials"
    ) || message.contains("oauth access token")
}

fn soap_auth_challenge(message: &str) -> Response {
    let mut response = soap_error(StatusCode::UNAUTHORIZED, message);
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"LPE EWS\""),
    );
    response
}

fn soap_error(status: StatusCode, message: &str) -> Response {
    let envelope = format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
            "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">",
            "<s:Body><s:Fault>",
            "<faultcode>s:Client</faultcode>",
            "<faultstring>{}</faultstring>",
            "</s:Fault></s:Body>",
            "</s:Envelope>"
        ),
        escape_xml(message)
    );
    xml_response(status, envelope)
}

fn xml_response(status: StatusCode, body: String) -> Response {
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/xml; charset=utf-8"),
    );
    response
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
