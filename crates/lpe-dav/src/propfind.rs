use lpe_storage::{
    AccessibleContact, AccessibleEvent, CollaborationCollection, CollaborationRights, DavTask,
};

use crate::{
    paths::{
        contact_collection_href, contact_href, etag, event_collection_href, event_href,
        task_collection_href, task_href, ADDRESSBOOK_HOME_PATH, CALENDAR_HOME_PATH, PRINCIPAL_PATH,
        ROOT_PATH,
    },
    responses::response_entry,
    serialize::{serialize_ical, serialize_vcard, serialize_vtodo, xml_escape},
};

pub(crate) fn root_propfind_entry() -> String {
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

pub(crate) fn principal_propfind_entry() -> String {
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

pub(crate) fn addressbook_collection_entry(collection: CollaborationCollection) -> String {
    response_entry(
        &contact_collection_href(&collection.id),
        collection_props(
            &collection.display_name,
            "<d:collection/><card:addressbook/>",
            None,
            None,
            Some(collection_metadata(
                &collection.owner_email,
                &collection.rights,
                true,
                "<d:supported-report-set><d:supported-report><d:report><card:addressbook-query/></d:report></d:supported-report><d:supported-report><d:report><card:addressbook-multiget/></d:report></d:supported-report></d:supported-report-set>",
            )),
        ),
    )
}

pub(crate) fn task_collection_entry(collection: CollaborationCollection) -> String {
    response_entry(
        &task_collection_href(&collection.id),
        collection_props(
            &collection.display_name,
            "<d:collection/><cal:calendar/>",
            None,
            None,
            Some(collection_metadata(
                &collection.owner_email,
                &collection.rights,
                true,
                "<d:supported-report-set><d:supported-report><d:report><cal:calendar-query/></d:report></d:supported-report><d:supported-report><d:report><cal:calendar-multiget/></d:report></d:supported-report></d:supported-report-set><cal:supported-calendar-component-set><cal:comp name=\"VTODO\"/></cal:supported-calendar-component-set>",
            )),
        ),
    )
}

pub(crate) fn calendar_collection_entry(collection: CollaborationCollection) -> String {
    response_entry(
        &event_collection_href(&collection.id),
        collection_props(
            &collection.display_name,
            "<d:collection/><cal:calendar/>",
            None,
            None,
            Some(collection_metadata(
                &collection.owner_email,
                &collection.rights,
                true,
                "<d:supported-report-set><d:supported-report><d:report><cal:calendar-query/></d:report></d:supported-report><d:supported-report><d:report><cal:calendar-multiget/></d:report></d:supported-report></d:supported-report-set>",
            )),
        ),
    )
}

pub(crate) fn collection_home_entry(
    path: &str,
    display_name: &str,
    resource_type: String,
) -> String {
    response_entry(
        path,
        collection_props(display_name, &resource_type, None, None, None),
    )
}

pub(crate) fn contact_resource_entry(contact: AccessibleContact) -> String {
    let body = serialize_vcard(&contact);
    response_entry(
        &contact_href(&contact.collection_id, contact.id),
        collection_props(
            &contact.name,
            "",
            Some("text/vcard; charset=utf-8"),
            Some(etag(&body)),
            Some(collection_metadata(
                &contact.owner_email,
                &contact.rights,
                false,
                "",
            )),
        ),
    )
}

pub(crate) fn event_resource_entry(event: AccessibleEvent) -> String {
    let body = serialize_ical(&event);
    response_entry(
        &event_href(&event.collection_id, event.id),
        collection_props(
            &event.title,
            "",
            Some("text/calendar; charset=utf-8"),
            Some(etag(&body)),
            Some(collection_metadata(
                &event.owner_email,
                &event.rights,
                false,
                "",
            )),
        ),
    )
}

pub(crate) fn task_resource_entry(task: DavTask) -> String {
    let body = serialize_vtodo(&task);
    response_entry(
        &task_href(&task.collection_id, task.id),
        collection_props(
            &task.title,
            "",
            Some("text/calendar; charset=utf-8"),
            Some(etag(&body)),
            Some(collection_metadata(
                &task.owner_email,
                &task.rights,
                false,
                "",
            )),
        ),
    )
}

pub(crate) fn contact_report_entry(contact: AccessibleContact) -> String {
    let body = serialize_vcard(&contact);
    response_entry(
        &contact_href(&contact.collection_id, contact.id),
        format!(
            "<d:propstat><d:prop><d:getetag>{}</d:getetag><card:address-data>{}</card:address-data></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>",
            xml_escape(&etag(&body)),
            xml_escape(&body)
        ),
    )
}

pub(crate) fn task_report_entry(task: DavTask) -> String {
    let body = serialize_vtodo(&task);
    response_entry(
        &task_href(&task.collection_id, task.id),
        format!(
            "<d:propstat><d:prop><d:getetag>{}</d:getetag><cal:calendar-data>{}</cal:calendar-data></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>",
            xml_escape(&etag(&body)),
            xml_escape(&body)
        ),
    )
}

pub(crate) fn event_report_entry(event: AccessibleEvent) -> String {
    let body = serialize_ical(&event);
    response_entry(
        &event_href(&event.collection_id, event.id),
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

fn collection_metadata(
    owner_email: &str,
    rights: &CollaborationRights,
    is_collection: bool,
    extra: &str,
) -> String {
    format!(
        "<d:owner><d:href>mailto:{}</d:href></d:owner>{}{}",
        xml_escape(owner_email),
        current_user_privilege_set(rights, is_collection),
        extra
    )
}

fn current_user_privilege_set(rights: &CollaborationRights, is_collection: bool) -> String {
    let mut privileges = vec!["<d:privilege><d:read/></d:privilege>".to_string()];
    if rights.may_write {
        privileges.push("<d:privilege><d:write/></d:privilege>".to_string());
        privileges.push("<d:privilege><d:write-content/></d:privilege>".to_string());
        privileges.push("<d:privilege><d:write-properties/></d:privilege>".to_string());
        if is_collection {
            privileges.push("<d:privilege><d:bind/></d:privilege>".to_string());
        }
    }
    if rights.may_delete {
        privileges.push("<d:privilege><d:unbind/></d:privilege>".to_string());
    }
    format!(
        "<d:current-user-privilege-set>{}</d:current-user-privilege-set>",
        privileges.join("")
    )
}

pub(crate) fn collection_resourcetype(kind: &str) -> String {
    match kind {
        "collection" => "<d:collection/>".to_string(),
        _ => String::new(),
    }
}
