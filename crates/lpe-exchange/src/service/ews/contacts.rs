use super::super::*;

pub(in crate::service) fn contact_change_key(
    contact: &AccessibleContact,
    sync_version: Option<&str>,
) -> String {
    stable_change_key(&[
        "contact",
        &contact.id.to_string(),
        sync_version.unwrap_or_default(),
        &contact.collection_id,
        &contact.name,
        &contact.role,
        &contact.email,
        &contact.phone,
        &contact.team,
        &contact.notes,
    ])
}

pub(in crate::service) fn contact_summary_xml(contact: &AccessibleContact) -> String {
    let change_key = contact_change_key(contact, None);
    contact_summary_xml_with_change_key(contact, &change_key)
}

fn contact_summary_xml_with_change_key(contact: &AccessibleContact, change_key: &str) -> String {
    format!(
        concat!(
            "<t:Contact>",
            "<t:ItemId Id=\"contact:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:Subject>{name}</t:Subject>",
            "<t:DisplayName>{name}</t:DisplayName>",
            "</t:Contact>"
        ),
        id = contact.id,
        change_key = escape_xml(change_key),
        name = escape_xml(&contact.name),
    )
}

pub(in crate::service) fn contact_item_xml(contact: &AccessibleContact) -> String {
    let change_key = contact_change_key(contact, None);
    contact_item_xml_with_change_key(contact, &change_key)
}

pub(in crate::service) fn contact_item_xml_with_change_key(
    contact: &AccessibleContact,
    change_key: &str,
) -> String {
    let email_entries = ews_contact_email_entries_xml(contact);
    let phone_entries = ews_contact_phone_entries_xml(contact);
    let business_home_page = ews_contact_url_by_label(contact, &["work", "business"])
        .map(|value| {
            format!(
                "<t:BusinessHomePage>{}</t:BusinessHomePage>",
                escape_xml(&value)
            )
        })
        .unwrap_or_default();
    let personal_home_page = ews_contact_url_by_label(contact, &["home", "personal"])
        .map(|value| {
            format!(
                "<t:PersonalHomePage>{}</t:PersonalHomePage>",
                escape_xml(&value)
            )
        })
        .unwrap_or_default();
    format!(
        concat!(
            "<t:Contact>",
            "<t:ItemId Id=\"contact:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{name}</t:Subject>",
            "<t:DisplayName>{name}</t:DisplayName>",
            "<t:FileAs>{name}</t:FileAs>",
            "<t:Title>{prefix}</t:Title>",
            "<t:GivenName>{given}</t:GivenName>",
            "<t:MiddleName>{middle}</t:MiddleName>",
            "<t:Surname>{surname}</t:Surname>",
            "<t:Generation>{suffix}</t:Generation>",
            "<t:Nickname>{nickname}</t:Nickname>",
            "<t:JobTitle>{role}</t:JobTitle>",
            "<t:Department>{team}</t:Department>",
            "<t:CompanyName>{company}</t:CompanyName>",
            "<t:EmailAddresses>{email_entries}</t:EmailAddresses>",
            "<t:PhoneNumbers>{phone_entries}</t:PhoneNumbers>",
            "{business_home_page}",
            "{personal_home_page}",
            "<t:Body BodyType=\"Text\">{notes}</t:Body>",
            "</t:Contact>"
        ),
        id = contact.id,
        change_key = escape_xml(change_key),
        folder_id = escape_xml(&contact.collection_id),
        name = escape_xml(&contact.name),
        prefix = escape_xml(&contact.structured_name.prefix),
        given = escape_xml(&contact_given_name(contact)),
        middle = escape_xml(&contact.structured_name.middle),
        surname = escape_xml(&contact_family_name(contact)),
        suffix = escape_xml(&contact.structured_name.suffix),
        nickname = escape_xml(&contact.structured_name.nickname),
        role = escape_xml(&contact_job_title(contact)),
        team = escape_xml(&contact.team),
        company = escape_xml(&contact_organization_name(contact)),
        email_entries = email_entries,
        phone_entries = phone_entries,
        business_home_page = business_home_page,
        personal_home_page = personal_home_page,
        notes = escape_xml(&contact.notes),
    )
}

pub(in crate::service) fn create_contact_success_response(contact: &AccessibleContact) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:Contact>",
            "<t:ItemId Id=\"contact:{id}\" ChangeKey=\"created\"/>",
            "<t:ParentFolderId Id=\"{folder_id}\"/>",
            "<t:Subject>{name}</t:Subject>",
            "<t:DisplayName>{name}</t:DisplayName>",
            "</t:Contact>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        id = contact.id,
        folder_id = escape_xml(&contact.collection_id),
        name = escape_xml(&contact.name),
    )
}

fn ews_contact_email_entries_xml(contact: &AccessibleContact) -> String {
    let mut entries = contact
        .emails_json
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| {
            row.get("email")
                .or_else(|| row.get("address"))
                .and_then(serde_json::Value::as_str)
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .take(3)
        .enumerate()
        .map(|(index, value)| {
            format!(
                "<t:Entry Key=\"EmailAddress{}\">{}</t:Entry>",
                index + 1,
                escape_xml(value)
            )
        })
        .collect::<Vec<_>>();
    if entries.is_empty() && !contact.email.trim().is_empty() {
        entries.push(format!(
            "<t:Entry Key=\"EmailAddress1\">{}</t:Entry>",
            escape_xml(&contact.email)
        ));
    }
    entries.join("")
}

fn ews_contact_phone_entries_xml(contact: &AccessibleContact) -> String {
    let labels = [
        ("mobile", "MobilePhone"),
        ("cell", "MobilePhone"),
        ("work", "BusinessPhone"),
        ("business", "BusinessPhone"),
        ("work2", "BusinessPhone2"),
        ("business2", "BusinessPhone2"),
        ("home", "HomePhone"),
        ("home2", "HomePhone2"),
    ];
    let mut entries = Vec::new();
    for (label, key) in labels {
        if let Some(value) = ews_contact_phone_by_label(contact, &[label]) {
            entries.push(format!(
                "<t:Entry Key=\"{key}\">{}</t:Entry>",
                escape_xml(&value)
            ));
        }
    }
    if entries.is_empty() && !contact.phone.trim().is_empty() {
        entries.push(format!(
            "<t:Entry Key=\"MobilePhone\">{}</t:Entry>",
            escape_xml(&contact.phone)
        ));
    }
    entries.join("")
}

fn ews_contact_phone_by_label(contact: &AccessibleContact, labels: &[&str]) -> Option<String> {
    contact_labeled_string(&contact.phones_json, "phone", labels)
}

fn ews_contact_url_by_label(contact: &AccessibleContact, labels: &[&str]) -> Option<String> {
    contact_labeled_string(&contact.urls_json, "url", labels)
        .or_else(|| contact_labeled_string(&contact.urls_json, "href", labels))
}

fn contact_labeled_string(value: &serde_json::Value, key: &str, labels: &[&str]) -> Option<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .find(|row| {
            let label = row
                .get("label")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            labels
                .iter()
                .any(|expected| label.eq_ignore_ascii_case(expected))
        })
        .and_then(|row| row.get(key).and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}
