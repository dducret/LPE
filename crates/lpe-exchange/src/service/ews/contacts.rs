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

pub(in crate::service) fn parse_create_contact_input(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<UpsertClientContactInput> {
    let contact = element_content(request, "Contact")
        .ok_or_else(|| anyhow!("CreateItem is missing Contact"))?;
    let email = contact_entry_value(contact, "EmailAddresses", "EmailAddress1")
        .or_else(|| element_text(contact, "EmailAddress"))
        .unwrap_or_else(|| principal.email.clone());
    let given_name = element_text(contact, "GivenName").unwrap_or_default();
    let middle_name = element_text(contact, "MiddleName").unwrap_or_default();
    let surname = element_text(contact, "Surname").unwrap_or_default();
    let fallback_name = [given_name.as_str(), middle_name.as_str(), surname.as_str()]
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let name = element_text(contact, "DisplayName")
        .or_else(|| element_text(contact, "FileAs"))
        .or_else(|| (!fallback_name.trim().is_empty()).then_some(fallback_name))
        .unwrap_or_else(|| email.clone());
    let body_tag = open_tag_text(contact, "Body").unwrap_or_default();
    let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
    let body_value = element_text(contact, "Body").unwrap_or_default();
    let notes = if body_type.eq_ignore_ascii_case("HTML") {
        html_to_text(&body_value)
    } else {
        body_value.clone()
    };

    Ok(UpsertClientContactInput {
        id: None,
        account_id: principal.account_id,
        name,
        role: element_text(contact, "JobTitle").unwrap_or_default(),
        email: email.clone(),
        phone: contact_entry_value(contact, "PhoneNumbers", "MobilePhone")
            .or_else(|| contact_entry_value(contact, "PhoneNumbers", "BusinessPhone"))
            .or_else(|| contact_entry_value(contact, "PhoneNumbers", "HomePhone"))
            .unwrap_or_default(),
        team: element_text(contact, "Department").unwrap_or_default(),
        notes,
        structured_name: ContactNameFields {
            prefix: element_text(contact, "Title").unwrap_or_default(),
            given: given_name,
            middle: middle_name,
            family: surname,
            suffix: element_text(contact, "Generation").unwrap_or_default(),
            nickname: element_text(contact, "Nickname").unwrap_or_default(),
            phonetic_given: String::new(),
            phonetic_family: String::new(),
        },
        emails_json: Some(ews_contact_emails_json(contact, &email)),
        phones_json: Some(ews_contact_phones_json(contact)),
        urls_json: Some(ews_contact_urls_json(contact)),
        organization_name: element_text(contact, "CompanyName").unwrap_or_default(),
        job_title: element_text(contact, "JobTitle").unwrap_or_default(),
        ..Default::default()
    })
}

pub(in crate::service) fn parse_update_contact_input(
    principal: &AccountPrincipal,
    existing: &AccessibleContact,
    request: &str,
) -> UpsertClientContactInput {
    let contact = element_content(request, "Contact").unwrap_or(request);
    let given_name = element_text(contact, "GivenName");
    let middle_name = element_text(contact, "MiddleName");
    let surname = element_text(contact, "Surname");
    let existing_given = contact_given_name(existing);
    let existing_middle = existing.structured_name.middle.clone();
    let existing_surname = contact_family_name(existing);
    let name_from_parts = (given_name.is_some() || middle_name.is_some() || surname.is_some())
        .then(|| {
            [
                given_name.as_deref().unwrap_or(&existing_given),
                middle_name.as_deref().unwrap_or(&existing_middle),
                surname.as_deref().unwrap_or(&existing_surname),
            ]
            .into_iter()
            .filter(|value| !value.trim().is_empty())
            .collect::<Vec<_>>()
            .join(" ")
        });
    let name = element_text(contact, "DisplayName")
        .or_else(|| element_text(contact, "FileAs"))
        .or(name_from_parts)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| existing.name.clone());
    let mut structured_name = existing.structured_name.clone();
    if let Some(value) = given_name {
        structured_name.given = value;
    }
    if let Some(value) = middle_name {
        structured_name.middle = value;
    }
    if let Some(value) = surname {
        structured_name.family = value;
    }
    structured_name.prefix = deleted_or_updated_text(
        request,
        contact,
        "contacts:Title",
        "Title",
        &structured_name.prefix,
    );
    structured_name.suffix = deleted_or_updated_text(
        request,
        contact,
        "contacts:Generation",
        "Generation",
        &structured_name.suffix,
    );
    structured_name.nickname = deleted_or_updated_text(
        request,
        contact,
        "contacts:Nickname",
        "Nickname",
        &structured_name.nickname,
    );
    let email = contact_entry_value(contact, "EmailAddresses", "EmailAddress1")
        .or_else(|| element_text(contact, "EmailAddress"))
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| existing.email.clone());
    let notes = if field_deleted(request, "item:Body") {
        String::new()
    } else if let Some(body_value) = element_text(contact, "Body") {
        let body_tag = open_tag_text(contact, "Body").unwrap_or_default();
        let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
        if body_type.eq_ignore_ascii_case("HTML") {
            html_to_text(&body_value)
        } else {
            body_value
        }
    } else {
        existing.notes.clone()
    };

    UpsertClientContactInput {
        id: Some(existing.id),
        account_id: principal.account_id,
        name,
        role: deleted_or_updated_text(
            request,
            contact,
            "contacts:JobTitle",
            "JobTitle",
            &contact_job_title(existing),
        ),
        email: email.clone(),
        phone: deleted_or_updated_contact_entry(
            request,
            contact,
            &[
                "contacts:PhoneNumber:MobilePhone",
                "contacts:PhoneNumber:BusinessPhone",
                "contacts:PhoneNumber:HomePhone",
            ],
            "PhoneNumbers",
            &["MobilePhone", "BusinessPhone", "HomePhone"],
            &existing.phone,
        ),
        team: deleted_or_updated_text(
            request,
            contact,
            "contacts:Department",
            "Department",
            &existing.team,
        ),
        notes,
        structured_name,
        emails_json: Some(ews_updated_contact_emails_json(
            request, contact, existing, &email,
        )),
        phones_json: Some(ews_updated_contact_phones_json(request, contact, existing)),
        urls_json: Some(ews_updated_contact_urls_json(request, contact, existing)),
        organization_name: deleted_or_updated_text(
            request,
            contact,
            "contacts:CompanyName",
            "CompanyName",
            &contact_organization_name(existing),
        ),
        job_title: deleted_or_updated_text(
            request,
            contact,
            "contacts:JobTitle",
            "JobTitle",
            &contact_job_title(existing),
        ),
        ..Default::default()
    }
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

fn contact_entry_value(contact: &str, collection_name: &str, key: &str) -> Option<String> {
    let collection = element_content(contact, collection_name)?;
    let mut rest = collection;
    while let Some(tag_start) = rest.find('<') {
        let raw_tag_text = &rest[tag_start + 1..];
        let tag_text = raw_tag_text.trim_start();
        let open_tag_start = tag_start + 1 + (raw_tag_text.len() - tag_text.len());
        if tag_text.starts_with('/') || tag_text.starts_with('?') || tag_text.starts_with('!') {
            rest = &tag_text[1..];
            continue;
        }
        let tag_end = tag_text.find('>')?;
        let open_tag = &tag_text[..tag_end];
        let qualified_name = open_tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()?;
        let content_start = open_tag_start + tag_end + 1;
        if qualified_name.rsplit(':').next() == Some("Entry")
            && attribute_value(open_tag, "Key") == Some(key)
        {
            let close_pattern = format!("</{qualified_name}>");
            let content = &rest[content_start..];
            let content_end = content.find(&close_pattern)?;
            return Some(xml_text(&content[..content_end]));
        }
        rest = &rest[content_start..];
    }
    element_text(collection, "Entry")
}

fn deleted_or_updated_contact_entry(
    request: &str,
    contact: &str,
    field_uris: &[&str],
    collection_name: &str,
    keys: &[&str],
    existing: &str,
) -> String {
    if field_uris
        .iter()
        .any(|field_uri| field_deleted(request, field_uri))
    {
        return String::new();
    }
    keys.iter()
        .find_map(|key| contact_entry_value(contact, collection_name, key))
        .unwrap_or_else(|| existing.to_string())
}

fn ews_contact_emails_json(contact: &str, primary: &str) -> serde_json::Value {
    let mut rows = Vec::new();
    push_json_contact_value(&mut rows, "email", "work", Some(primary));
    push_json_contact_value(
        &mut rows,
        "email",
        "email2",
        contact_entry_value(contact, "EmailAddresses", "EmailAddress2").as_deref(),
    );
    push_json_contact_value(
        &mut rows,
        "email",
        "email3",
        contact_entry_value(contact, "EmailAddresses", "EmailAddress3").as_deref(),
    );
    serde_json::Value::Array(rows)
}

fn ews_contact_phones_json(contact: &str) -> serde_json::Value {
    let mut rows = Vec::new();
    for (key, label) in [
        ("MobilePhone", "mobile"),
        ("BusinessPhone", "work"),
        ("BusinessPhone2", "work2"),
        ("HomePhone", "home"),
        ("HomePhone2", "home2"),
    ] {
        push_json_contact_value(
            &mut rows,
            "phone",
            label,
            contact_entry_value(contact, "PhoneNumbers", key).as_deref(),
        );
    }
    serde_json::Value::Array(rows)
}

fn ews_contact_urls_json(contact: &str) -> serde_json::Value {
    let mut rows = Vec::new();
    push_json_contact_value(
        &mut rows,
        "url",
        "work",
        element_text(contact, "BusinessHomePage").as_deref(),
    );
    push_json_contact_value(
        &mut rows,
        "url",
        "home",
        element_text(contact, "PersonalHomePage").as_deref(),
    );
    serde_json::Value::Array(rows)
}

fn ews_updated_contact_emails_json(
    request: &str,
    contact: &str,
    existing: &AccessibleContact,
    primary: &str,
) -> serde_json::Value {
    if field_deleted(request, "contacts:EmailAddress:EmailAddress2")
        || field_deleted(request, "contacts:EmailAddress:EmailAddress3")
        || contact.contains("EmailAddresses")
    {
        ews_contact_emails_json(contact, primary)
    } else {
        update_first_json_contact_value(&existing.emails_json, "email", primary)
    }
}

fn ews_updated_contact_phones_json(
    request: &str,
    contact: &str,
    existing: &AccessibleContact,
) -> serde_json::Value {
    if request.contains("contacts:PhoneNumber:") || contact.contains("PhoneNumbers") {
        ews_contact_phones_json(contact)
    } else {
        existing.phones_json.clone()
    }
}

fn ews_updated_contact_urls_json(
    request: &str,
    contact: &str,
    existing: &AccessibleContact,
) -> serde_json::Value {
    if field_deleted(request, "contacts:BusinessHomePage")
        || field_deleted(request, "contacts:PersonalHomePage")
        || element_text(contact, "BusinessHomePage").is_some()
        || element_text(contact, "PersonalHomePage").is_some()
    {
        ews_contact_urls_json(contact)
    } else {
        existing.urls_json.clone()
    }
}

fn push_json_contact_value(
    rows: &mut Vec<serde_json::Value>,
    key: &str,
    label: &str,
    value: Option<&str>,
) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        rows.push(serde_json::json!({ key: value, "label": label }));
    }
}

fn update_first_json_contact_value(
    existing: &serde_json::Value,
    key: &str,
    value: &str,
) -> serde_json::Value {
    let mut rows = existing.as_array().cloned().unwrap_or_default();
    if let Some(row) = rows.first_mut() {
        if let Some(object) = row.as_object_mut() {
            object.insert(
                key.to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
    } else {
        push_json_contact_value(&mut rows, key, "work", Some(value));
    }
    serde_json::Value::Array(rows)
}

fn contact_given_name(contact: &AccessibleContact) -> String {
    if contact.structured_name.given.trim().is_empty() {
        first_name(&contact.name)
    } else {
        contact.structured_name.given.clone()
    }
}

fn contact_family_name(contact: &AccessibleContact) -> String {
    if contact.structured_name.family.trim().is_empty() {
        last_name(&contact.name)
    } else {
        contact.structured_name.family.clone()
    }
}

fn contact_organization_name(contact: &AccessibleContact) -> String {
    if contact.organization_name.trim().is_empty() {
        contact.team.clone()
    } else {
        contact.organization_name.clone()
    }
}

fn contact_job_title(contact: &AccessibleContact) -> String {
    if contact.job_title.trim().is_empty() {
        contact.role.clone()
    } else {
        contact.job_title.clone()
    }
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

#[cfg(test)]
mod contact_update_tests {
    use super::*;

    #[test]
    fn ews_contact_narrow_update_omits_unowned_rich_fields() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::from_u128(1),
            account_id: Uuid::from_u128(2),
            email: "ada@example.test".to_string(),
            display_name: "Ada".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };
        let existing = AccessibleContact {
            id: Uuid::from_u128(3),
            name: "Ada Example".to_string(),
            email: "ada@example.test".to_string(),
            phone: "+1 555 0100".to_string(),
            addresses_json: serde_json::json!([{"full": "1 Example Way"}]),
            urls_json: serde_json::json!([{"url": "https://example.test"}]),
            raw_vcard: Some("BEGIN:VCARD\nEND:VCARD".to_string()),
            source: lpe_storage::ContactSourceFields {
                import_source: "carddav".to_string(),
                source_uid: Some("uid-1".to_string()),
                source_etag: Some("etag-1".to_string()),
                source_payload_json: serde_json::json!({"href": "/contacts/1.vcf"}),
            },
            ..AccessibleContact::default()
        };
        let request = "<m:UpdateItem><t:Contact><t:DisplayName>Ada Updated</t:DisplayName></t:Contact></m:UpdateItem>";

        let input = parse_update_contact_input(&principal, &existing, request);

        assert_eq!(input.name, "Ada Updated");
        assert_eq!(input.addresses_json, None);
        assert_eq!(input.raw_vcard, None);
        assert!(!input.raw_vcard_is_explicit);
        assert!(!input.source_is_explicit);
    }
}
