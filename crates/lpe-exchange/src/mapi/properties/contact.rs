use super::*;

pub(in crate::mapi) fn contact_property_value(
    contact: &AccessibleContact,
    item_id: u64,
    folder_id: u64,
    property_tag: u32,
) -> Option<MapiValue> {
    let property_tag = canonical_property_storage_tag(property_tag);
    let change_number = mapi_mailstore::change_number_for_store_id(item_id);
    match property_tag {
        PID_TAG_FOLDER_ID => Some(MapiValue::U64(folder_id)),
        PID_TAG_MID => Some(MapiValue::U64(item_id)),
        PID_TAG_INST_ID => Some(MapiValue::U64(item_id)),
        PID_TAG_INSTANCE_NUM => Some(MapiValue::U32(0)),
        PID_TAG_DISPLAY_NAME_W | PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
            Some(MapiValue::String(contact.name.clone()))
        }
        PID_TAG_DISPLAY_NAME_PREFIX_W => {
            Some(MapiValue::String(contact.structured_name.prefix.clone()))
        }
        PID_TAG_GIVEN_NAME_W => Some(MapiValue::String(contact_given_name(contact))),
        PID_TAG_MIDDLE_NAME_W => Some(MapiValue::String(contact.structured_name.middle.clone())),
        PID_TAG_SURNAME_W => Some(MapiValue::String(contact_family_name(contact))),
        PID_TAG_GENERATION_W => Some(MapiValue::String(contact.structured_name.suffix.clone())),
        PID_TAG_NICKNAME_W => Some(MapiValue::String(contact.structured_name.nickname.clone())),
        PID_TAG_EMAIL_ADDRESS_W | PID_TAG_SMTP_ADDRESS_W => {
            Some(MapiValue::String(contact.email.clone()))
        }
        // [MS-OXOCNTC] sections 2.2.1.2, 2.2.1.2.11, and 2.2.1.2.12
        // require these properties to be present and synchronized when an
        // electronic address is defined.
        PID_LID_ADDRESS_BOOK_PROVIDER_EMAIL_LIST_TAG => {
            contact_address_book_provider_email_list(contact).map(MapiValue::MultiI32)
        }
        PID_LID_ADDRESS_BOOK_PROVIDER_ARRAY_TYPE_TAG => {
            contact_address_book_provider_email_list(contact).map(|indexes| {
                MapiValue::U32(
                    indexes
                        .into_iter()
                        .fold(0, |mask, index| mask | (1 << index)),
                )
            })
        }
        PID_LID_EMAIL1_ADDRESS_TYPE_W_TAG => {
            contact_email_value(contact, 0).map(|_| MapiValue::String("SMTP".to_string()))
        }
        PID_LID_EMAIL1_DISPLAY_NAME_W_TAG
        | PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG
        | PID_LID_EMAIL1_ORIGINAL_DISPLAY_NAME_W_TAG
        | PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_DISPLAY_NAME_W_TAG
        | PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_EMAIL_ADDRESS_W_TAG => {
            contact_email_value(contact, 0).map(MapiValue::String)
        }
        PID_LID_EMAIL2_ADDRESS_TYPE_W_TAG => Some(MapiValue::String(
            contact_email_value(contact, 1)
                .map(|_| "SMTP".to_string())
                .unwrap_or_default(),
        )),
        PID_LID_EMAIL2_DISPLAY_NAME_W_TAG
        | PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG
        | PID_LID_EMAIL2_ORIGINAL_DISPLAY_NAME_W_TAG
        | PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_DISPLAY_NAME_W_TAG
        | PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_EMAIL_ADDRESS_W_TAG => Some(MapiValue::String(
            contact_email_value(contact, 1).unwrap_or_default(),
        )),
        PID_LID_EMAIL3_ADDRESS_TYPE_W_TAG => Some(MapiValue::String(
            contact_email_value(contact, 2)
                .map(|_| "SMTP".to_string())
                .unwrap_or_default(),
        )),
        PID_LID_EMAIL3_DISPLAY_NAME_W_TAG
        | PID_LID_EMAIL3_EMAIL_ADDRESS_W_TAG
        | PID_LID_EMAIL3_ORIGINAL_DISPLAY_NAME_W_TAG
        | PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_EMAIL_ADDRESS_W_TAG => Some(MapiValue::String(
            contact_email_value(contact, 2).unwrap_or_default(),
        )),
        PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS1_ADDRESS_TYPE_W_TAG => {
            contact_email_value(contact, 0).map(|_| MapiValue::String("SMTP".to_string()))
        }
        PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS2_ADDRESS_TYPE_W_TAG => Some(MapiValue::String(
            contact_email_value(contact, 1)
                .map(|_| "SMTP".to_string())
                .unwrap_or_default(),
        )),
        PID_LID_OUTLOOK_CONTACT_EMAIL_ALIAS3_ADDRESS_TYPE_W_TAG => Some(MapiValue::String(
            contact_email_value(contact, 2)
                .map(|_| "SMTP".to_string())
                .unwrap_or_default(),
        )),
        PID_LID_OUTLOOK_CONTACT_SOURCE_80E0_TAG
        | PID_LID_OUTLOOK_CONTACT_SOURCE_80E2_TAG
        | PID_LID_OUTLOOK_CONTACT_SOURCE_80E3_TAG
        | PID_LID_OUTLOOK_CONTACT_SOURCE_80E5_TAG
        | PID_LID_OUTLOOK_CONTACT_SOURCE_80E6_TAG
        | PID_LID_OUTLOOK_CONTACT_SOURCE_80E8_TAG => {
            outlook_contact_source_empty_value(property_tag)
        }
        tag if MapiPropertyTag::new(tag).property_id()
            == MapiPropertyTag::new(PID_NAME_OSC_CONTACT_SOURCES_TAG).property_id() =>
        {
            outlook_contact_source_empty_value(tag)
        }
        PID_TAG_MOBILE_TELEPHONE_NUMBER_W => Some(MapiValue::String(contact_phone_by_label(
            contact,
            &["mobile", "cell"],
        ))),
        PID_TAG_BUSINESS_TELEPHONE_NUMBER_W => Some(MapiValue::String(contact_phone_by_label(
            contact,
            &["work", "business"],
        ))),
        PID_TAG_HOME_TELEPHONE_NUMBER_W => Some(MapiValue::String(contact_phone_by_label(
            contact,
            &["home"],
        ))),
        PID_TAG_PRIMARY_TELEPHONE_NUMBER_W => Some(MapiValue::String(contact.phone.clone())),
        PID_TAG_BUSINESS2_TELEPHONE_NUMBERS_W => Some(MapiValue::MultiString(
            contact_phone_values_by_label(contact, &["work2", "business2"]),
        )),
        PID_TAG_COMPANY_NAME_W => Some(MapiValue::String(contact_organization_name(contact))),
        PID_TAG_DEPARTMENT_NAME_W => Some(MapiValue::String(contact.team.clone())),
        PID_TAG_TITLE_W => Some(MapiValue::String(contact_job_title(contact))),
        PID_TAG_PERSONAL_HOME_PAGE_W => Some(MapiValue::String(contact_url_by_label(
            contact,
            &["home", "personal"],
        ))),
        PID_TAG_BUSINESS_HOME_PAGE_W => Some(MapiValue::String(contact_url_by_label(
            contact,
            &["work", "business"],
        ))),
        PID_TAG_BODY_W => Some(MapiValue::String(contact.notes.clone())),
        PID_TAG_MESSAGE_CLASS_W => Some(MapiValue::String("IPM.Contact".to_string())),
        PID_TAG_ACCESS => Some(MapiValue::U32(MAPI_MESSAGE_ACCESS)),
        PID_TAG_MESSAGE_FLAGS => Some(MapiValue::U32(MSGFLAG_READ)),
        PID_TAG_MESSAGE_STATUS => Some(MapiValue::U32(0)),
        PID_TAG_HAS_ATTACHMENTS => Some(MapiValue::Bool(false)),
        PID_TAG_MESSAGE_SIZE => Some(mapi_message_size_value(contact_size(contact))),
        PID_TAG_MESSAGE_SIZE_EXTENDED => {
            Some(mapi_message_size_extended_value(contact_size(contact)))
        }
        PID_TAG_ENTRY_ID | PID_TAG_INSTANCE_KEY => Some(MapiValue::Binary(
            crate::mapi::identity::instance_key_for_object_id(item_id),
        )),
        PID_TAG_SOURCE_KEY => Some(MapiValue::Binary(mapi_mailstore::source_key_for_uuid(
            &contact.id,
        ))),
        PID_TAG_PARENT_SOURCE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::source_key_for_store_id(folder_id),
        )),
        PID_TAG_CHANGE_KEY => Some(MapiValue::Binary(
            mapi_mailstore::change_key_for_change_number(change_number),
        )),
        PID_TAG_PREDECESSOR_CHANGE_LIST => Some(MapiValue::Binary(
            mapi_mailstore::predecessor_change_list(change_number),
        )),
        PID_TAG_CHANGE_NUMBER => Some(MapiValue::U64(change_number)),
        _ => None,
    }
}

fn outlook_contact_source_empty_value(property_tag: u32) -> Option<MapiValue> {
    match MapiPropertyTag::new(property_tag).property_type_code() {
        0x0003 => Some(MapiValue::U32(0)),
        0x000B => Some(MapiValue::Bool(false)),
        0x001E | 0x001F => Some(MapiValue::String(String::new())),
        0x0048 => Some(MapiValue::Guid(Uuid::nil().into_bytes())),
        0x0102 => Some(MapiValue::Binary(Vec::new())),
        0x1003 => Some(MapiValue::MultiI32(Vec::new())),
        0x101E | 0x101F => Some(MapiValue::MultiString(Vec::new())),
        0x1102 => Some(MapiValue::MultiBinary(Vec::new())),
        _ => None,
    }
}

pub(in crate::mapi) fn contact_given_name(contact: &AccessibleContact) -> String {
    if !contact.structured_name.given.trim().is_empty() {
        return contact.structured_name.given.clone();
    }
    contact
        .name
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string()
}

pub(in crate::mapi) fn contact_family_name(contact: &AccessibleContact) -> String {
    if !contact.structured_name.family.trim().is_empty() {
        return contact.structured_name.family.clone();
    }
    contact
        .name
        .split_whitespace()
        .last()
        .filter(|value| *value != contact.name)
        .unwrap_or_default()
        .to_string()
}

pub(in crate::mapi) fn contact_organization_name(contact: &AccessibleContact) -> String {
    if contact.organization_name.trim().is_empty() {
        contact.team.clone()
    } else {
        contact.organization_name.clone()
    }
}

pub(in crate::mapi) fn contact_job_title(contact: &AccessibleContact) -> String {
    if contact.job_title.trim().is_empty() {
        contact.role.clone()
    } else {
        contact.job_title.clone()
    }
}

fn contact_phone_by_label(contact: &AccessibleContact, labels: &[&str]) -> String {
    contact_phone_values_by_label(contact, labels)
        .into_iter()
        .next()
        .unwrap_or_else(|| contact.phone.clone())
}

fn contact_phone_values_by_label(contact: &AccessibleContact, labels: &[&str]) -> Vec<String> {
    contact_labeled_json_values(&contact.phones_json, "phone", labels)
}

fn contact_email_value(contact: &AccessibleContact, index: usize) -> Option<String> {
    let mut values = Vec::new();
    let primary = contact.email.trim();
    if !primary.is_empty() {
        values.push(primary.to_string());
    }
    for value in contact_json_values(&contact.emails_json, "email") {
        if !values
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(&value))
        {
            values.push(value);
        }
    }
    values.into_iter().nth(index)
}

fn contact_address_book_provider_email_list(contact: &AccessibleContact) -> Option<Vec<i32>> {
    let indexes = (0..3)
        .filter(|index| contact_email_value(contact, *index).is_some())
        .map(|index| index as i32)
        .collect::<Vec<_>>();
    (!indexes.is_empty()).then_some(indexes)
}

fn contact_url_by_label(contact: &AccessibleContact, labels: &[&str]) -> String {
    contact_labeled_json_values(&contact.urls_json, "url", labels)
        .into_iter()
        .next()
        .or_else(|| {
            contact_labeled_json_values(&contact.urls_json, "href", labels)
                .into_iter()
                .next()
        })
        .unwrap_or_default()
}

fn contact_labeled_json_values(
    value: &serde_json::Value,
    key: &str,
    labels: &[&str],
) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| {
            let label = item
                .get("label")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            labels
                .iter()
                .any(|expected| label.eq_ignore_ascii_case(expected))
        })
        .filter_map(|item| item.get(key).and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn contact_json_values(value: &serde_json::Value, key: &str) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| item.get(key).and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

pub(in crate::mapi) fn default_contact_for_mapping(
    account_id: Uuid,
    collection_id: &str,
) -> AccessibleContact {
    AccessibleContact {
        id: Uuid::nil(),
        collection_id: collection_id.to_string(),
        owner_account_id: account_id,
        owner_email: String::new(),
        owner_display_name: String::new(),
        rights: default_mapping_rights(),
        name: String::new(),
        role: String::new(),
        email: String::new(),
        phone: String::new(),
        team: String::new(),
        notes: String::new(),
        ..Default::default()
    }
}

pub(in crate::mapi) fn contact_input_from_mapi(
    account_id: Uuid,
    id: Option<Uuid>,
    existing: &AccessibleContact,
    properties: &HashMap<u32, MapiValue>,
) -> UpsertClientContactInput {
    let mut structured_name = existing.structured_name.clone();
    if let Some(value) =
        optional_pending_text_property(properties, &[PID_TAG_DISPLAY_NAME_PREFIX_W])
    {
        structured_name.prefix = value;
    }
    if let Some(value) = optional_pending_text_property(properties, &[PID_TAG_GIVEN_NAME_W]) {
        structured_name.given = value;
    }
    if let Some(value) = optional_pending_text_property(properties, &[PID_TAG_MIDDLE_NAME_W]) {
        structured_name.middle = value;
    }
    if let Some(value) = optional_pending_text_property(properties, &[PID_TAG_SURNAME_W]) {
        structured_name.family = value;
    }
    if let Some(value) = optional_pending_text_property(properties, &[PID_TAG_GENERATION_W]) {
        structured_name.suffix = value;
    }
    if let Some(value) = optional_pending_text_property(properties, &[PID_TAG_NICKNAME_W]) {
        structured_name.nickname = value;
    }
    let name = optional_pending_text_property(
        properties,
        &[
            PID_TAG_DISPLAY_NAME_W,
            PID_TAG_SUBJECT_W,
            PID_TAG_NORMALIZED_SUBJECT_W,
        ],
    )
    .or_else(|| {
        (!structured_name.given.trim().is_empty() || !structured_name.family.trim().is_empty())
            .then(|| contact_display_name_from_structured(&structured_name))
            .filter(|value| !value.trim().is_empty())
    })
    .unwrap_or_else(|| existing.name.clone());
    let email1 = optional_pending_text_property(
        properties,
        &[
            PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG,
            PID_LID_EMAIL1_DISPLAY_NAME_W_TAG,
            PID_LID_EMAIL1_ORIGINAL_DISPLAY_NAME_W_TAG,
        ],
    );
    let email2 = optional_pending_text_property(
        properties,
        &[
            PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG,
            PID_LID_EMAIL2_DISPLAY_NAME_W_TAG,
            PID_LID_EMAIL2_ORIGINAL_DISPLAY_NAME_W_TAG,
        ],
    );
    let email3 = optional_pending_text_property(
        properties,
        &[
            PID_LID_EMAIL3_EMAIL_ADDRESS_W_TAG,
            PID_LID_EMAIL3_DISPLAY_NAME_W_TAG,
            PID_LID_EMAIL3_ORIGINAL_DISPLAY_NAME_W_TAG,
        ],
    );
    let email = optional_pending_text_property(
        properties,
        &[PID_TAG_SMTP_ADDRESS_W, PID_TAG_EMAIL_ADDRESS_W],
    )
    .or_else(|| email1.clone())
    .unwrap_or_else(|| existing.email.clone());
    let mobile_phone =
        optional_pending_text_property(properties, &[PID_TAG_MOBILE_TELEPHONE_NUMBER_W]);
    let business_phone =
        optional_pending_text_property(properties, &[PID_TAG_BUSINESS_TELEPHONE_NUMBER_W]);
    let home_phone = optional_pending_text_property(properties, &[PID_TAG_HOME_TELEPHONE_NUMBER_W]);
    let primary_phone =
        optional_pending_text_property(properties, &[PID_TAG_PRIMARY_TELEPHONE_NUMBER_W]);
    let phone = mobile_phone
        .clone()
        .or_else(|| business_phone.clone())
        .or_else(|| home_phone.clone())
        .or(primary_phone.clone())
        .unwrap_or_else(|| existing.phone.clone());
    let company = optional_pending_text_property(properties, &[PID_TAG_COMPANY_NAME_W])
        .unwrap_or_else(|| contact_organization_name(existing));
    let department = optional_pending_text_property(properties, &[PID_TAG_DEPARTMENT_NAME_W])
        .or_else(|| optional_pending_text_property(properties, &[PID_TAG_COMPANY_NAME_W]))
        .unwrap_or_else(|| existing.team.clone());
    let title = optional_pending_text_property(properties, &[PID_TAG_TITLE_W])
        .unwrap_or_else(|| contact_job_title(existing));
    let personal_url = optional_pending_text_property(properties, &[PID_TAG_PERSONAL_HOME_PAGE_W]);
    let business_url = optional_pending_text_property(properties, &[PID_TAG_BUSINESS_HOME_PAGE_W]);
    UpsertClientContactInput {
        id,
        account_id,
        name,
        role: title.clone(),
        email: email.clone(),
        phone: phone.clone(),
        team: department,
        notes: optional_pending_text_property(properties, &[PID_TAG_BODY_W])
            .unwrap_or_else(|| existing.notes.clone()),
        structured_name,
        emails_json: Some(contact_emails_json_from_mapi(
            existing,
            &email,
            email1.as_deref(),
            email2.as_deref(),
            email3.as_deref(),
        )),
        phones_json: Some(contact_phones_json_from_mapi(
            existing,
            &phone,
            mobile_phone.as_deref(),
            business_phone.as_deref().or(primary_phone.as_deref()),
            home_phone.as_deref(),
        )),
        urls_json: Some(contact_urls_json_from_mapi(
            &existing.urls_json,
            personal_url.as_deref(),
            business_url.as_deref(),
        )),
        organization_name: company,
        job_title: title,
        ..Default::default()
    }
}

fn contact_emails_json_from_mapi(
    existing: &AccessibleContact,
    primary: &str,
    email1: Option<&str>,
    email2: Option<&str>,
    email3: Option<&str>,
) -> serde_json::Value {
    if email1.is_none() && email2.is_none() && email3.is_none() {
        return update_primary_labeled_json(&existing.emails_json, "email", "work", primary);
    }
    let mut rows = Vec::new();
    if let Some(value) = email1
        .or((!primary.trim().is_empty()).then_some(primary))
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        rows.push(serde_json::json!({
            "email": value,
            "label": "work",
            "isDefault": true
        }));
    }
    for (value, label) in [(email2, "home"), (email3, "other")] {
        if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
            rows.push(serde_json::json!({
                "email": value,
                "label": label,
                "isDefault": false
            }));
        }
    }
    serde_json::Value::Array(rows)
}

fn contact_display_name_from_structured(name: &lpe_storage::ContactNameFields) -> String {
    [
        name.prefix.as_str(),
        name.given.as_str(),
        name.middle.as_str(),
        name.family.as_str(),
        name.suffix.as_str(),
    ]
    .into_iter()
    .map(str::trim)
    .filter(|value| !value.is_empty())
    .collect::<Vec<_>>()
    .join(" ")
}

fn update_primary_labeled_json(
    existing: &serde_json::Value,
    key: &str,
    label: &str,
    value: &str,
) -> serde_json::Value {
    let mut rows = existing.as_array().cloned().unwrap_or_default();
    if let Some(row) = rows.first_mut() {
        if let Some(object) = row.as_object_mut() {
            object.insert(
                key.to_string(),
                serde_json::Value::String(value.trim().to_string()),
            );
            object.insert(
                "label".to_string(),
                serde_json::Value::String(label.to_string()),
            );
            object.insert("isDefault".to_string(), serde_json::Value::Bool(true));
        }
    } else if !value.trim().is_empty() {
        rows.push(serde_json::json!({
            key: value.trim(),
            "label": label,
            "isDefault": true
        }));
    }
    serde_json::Value::Array(rows)
}

fn contact_phones_json_from_mapi(
    existing: &AccessibleContact,
    primary: &str,
    mobile: Option<&str>,
    business: Option<&str>,
    home: Option<&str>,
) -> serde_json::Value {
    let mut rows = Vec::new();
    push_labeled_value(&mut rows, "phone", "mobile", mobile);
    push_labeled_value(&mut rows, "phone", "work", business.or(Some(primary)));
    push_labeled_value(&mut rows, "phone", "home", home);
    if rows.is_empty() {
        existing.phones_json.clone()
    } else {
        serde_json::Value::Array(rows)
    }
}

fn contact_urls_json_from_mapi(
    existing: &serde_json::Value,
    personal: Option<&str>,
    business: Option<&str>,
) -> serde_json::Value {
    let mut rows = existing.as_array().cloned().unwrap_or_default();
    upsert_labeled_value(&mut rows, "url", "home", personal);
    upsert_labeled_value(&mut rows, "url", "work", business);
    serde_json::Value::Array(rows)
}

fn push_labeled_value(
    rows: &mut Vec<serde_json::Value>,
    key: &str,
    label: &str,
    value: Option<&str>,
) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        rows.push(serde_json::json!({ key: value, "label": label }));
    }
}

fn upsert_labeled_value(
    rows: &mut Vec<serde_json::Value>,
    key: &str,
    label: &str,
    value: Option<&str>,
) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    if let Some(row) = rows.iter_mut().find(|row| {
        row.get("label")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|current| current.eq_ignore_ascii_case(label))
    }) {
        if let Some(object) = row.as_object_mut() {
            object.insert(
                key.to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
    } else {
        rows.push(serde_json::json!({ key: value, "label": label }));
    }
}

fn reject_unsupported_mapi_contact_properties(properties: &HashMap<u32, MapiValue>) -> Result<()> {
    for tag in properties.keys() {
        let supported = matches!(
            canonical_property_storage_tag(*tag),
            PID_TAG_DISPLAY_NAME_W
                | PID_TAG_SUBJECT_W
                | PID_TAG_NORMALIZED_SUBJECT_W
                | PID_TAG_DISPLAY_NAME_PREFIX_W
                | PID_TAG_GIVEN_NAME_W
                | PID_TAG_MIDDLE_NAME_W
                | PID_TAG_SURNAME_W
                | PID_TAG_GENERATION_W
                | PID_TAG_NICKNAME_W
                | PID_TAG_TITLE_W
                | PID_TAG_SMTP_ADDRESS_W
                | PID_TAG_EMAIL_ADDRESS_W
                | PID_TAG_MOBILE_TELEPHONE_NUMBER_W
                | PID_TAG_BUSINESS_TELEPHONE_NUMBER_W
                | PID_TAG_HOME_TELEPHONE_NUMBER_W
                | PID_TAG_PRIMARY_TELEPHONE_NUMBER_W
                | PID_TAG_COMPANY_NAME_W
                | PID_TAG_DEPARTMENT_NAME_W
                | PID_TAG_PERSONAL_HOME_PAGE_W
                | PID_TAG_BUSINESS_HOME_PAGE_W
                | PID_TAG_BODY_W
                | PID_LID_EMAIL1_ADDRESS_TYPE_W_TAG
                | PID_LID_EMAIL1_DISPLAY_NAME_W_TAG
                | PID_LID_EMAIL1_EMAIL_ADDRESS_W_TAG
                | PID_LID_EMAIL1_ORIGINAL_DISPLAY_NAME_W_TAG
                | PID_LID_EMAIL2_ADDRESS_TYPE_W_TAG
                | PID_LID_EMAIL2_DISPLAY_NAME_W_TAG
                | PID_LID_EMAIL2_EMAIL_ADDRESS_W_TAG
                | PID_LID_EMAIL2_ORIGINAL_DISPLAY_NAME_W_TAG
                | PID_LID_EMAIL3_ADDRESS_TYPE_W_TAG
                | PID_LID_EMAIL3_DISPLAY_NAME_W_TAG
                | PID_LID_EMAIL3_EMAIL_ADDRESS_W_TAG
                | PID_LID_EMAIL3_ORIGINAL_DISPLAY_NAME_W_TAG
        );
        if !supported {
            return Err(anyhow!(
                "MAPI contact property {tag:#010X} is outside the canonical contact subset"
            ));
        }
    }
    Ok(())
}

pub(in crate::mapi) async fn apply_canonical_contact_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    contact_id: u64,
    values: Vec<(u32, MapiValue)>,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let contact = snapshot
        .contact_for_id(folder_id, contact_id)
        .ok_or_else(|| anyhow!("canonical MAPI contact was not found"))?;
    let properties = values.into_iter().collect::<HashMap<_, _>>();
    reject_unsupported_mapi_contact_properties(&properties)?;
    let input = contact_input_from_mapi(
        principal.account_id,
        Some(contact.canonical_id),
        &contact.contact,
        &properties,
    );
    store
        .update_accessible_contact(principal.account_id, contact.canonical_id, input)
        .await?;
    Ok(())
}
