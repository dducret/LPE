use super::super::*;

pub(in crate::service) fn public_folder_item_change_key(item: &PublicFolderItem) -> String {
    stable_change_key(&[
        "public-folder-item",
        &item.id.to_string(),
        &item.public_folder_id.to_string(),
        &item.change_counter.to_string(),
        &item.updated_at,
    ])
}

pub(in crate::service) fn public_folder_item_summary_xml(item: &PublicFolderItem) -> String {
    format!(
        concat!(
            "<t:Message>",
            "<t:ItemId Id=\"public-folder-item:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"public-folder:{folder_id}\"/>",
            "<t:ItemClass>{message_class}</t:ItemClass>",
            "<t:Subject>{subject}</t:Subject>",
            "<t:DateTimeReceived>{updated_at}</t:DateTimeReceived>",
            "<t:Size>{size}</t:Size>",
            "<t:HasAttachments>false</t:HasAttachments>",
            "<t:IsRead>{is_read}</t:IsRead>",
            "</t:Message>"
        ),
        id = item.id,
        change_key = escape_xml(&public_folder_item_change_key(item)),
        folder_id = item.public_folder_id,
        message_class = escape_xml(&item.message_class),
        subject = escape_xml(&item.subject),
        updated_at = escape_xml(&item.updated_at),
        size = item.body_text.len(),
        is_read = item.is_read,
    )
}

pub(in crate::service) fn public_folder_item_xml(item: &PublicFolderItem) -> String {
    let mut xml = public_folder_item_summary_xml(item);
    let body = item
        .body_html_sanitized
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(|html| format!("<t:Body BodyType=\"HTML\">{}</t:Body>", escape_xml(html)))
        .unwrap_or_else(|| {
            format!(
                "<t:Body BodyType=\"Text\">{}</t:Body>",
                escape_xml(&item.body_text)
            )
        });
    xml.insert_str(xml.len() - "</t:Message>".len(), &body);
    xml
}

pub(in crate::service) fn create_public_folder_item_success_response(
    item: &PublicFolderItem,
) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:Message>",
            "<t:ItemId Id=\"public-folder-item:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"public-folder:{folder_id}\"/>",
            "</t:Message>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        id = item.id,
        folder_id = item.public_folder_id,
        change_key = escape_xml(&public_folder_item_change_key(item)),
    )
}

pub(in crate::service) fn parse_update_public_folder_item_input(
    principal: &AccountPrincipal,
    existing: &PublicFolderItem,
    request: &str,
) -> UpsertPublicFolderItemInput {
    let message = element_content(request, "Message").unwrap_or(request);
    let body_text = if field_deleted(request, "item:Body") {
        String::new()
    } else if let Some(body_value) = element_text(message, "Body") {
        let body_tag = open_tag_text(message, "Body").unwrap_or_default();
        let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
        if body_type.eq_ignore_ascii_case("HTML") {
            html_to_text(&body_value)
        } else {
            body_value
        }
    } else {
        existing.body_text.clone()
    };
    UpsertPublicFolderItemInput {
        id: Some(existing.id),
        account_id: principal.account_id,
        public_folder_id: existing.public_folder_id,
        item_kind: existing.item_kind.clone(),
        message_class: element_text(message, "ItemClass")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| existing.message_class.clone()),
        subject: deleted_or_updated_text(
            request,
            message,
            "item:Subject",
            "Subject",
            &existing.subject,
        ),
        body_text,
        body_html_sanitized: existing.body_html_sanitized.clone(),
        source_payload_json: existing.source_payload_json.clone(),
    }
}

pub(in crate::service) fn public_folder_item_clone_input(
    principal: &AccountPrincipal,
    existing: &PublicFolderItem,
    target_public_folder_id: Uuid,
) -> UpsertPublicFolderItemInput {
    UpsertPublicFolderItemInput {
        id: None,
        account_id: principal.account_id,
        public_folder_id: target_public_folder_id,
        item_kind: existing.item_kind.clone(),
        message_class: existing.message_class.clone(),
        subject: existing.subject.clone(),
        body_text: existing.body_text.clone(),
        body_html_sanitized: existing.body_html_sanitized.clone(),
        source_payload_json: existing.source_payload_json.clone(),
    }
}
