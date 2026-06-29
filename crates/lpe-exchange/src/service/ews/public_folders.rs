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
