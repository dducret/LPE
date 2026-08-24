use super::super::*;

pub(in crate::service) struct UpdateItemChange<'a> {
    pub(in crate::service) reference: RequestedItemReference,
    content: &'a str,
}

pub(in crate::service) fn requested_update_item_changes(
    request: &str,
) -> Result<Vec<UpdateItemChange<'_>>> {
    let changes = element_contents(request, "ItemChange")
        .into_iter()
        .map(|content| {
            let references = requested_item_references(content);
            let [reference] = references.as_slice() else {
                bail!("each UpdateItem ItemChange requires exactly one ItemId");
            };
            Ok(UpdateItemChange {
                reference: reference.clone(),
                content,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if changes.is_empty() {
        bail!("UpdateItem requires at least one ItemChange");
    }
    Ok(changes)
}

pub(in crate::service) fn update_item_change_content<'a>(
    changes: &'a [UpdateItemChange<'a>],
    id: &str,
) -> Result<&'a str> {
    changes
        .iter()
        .find(|change| change.reference.id == id)
        .map(|change| change.content)
        .ok_or_else(|| anyhow!("UpdateItem ItemChange was not found for {id}"))
}

pub(in crate::service) fn parse_update_item_message_flags(
    change: &str,
) -> Result<(Option<bool>, Option<bool>)> {
    let updates = element_contents(change, "Updates");
    let [updates] = updates.as_slice() else {
        bail!("each UpdateItem ItemChange requires exactly one Updates collection");
    };
    let field_uris = attribute_values_for_tag(updates, "FieldURI", "FieldURI");
    if field_uris.is_empty()
        || field_uris.iter().any(|field_uri| {
            !matches!(
                *field_uri,
                "message:IsRead" | "message:Flag" | "message:FlagStatus"
            )
        })
        || !attribute_values_for_tag(updates, "ExtendedFieldURI", "PropertyTag").is_empty()
        || !attribute_values_for_tag(updates, "IndexedFieldURI", "FieldURI").is_empty()
    {
        bail!("UpdateItem message updates support only IsRead and FlagStatus");
    }

    let unread = element_text(updates, "IsRead")
        .map(|value| parse_xml_bool(&value).map(|is_read| !is_read))
        .transpose()?;
    let mut flagged = element_text(updates, "FlagStatus")
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "notflagged" => Ok(false),
            "flagged" | "complete" => Ok(true),
            other => bail!("unsupported message FlagStatus {other}"),
        })
        .transpose()?;
    if field_deleted(updates, "message:Flag") || field_deleted(updates, "message:FlagStatus") {
        flagged = Some(false);
    }
    if unread.is_none() && flagged.is_none() {
        bail!("UpdateItem message update is missing IsRead or FlagStatus");
    }
    Ok((unread, flagged))
}

pub(in crate::service) fn validate_supplied_item_change_key(
    references: &[RequestedItemReference],
    id: &str,
    current_change_key: &str,
) -> Result<()> {
    let supplied_change_key = references
        .iter()
        .find(|reference| reference.id == id)
        .and_then(|reference| reference.change_key.as_deref());
    if matches!(supplied_change_key, Some(change_key) if change_key != current_change_key) {
        bail!("stale EWS ChangeKey for {id}");
    }
    Ok(())
}

pub(in crate::service) fn validate_required_item_change_key(
    references: &[RequestedItemReference],
    id: &str,
    current_change_key: &str,
) -> Result<()> {
    let supplied_change_key = references
        .iter()
        .find(|reference| reference.id == id)
        .and_then(|reference| reference.change_key.as_deref())
        .ok_or_else(|| anyhow!("stale EWS ChangeKey for {id}: missing ChangeKey"))?;
    if supplied_change_key != current_change_key {
        bail!("stale EWS ChangeKey for {id}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        parse_update_item_message_flags, requested_update_item_changes, update_item_change_content,
        validate_required_item_change_key, validate_supplied_item_change_key,
    };
    use crate::service::ews::request_ids::RequestedItemReference;

    #[test]
    fn message_update_properties_are_local_and_reject_unsupported_fields() {
        let changes = requested_update_item_changes(concat!(
            "<m:UpdateItem><m:ItemChanges>",
            "<t:ItemChange><t:ItemId Id=\"message:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\"/>",
            "<t:Updates><t:SetItemField><t:FieldURI FieldURI=\"message:IsRead\"/>",
            "<t:Message><t:IsRead>true</t:IsRead></t:Message></t:SetItemField></t:Updates></t:ItemChange>",
            "<t:ItemChange><t:ItemId Id=\"message:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\"/>",
            "<t:Updates><t:SetItemField><t:FieldURI FieldURI=\"message:Flag\"/>",
            "<t:Message><t:Flag><t:FlagStatus>Flagged</t:FlagStatus></t:Flag></t:Message></t:SetItemField></t:Updates></t:ItemChange>",
            "</m:ItemChanges></m:UpdateItem>",
        ))
        .unwrap();

        assert_eq!(
            parse_update_item_message_flags(
                update_item_change_content(
                    &changes,
                    "message:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
                )
                .unwrap()
            )
            .unwrap(),
            (Some(false), None)
        );
        assert_eq!(
            parse_update_item_message_flags(
                update_item_change_content(
                    &changes,
                    "message:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
                )
                .unwrap()
            )
            .unwrap(),
            (None, Some(true))
        );
        assert!(parse_update_item_message_flags(
            "<t:ItemChange><t:Updates><t:SetItemField><t:FieldURI FieldURI=\"item:Subject\"/></t:SetItemField></t:Updates></t:ItemChange>"
        )
        .is_err());
    }

    #[test]
    fn stale_supplied_change_key_is_rejected_before_item_mutation() {
        let error = validate_supplied_item_change_key(
            &[RequestedItemReference {
                id: "message:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(),
                change_key: Some("stale".to_string()),
            }],
            "message:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "current",
        )
        .unwrap_err();

        assert!(error.to_string().contains("stale EWS ChangeKey"));
    }

    #[test]
    fn missing_required_change_key_is_a_conflict() {
        let error = validate_required_item_change_key(
            &[RequestedItemReference {
                id: "contact:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(),
                change_key: None,
            }],
            "contact:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "current",
        )
        .unwrap_err();

        assert!(error.to_string().contains("stale EWS ChangeKey"));
        assert!(error.to_string().contains("missing ChangeKey"));
    }
}
