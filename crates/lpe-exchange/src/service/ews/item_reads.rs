use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn get_item(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let ids = match requested_get_item_ids(request) {
            Ok(ids) => ids,
            Err(error) => {
                return Ok(get_item_error_response(
                    "ErrorItemNotFound",
                    &error.to_string(),
                ));
            }
        };
        let include_mime_content = requested_mime_content(request);
        let prefer_html_body = requested_html_body(request);
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
        let task_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("task:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();
        let message_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("message:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();
        let public_folder_item_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("public-folder-item:"))
            .filter_map(|id| Uuid::parse_str(id).ok())
            .collect::<Vec<_>>();
        let supported_id_count = contact_ids.len()
            + event_ids.len()
            + task_ids.len()
            + message_ids.len()
            + public_folder_item_ids.len();

        let contacts = self
            .store
            .fetch_accessible_contacts_by_ids(principal.account_id, &contact_ids)
            .await?;
        let events = self
            .store
            .fetch_accessible_events_by_ids(principal.account_id, &event_ids)
            .await?;
        let tasks = self
            .store
            .fetch_accessible_tasks_by_ids(principal.account_id, &task_ids)
            .await?;
        // [MS-OXWSMSG] sections 2.2.4.3 and 3.1.4.4 permit BccRecipients in a
        // Message response. The storage call binds protected recipients to the
        // authenticated owner; rendering further limits them to that owner's Sent item.
        let emails = self
            .store
            .fetch_jmap_emails_with_protected_bcc(principal.account_id, &message_ids)
            .await?;
        let public_folder_items = self
            .store
            .fetch_public_folder_items_by_ids(principal.account_id, &public_folder_item_ids)
            .await?;
        if contacts.len() + events.len() + tasks.len() + emails.len() + public_folder_items.len()
            != ids.len()
        {
            return Ok(get_item_error_response(
                "ErrorItemNotFound",
                "The requested item was not found or is not exposed by the EWS MVP.",
            ));
        }

        let mut items = String::new();
        let contact_change_keys =
            contact_change_keys(&self.store, principal.account_id, &contacts).await?;
        for contact in &contacts {
            items.push_str(&contact_item_xml_with_change_key(
                contact,
                change_key_for(&contact_change_keys, contact.id, "contact")?,
            ));
        }
        let event_change_keys =
            event_change_keys(&self.store, principal.account_id, &events).await?;
        for event in &events {
            items.push_str(&calendar_item_xml_with_change_key(
                event,
                change_key_for(&event_change_keys, event.id, "calendar")?,
            ));
        }
        let task_change_keys = task_change_keys(&self.store, principal.account_id, &tasks).await?;
        for task in &tasks {
            items.push_str(&task_item_xml_with_change_key(
                task,
                change_key_for(&task_change_keys, task.id, "task")?,
            ));
        }
        for email in emails {
            let attachments = if email.has_attachments {
                self.store
                    .fetch_message_attachments(principal.account_id, email.id)
                    .await?
            } else {
                Vec::new()
            };
            let mut attachment_contents = Vec::new();
            if include_mime_content {
                for attachment in &attachments {
                    let Some(content) = self
                        .store
                        .fetch_attachment_content(principal.account_id, &attachment.file_reference)
                        .await?
                    else {
                        return Ok(get_item_error_response(
                            "ErrorItemNotFound",
                            "The requested item attachment content was not found.",
                        ));
                    };
                    attachment_contents.push(content);
                }
            }
            items.push_str(&message_item_xml_with_details(
                &email,
                &attachments,
                include_mime_content.then_some(attachment_contents.as_slice()),
                prefer_html_body,
            ));
        }
        for item in public_folder_items {
            items.push_str(&public_folder_item_xml(&item));
        }

        if supported_id_count != ids.len()
            || count_tag_occurrences(&items, "<t:ItemId") != supported_id_count
        {
            return Ok(get_item_error_response(
                "ErrorItemNotFound",
                "The requested item was not found or is not exposed by the EWS MVP.",
            ));
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

    pub(in crate::service) async fn find_item(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let (folder_kind, parent) = match requested_find_item_parent(request) {
            Ok(scope) => scope,
            Err(error) => {
                return Ok(operation_error_response(
                    "FindItem",
                    "ErrorFolderNotFound",
                    &error.to_string(),
                ));
            }
        };
        if let Err(error) = validate_find_item_folder_ids(parent, folder_kind) {
            return Ok(operation_error_response(
                "FindItem",
                "ErrorFolderNotFound",
                &error.to_string(),
            ));
        }
        match folder_kind {
            FolderKind::Root => Ok(find_item_response(String::new())),
            FolderKind::Contacts => {
                let collection_id = requested_collection_id(parent).unwrap_or(CONTACTS_FOLDER_ID);
                if !self
                    .store
                    .fetch_accessible_contact_collections(principal.account_id)
                    .await?
                    .iter()
                    .any(|collection| collection.id == collection_id)
                {
                    return Ok(find_item_folder_not_found_response());
                }
                let contacts = self
                    .store
                    .fetch_accessible_contacts_in_collection(principal.account_id, collection_id)
                    .await?;
                let change_keys =
                    contact_change_keys(&self.store, principal.account_id, &contacts).await?;
                let items = contacts
                    .iter()
                    .map(|contact| {
                        Ok(contact_summary_xml_with_change_key(
                            contact,
                            change_key_for(&change_keys, contact.id, "contact")?,
                        ))
                    })
                    .collect::<Result<String>>()?;
                Ok(find_item_response(items))
            }
            FolderKind::Calendar => {
                let collection_id = requested_collection_id(parent).unwrap_or(CALENDAR_FOLDER_ID);
                if !self
                    .store
                    .fetch_accessible_calendar_collections(principal.account_id)
                    .await?
                    .iter()
                    .any(|collection| collection.id == collection_id)
                {
                    return Ok(find_item_folder_not_found_response());
                }
                let events = self
                    .store
                    .fetch_accessible_events_in_collection(principal.account_id, collection_id)
                    .await?;
                let change_keys =
                    event_change_keys(&self.store, principal.account_id, &events).await?;
                let items = events
                    .iter()
                    .map(|event| {
                        Ok(calendar_item_summary_xml_with_change_key(
                            event,
                            change_key_for(&change_keys, event.id, "calendar")?,
                        ))
                    })
                    .collect::<Result<String>>()?;
                Ok(find_item_response(items))
            }
            FolderKind::Tasks => {
                let collection_id = requested_collection_id(parent).unwrap_or(TASKS_FOLDER_ID);
                if !self
                    .store
                    .fetch_accessible_task_collections(principal.account_id)
                    .await?
                    .iter()
                    .any(|collection| collection.id == collection_id)
                {
                    return Ok(find_item_folder_not_found_response());
                }
                let tasks = self
                    .store
                    .fetch_accessible_tasks_in_collection(principal.account_id, collection_id)
                    .await?;
                let change_keys =
                    task_change_keys(&self.store, principal.account_id, &tasks).await?;
                let items = tasks
                    .iter()
                    .map(|task| {
                        Ok(task_item_summary_xml_with_change_key(
                            task,
                            change_key_for(&change_keys, task.id, "task")?,
                        ))
                    })
                    .collect::<Result<String>>()?;
                Ok(find_item_response(items))
            }
            FolderKind::Mailbox => {
                let Some(mailbox_id) = self
                    .requested_mailbox_folder_ids(principal, parent)
                    .await?
                    .into_iter()
                    .next()
                else {
                    return Ok(find_item_folder_not_found_response());
                };
                if !self
                    .store
                    .fetch_jmap_mailboxes(principal.account_id)
                    .await?
                    .iter()
                    .any(|mailbox| mailbox.id == mailbox_id)
                {
                    return Ok(find_item_folder_not_found_response());
                }
                if attribute_value_after(request, "IndexedPageItemView", "BasePoint")
                    .is_some_and(|base_point| !base_point.eq_ignore_ascii_case("Beginning"))
                {
                    bail!("FindItem supports IndexedPageItemView only from Beginning");
                }
                let offset = ews_usize_attribute(request, "IndexedPageItemView", "Offset")
                    .unwrap_or(0) as u64;
                let limit =
                    ews_usize_attribute(request, "IndexedPageItemView", "MaxEntriesReturned")
                        .unwrap_or(MAILBOX_QUERY_LIMIT as usize)
                        .clamp(1, MAILBOX_QUERY_LIMIT as usize) as u64;
                let query = self
                    .store
                    .query_jmap_email_ids(
                        principal.account_id,
                        Some(mailbox_id),
                        None,
                        offset,
                        limit,
                    )
                    .await?;
                let emails = self
                    .store
                    .fetch_jmap_emails(principal.account_id, &query.ids)
                    .await?;
                // [MS-OXWSSRCH] §3.1.4.2: fail closed rather than render a partial page
                // or its count if canonical visibility changed after the ID query.
                if emails.len() != query.ids.len()
                    || emails.iter().any(|email| {
                        email
                            .mailbox_states
                            .iter()
                            .all(|state| state.mailbox_id != mailbox_id)
                    })
                {
                    return Ok(find_item_folder_not_found_response());
                }
                let returned = emails
                    .iter()
                    .map(|email| message_summary_xml_for_mailbox(email, mailbox_id))
                    .collect();
                Ok(find_item_page_response(
                    returned,
                    query.total,
                    offset.saturating_add(query.ids.len() as u64) >= query.total,
                ))
            }
            FolderKind::PublicFolders => {
                let Some(folder_id) = requested_public_folder_ids(parent).into_iter().next() else {
                    return Ok(find_item_folder_not_found_response());
                };
                if self
                    .store
                    .fetch_public_folder(principal.account_id, folder_id)
                    .await
                    .is_err()
                {
                    return Ok(find_item_folder_not_found_response());
                }
                let items = self
                    .store
                    .fetch_public_folder_items(principal.account_id, folder_id)
                    .await?;
                Ok(find_item_response(
                    items.iter().map(public_folder_item_summary_xml).collect(),
                ))
            }
        }
    }
}

fn requested_get_item_ids(request: &str) -> Result<Vec<String>> {
    let wrappers = element_contents(request, "ItemIds");
    let [wrapper] = wrappers.as_slice() else {
        bail!("GetItem requires exactly one ItemIds collection");
    };
    let references = requested_item_references(wrapper);
    if references.is_empty() || requested_item_references(request).len() != references.len() {
        bail!("GetItem requires one nonempty ItemIds collection");
    }
    for reference in &references {
        let Some((kind, id)) = reference.id.split_once(':') else {
            bail!("GetItem item id is not supported");
        };
        if !matches!(
            kind,
            "contact" | "event" | "task" | "message" | "public-folder-item"
        ) || Uuid::parse_str(id).is_err()
        {
            bail!("GetItem item id is not supported");
        }
    }
    Ok(references
        .into_iter()
        .map(|reference| reference.id)
        .collect())
}

fn requested_find_item_parent(request: &str) -> Result<(FolderKind, &str)> {
    let parents = element_contents(request, "ParentFolderIds");
    let [parent] = parents.as_slice() else {
        bail!("FindItem requires exactly one ParentFolderIds collection");
    };
    let target_count = attribute_values_for_tag(parent, "FolderId", "Id").len()
        + attribute_values_for_tag(parent, "DistinguishedFolderId", "Id").len();
    if target_count != 1 {
        bail!("FindItem requires exactly one parent folder id");
    }
    let folder_kind = requested_folder_kind(parent)
        .ok_or_else(|| anyhow!("FindItem parent folder is not supported"))?;
    Ok((folder_kind, parent))
}

fn find_item_folder_not_found_response() -> String {
    operation_error_response(
        "FindItem",
        "ErrorFolderNotFound",
        "The requested folder is not exposed by the EWS MVP.",
    )
}

// [MS-OXWSMSG] section 3.1.4.5: malformed explicit folder references must
// not be treated as an empty result set.
fn validate_find_item_folder_ids(request: &str, folder_kind: FolderKind) -> Result<()> {
    let ids = requested_folder_ids(request);
    match folder_kind {
        FolderKind::Mailbox => {
            for id in ids {
                let id = id.strip_prefix("mailbox:").unwrap_or(&id);
                Uuid::parse_str(id)
                    .map_err(|_| anyhow!("FindItem mailbox folder id is invalid"))?;
            }
        }
        FolderKind::PublicFolders => {
            for id in ids {
                let id = id
                    .strip_prefix("public-folder:")
                    .ok_or_else(|| anyhow!("FindItem public folder id is invalid"))?;
                Uuid::parse_str(id).map_err(|_| anyhow!("FindItem public folder id is invalid"))?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{validate_find_item_folder_ids, FolderKind};

    #[test]
    fn find_item_rejects_malformed_canonical_folder_ids() {
        assert!(validate_find_item_folder_ids(
            r#"<t:FolderId Id="mailbox:not-a-uuid"/>"#,
            FolderKind::Mailbox,
        )
        .is_err());
        assert!(validate_find_item_folder_ids(
            r#"<t:FolderId Id="public-folder:not-a-uuid"/>"#,
            FolderKind::PublicFolders,
        )
        .is_err());
    }
}
