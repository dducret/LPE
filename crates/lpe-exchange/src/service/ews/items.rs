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
        let include_mime_content = requested_mime_content(request);
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
        for task in self
            .store
            .fetch_accessible_tasks_by_ids(principal.account_id, &task_ids)
            .await?
        {
            items.push_str(&task_item_xml(&task));
        }
        for email in self
            .store
            .fetch_jmap_emails(principal.account_id, &message_ids)
            .await?
            .into_iter()
        {
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
            ));
        }
        for item in self
            .store
            .fetch_public_folder_items_by_ids(principal.account_id, &public_folder_item_ids)
            .await?
        {
            items.push_str(&public_folder_item_xml(&item));
        }

        if !ids.is_empty()
            && (supported_id_count != ids.len()
                || count_tag_occurrences(&items, "<t:ItemId") != supported_id_count)
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
            FolderKind::Tasks => {
                let collection_id = requested_collection_id(request).unwrap_or(TASKS_FOLDER_ID);
                let tasks = self
                    .store
                    .fetch_accessible_tasks_in_collection(principal.account_id, collection_id)
                    .await?;
                Ok(find_item_response(
                    tasks.iter().map(task_item_summary_xml).collect(),
                ))
            }
            FolderKind::Mailbox => {
                let Some(mailbox_id) = self
                    .requested_mailbox_folder_ids(principal, request)
                    .await?
                    .into_iter()
                    .next()
                else {
                    return Ok(find_item_response(String::new()));
                };
                let query = self
                    .store
                    .query_jmap_email_ids(
                        principal.account_id,
                        Some(mailbox_id),
                        None,
                        0,
                        MAILBOX_QUERY_LIMIT,
                    )
                    .await?;
                let emails = self
                    .store
                    .fetch_jmap_emails(principal.account_id, &query.ids)
                    .await?;
                Ok(find_item_response(
                    emails
                        .iter()
                        .filter(|email| email.mailbox_id == mailbox_id)
                        .map(message_summary_xml)
                        .collect(),
                ))
            }
            FolderKind::PublicFolders => {
                let Some(folder_id) = requested_public_folder_ids(request).into_iter().next()
                else {
                    return Ok(find_item_response(String::new()));
                };
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
