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
        let contacts = self
            .store
            .fetch_accessible_contacts_by_ids(principal.account_id, &contact_ids)
            .await?;
        let contact_change_keys =
            contact_change_keys(&self.store, principal.account_id, &contacts).await?;
        for contact in &contacts {
            items.push_str(&contact_item_xml_with_change_key(
                contact,
                change_key_for(&contact_change_keys, contact.id, "contact")?,
            ));
        }
        let events = self
            .store
            .fetch_accessible_events_by_ids(principal.account_id, &event_ids)
            .await?;
        let event_change_keys =
            event_change_keys(&self.store, principal.account_id, &events).await?;
        for event in &events {
            items.push_str(&calendar_item_xml_with_change_key(
                event,
                change_key_for(&event_change_keys, event.id, "calendar")?,
            ));
        }
        let tasks = self
            .store
            .fetch_accessible_tasks_by_ids(principal.account_id, &task_ids)
            .await?;
        let task_change_keys = task_change_keys(&self.store, principal.account_id, &tasks).await?;
        for task in &tasks {
            items.push_str(&task_item_xml_with_change_key(
                task,
                change_key_for(&task_change_keys, task.id, "task")?,
            ));
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
                let collection_id = requested_collection_id(request).unwrap_or(CALENDAR_FOLDER_ID);
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
                let collection_id = requested_collection_id(request).unwrap_or(TASKS_FOLDER_ID);
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
                    .requested_mailbox_folder_ids(principal, request)
                    .await?
                    .into_iter()
                    .next()
                else {
                    return Ok(find_item_response(String::new()));
                };
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
                let returned = emails
                    .iter()
                    .filter(|email| {
                        email
                            .mailbox_states
                            .iter()
                            .any(|state| state.mailbox_id == mailbox_id)
                    })
                    .map(|email| message_summary_xml_for_mailbox(email, mailbox_id))
                    .collect();
                Ok(find_item_page_response(
                    returned,
                    query.total,
                    offset.saturating_add(query.ids.len() as u64) >= query.total,
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

    pub(in crate::service) async fn update_item(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            self.validate_mutating_item_change_keys(principal, request)
                .await?;
            let item_references = requested_item_references(request);
            let ids = item_references
                .iter()
                .map(|reference| reference.id.clone())
                .collect::<Vec<_>>();
            let contact_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("contact:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let event_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("event:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let task_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("task:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let public_folder_item_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("public-folder-item:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty()
                || contact_ids.len()
                    + event_ids.len()
                    + task_ids.len()
                    + message_ids.len()
                    + public_folder_item_ids.len()
                    != ids.len()
            {
                return Ok(operation_error_response(
                    "UpdateItem",
                    "ErrorInvalidOperation",
                    "UpdateItem currently supports only contact, calendar, task, public folder item, and read/flag message item ids.",
                ));
            }

            let mut items = String::new();
            if !message_ids.is_empty() {
                let Some((unread, flagged)) = parse_update_message_flags(request)? else {
                    return Ok(operation_error_response(
                        "UpdateItem",
                        "ErrorInvalidOperation",
                        "UpdateItem message updates currently support only IsRead and FlagStatus.",
                    ));
                };
                for message_id in message_ids {
                    let existing = self
                        .store
                        .fetch_jmap_emails(principal.account_id, &[message_id])
                        .await?
                        .into_iter()
                        .next()
                        .ok_or_else(|| anyhow!("message not found"))?;
                    validate_supplied_item_change_key(
                        &item_references,
                        &format!("message:{message_id}"),
                        &message_change_key(&existing),
                    )?;
                    let updated = self
                        .store
                        .update_jmap_email_flags(
                            principal.account_id,
                            message_id,
                            unread,
                            flagged,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-update-message-flags".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                    items.push_str(&message_item_xml(&updated));
                }
            }
            for contact_id in contact_ids {
                let existing = self
                    .store
                    .fetch_accessible_contacts_by_ids(principal.account_id, &[contact_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("contact not found"))?;
                let change_keys = contact_change_keys(
                    &self.store,
                    principal.account_id,
                    std::slice::from_ref(&existing),
                )
                .await?;
                validate_supplied_item_change_key(
                    &item_references,
                    &format!("contact:{contact_id}"),
                    change_key_for(&change_keys, contact_id, "contact")?,
                )?;
                let updated = self
                    .store
                    .update_accessible_contact(
                        principal.account_id,
                        contact_id,
                        parse_update_contact_input(principal, &existing, request),
                    )
                    .await?;
                let change_keys =
                    contact_change_keys(&self.store, principal.account_id, std::slice::from_ref(&updated))
                        .await?;
                items.push_str(&contact_item_xml_with_change_key(
                    &updated,
                    change_key_for(&change_keys, updated.id, "contact")?,
                ));
            }
            for event_id in event_ids {
                let existing = self
                    .store
                    .fetch_accessible_events_by_ids(principal.account_id, &[event_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("event not found"))?;
                let change_keys = event_change_keys(
                    &self.store,
                    principal.account_id,
                    std::slice::from_ref(&existing),
                )
                .await?;
                validate_supplied_item_change_key(
                    &item_references,
                    &format!("event:{event_id}"),
                    change_key_for(&change_keys, event_id, "calendar")?,
                )?;
                let updated = self
                    .store
                    .update_accessible_event(
                        principal.account_id,
                        event_id,
                        parse_update_event_input(principal, &existing, request)?,
                    )
                    .await?;
                let change_keys =
                    event_change_keys(&self.store, principal.account_id, std::slice::from_ref(&updated))
                        .await?;
                items.push_str(&calendar_item_xml_with_change_key(
                    &updated,
                    change_key_for(&change_keys, updated.id, "calendar")?,
                ));
            }
            for task_id in task_ids {
                let existing = self
                    .store
                    .fetch_accessible_tasks_by_ids(principal.account_id, &[task_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("task not found"))?;
                let change_keys = task_change_keys(
                    &self.store,
                    principal.account_id,
                    std::slice::from_ref(&existing),
                )
                .await?;
                validate_supplied_item_change_key(
                    &item_references,
                    &format!("task:{task_id}"),
                    change_key_for(&change_keys, task_id, "task")?,
                )?;
                let updated = self
                    .store
                    .update_accessible_task(
                        principal.account_id,
                        task_id,
                        parse_update_task_input(principal, &existing, request)?,
                    )
                    .await?;
                let change_keys =
                    task_change_keys(&self.store, principal.account_id, std::slice::from_ref(&updated))
                        .await?;
                items.push_str(&task_item_xml_with_change_key(
                    &updated,
                    change_key_for(&change_keys, updated.id, "task")?,
                ));
            }
            let public_folder_items = self
                .store
                .fetch_public_folder_items_by_ids(principal.account_id, &public_folder_item_ids)
                .await?;
            if public_folder_items.len() != public_folder_item_ids.len() {
                return Ok(operation_error_response(
                    "UpdateItem",
                    "ErrorItemNotFound",
                    "public folder item not found",
                ));
            }
            for existing in public_folder_items {
                validate_supplied_item_change_key(
                    &item_references,
                    &format!("public-folder-item:{}", existing.id),
                    &public_folder_item_change_key(&existing),
                )?;
                let updated = self
                    .store
                    .upsert_public_folder_item(
                        parse_update_public_folder_item_input(principal, &existing, request),
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-update-public-folder-item".to_string(),
                            subject: existing.id.to_string(),
                        },
                    )
                    .await?;
                items.push_str(&public_folder_item_xml(&updated));
            }

            Ok(update_item_success_response(items))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "UpdateItem",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )
        }))
    }

    pub(in crate::service) async fn create_item(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            if element_content(request, "AcceptSharingInvitation").is_some() {
                return self.accept_sharing_invitation(principal, request).await;
            }
            if element_content(request, "Contact").is_some() {
                let collection_id = requested_collection_id_in(request, "SavedItemFolderId");
                let contact = self
                    .store
                    .create_accessible_contact(
                        principal.account_id,
                        collection_id,
                        parse_create_contact_input(principal, request)?,
                    )
                    .await?;
                let change_keys = contact_change_keys(
                    &self.store,
                    principal.account_id,
                    std::slice::from_ref(&contact),
                )
                .await?;
                return Ok(create_contact_success_response(
                    &contact,
                    change_key_for(&change_keys, contact.id, "contact")?,
                ));
            }
            if element_content(request, "CalendarItem").is_some() {
                let collection_id = requested_collection_id_in(request, "SavedItemFolderId");
                let event = self
                    .store
                    .create_accessible_event(
                        principal.account_id,
                        collection_id,
                        parse_create_event_input(principal, request)?,
                    )
                    .await?;
                let change_keys = event_change_keys(
                    &self.store,
                    principal.account_id,
                    std::slice::from_ref(&event),
                )
                .await?;
                return Ok(create_event_success_response(
                    &event,
                    change_key_for(&change_keys, event.id, "calendar")?,
                ));
            }
            if element_content(request, "Task").is_some() {
                let task = self
                    .store
                    .create_accessible_task(
                        principal.account_id,
                        parse_create_task_input(principal, request)?,
                    )
                    .await?;
                let change_keys = task_change_keys(
                    &self.store,
                    principal.account_id,
                    std::slice::from_ref(&task),
                )
                .await?;
                return Ok(create_task_success_response(
                    &task,
                    change_key_for(&change_keys, task.id, "task")?,
                ));
            }

            let input = parse_create_message_input(principal, request)?;
            let subject_for_audit = input.subject.clone();
            let disposition = attribute_value_after(request, "CreateItem", "MessageDisposition")
                .unwrap_or("SaveOnly");

            match disposition {
                "SaveOnly" => {
                    if let Some(public_folder_id) =
                        requested_public_folder_ids(request).into_iter().next()
                    {
                        let item = self
                            .store
                            .upsert_public_folder_item(
                                UpsertPublicFolderItemInput {
                                    id: None,
                                    account_id: principal.account_id,
                                    public_folder_id,
                                    item_kind: "post".to_string(),
                                    message_class: "IPM.Post".to_string(),
                                    subject: input.subject,
                                    body_text: input.body_text,
                                    body_html_sanitized: input.body_html_sanitized,
                                    source_payload_json: "{}".to_string(),
                                },
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "ews-create-public-folder-item".to_string(),
                                    subject: subject_for_audit,
                                },
                            )
                            .await?;
                        return Ok(create_public_folder_item_success_response(&item));
                    }
                    if let Some(mailbox_id) = self
                        .requested_mailbox_folder_ids(principal, request)
                        .await?
                        .into_iter()
                        .next()
                    {
                        let imported = self
                            .store
                            .import_jmap_email(
                                imported_email_input(input, mailbox_id),
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "ews-import-custom-mailbox-message".to_string(),
                                    subject: subject_for_audit,
                                },
                            )
                            .await?;
                        return Ok(create_item_success_response(&imported));
                    }
                    let draft = self
                        .store
                        .save_draft_message(
                            input,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-save-draft-message".to_string(),
                                subject: subject_for_audit,
                            },
                        )
                        .await?;
                    let email = self
                        .store
                        .fetch_jmap_emails(principal.account_id, &[draft.message_id])
                        .await?
                        .into_iter()
                        .next()
                        .ok_or_else(|| anyhow!("saved draft was not found after creation"))?;
                    Ok(create_item_success_response(&email))
                }
                "SendOnly" | "SendAndSaveCopy" => {
                    let submitted = self
                        .store
                        .submit_message(
                            input,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-submit-message".to_string(),
                                subject: subject_for_audit,
                            },
                        )
                        .await?;
                    let email = self
                        .store
                        .fetch_jmap_emails(principal.account_id, &[submitted.message_id])
                        .await?
                        .into_iter()
                        .next()
                        .ok_or_else(|| anyhow!("submitted message was not found after creation"))?;
                    Ok(create_item_success_response(&email))
                }
                other => Ok(operation_error_response(
                    "CreateItem",
                    "ErrorInvalidOperation",
                    &format!("unsupported CreateItem MessageDisposition {other}"),
                )),
            }
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "CreateItem",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )
        }))
    }

    pub(in crate::service) async fn send_item(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            self.validate_mutating_item_change_keys(principal, request)
                .await?;
            let draft_ids = requested_item_ids(request)
                .into_iter()
                .filter_map(|id| canonical_message_id_from_ews_id(&id))
                .collect::<Vec<_>>();
            if draft_ids.is_empty() {
                bail!("SendItem requires at least one message ItemId.");
            }
            for draft_id in draft_ids {
                self.store
                    .submit_draft_message(
                        principal.account_id,
                        draft_id,
                        principal.account_id,
                        "ews-senditem",
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-senditem".to_string(),
                            subject: draft_id.to_string(),
                        },
                    )
                    .await?;
            }
            Ok(simple_operation_success_response("SendItem"))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("SendItem", "ErrorInvalidOperation", &error.to_string())
        }))
    }

    pub(in crate::service) async fn mark_all_items_as_read(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            if !requested_public_folder_ids(request).is_empty() {
                bail!("MarkAllItemsAsRead currently supports canonical mailbox folders only.");
            }
            let folder_ids = self
                .requested_mailbox_folder_ids(principal, request)
                .await?;
            if folder_ids.is_empty() {
                bail!("MarkAllItemsAsRead requires a mailbox folder id.");
            }
            let read_flag = element_text(request, "ReadFlag")
                .map(|value| !value.eq_ignore_ascii_case("false"))
                .unwrap_or(true);
            for folder_id in folder_ids {
                let message_ids = self
                    .store
                    .query_jmap_email_ids(principal.account_id, Some(folder_id), None, 0, 10_000)
                    .await?
                    .ids;
                for message_id in message_ids {
                    self.store
                        .update_jmap_email_flags(
                            principal.account_id,
                            message_id,
                            Some(!read_flag),
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-mark-all-items-as-read".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                }
            }
            Ok(simple_operation_success_response("MarkAllItemsAsRead"))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "MarkAllItemsAsRead",
                "ErrorInvalidOperation",
                &error.to_string(),
            )
        }))
    }

    pub(in crate::service) async fn archive_item(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            self.validate_mutating_item_change_keys(principal, request)
                .await?;
            let ids = requested_item_ids(request);
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if ids.is_empty() || message_ids.len() != ids.len() {
                return Ok(operation_error_response(
                    "ArchiveItem",
                    "ErrorInvalidOperation",
                    "ArchiveItem currently supports only canonical message item ids.",
                ));
            }

            let mailboxes = self
                .store
                .ensure_jmap_system_mailboxes(principal.account_id)
                .await?;
            let Some(archive_mailbox_id) = mailboxes
                .iter()
                .find(|mailbox| mailbox.role == "archive")
                .map(|mailbox| mailbox.id)
            else {
                return Ok(operation_error_response(
                    "ArchiveItem",
                    "ErrorFolderNotFound",
                    "The canonical Archive mailbox was not found.",
                ));
            };

            let existing = self
                .store
                .fetch_jmap_emails(principal.account_id, &message_ids)
                .await?;
            if existing.len() != message_ids.len() {
                return Ok(operation_error_response(
                    "ArchiveItem",
                    "ErrorItemNotFound",
                    "message not found",
                ));
            }

            let mut items = String::new();
            for message_id in message_ids {
                let moved = self
                    .store
                    .move_jmap_email(
                        principal.account_id,
                        message_id,
                        archive_mailbox_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-archive-message".to_string(),
                            subject: format!("{message_id}->{archive_mailbox_id}"),
                        },
                    )
                    .await?;
                items.push_str(&message_item_xml(&moved));
            }

            Ok(archive_item_success_response(items))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "ArchiveItem",
                ews_error_code_or(&error, "ErrorItemNotFound"),
                &error.to_string(),
            )
        }))
    }

    pub(in crate::service) async fn copy_item(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            self.validate_mutating_item_change_keys(principal, request)
                .await?;
            let ids = requested_item_ids(request);
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let public_folder_item_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("public-folder-item:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty()
                || message_ids.len() + public_folder_item_ids.len() != ids.len()
                || (!message_ids.is_empty() && !public_folder_item_ids.is_empty())
            {
                return Ok(operation_error_response(
                    "CopyItem",
                    "ErrorInvalidOperation",
                    "CopyItem currently supports only canonical message ids or public folder item ids.",
                ));
            }
            if !public_folder_item_ids.is_empty() {
                let target_public_folder_ids = requested_public_folder_ids(request);
                if target_public_folder_ids.len() != 1 {
                    return Ok(operation_error_response(
                        "CopyItem",
                        "ErrorInvalidOperation",
                        "CopyItem requires exactly one canonical public-folder target for public folder items.",
                    ));
                }
                let target_public_folder_id = target_public_folder_ids[0];
                let existing_items = self
                    .store
                    .fetch_public_folder_items_by_ids(principal.account_id, &public_folder_item_ids)
                    .await?;
                if existing_items.len() != public_folder_item_ids.len() {
                    return Ok(operation_error_response(
                        "CopyItem",
                        "ErrorItemNotFound",
                        "public folder item not found",
                    ));
                }
                let mut items = String::new();
                for existing in existing_items {
                    let copied = self
                        .store
                        .upsert_public_folder_item(
                            public_folder_item_clone_input(
                                principal,
                                &existing,
                                target_public_folder_id,
                            ),
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-copy-public-folder-item".to_string(),
                                subject: format!("{}->{target_public_folder_id}", existing.id),
                            },
                        )
                        .await?;
                    items.push_str(&public_folder_item_xml(&copied));
                }
                return Ok(copy_item_success_response(items));
            }

            let target_mailbox_ids = self
                .requested_mailbox_folder_ids(principal, request)
                .await?;
            if target_mailbox_ids.len() != 1 {
                return Ok(operation_error_response(
                    "CopyItem",
                    "ErrorInvalidOperation",
                    "CopyItem requires exactly one canonical mailbox target folder.",
                ));
            }
            let target_mailbox_id = target_mailbox_ids[0];
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            if !mailboxes
                .iter()
                .any(|mailbox| mailbox.id == target_mailbox_id)
            {
                return Ok(operation_error_response(
                    "CopyItem",
                    "ErrorFolderNotFound",
                    "target mailbox folder not found",
                ));
            }

            let mut items = String::new();
            for message_id in message_ids {
                let copied = self
                    .store
                    .copy_jmap_email(
                        principal.account_id,
                        message_id,
                        target_mailbox_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-copy-message".to_string(),
                            subject: format!("{message_id}->{target_mailbox_id}"),
                        },
                    )
                    .await?;
                items.push_str(&message_item_xml(&copied));
            }

            Ok(copy_item_success_response(items))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "CopyItem",
                ews_error_code_or(&error, "ErrorItemNotFound"),
                &error.to_string(),
            )
        }))
    }

    pub(in crate::service) async fn move_item(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            self.validate_mutating_item_change_keys(principal, request)
                .await?;
            let ids = requested_item_ids(request);
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let public_folder_item_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("public-folder-item:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty()
                || message_ids.len() + public_folder_item_ids.len() != ids.len()
                || (!message_ids.is_empty() && !public_folder_item_ids.is_empty())
            {
                return Ok(operation_error_response(
                    "MoveItem",
                    "ErrorInvalidOperation",
                    "MoveItem currently supports only canonical message ids or public folder item ids.",
                ));
            }
            if !public_folder_item_ids.is_empty() {
                let target_public_folder_ids = requested_public_folder_ids(request);
                if target_public_folder_ids.len() != 1 {
                    return Ok(operation_error_response(
                        "MoveItem",
                        "ErrorInvalidOperation",
                        "MoveItem requires exactly one canonical public-folder target for public folder items.",
                    ));
                }
                let target_public_folder_id = target_public_folder_ids[0];
                let existing_items = self
                    .store
                    .fetch_public_folder_items_by_ids(principal.account_id, &public_folder_item_ids)
                    .await?;
                if existing_items.len() != public_folder_item_ids.len() {
                    return Ok(operation_error_response(
                        "MoveItem",
                        "ErrorItemNotFound",
                        "public folder item not found",
                    ));
                }
                let mut items = String::new();
                for existing in existing_items {
                    let moved = self
                        .store
                        .upsert_public_folder_item(
                            public_folder_item_clone_input(
                                principal,
                                &existing,
                                target_public_folder_id,
                            ),
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-move-public-folder-item-copy".to_string(),
                                subject: format!("{}->{target_public_folder_id}", existing.id),
                            },
                        )
                        .await?;
                    self.store
                        .delete_public_folder_item(
                            principal.account_id,
                            existing.public_folder_id,
                            existing.id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-move-public-folder-item-delete".to_string(),
                                subject: existing.id.to_string(),
                            },
                        )
                        .await?;
                    items.push_str(&public_folder_item_xml(&moved));
                }
                return Ok(move_item_success_response(items));
            }

            let target_mailbox_ids = self
                .requested_mailbox_folder_ids(principal, request)
                .await?;
            if target_mailbox_ids.len() != 1 {
                return Ok(operation_error_response(
                    "MoveItem",
                    "ErrorInvalidOperation",
                    "MoveItem requires exactly one canonical mailbox target folder.",
                ));
            }
            let target_mailbox_id = target_mailbox_ids[0];
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            if !mailboxes
                .iter()
                .any(|mailbox| mailbox.id == target_mailbox_id)
            {
                return Ok(operation_error_response(
                    "MoveItem",
                    "ErrorFolderNotFound",
                    "target mailbox folder not found",
                ));
            }

            let mut items = String::new();
            for message_id in message_ids {
                let moved = self
                    .store
                    .move_jmap_email(
                        principal.account_id,
                        message_id,
                        target_mailbox_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-move-message".to_string(),
                            subject: format!("{message_id}->{target_mailbox_id}"),
                        },
                    )
                    .await?;
                items.push_str(&message_item_xml(&moved));
            }

            Ok(move_item_success_response(items))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "MoveItem",
                ews_error_code_or(&error, "ErrorItemNotFound"),
                &error.to_string(),
            )
        }))
    }

    pub(in crate::service) async fn delete_item(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            self.validate_mutating_item_change_keys(principal, request)
                .await?;
            let ids = requested_item_ids(request);
            let contact_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("contact:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let event_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("event:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let task_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("task:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let public_folder_item_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("public-folder-item:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if ids.is_empty()
                || contact_ids.len()
                    + event_ids.len()
                    + task_ids.len()
                    + message_ids.len()
                    + public_folder_item_ids.len()
                    != ids.len()
            {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorInvalidOperation",
                    "DeleteItem currently supports only contact, calendar, task, message, and public folder item ids.",
                ));
            }
            for contact_id in contact_ids {
                self.store
                    .delete_accessible_contact(principal.account_id, contact_id)
                    .await?;
            }
            for event_id in event_ids {
                self.store
                    .delete_accessible_event(principal.account_id, event_id)
                    .await?;
            }
            for task_id in task_ids {
                self.store
                    .delete_accessible_task(principal.account_id, task_id)
                    .await?;
            }
            let delete_type = attribute_value_after(request, "DeleteItem", "DeleteType")
                .map(EwsDeleteType::parse)
                .transpose()?
                .unwrap_or(EwsDeleteType::MoveToDeletedItems);
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            let trash_mailbox_id = mailboxes
                .iter()
                .find(|mailbox| mailbox.role == "trash")
                .map(|mailbox| mailbox.id);

            for message_id in message_ids {
                let existing = self
                    .store
                    .fetch_jmap_emails(principal.account_id, &[message_id])
                    .await?;
                let Some(email) = existing.into_iter().next() else {
                    return Ok(operation_error_response(
                        "DeleteItem",
                        "ErrorItemNotFound",
                        "message not found",
                    ));
                };
                if delete_type == EwsDeleteType::HardDelete || email.mailbox_role == "trash" {
                    self.store
                        .delete_jmap_email(
                            principal.account_id,
                            message_id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-delete-message".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                } else if let Some(trash_mailbox_id) = trash_mailbox_id {
                    self.store
                        .move_jmap_email(
                            principal.account_id,
                            message_id,
                            trash_mailbox_id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-move-message-to-trash".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                } else {
                    self.store
                        .delete_jmap_email(
                            principal.account_id,
                            message_id,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-delete-message-without-trash".to_string(),
                                subject: message_id.to_string(),
                            },
                        )
                        .await?;
                }
            }
            let public_folder_items = self
                .store
                .fetch_public_folder_items_by_ids(principal.account_id, &public_folder_item_ids)
                .await?;
            if public_folder_items.len() != public_folder_item_ids.len() {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorItemNotFound",
                    "public folder item not found",
                ));
            }
            for item in public_folder_items {
                self.store
                    .delete_public_folder_item(
                        principal.account_id,
                        item.public_folder_id,
                        item.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-delete-public-folder-item".to_string(),
                            subject: item.id.to_string(),
                        },
                    )
                    .await?;
            }

            Ok(delete_item_success_response())
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "DeleteItem",
                ews_error_code_or(&error, "ErrorItemNotFound"),
                &error.to_string(),
            )
        }))
    }

    pub(in crate::service) async fn validate_mutating_item_change_keys(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<()> {
        for reference in requested_item_references(request)
            .into_iter()
            .filter(|reference| reference.change_key.is_some())
        {
            if let Some(id) = reference.id.strip_prefix("message:") {
                let Ok(id) = Uuid::parse_str(id) else {
                    continue;
                };
                if let Some(email) = self
                    .store
                    .fetch_jmap_emails(principal.account_id, &[id])
                    .await?
                    .into_iter()
                    .next()
                {
                    validate_supplied_item_change_key(
                        std::slice::from_ref(&reference),
                        &reference.id,
                        &message_change_key(&email),
                    )?;
                }
            } else if let Some(id) = reference.id.strip_prefix("contact:") {
                let Ok(id) = Uuid::parse_str(id) else {
                    continue;
                };
                if let Some(contact) = self
                    .store
                    .fetch_accessible_contacts_by_ids(principal.account_id, &[id])
                    .await?
                    .into_iter()
                    .next()
                {
                    let keys = contact_change_keys(
                        &self.store,
                        principal.account_id,
                        std::slice::from_ref(&contact),
                    )
                    .await?;
                    validate_supplied_item_change_key(
                        std::slice::from_ref(&reference),
                        &reference.id,
                        change_key_for(&keys, id, "contact")?,
                    )?;
                }
            } else if let Some(id) = reference.id.strip_prefix("event:") {
                let Ok(id) = Uuid::parse_str(id) else {
                    continue;
                };
                if let Some(event) = self
                    .store
                    .fetch_accessible_events_by_ids(principal.account_id, &[id])
                    .await?
                    .into_iter()
                    .next()
                {
                    let keys = event_change_keys(
                        &self.store,
                        principal.account_id,
                        std::slice::from_ref(&event),
                    )
                    .await?;
                    validate_supplied_item_change_key(
                        std::slice::from_ref(&reference),
                        &reference.id,
                        change_key_for(&keys, id, "calendar")?,
                    )?;
                }
            } else if let Some(id) = reference.id.strip_prefix("task:") {
                let Ok(id) = Uuid::parse_str(id) else {
                    continue;
                };
                if let Some(task) = self
                    .store
                    .fetch_accessible_tasks_by_ids(principal.account_id, &[id])
                    .await?
                    .into_iter()
                    .next()
                {
                    let keys = task_change_keys(
                        &self.store,
                        principal.account_id,
                        std::slice::from_ref(&task),
                    )
                    .await?;
                    validate_supplied_item_change_key(
                        std::slice::from_ref(&reference),
                        &reference.id,
                        change_key_for(&keys, id, "task")?,
                    )?;
                }
            } else if let Some(id) = reference.id.strip_prefix("public-folder-item:") {
                let Ok(id) = Uuid::parse_str(id) else {
                    continue;
                };
                if let Some(item) = self
                    .store
                    .fetch_public_folder_items_by_ids(principal.account_id, &[id])
                    .await?
                    .into_iter()
                    .next()
                {
                    validate_supplied_item_change_key(
                        std::slice::from_ref(&reference),
                        &reference.id,
                        &public_folder_item_change_key(&item),
                    )?;
                }
            }
        }
        Ok(())
    }
}

fn validate_supplied_item_change_key(
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

#[cfg(test)]
mod tests {
    use super::validate_supplied_item_change_key;
    use crate::service::ews::request_ids::RequestedItemReference;

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
}
