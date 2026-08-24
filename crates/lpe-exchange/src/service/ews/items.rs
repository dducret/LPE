use super::super::*;

const EWS_COPY_MOVE_ITEM_LIMIT: usize = 100;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    // [MS-OXWSMSG] section 3.1.4.7: validate every bounded target and payload
    // before the first canonical item mutation.
    pub(in crate::service) async fn update_item(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            self.validate_mutating_item_change_keys(principal, request)
                .await?;
            let item_changes = requested_update_item_changes(request)?;
            if item_changes.len() != 1 {
                return Ok(operation_error_response(
                    "UpdateItem",
                    "ErrorInvalidOperation",
                    "UpdateItem supports exactly one ItemChange until canonical atomic batching exists.",
                ));
            }
            let item_references = item_changes
                .iter()
                .map(|change| change.reference.clone())
                .collect::<Vec<_>>();
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

            if (!contact_ids.is_empty() || !event_ids.is_empty()) && ids.len() > 1 {
                return Ok(operation_error_response(
                    "UpdateItem",
                    "ErrorInvalidOperation",
                    "UpdateItem does not support atomic batches containing contact or calendar changes.",
                ));
            }

            let mut contact_update = None;
            if let Some(contact_id) = contact_ids.first().copied() {
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
                let id = format!("contact:{contact_id}");
                validate_required_item_change_key(
                    &item_references,
                    &id,
                    change_key_for(&change_keys, contact_id, "contact")?,
                )?;
                contact_update = Some((
                    contact_id,
                    existing,
                    update_item_change_content(&item_changes, &id)?,
                ));
            }

            let mut event_update = None;
            if let Some(event_id) = event_ids.first().copied() {
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
                let id = format!("event:{event_id}");
                validate_required_item_change_key(
                    &item_references,
                    &id,
                    change_key_for(&change_keys, event_id, "calendar")?,
                )?;
                let update_request = update_item_change_content(&item_changes, &id)?;
                let input = parse_update_event_input(principal, &existing, update_request)?;
                event_update = Some((event_id, existing, input));
            }

            let contact_update = contact_update
                .map(|(contact_id, existing, update_request)| {
                    let input = parse_update_contact_input(principal, &existing, update_request);
                    validate_contact_photo(&self.validator, &input)?;
                    Ok::<_, anyhow::Error>((contact_id, input))
                })
                .transpose()?;
            let message_update = if !message_ids.is_empty() {
                let (unread, flagged) = parse_update_item_message_flags(
                    update_item_change_content(&item_changes, &format!("message:{}", message_ids[0]))?,
                )?;
                let messages = self
                    .store
                    .fetch_jmap_emails(principal.account_id, &message_ids)
                    .await?;
                if messages.len() != message_ids.len() {
                    bail!("message not found");
                }
                for existing in &messages {
                    validate_supplied_item_change_key(
                        &item_references,
                        &format!("message:{}", existing.id),
                        &message_change_key(&existing),
                    )?;
                }
                Some((unread, flagged))
            } else {
                None
            };
            let mut task_updates = Vec::new();
            for task_id in &task_ids {
                let existing = self
                    .store
                    .fetch_accessible_tasks_by_ids(principal.account_id, &[*task_id])
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
                let id = format!("task:{task_id}");
                validate_supplied_item_change_key(
                    &item_references,
                    &id,
                    change_key_for(&change_keys, *task_id, "task")?,
                )?;
                task_updates.push((
                    *task_id,
                    parse_update_task_input(
                        principal,
                        &existing,
                        update_item_change_content(&item_changes, &id)?,
                    )?,
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
            let mut public_folder_updates = Vec::new();
            for existing in public_folder_items {
                let id = format!("public-folder-item:{}", existing.id);
                validate_supplied_item_change_key(
                    &item_references,
                    &id,
                    &public_folder_item_change_key(&existing),
                )?;
                public_folder_updates.push((
                    existing.id,
                    parse_update_public_folder_item_input(
                        principal,
                        &existing,
                        update_item_change_content(&item_changes, &id)?,
                    ),
                ));
            }

            let mut items = String::new();
            if let Some((unread, flagged)) = message_update {
                for message_id in message_ids {
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
            if let Some((contact_id, input)) = contact_update {
                let updated = self
                    .store
                    .update_accessible_contact(
                        principal.account_id,
                        contact_id,
                        input,
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
            if let Some((event_id, _existing, input)) = event_update {
                let updated = self
                    .store
                    .update_accessible_event(
                        principal.account_id,
                        event_id,
                        input,
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
            for (task_id, input) in task_updates {
                let updated = self
                    .store
                    .update_accessible_task(
                        principal.account_id,
                        task_id,
                        input,
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
            for (item_id, input) in public_folder_updates {
                let updated = self
                    .store
                    .upsert_public_folder_item(
                        input,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-update-public-folder-item".to_string(),
                            subject: item_id.to_string(),
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
            if !element_contents(request, "AcceptSharingInvitation").is_empty() {
                super::sharing::validate_accept_sharing_invitation_shape(request)?;
                return self.accept_sharing_invitation(principal, request).await;
            }
            validate_create_item_shape(request)?;
            let saved_item_folder = requested_create_item_saved_folder_target(request)?;
            if element_content(request, "Contact").is_some() {
                let collection_id = create_item_collection_id(saved_item_folder.as_ref())?;
                let input = parse_create_contact_input(principal, request)?;
                validate_contact_photo(&self.validator, &input)?;
                let contact = self
                    .store
                    .create_accessible_contact(principal.account_id, collection_id, input)
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
                let collection_id = create_item_collection_id(saved_item_folder.as_ref())?;
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
                create_item_collection_id(saved_item_folder.as_ref())?;
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
                    if let Some(CreateItemSavedFolderTarget::PublicFolder(public_folder_id)) =
                        saved_item_folder.as_ref()
                    {
                        let item = self
                            .store
                            .upsert_public_folder_item(
                                UpsertPublicFolderItemInput {
                                    id: None,
                                    account_id: principal.account_id,
                                    public_folder_id: *public_folder_id,
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
                    let mailbox_id = match saved_item_folder.as_ref() {
                        Some(CreateItemSavedFolderTarget::Mailbox(id)) => Some(*id),
                        Some(CreateItemSavedFolderTarget::BareUuid(id)) => {
                            Some(Uuid::parse_str(id)?)
                        }
                        Some(CreateItemSavedFolderTarget::MailboxRole(role)) => self
                            .store
                            .fetch_jmap_mailboxes(principal.account_id)
                            .await?
                            .into_iter()
                            .find(|mailbox| mailbox.role == *role)
                            .map(|mailbox| mailbox.id),
                        Some(CreateItemSavedFolderTarget::Collection(_)) => {
                            bail!("CreateItem Message requires a canonical mailbox target")
                        }
                        Some(CreateItemSavedFolderTarget::PublicFolder(_)) | None => None,
                    };
                    if let Some(mailbox_id) = mailbox_id {
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
                    if saved_item_folder.is_some() {
                        if disposition == "SendOnly" {
                            bail!(
                                "CreateItem SendOnly does not support SavedItemFolderId because it does not save a copy"
                            );
                        }
                        self.validate_send_and_save_copy_target(principal, saved_item_folder.as_ref())
                            .await?;
                    }
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
                        .submitted_message_in_canonical_sent(
                            principal.account_id,
                            submitted.message_id,
                            submitted.sent_mailbox_id,
                        )
                        .await?;
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

    // [MS-OXWSMSG] section 3.1.4.6: SendItem accepts Message item identifiers;
    // canonical submission remains the authority for Sent membership.
    pub(in crate::service) async fn send_item(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let item_references = requested_operation_item_references(request, "SendItem")?;
            if item_references.len() != 1 {
                bail!(
                    "SendItem supports exactly one ItemId until canonical atomic submission exists"
                );
            }
            self.validate_mutating_item_change_keys(principal, request)
                .await?;
            let draft_ids = item_references
                .iter()
                .map(|reference| {
                    reference
                        .id
                        .strip_prefix("message:")
                        .ok_or_else(|| anyhow!("SendItem requires canonical message ItemIds."))
                        .and_then(|id| {
                            Uuid::parse_str(id).map_err(|_| {
                                anyhow!("SendItem received an invalid message ItemId.")
                            })
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            if draft_ids.is_empty() {
                bail!("SendItem requires at least one message ItemId.");
            }
            let drafts = self
                .store
                .fetch_jmap_emails(principal.account_id, &draft_ids)
                .await?;
            if drafts.len() != draft_ids.len()
                || drafts.iter().any(|draft| {
                    !draft
                        .mailbox_states
                        .iter()
                        .any(|state| state.role == "drafts")
                        && draft.mailbox_role != "drafts"
                })
            {
                bail!("SendItem requires accessible canonical drafts.");
            }
            for draft_id in draft_ids {
                let submitted = self
                    .store
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
                self.submitted_message_in_canonical_sent(
                    principal.account_id,
                    submitted.message_id,
                    submitted.sent_mailbox_id,
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

    pub(in crate::service) async fn archive_item(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let item_references = requested_operation_item_references(request, "ArchiveItem")?;
            let source_mailbox_id = requested_archive_source_mailbox_id(request)?;
            self.validate_mutating_item_change_keys(principal, request)
                .await?;
            let message_ids = item_references
                .iter()
                .map(|reference| reference.id.as_str())
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if item_references.is_empty() || message_ids.len() != item_references.len() {
                return Ok(operation_error_response(
                    "ArchiveItem",
                    "ErrorInvalidOperation",
                    "ArchiveItem currently supports only canonical message item ids.",
                ));
            }
            if message_ids.len() > 100 {
                return Ok(operation_error_response(
                    "ArchiveItem",
                    "ErrorInvalidOperation",
                    "ArchiveItem supports at most 100 canonical message item ids.",
                ));
            }

            let mailboxes = self.store.fetch_jmap_mailboxes(principal.account_id).await?;
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
            if !mailboxes
                .iter()
                .any(|mailbox| mailbox.id == source_mailbox_id)
            {
                return Ok(operation_error_response(
                    "ArchiveItem",
                    "ErrorFolderNotFound",
                    "The canonical Archive source mailbox was not found.",
                ));
            }
            if source_mailbox_id == archive_mailbox_id {
                return Ok(operation_error_response(
                    "ArchiveItem",
                    "ErrorInvalidOperation",
                    "ArchiveItem source messages must not belong to the canonical Archive mailbox.",
                ));
            }

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
            if existing
                .iter()
                .any(|message| !message.mailbox_ids.contains(&source_mailbox_id))
            {
                return Ok(operation_error_response(
                    "ArchiveItem",
                    "ErrorItemNotFound",
                    "message not found in the specified Archive source mailbox",
                ));
            }
            if existing.iter().any(|message| {
                message
                    .mailbox_ids
                    .iter()
                    .any(|mailbox_id| *mailbox_id == archive_mailbox_id)
            }) {
                return Ok(operation_error_response(
                    "ArchiveItem",
                    "ErrorInvalidOperation",
                    "ArchiveItem source messages must not already belong to the canonical Archive mailbox.",
                ));
            }

            self.store
                .move_jmap_emails(
                    principal.account_id,
                    &message_ids,
                    archive_mailbox_id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-archive-message".to_string(),
                        subject: format!("{} messages -> {archive_mailbox_id}", message_ids.len()),
                    },
                )
                .await?;

            let mut items = String::new();
            for message in self
                .store
                .fetch_jmap_emails(principal.account_id, &message_ids)
                .await?
            {
                items.push_str(&message_item_xml(&message));
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
            let ids = requested_operation_item_references(request, "CopyItem")?
                .into_iter()
                .map(|reference| reference.id)
                .collect::<Vec<_>>();
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
                || ids.len() > EWS_COPY_MOVE_ITEM_LIMIT
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
                let target = self
                    .store
                    .fetch_public_folder(principal.account_id, target_public_folder_id)
                    .await?;
                if !target.rights.may_write {
                    bail!("public folder write access is not granted");
                }
                // The store transaction preflights every source before writing
                // any clone, so a later invalid source cannot partially copy.
                let mut items = String::new();
                for copied in self
                    .store
                    .copy_ews_public_folder_items(
                        principal.account_id,
                        &public_folder_item_ids,
                        target_public_folder_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-copy-public-folder-item".to_string(),
                            subject: format!("{}->{target_public_folder_id}", public_folder_item_ids.len()),
                        },
                    )
                    .await?
                {
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
            let Some(target_mailbox) = mailboxes
                .iter()
                .find(|mailbox| mailbox.id == target_mailbox_id)
            else {
                return Ok(operation_error_response(
                    "CopyItem",
                    "ErrorFolderNotFound",
                    "target mailbox folder not found",
                ));
            };

            let source_messages = self
                .store
                .fetch_jmap_emails(principal.account_id, &message_ids)
                .await?;
            if source_messages.len() != message_ids.len() {
                bail!("message not found");
            }
            if source_messages
                .iter()
                .any(|message| message.mailbox_ids.contains(&target_mailbox_id))
            {
                bail!("message already exists in target mailbox");
            }

            let response_items =
                super::item_batch_responses::message_responses_in_target(source_messages, target_mailbox);
            self.store
                .copy_jmap_emails(
                    principal.account_id,
                    &message_ids,
                    target_mailbox_id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-copy-message".to_string(),
                        subject: format!("{}->{target_mailbox_id}", message_ids.len()),
                    },
                )
                .await?;
            let mut items = String::new();
            for copied in response_items {
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
                || ids.len() > EWS_COPY_MOVE_ITEM_LIMIT
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
                let target = self
                    .store
                    .fetch_public_folder(principal.account_id, target_public_folder_id)
                    .await?;
                if !target.rights.may_write {
                    bail!("public folder write access is not granted");
                }
                // The store transaction preflights every source before writing
                // a clone or deleting its source.
                let mut items = String::new();
                for moved in self
                    .store
                    .move_ews_public_folder_items(
                        principal.account_id,
                        &public_folder_item_ids,
                        target_public_folder_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-move-public-folder-item".to_string(),
                            subject: format!("{}->{target_public_folder_id}", public_folder_item_ids.len()),
                        },
                    )
                    .await?
                {
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
            let Some(target_mailbox) = mailboxes
                .iter()
                .find(|mailbox| mailbox.id == target_mailbox_id)
            else {
                return Ok(operation_error_response(
                    "MoveItem",
                    "ErrorFolderNotFound",
                    "target mailbox folder not found",
                ));
            };

            let source_messages = self
                .store
                .fetch_jmap_emails(principal.account_id, &message_ids)
                .await?;
            if source_messages.len() != message_ids.len() {
                bail!("message not found");
            }
            if source_messages
                .iter()
                .any(|message| message.mailbox_ids.contains(&target_mailbox_id))
            {
                bail!("message already exists in target mailbox");
            }

            let response_items =
                super::item_batch_responses::message_responses_in_target(source_messages, target_mailbox);
            self.store
                .move_jmap_emails(
                    principal.account_id,
                    &message_ids,
                    target_mailbox_id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-move-message".to_string(),
                        subject: format!("{}->{target_mailbox_id}", message_ids.len()),
                    },
                )
                .await?;
            let mut items = String::new();
            for moved in response_items {
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

    // [MS-OXWSMSG] section 3.1.4.3: reject an invalid bounded delete request
    // before it can partially mutate canonical item lifecycles.
    pub(in crate::service) async fn delete_item(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let item_references = match requested_operation_item_references(request, "DeleteItem") {
                Ok(item_references) => item_references,
                Err(error) => {
                    return Ok(operation_error_response(
                        "DeleteItem",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    ));
                }
            };
            if item_references.len() != 1 {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorInvalidOperation",
                    "DeleteItem supports exactly one ItemId until canonical atomic batching exists.",
                ));
            }
            self.validate_mutating_item_change_keys(principal, request)
                .await?;
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
                    "DeleteItem",
                    "ErrorInvalidOperation",
                    "DeleteItem currently supports only contact, calendar, task, message, and public folder item ids.",
                ));
            }
            let delete_type = attribute_value_after(request, "DeleteItem", "DeleteType")
                .map(EwsDeleteType::parse)
                .transpose()?
                .unwrap_or(EwsDeleteType::MoveToDeletedItems);

            if delete_type == EwsDeleteType::SoftDelete {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorInvalidOperation",
                    "DeleteItem does not support DeleteType=SoftDelete until EWS projects canonical recoverable items.",
                ));
            }

            if (!contact_ids.is_empty() || !event_ids.is_empty()) && ids.len() > 1 {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorInvalidOperation",
                    "DeleteItem does not support atomic batches containing contact or calendar items.",
                ));
            }
            if !contact_ids.is_empty() && delete_type != EwsDeleteType::HardDelete {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorInvalidOperation",
                    "DeleteItem supports contacts only with DeleteType=HardDelete.",
                ));
            }
            if !event_ids.is_empty() && delete_type == EwsDeleteType::SoftDelete {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorInvalidOperation",
                    "DeleteItem does not support DeleteType=SoftDelete for calendar items.",
                ));
            }

            for contact_id in &contact_ids {
                let contact = self
                    .store
                    .fetch_accessible_contacts_by_ids(principal.account_id, &[*contact_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("contact not found"))?;
                let change_keys = contact_change_keys(
                    &self.store,
                    principal.account_id,
                    std::slice::from_ref(&contact),
                )
                .await?;
                validate_required_item_change_key(
                    &item_references,
                    &format!("contact:{contact_id}"),
                    change_key_for(&change_keys, *contact_id, "contact")?,
                )?;
            }
            for event_id in &event_ids {
                let event = self
                    .store
                    .fetch_accessible_events_by_ids(principal.account_id, &[*event_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("event not found"))?;
                let change_keys = event_change_keys(
                    &self.store,
                    principal.account_id,
                    std::slice::from_ref(&event),
                )
                .await?;
                validate_required_item_change_key(
                    &item_references,
                    &format!("event:{event_id}"),
                    change_key_for(&change_keys, *event_id, "calendar")?,
                )?;
            }
            let tasks = self
                .store
                .fetch_accessible_tasks_by_ids(principal.account_id, &task_ids)
                .await?;
            if tasks.len() != task_ids.len() {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorItemNotFound",
                    "task not found",
                ));
            }
            for task in &tasks {
                let change_keys = task_change_keys(
                    &self.store,
                    principal.account_id,
                    std::slice::from_ref(task),
                )
                .await?;
                validate_supplied_item_change_key(
                    &item_references,
                    &format!("task:{}", task.id),
                    change_key_for(&change_keys, task.id, "task")?,
                )?;
            }
            let messages = self
                .store
                .fetch_jmap_emails(principal.account_id, &message_ids)
                .await?;
            if messages.len() != message_ids.len() {
                return Ok(operation_error_response(
                    "DeleteItem",
                    "ErrorItemNotFound",
                    "message not found",
                ));
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
            for item in &public_folder_items {
                validate_supplied_item_change_key(
                    &item_references,
                    &format!("public-folder-item:{}", item.id),
                    &public_folder_item_change_key(item),
                )?;
            }
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            let trash_mailbox_id = mailboxes
                .iter()
                .find(|mailbox| mailbox.role == "trash")
                .map(|mailbox| mailbox.id);

            for contact_id in contact_ids {
                self.store
                    .delete_accessible_contact(principal.account_id, contact_id)
                    .await?;
            }
            for event_id in event_ids {
                if delete_type == EwsDeleteType::MoveToDeletedItems {
                    self.store
                        .move_accessible_event_to_deleted_items(
                            principal.account_id,
                            event_id,
                            None,
                        )
                        .await?;
                } else {
                    self.store
                        .delete_accessible_event(principal.account_id, event_id)
                        .await?;
                }
            }
            for task_id in task_ids {
                self.store
                    .delete_accessible_task(principal.account_id, task_id)
                    .await?;
            }

            for message_id in message_ids {
                let email = messages
                    .iter()
                    .find(|email| email.id == message_id)
                    .ok_or_else(|| anyhow!("message not found"))?;
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

    async fn submitted_message_in_canonical_sent(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        sent_mailbox_id: Uuid,
    ) -> Result<JmapEmail> {
        let email = self
            .store
            .fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("submitted message was not found in canonical Sent"))?;
        if email.mailbox_role != "sent"
            || !email
                .mailbox_states
                .iter()
                .any(|state| state.mailbox_id == sent_mailbox_id && state.role == "sent")
        {
            bail!("submitted message was not visible in canonical Sent");
        }
        Ok(email)
    }

    async fn validate_send_and_save_copy_target(
        &self,
        principal: &AccountPrincipal,
        target: Option<&CreateItemSavedFolderTarget>,
    ) -> Result<()> {
        let Some(target) = target else {
            return Ok(());
        };
        let mailbox_id = match target {
            CreateItemSavedFolderTarget::Mailbox(id) => *id,
            CreateItemSavedFolderTarget::MailboxRole("sent") => return Ok(()),
            CreateItemSavedFolderTarget::BareUuid(id) => Uuid::parse_str(id)?,
            _ => bail!(
                "CreateItem SendAndSaveCopy supports only the canonical Sent mailbox as SavedItemFolderId"
            ),
        };
        let is_canonical_sent = self
            .store
            .fetch_jmap_mailboxes(principal.account_id)
            .await?
            .into_iter()
            .any(|mailbox| mailbox.id == mailbox_id && mailbox.role == "sent");
        if !is_canonical_sent {
            bail!(
                "CreateItem SendAndSaveCopy SavedItemFolderId is not the visible canonical Sent mailbox"
            );
        }
        Ok(())
    }
}

/// [MS-OXWSARCH] §3.1.4.1.3.1 requires one ArchiveSourceFolderId before the
/// ItemIds collection. LPE accepts only one visible canonical mailbox source.
fn requested_archive_source_mailbox_id(request: &str) -> Result<Uuid> {
    let operations = element_contents(request, "ArchiveItem");
    let [operation] = operations.as_slice() else {
        bail!("ArchiveItem requires exactly one operation element");
    };
    let sources = direct_child_contents(operation, "ArchiveSourceFolderId");
    if sources.len() != 1 || element_contents(operation, "ArchiveSourceFolderId").len() != 1 {
        bail!("ArchiveItem requires exactly one direct ArchiveSourceFolderId");
    }
    let source = sources[0];
    let folders = direct_child_contents(source, "FolderId");
    let distinguished = direct_child_contents(source, "DistinguishedFolderId");
    let ids = attribute_values_for_tag(source, "FolderId", "Id");
    if folders.len() != 1
        || distinguished.len() != 0
        || ids.len() != 1
        || attribute_values_for_tag(source, "DistinguishedFolderId", "Id").len() != 0
    {
        bail!("ArchiveItem ArchiveSourceFolderId requires one canonical mailbox FolderId");
    }
    let id = ids[0]
        .strip_prefix("mailbox:")
        .ok_or_else(|| anyhow!("ArchiveItem source folder must be a canonical mailbox id"))?;
    Uuid::parse_str(id).map_err(|_| anyhow!("ArchiveItem source mailbox id is invalid"))
}

// [MS-OXWSMSG] section 3.1.4.2: this bounded adapter accepts exactly one
// supported canonical item and one unambiguous saved-item parent before it
// starts the corresponding canonical creation transaction.
fn validate_create_item_shape(request: &str) -> Result<()> {
    let item_collections = element_contents(request, "Items");
    let [items] = item_collections.as_slice() else {
        bail!("CreateItem requires exactly one Items collection");
    };
    let item_count = ["Message", "Contact", "CalendarItem", "Task"]
        .into_iter()
        .map(|name| element_contents(items, name).len())
        .sum::<usize>();
    if item_count != 1 {
        bail!("CreateItem requires exactly one supported canonical item");
    }
    if element_content(items, "Attachments").is_some() {
        bail!(
            "CreateItem does not support embedded attachments; use CreateAttachment for one canonical attachment"
        );
    }

    requested_create_item_saved_folder_target(request).map(|_| ())
}
