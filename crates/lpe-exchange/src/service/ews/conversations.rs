use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn find_conversation(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let emails = self
                .conversation_source_emails(principal, request, true)
                .await?;
            Ok(find_conversation_response(&emails, request))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "FindConversation",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )
        }))
    }

    pub(in crate::service) async fn get_conversation_items(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let conversation_ids = requested_conversation_ids(request)?;
            if conversation_ids.is_empty() {
                bail!("GetConversationItems requires at least one ConversationId.");
            }
            let mut emails = self
                .conversation_source_emails(principal, request, false)
                .await?;
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            filter_ignored_conversation_folders(&mut emails, request, &mailboxes);
            Ok(get_conversation_items_response(
                &emails,
                &conversation_ids,
                request,
            ))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "GetConversationItems",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )
        }))
    }

    pub(in crate::service) async fn apply_conversation_action(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let actions = parse_conversation_actions(request);
            if actions.is_empty() {
                bail!("ApplyConversationAction requires at least one ConversationAction.");
            }
            let all_ids = self
                .store
                .fetch_all_jmap_email_ids(principal.account_id)
                .await?;
            let emails = self
                .store
                .fetch_jmap_emails(principal.account_id, &all_ids)
                .await?;
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;

            for action in actions {
                let conversation_id = action
                    .conversation_id
                    .ok_or_else(|| anyhow!("ConversationAction is missing ConversationId."))?;
                let message_ids = emails
                    .iter()
                    .filter(|email| email.thread_id == conversation_id)
                    .map(|email| email.id)
                    .collect::<Vec<_>>();
                if message_ids.is_empty() {
                    return Ok(operation_error_response(
                        "ApplyConversationAction",
                        "ErrorItemNotFound",
                        "conversation not found",
                    ));
                }
                match action.action.as_str() {
                    "Move" => {
                        let target_mailbox_id = action.target_mailbox_id.ok_or_else(|| {
                            anyhow!("Move conversation action requires DestinationFolderId.")
                        })?;
                        if !mailboxes.iter().any(|mailbox| mailbox.id == target_mailbox_id) {
                            return Ok(operation_error_response(
                                "ApplyConversationAction",
                                "ErrorFolderNotFound",
                                "destination folder not found",
                            ));
                        }
                        for message_id in message_ids {
                            self.store
                                .move_jmap_email(
                                    principal.account_id,
                                    message_id,
                                    target_mailbox_id,
                                    AuditEntryInput {
                                        actor: principal.email.clone(),
                                        action: "ews-conversation-move".to_string(),
                                        subject: format!(
                                            "{conversation_id}:{message_id}->{target_mailbox_id}"
                                        ),
                                    },
                                )
                                .await?;
                        }
                    }
                    "Delete" => {
                        for message_id in message_ids {
                            self.store
                                .delete_jmap_email(
                                    principal.account_id,
                                    message_id,
                                    AuditEntryInput {
                                        actor: principal.email.clone(),
                                        action: "ews-conversation-delete".to_string(),
                                        subject: format!("{conversation_id}:{message_id}"),
                                    },
                                )
                                .await?;
                        }
                    }
                    "SetReadState" => {
                        let unread = !action.read.unwrap_or(true);
                        for message_id in message_ids {
                            self.store
                                .update_jmap_email_flags(
                                    principal.account_id,
                                    message_id,
                                    Some(unread),
                                    None,
                                    AuditEntryInput {
                                        actor: principal.email.clone(),
                                        action: "ews-conversation-read-state".to_string(),
                                        subject: format!("{conversation_id}:{message_id}"),
                                    },
                                )
                                .await?;
                        }
                    }
                    value if value.starts_with("Always") => {
                        return Ok(operation_error_response(
                            "ApplyConversationAction",
                            "ErrorInvalidOperation",
                            "Persistent future-message conversation actions are not supported without first-class canonical thread lifecycle state.",
                        ));
                    }
                    other => {
                        return Ok(operation_error_response(
                            "ApplyConversationAction",
                            "ErrorInvalidOperation",
                            &format!("unsupported conversation action {other}"),
                        ));
                    }
                }
            }

            Ok(simple_operation_success_response("ApplyConversationAction"))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "ApplyConversationAction",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )
        }))
    }

    async fn conversation_source_emails(
        &self,
        principal: &AccountPrincipal,
        request: &str,
        require_parent_folder: bool,
    ) -> Result<Vec<JmapEmail>> {
        let folder_ids = if require_parent_folder {
            vec![
                self.requested_conversation_parent_mailbox_id(principal, request)
                    .await?,
            ]
        } else {
            Vec::new()
        };
        let ids = if folder_ids.is_empty() {
            self.store
                .fetch_all_jmap_email_ids(principal.account_id)
                .await?
        } else {
            let mut ids = Vec::new();
            for folder_id in &folder_ids {
                ids.extend(
                    self.store
                        .query_jmap_email_ids(
                            principal.account_id,
                            Some(*folder_id),
                            None,
                            0,
                            MAILBOX_QUERY_LIMIT,
                        )
                        .await?
                        .ids,
                );
            }
            ids.sort();
            ids.dedup();
            ids
        };
        let mut emails = self
            .store
            .fetch_jmap_emails(principal.account_id, &ids)
            .await?;
        if !folder_ids.is_empty() {
            let folder_set = folder_ids.into_iter().collect::<HashSet<_>>();
            emails.retain(|email| email.mailbox_ids.iter().any(|id| folder_set.contains(id)));
        }
        Ok(emails)
    }

    async fn requested_conversation_parent_mailbox_id(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<Uuid> {
        let parents = element_contents(request, "ParentFolderId");
        if parents.len() != 1 {
            bail!("FindConversation requires exactly one mailbox ParentFolderId");
        }
        let parent = parents[0];
        let folder_ids = attribute_values_for_tag(parent, "FolderId", "Id");
        let distinguished_ids = attribute_values_for_tag(parent, "DistinguishedFolderId", "Id");
        if folder_ids.len() + distinguished_ids.len() != 1 {
            bail!("FindConversation requires exactly one mailbox ParentFolderId");
        }
        let mailboxes = self
            .store
            .fetch_jmap_mailboxes(principal.account_id)
            .await?;
        let mailbox_id = if let Some(folder_id) = folder_ids.first() {
            let id = folder_id
                .strip_prefix("mailbox:")
                .ok_or_else(|| anyhow!("FindConversation parent folder is not a mailbox"))?;
            Uuid::parse_str(id)
                .map_err(|_| anyhow!("FindConversation parent mailbox id is invalid"))?
        } else {
            let role = ews_distinguished_mailbox_role(distinguished_ids[0])
                .ok_or_else(|| anyhow!("FindConversation parent folder is not supported"))?;
            mailboxes
                .iter()
                .find(|mailbox| mailbox.role == role)
                .map(|mailbox| mailbox.id)
                .ok_or_else(|| anyhow!("FindConversation parent mailbox is not visible"))?
        };
        if mailboxes.iter().any(|mailbox| mailbox.id == mailbox_id) {
            Ok(mailbox_id)
        } else {
            bail!("FindConversation parent mailbox is not visible")
        }
    }
}

#[derive(Debug)]
pub(in crate::service) struct ConversationActionRequest {
    pub(in crate::service) action: String,
    pub(in crate::service) conversation_id: Option<Uuid>,
    pub(in crate::service) target_mailbox_id: Option<Uuid>,
    pub(in crate::service) read: Option<bool>,
}

pub(in crate::service) fn find_conversation_response(
    emails: &[JmapEmail],
    request: &str,
) -> String {
    let mut thread_ids = emails
        .iter()
        .map(|email| email.thread_id)
        .collect::<Vec<_>>();
    thread_ids.sort();
    thread_ids.dedup();
    thread_ids.sort_by(|left, right| {
        conversation_last_delivery(emails, right).cmp(&conversation_last_delivery(emails, left))
    });

    let offset = ews_usize_attribute(request, "IndexedPageItemView", "Offset").unwrap_or(0);
    let max = ews_usize_attribute(request, "IndexedPageItemView", "MaxEntriesReturned")
        .unwrap_or(MAILBOX_QUERY_LIMIT as usize);
    let total = thread_ids.len();
    let conversations = thread_ids
        .into_iter()
        .skip(offset)
        .take(max)
        .filter_map(|thread_id| {
            let messages = emails
                .iter()
                .filter(|email| email.thread_id == thread_id)
                .collect::<Vec<_>>();
            (!messages.is_empty()).then(|| conversation_summary_xml(thread_id, &messages))
        })
        .collect::<String>();
    let returned = count_tag_occurrences(&conversations, "<t:ConversationId");
    let includes_last = offset.saturating_add(returned) >= total;

    format!(
        concat!(
            "<m:FindConversationResponse ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Conversations>{conversations}</m:Conversations>",
            "<m:TotalConversationsInView>{total}</m:TotalConversationsInView>",
            "<m:IndexedOffset>{offset}</m:IndexedOffset>",
            "<m:IncludesLastItemInRange>{includes_last}</m:IncludesLastItemInRange>",
            "</m:FindConversationResponse>"
        ),
        conversations = conversations,
        total = total,
        offset = offset,
        includes_last = includes_last,
    )
}

pub(in crate::service) fn get_conversation_items_response(
    emails: &[JmapEmail],
    conversation_ids: &[Uuid],
    request: &str,
) -> String {
    let response_messages = conversation_ids
        .iter()
        .map(|thread_id| {
            let mut messages = emails
                .iter()
                .filter(|email| email.thread_id == *thread_id)
                .collect::<Vec<_>>();
            let descending = element_text(request, "SortOrder")
                .map(|value| value.contains("Descending"))
                .unwrap_or(false);
            messages.sort_by(|left, right| {
                let ordering = left.received_at.cmp(&right.received_at);
                if descending {
                    ordering.reverse()
                } else {
                    ordering
                }
            });
            if messages.is_empty() {
                return operation_response_message(
                    "GetConversationItems",
                    "ErrorItemNotFound",
                    "conversation not found",
                );
            }
            let nodes = messages
                .iter()
                .map(|email| conversation_node_xml(email))
                .collect::<String>();
            let sync_state = format!(
                "conversation:{thread_id}:{}",
                messages
                    .iter()
                    .map(|email| email.id.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            format!(
                concat!(
                    "<m:GetConversationItemsResponseMessage ResponseClass=\"Success\">",
                    "<m:ResponseCode>NoError</m:ResponseCode>",
                    "<m:Conversation>",
                    "<t:ConversationId Id=\"conversation:{thread_id}\"/>",
                    "<t:SyncState>{sync_state}</t:SyncState>",
                    "<t:ConversationNodes>{nodes}</t:ConversationNodes>",
                    "</m:Conversation>",
                    "</m:GetConversationItemsResponseMessage>"
                ),
                thread_id = thread_id,
                sync_state = escape_xml(&sync_state),
                nodes = nodes,
            )
        })
        .collect::<String>();

    format!(
        concat!(
            "<m:GetConversationItemsResponse>",
            "<m:ResponseMessages>{response_messages}</m:ResponseMessages>",
            "</m:GetConversationItemsResponse>"
        ),
        response_messages = response_messages,
    )
}

pub(in crate::service) fn requested_conversation_ids(request: &str) -> Result<Vec<Uuid>> {
    attribute_values_for_tag(request, "ConversationId", "Id")
        .into_iter()
        .map(|value| {
            parse_conversation_id(value).ok_or_else(|| anyhow!("ConversationId is invalid"))
        })
        .collect()
}

fn parse_conversation_id(value: &str) -> Option<Uuid> {
    Uuid::parse_str(value.strip_prefix("conversation:").unwrap_or(value)).ok()
}

pub(in crate::service) fn parse_conversation_actions(
    request: &str,
) -> Vec<ConversationActionRequest> {
    element_contents(request, "ConversationAction")
        .into_iter()
        .map(|action_xml| ConversationActionRequest {
            action: element_text(action_xml, "Action").unwrap_or_default(),
            conversation_id: attribute_values_for_tag(action_xml, "ConversationId", "Id")
                .into_iter()
                .next()
                .and_then(parse_conversation_id),
            target_mailbox_id: requested_mailbox_folder_ids_in(action_xml, "DestinationFolderId")
                .into_iter()
                .next(),
            read: element_text(action_xml, "Read").and_then(|value| parse_xml_bool(&value).ok()),
        })
        .collect()
}

pub(in crate::service) fn filter_ignored_conversation_folders(
    emails: &mut Vec<JmapEmail>,
    request: &str,
    visible_mailboxes: &[JmapMailbox],
) {
    let Some(ignore_xml) = element_content(request, "FoldersToIgnore") else {
        return;
    };
    let visible_ids = visible_mailboxes
        .iter()
        .map(|mailbox| mailbox.id)
        .collect::<HashSet<_>>();
    let ignored_ids = requested_mailbox_folder_ids(ignore_xml)
        .into_iter()
        .filter(|id| visible_ids.contains(id))
        .collect::<HashSet<_>>();
    let ignored_roles = attribute_values_for_tag(ignore_xml, "DistinguishedFolderId", "Id")
        .into_iter()
        .filter_map(ews_distinguished_mailbox_role)
        .collect::<HashSet<_>>();
    emails.retain(|email| {
        !email.mailbox_ids.iter().any(|id| ignored_ids.contains(id))
            && !email
                .mailbox_states
                .iter()
                .any(|state| ignored_roles.contains(state.role.as_str()))
    });
}

fn conversation_summary_xml(thread_id: Uuid, messages: &[&JmapEmail]) -> String {
    let topic = messages
        .iter()
        .find_map(|email| (!email.subject.trim().is_empty()).then_some(email.subject.as_str()))
        .unwrap_or("(no subject)");
    let last_delivery = messages
        .iter()
        .map(|email| email.received_at.as_str())
        .max()
        .unwrap_or_default();
    let has_attachments = messages.iter().any(|email| email.has_attachments);
    let unread_count = messages.iter().filter(|email| email.unread).count();
    let size: i64 = messages.iter().map(|email| email.size_octets.max(0)).sum();
    let item_ids = messages
        .iter()
        .map(|email| {
            format!(
                "<t:ItemId Id=\"message:{}\" ChangeKey=\"{}\"/>",
                email.id,
                escape_xml(&message_change_key(email))
            )
        })
        .collect::<String>();

    format!(
        concat!(
            "<t:Conversation>",
            "<t:ConversationId Id=\"conversation:{thread_id}\"/>",
            "<t:ConversationTopic>{topic}</t:ConversationTopic>",
            "{recipients}",
            "{senders}",
            "<t:LastDeliveryTime>{last_delivery}</t:LastDeliveryTime>",
            "<t:GlobalLastDeliveryTime>{last_delivery}</t:GlobalLastDeliveryTime>",
            "<t:HasAttachments>{has_attachments}</t:HasAttachments>",
            "<t:GlobalHasAttachments>{has_attachments}</t:GlobalHasAttachments>",
            "<t:MessageCount>{message_count}</t:MessageCount>",
            "<t:GlobalMessageCount>{message_count}</t:GlobalMessageCount>",
            "<t:UnreadCount>{unread_count}</t:UnreadCount>",
            "<t:Size>{size}</t:Size>",
            "<t:GlobalSize>{size}</t:GlobalSize>",
            "<t:ItemClasses><t:ItemClass>IPM.Note</t:ItemClass></t:ItemClasses>",
            "<t:GlobalItemClasses><t:ItemClass>IPM.Note</t:ItemClass></t:GlobalItemClasses>",
            "<t:Importance>Normal</t:Importance>",
            "<t:GlobalImportance>Normal</t:GlobalImportance>",
            "<t:ItemIds>{item_ids}</t:ItemIds>",
            "<t:GlobalItemIds>{item_ids}</t:GlobalItemIds>",
            "</t:Conversation>"
        ),
        thread_id = thread_id,
        topic = escape_xml(topic),
        recipients =
            conversation_strings_xml("UniqueRecipients", &conversation_recipients(messages)),
        senders = conversation_strings_xml("UniqueSenders", &conversation_senders(messages)),
        last_delivery = escape_xml(last_delivery),
        has_attachments = has_attachments,
        message_count = messages.len(),
        unread_count = unread_count,
        size = size,
        item_ids = item_ids,
    )
}

fn conversation_node_xml(email: &JmapEmail) -> String {
    format!(
        concat!(
            "<t:ConversationNode>",
            "{internet_message_id}",
            "<t:Items>{item}</t:Items>",
            "</t:ConversationNode>"
        ),
        internet_message_id = email
            .internet_message_id
            .as_ref()
            .map(|value| format!(
                "<t:InternetMessageId>{}</t:InternetMessageId>",
                escape_xml(value)
            ))
            .unwrap_or_default(),
        item = message_item_xml(email),
    )
}

fn conversation_last_delivery(emails: &[JmapEmail], thread_id: &Uuid) -> String {
    emails
        .iter()
        .filter(|email| &email.thread_id == thread_id)
        .map(|email| email.received_at.clone())
        .max()
        .unwrap_or_default()
}

fn conversation_recipients(messages: &[&JmapEmail]) -> Vec<String> {
    let mut recipients = messages
        .iter()
        .flat_map(|email| email.to.iter().chain(email.cc.iter()))
        .map(|address| {
            address
                .display_name
                .clone()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| address.address.clone())
        })
        .collect::<Vec<_>>();
    recipients.sort();
    recipients.dedup();
    recipients
}

fn conversation_senders(messages: &[&JmapEmail]) -> Vec<String> {
    let mut senders = messages
        .iter()
        .map(|email| {
            email
                .from_display
                .clone()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| email.from_address.clone())
        })
        .collect::<Vec<_>>();
    senders.sort();
    senders.dedup();
    senders
}

fn conversation_strings_xml(element: &str, values: &[String]) -> String {
    let strings = values
        .iter()
        .map(|value| format!("<t:String>{}</t:String>", escape_xml(value)))
        .collect::<String>();
    format!("<t:{element}>{strings}</t:{element}><t:Global{element}>{strings}</t:Global{element}>",)
}
