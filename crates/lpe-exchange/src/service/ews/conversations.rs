use super::super::*;

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
                escape_xml(&email.delivery_status)
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
