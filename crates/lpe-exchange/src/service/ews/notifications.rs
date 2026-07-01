use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn subscribe(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        if element_content(request, "PullSubscriptionRequest").is_none() {
            return Ok(operation_error_response(
                "Subscribe",
                "ErrorInvalidOperation",
                "Subscribe currently supports only EWS pull subscriptions.",
            ));
        }

        let subscription = self.register_pull_subscription(principal, request).await?;
        Ok(subscribe_success_response(&subscription.0, &subscription.1))
    }

    pub(in crate::service) async fn get_events(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let subscription_id = element_text(request, "SubscriptionId")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| notification_subscription_id(principal.account_id, request));
        let previous_watermark = element_text(request, "Watermark")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| notification_watermark(&subscription_id, None, 0));

        self.durable_events_response(
            "GetEvents",
            principal,
            &subscription_id,
            &previous_watermark,
        )
        .await
    }

    pub(in crate::service) async fn get_streaming_events(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let subscription_id = element_text(request, "SubscriptionId")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| notification_subscription_id(principal.account_id, request));
        let previous_watermark = element_text(request, "Watermark")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| notification_watermark(&subscription_id, None, 0));
        self.durable_events_response(
            "GetStreamingEvents",
            principal,
            &subscription_id,
            &previous_watermark,
        )
        .await
    }

    pub(in crate::service) async fn durable_events_response(
        &self,
        operation: &str,
        principal: &AccountPrincipal,
        subscription_id: &str,
        previous_watermark: &str,
    ) -> Result<String> {
        let after_cursor = notification_watermark_sequence(previous_watermark).unwrap_or(0) as i64;
        let poll = self
            .store
            .poll_mapi_notifications(principal.account_id, after_cursor)
            .await?;
        let event_pending = poll.event_pending;
        let cursor = poll.cursor;
        if after_cursor > 0 && cursor.is_none() {
            return Ok(operation_error_response(
                operation,
                "ErrorInvalidWatermark",
                "The requested EWS notification watermark is no longer available in canonical change-log retention.",
            ));
        }
        let mut notifications = Vec::new();
        for event in poll.events {
            let Some(mailbox_id) = event.canonical_folder_id() else {
                continue;
            };
            let Some(item_id) = event.canonical_message_id() else {
                continue;
            };
            let sequence = event
                .change_cursor()
                .unwrap_or_else(|| cursor.unwrap_or(after_cursor))
                .max(0) as u64;
            let kind = match event.change_kind().unwrap_or_default() {
                "deleted" | "destroyed" | "removed" => EwsNotificationKind::Deleted,
                "created" | "inserted" | "new" => EwsNotificationKind::NewMail,
                _ => EwsNotificationKind::Created,
            };
            notifications.push(EwsQueuedNotification {
                sequence,
                kind,
                item_id,
                mailbox_id,
                change_key: sequence.to_string(),
                timestamp: "1970-01-01T00:00:00Z".to_string(),
            });
        }
        if !notifications.is_empty() {
            return Ok(match operation {
                "GetStreamingEvents" => get_streaming_events_queued_response(
                    subscription_id,
                    previous_watermark,
                    &notifications,
                    event_pending && notifications.len() >= 100,
                ),
                _ => get_events_queued_response(
                    subscription_id,
                    previous_watermark,
                    &notifications,
                    event_pending && notifications.len() >= 100,
                ),
            });
        }
        Ok(match operation {
            "GetStreamingEvents" => {
                get_streaming_events_status_response(subscription_id, previous_watermark)
            }
            _ => get_events_status_response(subscription_id, previous_watermark),
        })
    }

    pub(in crate::service) async fn unsubscribe(&self, request: &str) -> Result<String> {
        let subscription_id = element_text(request, "SubscriptionId").unwrap_or_default();
        if subscription_id.trim().is_empty() {
            return Ok(operation_error_response(
                "Unsubscribe",
                "ErrorInvalidSubscription",
                "Unsubscribe requires a SubscriptionId.",
            ));
        }

        Ok(unsubscribe_success_response())
    }

    async fn register_pull_subscription(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<(String, String)> {
        let subscription_id = notification_subscription_id(principal.account_id, request);
        let folder_marker = self
            .notification_request_folder_marker(principal, request)
            .await?;
        let requested_watermark =
            element_text(request, "Watermark").filter(|value| !value.trim().is_empty());
        let current_cursor = self
            .store
            .fetch_mapi_notification_cursor(principal.account_id)
            .await?
            .unwrap_or(0)
            .max(0) as u64;
        let watermark = requested_watermark.clone().unwrap_or_else(|| {
            notification_watermark(&subscription_id, folder_marker.as_deref(), current_cursor)
        });
        Ok((subscription_id, watermark))
    }

    async fn notification_request_folder_marker(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<Option<String>> {
        if let Some(mailbox_id) = self
            .requested_mailbox_folder_ids(principal, request)
            .await?
            .into_iter()
            .next()
        {
            return Ok(Some(format!("mailbox:{mailbox_id}")));
        }
        if let Some(role) = requested_mailbox_role(request) {
            return Ok(self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?
                .into_iter()
                .find(|mailbox| mailbox.role == role)
                .map(|mailbox| format!("mailbox:{}", mailbox.id))
                .or_else(|| Some(format!("role:{role}"))));
        }
        if pull_subscription_subscribes_to_all_folders(request) {
            return Ok(Some("all".to_string()));
        }
        Ok(None)
    }
}

#[derive(Clone, Debug)]
pub(in crate::service) struct EwsQueuedNotification {
    pub(in crate::service) sequence: u64,
    pub(in crate::service) kind: EwsNotificationKind,
    pub(in crate::service) item_id: Uuid,
    pub(in crate::service) mailbox_id: Uuid,
    pub(in crate::service) change_key: String,
    pub(in crate::service) timestamp: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::service) enum EwsNotificationKind {
    Created,
    Deleted,
    NewMail,
}

pub(in crate::service) fn subscribe_success_response(
    subscription_id: &str,
    watermark: &str,
) -> String {
    format!(
        concat!(
            "<m:SubscribeResponse>",
            "<m:ResponseMessages>",
            "<m:SubscribeResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:SubscriptionId>{subscription_id}</m:SubscriptionId>",
            "<m:Watermark>{watermark}</m:Watermark>",
            "</m:SubscribeResponseMessage>",
            "</m:ResponseMessages>",
            "</m:SubscribeResponse>"
        ),
        subscription_id = escape_xml(subscription_id),
        watermark = escape_xml(watermark),
    )
}

pub(in crate::service) fn get_events_queued_response(
    subscription_id: &str,
    previous_watermark: &str,
    events: &[EwsQueuedNotification],
    has_more: bool,
) -> String {
    let mut event_xml = String::new();
    for event in events {
        event_xml.push_str(&queued_notification_event_xml(subscription_id, event));
    }
    format!(
        concat!(
            "<m:GetEventsResponse>",
            "<m:ResponseMessages>",
            "<m:GetEventsResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Notification>",
            "<t:SubscriptionId>{subscription_id}</t:SubscriptionId>",
            "<t:PreviousWatermark>{previous_watermark}</t:PreviousWatermark>",
            "<t:MoreEvents>{more_events}</t:MoreEvents>",
            "{event_xml}",
            "</m:Notification>",
            "</m:GetEventsResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetEventsResponse>"
        ),
        subscription_id = escape_xml(subscription_id),
        previous_watermark = escape_xml(previous_watermark),
        more_events = if has_more { "true" } else { "false" },
        event_xml = event_xml,
    )
}

pub(in crate::service) fn get_streaming_events_queued_response(
    subscription_id: &str,
    previous_watermark: &str,
    events: &[EwsQueuedNotification],
    has_more: bool,
) -> String {
    get_events_queued_response(subscription_id, previous_watermark, events, has_more)
        .replace("GetEventsResponse", "GetStreamingEventsResponse")
        .replace(
            "GetEventsResponseMessage",
            "GetStreamingEventsResponseMessage",
        )
}

pub(in crate::service) fn queued_notification_event_xml(
    subscription_id: &str,
    event: &EwsQueuedNotification,
) -> String {
    let event_name = match event.kind {
        EwsNotificationKind::Created => "CreatedEvent",
        EwsNotificationKind::Deleted => "DeletedEvent",
        EwsNotificationKind::NewMail => "NewMailEvent",
    };
    let folder_marker = format!("mailbox:{}", event.mailbox_id);
    let watermark = notification_watermark(subscription_id, Some(&folder_marker), event.sequence);
    format!(
        concat!(
            "<t:{event_name}>",
            "<t:Watermark>{watermark}</t:Watermark>",
            "<t:TimeStamp>{timestamp}</t:TimeStamp>",
            "<t:ItemId Id=\"message:{item_id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"mailbox:{mailbox_id}\" ChangeKey=\"{folder_change_key}\"/>",
            "</t:{event_name}>",
        ),
        event_name = event_name,
        watermark = escape_xml(&watermark),
        timestamp = escape_xml(&event.timestamp),
        item_id = event.item_id,
        change_key = escape_xml(&event.change_key),
        mailbox_id = event.mailbox_id,
        folder_change_key = escape_xml(&folder_change_key(&event.mailbox_id.to_string())),
    )
}

pub(in crate::service) fn get_events_status_response(
    subscription_id: &str,
    previous_watermark: &str,
) -> String {
    let folder_marker = notification_watermark_folder_marker(previous_watermark);
    let previous_sequence = notification_watermark_sequence(previous_watermark).unwrap_or(0);
    let next_sequence = if previous_sequence == 0 {
        1
    } else {
        previous_sequence
    };
    let next_watermark =
        notification_watermark(subscription_id, folder_marker.as_deref(), next_sequence);
    format!(
        concat!(
            "<m:GetEventsResponse>",
            "<m:ResponseMessages>",
            "<m:GetEventsResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Notification>",
            "<t:SubscriptionId>{subscription_id}</t:SubscriptionId>",
            "<t:PreviousWatermark>{previous_watermark}</t:PreviousWatermark>",
            "<t:MoreEvents>false</t:MoreEvents>",
            "<t:StatusEvent>",
            "<t:Watermark>{next_watermark}</t:Watermark>",
            "</t:StatusEvent>",
            "</m:Notification>",
            "</m:GetEventsResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetEventsResponse>"
        ),
        subscription_id = escape_xml(subscription_id),
        previous_watermark = escape_xml(previous_watermark),
        next_watermark = escape_xml(&next_watermark),
    )
}

pub(in crate::service) fn get_streaming_events_status_response(
    subscription_id: &str,
    previous_watermark: &str,
) -> String {
    get_events_status_response(subscription_id, previous_watermark)
        .replace("GetEventsResponse", "GetStreamingEventsResponse")
        .replace(
            "GetEventsResponseMessage",
            "GetStreamingEventsResponseMessage",
        )
}

pub(in crate::service) fn unsubscribe_success_response() -> String {
    concat!(
        "<m:UnsubscribeResponse>",
        "<m:ResponseMessages>",
        "<m:UnsubscribeResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "</m:UnsubscribeResponseMessage>",
        "</m:ResponseMessages>",
        "</m:UnsubscribeResponse>"
    )
    .to_string()
}

pub(in crate::service) fn notification_subscription_id(account_id: Uuid, request: &str) -> String {
    let folder_ids = requested_folder_ids(request).join(",");
    let distinguished_folder_id = requested_distinguished_folder_id(request).unwrap_or_default();
    let account_id = account_id.to_string();
    let mut hash = 0xcbf29ce484222325_u64;
    for part in [
        "ews-pull-subscription",
        &account_id,
        &folder_ids,
        distinguished_folder_id,
    ] {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    format!(
        "00000000-0000-4000-8000-{tail:012x}",
        tail = hash & 0xffff_ffff_ffff
    )
}

pub(in crate::service) fn pull_subscription_subscribes_to_all_folders(request: &str) -> bool {
    open_tag_text(request, "PullSubscriptionRequest")
        .and_then(|tag| attribute_value(tag, "SubscribeToAllFolders"))
        .is_some_and(parse_xml_bool_attr)
}

pub(in crate::service) fn notification_watermark(
    subscription_id: &str,
    folder_marker: Option<&str>,
    sequence: u64,
) -> String {
    match folder_marker {
        Some(folder_marker) => format!("lpe:{subscription_id}:{folder_marker}:{sequence}"),
        None => format!("lpe:{subscription_id}:all:{sequence}"),
    }
}

pub(in crate::service) fn notification_watermark_folder_marker(watermark: &str) -> Option<String> {
    let mut parts = watermark.split(':');
    if parts.next()? != "lpe" {
        return None;
    }
    parts.next()?;
    let kind = parts.next()?;
    match kind {
        "mailbox" => Uuid::parse_str(parts.next()?)
            .ok()
            .map(|mailbox_id| format!("mailbox:{mailbox_id}")),
        "role" => parts.next().map(|role| format!("role:{role}")),
        _ => None,
    }
}

pub(in crate::service) fn notification_watermark_sequence(watermark: &str) -> Option<u64> {
    watermark.rsplit(':').next()?.parse().ok()
}
