use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};

use super::super::*;

const PAGE_LIMIT: usize = 100;
const PULL_TIMEOUT_MAX: u32 = 1_440;

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
        let scope = match self.parse_pull_subscription(principal, request).await {
            Ok(scope) => scope,
            Err(error) => {
                return Ok(operation_error_response(
                    "Subscribe",
                    "ErrorInvalidRequest",
                    &error,
                ))
            }
        };
        let seed = self
            .store
            .replay_ews_notification_events(
                principal.account_id,
                0,
                &scope.folders,
                &scope.events,
                PAGE_LIMIT,
            )
            .await?
            .current_cursor
            .unwrap_or(0)
            .max(0) as u64;
        let subscription = NotificationSubscription {
            account_id: principal.account_id,
            folders: scope.folders,
            events: scope.events,
            seed,
        };
        let subscription_id = notification_subscription_id(&subscription);
        let watermark = notification_watermark(&subscription_id, seed);
        Ok(subscribe_success_response(&subscription_id, &watermark))
    }

    pub(in crate::service) async fn get_events(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let (subscription_id, watermark) = match parse_pull_event_request(request) {
            Ok(value) => value,
            Err(error) => {
                return Ok(operation_error_response(
                    "GetEvents",
                    "ErrorInvalidWatermark",
                    &error,
                ))
            }
        };
        let subscription = match parse_notification_subscription(principal, &subscription_id) {
            Ok(value) => value,
            Err(error) => {
                return Ok(operation_error_response(
                    "GetEvents",
                    "ErrorInvalidSubscription",
                    &error,
                ))
            }
        };
        let after = match parse_notification_watermark(&subscription_id, &watermark, &subscription)
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(operation_error_response(
                    "GetEvents",
                    "ErrorInvalidWatermark",
                    &error,
                ))
            }
        };
        self.durable_events_response(
            "GetEvents",
            principal,
            &subscription_id,
            &watermark,
            subscription,
            after,
        )
        .await
    }

    pub(in crate::service) async fn get_streaming_events(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let subscription_id = match parse_streaming_event_request(request) {
            Ok(value) => value,
            Err(error) => {
                return Ok(operation_error_response(
                    "GetStreamingEvents",
                    "ErrorInvalidSubscription",
                    &error,
                ))
            }
        };
        let subscription = match parse_notification_subscription(principal, &subscription_id) {
            Ok(value) => value,
            Err(error) => {
                return Ok(operation_error_response(
                    "GetStreamingEvents",
                    "ErrorInvalidSubscription",
                    &error,
                ))
            }
        };
        let watermark = notification_watermark(&subscription_id, subscription.seed);
        self.durable_events_response(
            "GetStreamingEvents",
            principal,
            &subscription_id,
            &watermark,
            subscription.clone(),
            subscription.seed,
        )
        .await
    }

    async fn durable_events_response(
        &self,
        operation: &str,
        principal: &AccountPrincipal,
        subscription_id: &str,
        previous_watermark: &str,
        subscription: NotificationSubscription,
        after: u64,
    ) -> Result<String> {
        let replay = self
            .store
            .replay_ews_notification_events(
                principal.account_id,
                after.min(i64::MAX as u64) as i64,
                &subscription.folders,
                &subscription.events,
                PAGE_LIMIT,
            )
            .await?;
        if replay.expired {
            return Ok(operation_error_response(
                operation,
                "ErrorInvalidWatermark",
                "The requested EWS notification watermark is no longer available in canonical change-log retention.",
            ));
        }
        let inbox_ids = if subscription
            .events
            .contains(&EwsNotificationEventType::NewMail)
        {
            self.store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?
                .into_iter()
                .filter(|mailbox| mailbox.role == "inbox")
                .map(|mailbox| mailbox.id)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let notifications = replay
            .events
            .into_iter()
            .filter(|event| {
                event.change_kind != "created"
                    || subscription
                        .events
                        .contains(&EwsNotificationEventType::Created)
                    || inbox_ids.contains(&event.mailbox_id)
            })
            .map(|event| EwsQueuedNotification {
                sequence: event.cursor.max(0) as u64,
                kind: notification_kind_for_change(
                    &event.change_kind,
                    &subscription.events,
                    inbox_ids.contains(&event.mailbox_id),
                ),
                item_id: event.message_id,
                mailbox_id: event.mailbox_id,
                change_key: versioned_change_key(
                    "message",
                    &event.message_id.to_string(),
                    &event.modseq.to_string(),
                ),
                timestamp: event.created_at,
            })
            .collect::<Vec<_>>();
        let next_watermark =
            notification_watermark(subscription_id, replay.next_cursor.max(0) as u64);
        if !notifications.is_empty() {
            return Ok(if operation == "GetStreamingEvents" {
                get_streaming_events_queued_response(
                    subscription_id,
                    previous_watermark,
                    &notifications,
                    replay.more_events,
                )
            } else {
                get_events_queued_response(
                    subscription_id,
                    previous_watermark,
                    &notifications,
                    replay.more_events,
                )
            });
        }
        Ok(if operation == "GetStreamingEvents" {
            get_streaming_events_status_response(
                subscription_id,
                previous_watermark,
                &next_watermark,
                replay.more_events,
            )
        } else {
            get_events_status_response(
                subscription_id,
                previous_watermark,
                &next_watermark,
                replay.more_events,
            )
        })
    }

    pub(in crate::service) async fn unsubscribe(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let subscription_id = match exactly_one_text(request, "SubscriptionId") {
            Ok(value) => value,
            Err(error) => {
                return Ok(operation_error_response(
                    "Unsubscribe",
                    "ErrorInvalidSubscription",
                    &error,
                ))
            }
        };
        if let Err(error) = parse_notification_subscription(principal, &subscription_id) {
            return Ok(operation_error_response(
                "Unsubscribe",
                "ErrorInvalidSubscription",
                &error,
            ));
        }
        Ok(unsubscribe_success_response())
    }

    async fn parse_pull_subscription(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> std::result::Result<NotificationScope, String> {
        // [MS-OXWSNTIF] §§3.1.4.3, 3.1.4.3.3.3-.5: LPE persists no EWS
        // subscription row, so the complete bounded scope is tokenized here.
        let pulls = element_contents(request, "PullSubscriptionRequest");
        if pulls.len() != 1
            || !element_contents(request, "PushSubscriptionRequest").is_empty()
            || !element_contents(request, "StreamingSubscriptionRequest").is_empty()
        {
            return Err("Subscribe requires exactly one PullSubscriptionRequest.".to_string());
        }
        let pull = pulls[0];
        if !element_contents(pull, "Watermark").is_empty() {
            return Err(
                "Subscribe does not accept a resume Watermark without durable subscription state."
                    .to_string(),
            );
        }
        let timeout = exactly_one_text(pull, "Timeout")?
            .parse::<u32>()
            .map_err(|_| "Subscribe Timeout must be an integer number of minutes.".to_string())?;
        if !(1..=PULL_TIMEOUT_MAX).contains(&timeout) {
            return Err(
                "Subscribe Timeout is outside LPE's supported 1..=1440 minute range.".to_string(),
            );
        }
        let events = parse_notification_event_types(pull)?;
        let all = match element_contents(pull, "SubscribeToAllFolders").as_slice() {
            [] => false,
            [value] => parse_xml_bool(&xml_text(value))?,
            _ => {
                return Err("Subscribe accepts at most one SubscribeToAllFolders value.".to_string())
            }
        };
        let folder_sets = element_contents(pull, "FolderIds");
        if folder_sets.len() > 1 || (all && !folder_sets.is_empty()) {
            return Err("Subscribe requires exactly one unambiguous folder scope.".to_string());
        }
        if all {
            return Ok(NotificationScope {
                folders: EwsNotificationFolderScope::All,
                events,
            });
        }
        let Some(folder_set) = folder_sets.first() else {
            return Err(
                "Subscribe requires SubscribeToAllFolders or non-empty mailbox FolderIds."
                    .to_string(),
            );
        };
        let folder_count = element_contents(folder_set, "FolderId").len();
        let distinguished_count = element_contents(folder_set, "DistinguishedFolderId").len();
        let raw_folder_ids = attribute_values_for_tag(folder_set, "FolderId", "Id");
        let raw_distinguished_ids =
            attribute_values_for_tag(folder_set, "DistinguishedFolderId", "Id");
        if folder_count + distinguished_count == 0
            || raw_folder_ids.len() + raw_distinguished_ids.len()
                != folder_count + distinguished_count
        {
            return Err(
                "Subscribe FolderIds must contain only Id-bearing mailbox references.".to_string(),
            );
        }
        let mailboxes = self
            .store
            .fetch_jmap_mailboxes(principal.account_id)
            .await
            .map_err(|error| error.to_string())?;
        let mut ids = Vec::new();
        for raw_id in raw_folder_ids {
            let id = raw_id
                .strip_prefix("mailbox:")
                .ok_or_else(|| "Subscribe supports only canonical mailbox FolderIds.".to_string())
                .and_then(|id| {
                    Uuid::parse_str(id).map_err(|_| "Subscribe FolderId is invalid.".to_string())
                })?;
            if !mailboxes.iter().any(|mailbox| mailbox.id == id) {
                return Err(
                    "Subscribe FolderId is not visible to the authenticated mailbox.".to_string(),
                );
            }
            ids.push(id);
        }
        for raw_id in raw_distinguished_ids {
            let role = EwsDistinguishedFolderIdName::parse(raw_id)
                .and_then(EwsDistinguishedFolderIdName::mailbox_role)
                .ok_or_else(|| {
                    "Subscribe does not support that distinguished folder.".to_string()
                })?;
            let mailbox = mailboxes
                .iter()
                .find(|mailbox| mailbox.role == role)
                .ok_or_else(|| "Subscribe distinguished folder is unavailable.".to_string())?;
            ids.push(mailbox.id);
        }
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != folder_count + distinguished_count {
            return Err("Subscribe FolderIds must not contain duplicates.".to_string());
        }
        Ok(NotificationScope {
            folders: EwsNotificationFolderScope::Mailboxes(ids),
            events,
        })
    }
}

#[derive(Clone, Debug)]
struct NotificationScope {
    folders: EwsNotificationFolderScope,
    events: Vec<EwsNotificationEventType>,
}

#[derive(Clone, Debug)]
struct NotificationSubscription {
    account_id: Uuid,
    folders: EwsNotificationFolderScope,
    events: Vec<EwsNotificationEventType>,
    seed: u64,
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
    Modified,
    NewMail,
}

pub(in crate::service) fn subscribe_success_response(
    subscription_id: &str,
    watermark: &str,
) -> String {
    format!(
        "<m:SubscribeResponse><m:ResponseMessages><m:SubscribeResponseMessage ResponseClass=\"Success\"><m:ResponseCode>NoError</m:ResponseCode><m:SubscriptionId>{}</m:SubscriptionId><m:Watermark>{}</m:Watermark></m:SubscribeResponseMessage></m:ResponseMessages></m:SubscribeResponse>",
        escape_xml(subscription_id), escape_xml(watermark)
    )
}

pub(in crate::service) fn get_events_queued_response(
    subscription_id: &str,
    previous: &str,
    events: &[EwsQueuedNotification],
    more: bool,
) -> String {
    let events = events
        .iter()
        .map(|event| queued_notification_event_xml(subscription_id, event))
        .collect::<String>();
    notification_response("GetEvents", subscription_id, previous, &events, more)
}

pub(in crate::service) fn get_streaming_events_queued_response(
    subscription_id: &str,
    previous: &str,
    events: &[EwsQueuedNotification],
    more: bool,
) -> String {
    let events = events
        .iter()
        .map(|event| queued_notification_event_xml(subscription_id, event))
        .collect::<String>();
    notification_response(
        "GetStreamingEvents",
        subscription_id,
        previous,
        &events,
        more,
    )
}

fn notification_response(
    operation: &str,
    subscription_id: &str,
    previous: &str,
    events: &str,
    more: bool,
) -> String {
    format!(
        "<m:{operation}Response><m:ResponseMessages><m:{operation}ResponseMessage ResponseClass=\"Success\"><m:ResponseCode>NoError</m:ResponseCode><m:Notification><t:SubscriptionId>{}</t:SubscriptionId><t:PreviousWatermark>{}</t:PreviousWatermark><t:MoreEvents>{}</t:MoreEvents>{events}</m:Notification></m:{operation}ResponseMessage></m:ResponseMessages></m:{operation}Response>",
        escape_xml(subscription_id), escape_xml(previous), if more { "true" } else { "false" }
    )
}

pub(in crate::service) fn queued_notification_event_xml(
    subscription_id: &str,
    event: &EwsQueuedNotification,
) -> String {
    let name = match event.kind {
        EwsNotificationKind::Created => "CreatedEvent",
        EwsNotificationKind::Deleted => "DeletedEvent",
        EwsNotificationKind::Modified => "ModifiedEvent",
        EwsNotificationKind::NewMail => "NewMailEvent",
    };
    format!(
        "<t:{name}><t:Watermark>{}</t:Watermark><t:TimeStamp>{}</t:TimeStamp><t:ItemId Id=\"message:{}\" ChangeKey=\"{}\"/><t:ParentFolderId Id=\"mailbox:{}\" ChangeKey=\"{}\"/></t:{name}>",
        escape_xml(&notification_watermark(subscription_id, event.sequence)),
        escape_xml(&event.timestamp), event.item_id, escape_xml(&event.change_key), event.mailbox_id,
        escape_xml(&folder_change_key(&event.mailbox_id.to_string())),
    )
}

pub(in crate::service) fn get_events_status_response(
    subscription_id: &str,
    previous: &str,
    next: &str,
    more: bool,
) -> String {
    notification_response(
        "GetEvents",
        subscription_id,
        previous,
        &format!(
            "<t:StatusEvent><t:Watermark>{}</t:Watermark></t:StatusEvent>",
            escape_xml(next)
        ),
        more,
    )
}

pub(in crate::service) fn get_streaming_events_status_response(
    subscription_id: &str,
    previous: &str,
    next: &str,
    more: bool,
) -> String {
    notification_response(
        "GetStreamingEvents",
        subscription_id,
        previous,
        &format!(
            "<t:StatusEvent><t:Watermark>{}</t:Watermark></t:StatusEvent>",
            escape_xml(next)
        ),
        more,
    )
}

pub(in crate::service) fn unsubscribe_success_response() -> String {
    "<m:UnsubscribeResponse><m:ResponseMessages><m:UnsubscribeResponseMessage ResponseClass=\"Success\"><m:ResponseCode>NoError</m:ResponseCode></m:UnsubscribeResponseMessage></m:ResponseMessages></m:UnsubscribeResponse>".to_string()
}

fn parse_pull_event_request(request: &str) -> std::result::Result<(String, String), String> {
    Ok((
        exactly_one_text(request, "SubscriptionId")?,
        exactly_one_text(request, "Watermark")?,
    ))
}

fn parse_streaming_event_request(request: &str) -> std::result::Result<String, String> {
    // [MS-OXWSNTIF] §3.1.4.2.3.1: accepted as one immediate replay request,
    // not a long-held connection or a multi-subscription affinity channel.
    let sets = element_contents(request, "SubscriptionIds");
    if sets.len() != 1 {
        return Err("GetStreamingEvents requires exactly one SubscriptionIds payload.".to_string());
    }
    let timeout = exactly_one_text(request, "ConnectionTimeout")?
        .parse::<u32>()
        .map_err(|_| "GetStreamingEvents ConnectionTimeout must be an integer.".to_string())?;
    if !(1..=30).contains(&timeout) {
        return Err("GetStreamingEvents ConnectionTimeout must be in 1..=30 minutes.".to_string());
    }
    exactly_one_text(sets[0], "SubscriptionId")
}

fn exactly_one_text(xml: &str, name: &str) -> std::result::Result<String, String> {
    match element_contents(xml, name).as_slice() {
        [value] if !xml_text(value).is_empty() => Ok(xml_text(value)),
        [..] => Err(format!(
            "Notification request requires exactly one non-empty {name}."
        )),
    }
}

fn parse_notification_event_types(
    xml: &str,
) -> std::result::Result<Vec<EwsNotificationEventType>, String> {
    let sets = element_contents(xml, "EventTypes");
    if sets.len() != 1 {
        return Err("Subscribe requires exactly one EventTypes payload.".to_string());
    }
    let mut result = Vec::new();
    for value in element_contents(sets[0], "EventType") {
        let value = match xml_text(value).as_str() {
            "CreatedEvent" => EwsNotificationEventType::Created,
            "DeletedEvent" => EwsNotificationEventType::Deleted,
            "ModifiedEvent" => EwsNotificationEventType::Modified,
            "NewMailEvent" => EwsNotificationEventType::NewMail,
            _ => return Err("Subscribe EventTypes contains an unsupported event type.".to_string()),
        };
        if result.contains(&value) {
            return Err("Subscribe EventTypes must not contain duplicates.".to_string());
        }
        result.push(value);
    }
    if result.is_empty() {
        return Err("Subscribe EventTypes must not be empty.".to_string());
    }
    result.sort_by_key(|value| notification_event_type_name(*value));
    Ok(result)
}

fn parse_xml_bool(value: &str) -> std::result::Result<bool, String> {
    match value {
        "true" | "True" | "1" => Ok(true),
        "false" | "False" | "0" => Ok(false),
        _ => Err("SubscribeToAllFolders must be a Boolean value.".to_string()),
    }
}

fn notification_subscription_id(subscription: &NotificationSubscription) -> String {
    let scope = notification_scope_payload(&subscription.folders);
    let events = subscription
        .events
        .iter()
        .map(|value| notification_event_type_name(*value))
        .collect::<Vec<_>>()
        .join(",");
    let payload = URL_SAFE_NO_PAD.encode(format!("{scope}|{events}|{}", subscription.seed));
    let account = subscription.account_id.simple().to_string();
    format!(
        "lpe-sub.v1.{account}.{payload}.{}",
        notification_digest("subscription", &[&account, &payload])
    )
}

fn parse_notification_subscription(
    principal: &AccountPrincipal,
    token: &str,
) -> std::result::Result<NotificationSubscription, String> {
    let parts = token.split('.').collect::<Vec<_>>();
    let ["lpe-sub", "v1", account, payload, signature] = parts.as_slice() else {
        return Err("SubscriptionId is not a supported LPE notification token.".to_string());
    };
    if notification_digest("subscription", &[account, payload]) != *signature {
        return Err("SubscriptionId integrity validation failed.".to_string());
    }
    let account_id = Uuid::parse_str(account)
        .map_err(|_| "SubscriptionId account binding is invalid.".to_string())?;
    if account_id != principal.account_id {
        return Err("SubscriptionId belongs to another authenticated mailbox.".to_string());
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(payload.as_bytes())
        .map_err(|_| "SubscriptionId payload is invalid.".to_string())?;
    let decoded =
        String::from_utf8(decoded).map_err(|_| "SubscriptionId payload is invalid.".to_string())?;
    let mut values = decoded.split('|');
    let folders = parse_notification_scope_payload(
        values
            .next()
            .ok_or_else(|| "SubscriptionId scope is missing.".to_string())?,
    )?;
    let events = parse_notification_event_payload(
        values
            .next()
            .ok_or_else(|| "SubscriptionId event filter is missing.".to_string())?,
    )?;
    let seed = values
        .next()
        .ok_or_else(|| "SubscriptionId cursor is missing.".to_string())?
        .parse::<u64>()
        .map_err(|_| "SubscriptionId cursor is invalid.".to_string())?;
    if values.next().is_some() {
        return Err("SubscriptionId payload is malformed.".to_string());
    }
    Ok(NotificationSubscription {
        account_id,
        folders,
        events,
        seed,
    })
}

fn notification_watermark(subscription_id: &str, cursor: u64) -> String {
    let subscription = URL_SAFE_NO_PAD.encode(subscription_id.as_bytes());
    let cursor = cursor.to_string();
    format!(
        "lpe-wm.v1.{subscription}.{cursor}.{}",
        notification_digest("watermark", &[subscription_id, &cursor])
    )
}

fn parse_notification_watermark(
    subscription_id: &str,
    watermark: &str,
    subscription: &NotificationSubscription,
) -> std::result::Result<u64, String> {
    // [MS-OXWSNTIF] §§2.2.5.1-.2: validate the complete opaque bookmark.
    let parts = watermark.split('.').collect::<Vec<_>>();
    let ["lpe-wm", "v1", encoded, cursor, signature] = parts.as_slice() else {
        return Err("Watermark is not a supported LPE notification token.".to_string());
    };
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded.as_bytes())
        .map_err(|_| "Watermark subscription binding is invalid.".to_string())?;
    if decoded != subscription_id.as_bytes() {
        return Err("Watermark does not match SubscriptionId.".to_string());
    }
    if notification_digest("watermark", &[subscription_id, cursor]) != *signature {
        return Err("Watermark integrity validation failed.".to_string());
    }
    let cursor = cursor
        .parse::<u64>()
        .map_err(|_| "Watermark cursor is invalid.".to_string())?;
    if cursor < subscription.seed {
        return Err("Watermark predates its subscription cursor.".to_string());
    }
    Ok(cursor)
}

fn notification_scope_payload(scope: &EwsNotificationFolderScope) -> String {
    match scope {
        EwsNotificationFolderScope::All => "all".to_string(),
        EwsNotificationFolderScope::Mailboxes(ids) => format!(
            "mailboxes:{}",
            ids.iter()
                .map(Uuid::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

fn parse_notification_scope_payload(
    value: &str,
) -> std::result::Result<EwsNotificationFolderScope, String> {
    if value == "all" {
        return Ok(EwsNotificationFolderScope::All);
    }
    let ids = value
        .strip_prefix("mailboxes:")
        .ok_or_else(|| "SubscriptionId folder scope is invalid.".to_string())?
        .split(',')
        .map(|id| {
            Uuid::parse_str(id).map_err(|_| "SubscriptionId folder scope is invalid.".to_string())
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if ids.is_empty() || ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err("SubscriptionId folder scope is invalid.".to_string());
    }
    Ok(EwsNotificationFolderScope::Mailboxes(ids))
}

fn parse_notification_event_payload(
    value: &str,
) -> std::result::Result<Vec<EwsNotificationEventType>, String> {
    let events = value
        .split(',')
        .map(|value| match value {
            "CreatedEvent" => Ok(EwsNotificationEventType::Created),
            "DeletedEvent" => Ok(EwsNotificationEventType::Deleted),
            "ModifiedEvent" => Ok(EwsNotificationEventType::Modified),
            "NewMailEvent" => Ok(EwsNotificationEventType::NewMail),
            _ => Err("SubscriptionId event filter is invalid.".to_string()),
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if events.is_empty()
        || events.windows(2).any(|pair| {
            notification_event_type_name(pair[0]) >= notification_event_type_name(pair[1])
        })
    {
        return Err("SubscriptionId event filter is invalid.".to_string());
    }
    Ok(events)
}

fn notification_event_type_name(value: EwsNotificationEventType) -> &'static str {
    match value {
        EwsNotificationEventType::Created => "CreatedEvent",
        EwsNotificationEventType::Deleted => "DeletedEvent",
        EwsNotificationEventType::Modified => "ModifiedEvent",
        EwsNotificationEventType::NewMail => "NewMailEvent",
    }
}

fn notification_kind_for_change(
    change: &str,
    events: &[EwsNotificationEventType],
    is_inbox: bool,
) -> EwsNotificationKind {
    match change {
        "destroyed" | "expunged" => EwsNotificationKind::Deleted,
        "updated" => EwsNotificationKind::Modified,
        // [MS-OXWSNTIF] §§2.2.4.1-.8: NewMail is a delivered Inbox event;
        // other canonical creations retain the CreatedEvent projection.
        "created" if is_inbox && events.contains(&EwsNotificationEventType::NewMail) => {
            EwsNotificationKind::NewMail
        }
        _ => EwsNotificationKind::Created,
    }
}

fn notification_digest(domain: &str, values: &[&str]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"lpe-ews-notifications-v1\0");
    digest.update(domain.as_bytes());
    for value in values {
        digest.update([0]);
        digest.update(value.as_bytes());
    }
    URL_SAFE_NO_PAD.encode(digest.finalize())
}
