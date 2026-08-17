use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn get_reminders(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let include_inactive = element_text(request, "IncludeDismissedReminders")
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let reminders = self
            .store
            .query_client_reminders(principal.account_id, ReminderQuery { include_inactive })
            .await?;
        Ok(get_reminders_response(&reminders))
    }

    pub(in crate::service) async fn perform_reminder_action(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let action = parse_single_reminder_action(request)?;
            match action.reminder.source_type.as_str() {
                "mail" | "message" => {
                    self.store
                        .update_jmap_email_followup_flags(
                            principal.account_id,
                            action.reminder.source_id,
                            JmapEmailFollowupUpdate {
                                reminder_dismissed_at: if action.snooze_until.is_none() {
                                    Some("now".to_string())
                                } else {
                                    None
                                },
                                reminder_at: action.snooze_until.clone(),
                                reminder_set: action.snooze_until.as_ref().map(|_| true),
                                ..JmapEmailFollowupUpdate::default()
                            },
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-perform-reminder-action".to_string(),
                                subject: action.reminder.source_id.to_string(),
                            },
                        )
                        .await?;
                }
                "calendar" | "task" => {
                    if let Some(reminder_at) = action.snooze_until {
                        self.store
                            .snooze_reminder_occurrence(
                                principal.account_id,
                                &action.reminder.source_type,
                                action.reminder.source_id,
                                action.reminder.occurrence_start_at.as_deref().ok_or_else(
                                    || {
                                        anyhow!(
                                            "{} reminder ItemId requires an occurrence identity",
                                            action.reminder.source_type
                                        )
                                    },
                                )?,
                                &reminder_at,
                            )
                            .await?;
                    } else {
                        self.store
                            .dismiss_reminder_occurrence(
                                principal.account_id,
                                &action.reminder.source_type,
                                action.reminder.source_id,
                                action.reminder.occurrence_start_at.as_deref(),
                                "now",
                            )
                            .await?;
                    }
                }
                _ => bail!(
                    "unsupported reminder source `{}`",
                    action.reminder.source_type
                ),
            }
            Ok(simple_operation_success_response("PerformReminderAction"))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response(
                "PerformReminderAction",
                "ErrorInvalidOperation",
                &error.to_string(),
            )
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::service) struct ParsedReminderItemId {
    pub(in crate::service) source_type: String,
    pub(in crate::service) source_id: Uuid,
    pub(in crate::service) occurrence_start_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedReminderAction {
    reminder: ParsedReminderItemId,
    snooze_until: Option<String>,
}

/// The current canonical reminder APIs commit one source at a time.  Keep the
/// EWS boundary to one action until storage exposes an all-or-nothing batch
/// mutation, rather than acknowledging a partially applied SOAP batch.
fn parse_single_reminder_action(request: &str) -> Result<ParsedReminderAction> {
    let actions = element_contents(request, "ReminderItemAction");
    let action = match actions.as_slice() {
        [action] => *action,
        [] => request,
        _ => bail!(
            "PerformReminderAction supports exactly one ReminderItemAction until canonical batch mutation is available."
        ),
    };
    let reminder_ids = attribute_values_for_tag(action, "ItemId", "Id");
    let [reminder_id] = reminder_ids.as_slice() else {
        bail!("PerformReminderAction requires exactly one reminder ItemId.");
    };
    let reminder = parse_reminder_item_id(reminder_id)
        .ok_or_else(|| anyhow!("unsupported reminder ItemId `{reminder_id}`"))?;
    let action_type = element_text(action, "ActionType")
        .or_else(|| element_text(action, "ReminderItemActionType"))
        .or_else(|| element_text(action, "ReminderAction"))
        .unwrap_or_else(|| "Dismiss".to_string());
    let snooze_until = if action_type.eq_ignore_ascii_case("Snooze") {
        Some(
            element_text(action, "NewReminderTime")
                .or_else(|| element_text(action, "SnoozeUntil"))
                .or_else(|| element_text(action, "ReminderTime"))
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    anyhow!("PerformReminderAction Snooze requires a new reminder time.")
                })?,
        )
    } else if action_type.eq_ignore_ascii_case("Dismiss") {
        None
    } else {
        bail!("PerformReminderAction currently supports only Dismiss and Snooze.");
    };
    Ok(ParsedReminderAction {
        reminder,
        snooze_until,
    })
}

pub(in crate::service) fn get_reminders_response(reminders: &[ClientReminder]) -> String {
    let mut reminders_xml = String::new();
    for reminder in reminders {
        let reminder_id = reminder_item_id(reminder);
        reminders_xml.push_str(&format!(
            concat!(
                "<t:Reminder>",
                "<t:Subject>{title}</t:Subject>",
                "<t:Location/>",
                "<t:ReminderTime>{reminder_at}</t:ReminderTime>",
                "<t:StartDate>{start_at}</t:StartDate>",
                "<t:EndDate>{due_at}</t:EndDate>",
                "<t:ItemId Id=\"{id}\" ChangeKey=\"{status}\"/>",
                "</t:Reminder>"
            ),
            title = escape_xml(&reminder.title),
            reminder_at = escape_xml(&reminder.reminder_at),
            start_at = escape_xml(
                reminder
                    .occurrence_start_at
                    .as_deref()
                    .or(reminder.due_at.as_deref())
                    .unwrap_or(&reminder.reminder_at)
            ),
            due_at = escape_xml(reminder.due_at.as_deref().unwrap_or(&reminder.reminder_at)),
            id = escape_xml(&reminder_id),
            status = escape_xml(&reminder.status),
        ));
    }
    format!(
        concat!(
            "<m:GetRemindersResponse>",
            "<m:ResponseMessages>",
            "<m:GetRemindersResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Reminders>{reminders_xml}</m:Reminders>",
            "</m:GetRemindersResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetRemindersResponse>"
        ),
        reminders_xml = reminders_xml
    )
}

fn reminder_item_id(reminder: &ClientReminder) -> String {
    if let Some(occurrence_start_at) = reminder.occurrence_start_at.as_deref() {
        format!(
            "{}:{}:{}",
            reminder.source_type, reminder.source_id, occurrence_start_at
        )
    } else {
        format!("{}:{}", reminder.source_type, reminder.source_id)
    }
}

pub(in crate::service) fn parse_reminder_item_id(id: &str) -> Option<ParsedReminderItemId> {
    let mut parts = id.splitn(3, ':');
    let source_type = parts.next()?.to_ascii_lowercase();
    let source_id = Uuid::parse_str(parts.next()?).ok()?;
    let occurrence_start_at = parts
        .next()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string);
    Some(ParsedReminderItemId {
        source_type,
        source_id,
        occurrence_start_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reminder_action_keeps_snooze_time_bound_to_its_single_item() {
        let parsed = parse_single_reminder_action(
            r#"<m:ReminderItemAction><t:ActionType>Snooze</t:ActionType><t:NewReminderTime>2026-08-17T10:30:00Z</t:NewReminderTime><t:ItemId Id="calendar:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2026-08-17T09:00:00Z"/></m:ReminderItemAction>"#,
        )
        .unwrap();
        assert_eq!(parsed.reminder.source_type, "calendar");
        assert_eq!(parsed.snooze_until.as_deref(), Some("2026-08-17T10:30:00Z"));
    }

    #[test]
    fn reminder_action_rejects_multiple_mutations_without_a_batch_transaction() {
        let error = parse_single_reminder_action(
            r#"<m:ReminderItemActions><t:ReminderItemAction><t:ActionType>Dismiss</t:ActionType><t:ItemId Id="calendar:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2026-08-17T09:00:00Z"/></t:ReminderItemAction><t:ReminderItemAction><t:ActionType>Snooze</t:ActionType><t:NewReminderTime>2026-08-17T10:30:00Z</t:NewReminderTime><t:ItemId Id="task:bbbbbbbb-cccc-dddd-eeee-ffffffffffff:2026-08-17T09:00:00Z"/></t:ReminderItemAction></m:ReminderItemActions>"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("exactly one ReminderItemAction"));
    }
}
