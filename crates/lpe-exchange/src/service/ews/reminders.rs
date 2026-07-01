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
            let action = element_text(request, "ActionType")
                .or_else(|| element_text(request, "ReminderItemActionType"))
                .or_else(|| element_text(request, "ReminderAction"))
                .unwrap_or_default();
            let action = if action.is_empty() {
                "Dismiss".to_string()
            } else {
                action
            };
            if !action.eq_ignore_ascii_case("Dismiss") && !action.eq_ignore_ascii_case("Snooze") {
                bail!("PerformReminderAction currently supports only Dismiss and Snooze.");
            }
            let snooze_until = if action.eq_ignore_ascii_case("Snooze") {
                Some(
                    element_text(request, "NewReminderTime")
                        .or_else(|| element_text(request, "SnoozeUntil"))
                        .or_else(|| element_text(request, "ReminderTime"))
                        .filter(|value| !value.trim().is_empty())
                        .ok_or_else(|| {
                            anyhow!("PerformReminderAction Snooze requires a new reminder time.")
                        })?,
                )
            } else {
                None
            };
            let reminder_ids = requested_item_ids(request);
            if reminder_ids.is_empty() {
                bail!("PerformReminderAction requires reminder ItemId values.");
            }
            for reminder_id in reminder_ids {
                let parsed = parse_reminder_item_id(&reminder_id)
                    .ok_or_else(|| anyhow!("unsupported reminder ItemId `{reminder_id}`"))?;
                match parsed.source_type.as_str() {
                    "mail" | "message" => {
                        self.store
                            .update_jmap_email_followup_flags(
                                principal.account_id,
                                parsed.source_id,
                                JmapEmailFollowupUpdate {
                                    reminder_dismissed_at: if snooze_until.is_none() {
                                        Some("now".to_string())
                                    } else {
                                        None
                                    },
                                    reminder_at: snooze_until.clone(),
                                    reminder_set: snooze_until.as_ref().map(|_| true),
                                    ..JmapEmailFollowupUpdate::default()
                                },
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "ews-perform-reminder-action".to_string(),
                                    subject: parsed.source_id.to_string(),
                                },
                            )
                            .await?;
                    }
                    "calendar" => {
                        if let Some(reminder_at) = snooze_until.clone() {
                            self.store
                                .update_accessible_event_reminder(
                                    principal.account_id,
                                    parsed.source_id,
                                    Some(true),
                                    Some(reminder_at),
                                    None,
                                )
                                .await?;
                        } else {
                            self.store
                                .dismiss_reminder_occurrence(
                                    principal.account_id,
                                    &parsed.source_type,
                                    parsed.source_id,
                                    parsed.occurrence_start_at.as_deref(),
                                    "now",
                                )
                                .await?;
                        }
                    }
                    "task" => {
                        if let Some(reminder_at) = snooze_until.clone() {
                            self.store
                                .update_accessible_task_reminder(
                                    principal.account_id,
                                    parsed.source_id,
                                    Some(true),
                                    Some(reminder_at),
                                    None,
                                    Some(true),
                                )
                                .await?;
                        } else {
                            self.store
                                .dismiss_reminder_occurrence(
                                    principal.account_id,
                                    &parsed.source_type,
                                    parsed.source_id,
                                    parsed.occurrence_start_at.as_deref(),
                                    "now",
                                )
                                .await?;
                        }
                    }
                    _ => bail!("unsupported reminder source `{}`", parsed.source_type),
                }
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

#[derive(Debug, Clone)]
pub(in crate::service) struct ParsedReminderItemId {
    pub(in crate::service) source_type: String,
    pub(in crate::service) source_id: Uuid,
    pub(in crate::service) occurrence_start_at: Option<String>,
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
