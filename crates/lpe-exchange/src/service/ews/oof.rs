use super::super::*;
use crate::ews_types::{EwsExternalAudience, EwsOofState};
use lpe_core::sieve::{Action, Statement};

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn get_user_oof_settings(
        &self,
        principal: &AccountPrincipal,
    ) -> Result<String> {
        let script = self
            .store
            .fetch_active_sieve_script(principal.account_id)
            .await?;
        Ok(get_user_oof_settings_response(&oof_projection_from_script(
            script.as_ref().map(|script| script.content.as_str()),
        )))
    }

    pub(in crate::service) async fn set_user_oof_settings(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let settings = element_content(request, "UserOofSettings")
                .or_else(|| element_content(request, "OofSettings"))
                .unwrap_or(request);
            let state =
                element_text(settings, "OofState").unwrap_or_else(|| "Disabled".to_string());
            match state.trim().to_ascii_lowercase().as_str() {
                "disabled" => {
                    self.store
                        .set_active_sieve_script(
                            principal.account_id,
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-oof-disable".to_string(),
                                subject: principal.account_id.to_string(),
                            },
                        )
                        .await?;
                }
                "enabled" | "scheduled" => {
                    let state = parse_oof_state(&state)?;
                    let message = element_content(settings, "InternalReply")
                        .and_then(|reply| element_text(reply, "Message"))
                        .or_else(|| {
                            element_content(settings, "ExternalReply")
                                .and_then(|reply| element_text(reply, "Message"))
                        })
                        .unwrap_or_default();
                    if message.trim().is_empty() {
                        bail!("OOF message is required when enabling OOF");
                    }
                    let external_audience = normalize_oof_external_audience(
                        &element_text(settings, "ExternalAudience")
                            .unwrap_or_else(|| "All".to_string()),
                    )?;
                    let duration = match state {
                        EwsOofState::Scheduled => Some(parse_oof_duration(settings)?),
                        EwsOofState::Enabled => None,
                        EwsOofState::Disabled => unreachable!("disabled OOF is handled separately"),
                    };
                    self.store
                        .put_sieve_script(
                            principal.account_id,
                            "ews-oof",
                            &vacation_sieve_script(
                                &message,
                                state,
                                external_audience,
                                duration.as_ref(),
                            ),
                            true,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "ews-oof-enable".to_string(),
                                subject: principal.account_id.to_string(),
                            },
                        )
                        .await?;
                }
                other => bail!("unsupported OofState {other}"),
            }
            Ok(set_user_oof_settings_success_response())
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            set_user_oof_settings_error_response("ErrorInvalidOperation", &error.to_string())
        }))
    }
}

#[derive(Debug, Clone)]
pub(in crate::service) struct OofDuration {
    pub(in crate::service) start_time: String,
    pub(in crate::service) end_time: String,
}

#[derive(Debug, Clone)]
pub(in crate::service) struct OofProjection {
    pub(in crate::service) state: EwsOofState,
    pub(in crate::service) external_audience: String,
    pub(in crate::service) text_body: String,
    pub(in crate::service) duration: Option<OofDuration>,
}

impl OofProjection {
    fn disabled() -> Self {
        Self {
            state: EwsOofState::Disabled,
            external_audience: "None".to_string(),
            text_body: String::new(),
            duration: None,
        }
    }
}

pub(in crate::service) fn get_user_oof_settings_response(projection: &OofProjection) -> String {
    let state = projection.state.as_ews();
    let audience = &projection.external_audience;
    let duration = if let Some(duration) = &projection.duration {
        format!(
            concat!(
                "<t:Duration>",
                "<t:StartTime>{start_time}</t:StartTime>",
                "<t:EndTime>{end_time}</t:EndTime>",
                "</t:Duration>"
            ),
            start_time = escape_xml(&duration.start_time),
            end_time = escape_xml(&duration.end_time),
        )
    } else {
        String::new()
    };
    let message = escape_xml(&projection.text_body);
    format!(
        concat!(
            "<m:GetUserOofSettingsResponse>",
            "<m:ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "</m:ResponseMessage>",
            "<t:OofSettings>",
            "<t:OofState>{state}</t:OofState>",
            "<t:ExternalAudience>{audience}</t:ExternalAudience>",
            "{duration}",
            "<t:InternalReply><t:Message>{message}</t:Message></t:InternalReply>",
            "<t:ExternalReply><t:Message>{message}</t:Message></t:ExternalReply>",
            "</t:OofSettings>",
            "<m:AllowExternalOof>{audience}</m:AllowExternalOof>",
            "</m:GetUserOofSettingsResponse>"
        ),
        state = state,
        audience = audience,
        duration = duration,
        message = message,
    )
}

pub(in crate::service) fn set_user_oof_settings_success_response() -> String {
    concat!(
        "<m:SetUserOofSettingsResponse>",
        "<m:ResponseMessage ResponseClass=\"Success\">",
        "<m:ResponseCode>NoError</m:ResponseCode>",
        "</m:ResponseMessage>",
        "</m:SetUserOofSettingsResponse>"
    )
    .to_string()
}

pub(in crate::service) fn parse_oof_state(value: &str) -> Result<EwsOofState> {
    EwsOofState::parse(value)
}

pub(in crate::service) fn parse_oof_duration(settings: &str) -> Result<OofDuration> {
    let duration = element_content(settings, "Duration")
        .ok_or_else(|| anyhow!("Duration is required when OofState is Scheduled"))?;
    let start_time = element_text(duration, "StartTime")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Duration StartTime is required when OofState is Scheduled"))?;
    let end_time = element_text(duration, "EndTime")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Duration EndTime is required when OofState is Scheduled"))?;
    Ok(OofDuration {
        start_time,
        end_time,
    })
}

pub(in crate::service) fn oof_projection_from_script(content: Option<&str>) -> OofProjection {
    let Some(content) = content else {
        return OofProjection::disabled();
    };
    let Ok(script) = lpe_core::sieve::parse_script(content) else {
        return OofProjection::disabled();
    };
    let Some(text_body) = find_vacation_reason(&script.statements) else {
        return OofProjection::disabled();
    };
    let state = match oof_metadata_value(content, "State").as_deref() {
        Some("Scheduled") => EwsOofState::Scheduled,
        Some("Enabled") | None => EwsOofState::Enabled,
        Some("Disabled") => return OofProjection::disabled(),
        Some(_) => EwsOofState::Enabled,
    };
    let external_audience = oof_metadata_value(content, "ExternalAudience")
        .and_then(|value| {
            normalize_oof_external_audience(&value)
                .ok()
                .map(str::to_string)
        })
        .unwrap_or_else(|| "All".to_string());
    let duration = if state == EwsOofState::Scheduled {
        match (
            oof_metadata_value(content, "StartTime"),
            oof_metadata_value(content, "EndTime"),
        ) {
            (Some(start_time), Some(end_time)) => Some(OofDuration {
                start_time,
                end_time,
            }),
            _ => None,
        }
    } else {
        None
    };

    OofProjection {
        state,
        external_audience,
        text_body,
        duration,
    }
}

fn oof_metadata_value(content: &str, name: &str) -> Option<String> {
    let prefix = format!("# LPE-EWS-OOF-{name}:");
    content.lines().find_map(|line| {
        line.trim_start()
            .strip_prefix(&prefix)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn find_vacation_reason(statements: &[Statement]) -> Option<String> {
    for statement in statements {
        match statement {
            Statement::Action(Action::Vacation { reason, .. }) => return Some(reason.clone()),
            Statement::If {
                branches,
                else_block,
            } => {
                for (_, branch) in branches {
                    if let Some(reason) = find_vacation_reason(branch) {
                        return Some(reason);
                    }
                }
                if let Some(else_block) = else_block {
                    if let Some(reason) = find_vacation_reason(else_block) {
                        return Some(reason);
                    }
                }
            }
            _ => {}
        }
    }
    None
}

pub(in crate::service) fn normalize_oof_external_audience(value: &str) -> Result<&'static str> {
    Ok(EwsExternalAudience::parse(value)?.as_ews())
}

pub(in crate::service) fn vacation_sieve_script(
    text_body: &str,
    state: EwsOofState,
    external_audience: &str,
    duration: Option<&OofDuration>,
) -> String {
    let text_body = sieve_quote(text_body.trim());
    let mut script = format!(
        "# LPE-EWS-OOF-State: {}\r\n# LPE-EWS-OOF-ExternalAudience: {}\r\n",
        state.as_ews(),
        external_audience
    );
    if let Some(duration) = duration {
        script.push_str(&format!(
            "# LPE-EWS-OOF-StartTime: {}\r\n# LPE-EWS-OOF-EndTime: {}\r\n",
            duration.start_time, duration.end_time
        ));
    }
    script.push_str(&format!(
        "require [\"vacation\"];\r\nvacation :days 7 \"{text_body}\";\r\n"
    ));
    script
}

fn sieve_quote(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
