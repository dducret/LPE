use std::collections::BTreeMap;

use anyhow::{bail, Result};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{
    calendar_attendee_labels, normalize_calendar_meeting_uid, parse_calendar_participants_metadata,
    serialize_calendar_participants_metadata, AuditEntryInput, CalendarMeetingResponse,
    CanonicalChangeCategory, Storage,
};

use super::{calendar_event_organizer_matches_mailbox, calendar_response_organizer_matches};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MeetingResponseOutcome {
    Applied,
    Superseded,
    Idempotent,
    IgnoredOrganizerMismatch,
    IgnoredNoCandidate,
    IgnoredAmbiguousCandidate,
    IgnoredInvalidDurableState,
}

impl MeetingResponseOutcome {
    pub(super) fn server_processed(self) -> bool {
        matches!(self, Self::Applied | Self::Superseded | Self::Idempotent)
    }

    fn audit_action(self) -> &'static str {
        match self {
            Self::Applied => "calendar.meeting-response.applied",
            Self::Superseded => "calendar.meeting-response.superseded",
            Self::Idempotent => "calendar.meeting-response.idempotent",
            Self::IgnoredOrganizerMismatch => {
                "calendar.meeting-response.ignored-organizer-mismatch"
            }
            Self::IgnoredNoCandidate => "calendar.meeting-response.ignored-no-candidate",
            Self::IgnoredAmbiguousCandidate => {
                "calendar.meeting-response.ignored-ambiguous-candidate"
            }
            Self::IgnoredInvalidDurableState => {
                "calendar.meeting-response.ignored-invalid-durable-state"
            }
        }
    }
}

impl Storage {
    pub(super) async fn apply_calendar_meeting_response_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        organizer_account_id: Uuid,
        organizer_email: &str,
        response: &CalendarMeetingResponse,
    ) -> Result<MeetingResponseOutcome> {
        if !calendar_response_organizer_matches(
            response
                .organizer
                .as_ref()
                .map(|organizer| organizer.email.as_str()),
            organizer_email,
        ) {
            return Ok(MeetingResponseOutcome::IgnoredOrganizerMismatch);
        }
        let (meeting_start, meeting_end) = if response.method == "COUNTER" {
            (
                response.original_start.as_deref(),
                response.original_end.as_deref(),
            )
        } else {
            (
                response.meeting_start.as_deref(),
                response.meeting_end.as_deref(),
            )
        };
        let event_rows = sqlx::query(
            r#"
            SELECT id, calendar_id, uid, sequence,
                   organizer_json::text AS organizer_json,
                   attendees_json::text AS attendees_json,
                   meeting_response_state_json::text AS meeting_response_state_json
            FROM calendar_events
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND uid = $3
              AND lifecycle_state = 'active'
              AND status <> 'cancelled'
              AND exception_for_event_id IS NULL
              AND exception_recurrence_id IS NULL
              AND ($4::integer IS NULL OR sequence = $4)
              AND ($5::timestamptz IS NULL OR starts_at = $5::timestamptz)
              AND ($6::timestamptz IS NULL OR ends_at = $6::timestamptz)
            ORDER BY id
            FOR UPDATE
            "#,
        )
        .bind(tenant_id)
        .bind(organizer_account_id)
        .bind(normalize_calendar_meeting_uid(&response.uid))
        .bind(response.meeting_sequence)
        .bind(meeting_start)
        .bind(meeting_end)
        .fetch_all(&mut **tx)
        .await?;
        let mut candidates = Vec::new();
        for event in event_rows {
            let attendees_json: String = event.try_get("attendees_json")?;
            let participants = parse_calendar_participants_metadata(&attendees_json);
            let organizer_json: String = event.try_get("organizer_json")?;
            if !calendar_event_organizer_matches_mailbox(
                &participants,
                &organizer_json,
                organizer_email,
            ) {
                continue;
            }
            let attendee_indices = participants
                .attendees
                .iter()
                .enumerate()
                .filter_map(|(index, attendee)| {
                    (!attendee.email.is_empty()
                        && attendee
                            .email
                            .eq_ignore_ascii_case(&response.attendee_email))
                    .then_some(index)
                })
                .collect::<Vec<_>>();
            let [attendee_index] = attendee_indices.as_slice() else {
                continue;
            };
            candidates.push((event, participants, *attendee_index));
        }
        let (event, mut participants, attendee_index) = match candidates.len() {
            0 => return Ok(MeetingResponseOutcome::IgnoredNoCandidate),
            1 => candidates
                .pop()
                .expect("one meeting-response correlation candidate"),
            _ => return Ok(MeetingResponseOutcome::IgnoredAmbiguousCandidate),
        };
        let event_id: Uuid = event.try_get("id")?;
        let calendar_id: Uuid = event.try_get("calendar_id")?;
        let uid: String = event.try_get("uid")?;
        let event_sequence: i32 = event.try_get("sequence")?;
        let response_state_json: String = event.try_get("meeting_response_state_json")?;
        let Ok(mut response_state) = serde_json::from_str::<
            BTreeMap<String, CalendarMeetingResponseWatermark>,
        >(&response_state_json) else {
            return Ok(MeetingResponseOutcome::IgnoredInvalidDurableState);
        };
        let attendee = &mut participants.attendees[attendee_index];
        let attendee_key = crate::normalize_email(&attendee.email);
        let counter_proposal = response.method == "COUNTER";
        let response_matches_current = attendee.partstat == response.partstat
            && attendee.counter_proposal == counter_proposal
            && attendee.proposed_start == response.proposed_start
            && attendee.proposed_end == response.proposed_end;

        match calendar_response_watermark_decision(
            response_state.get(&attendee_key),
            event_sequence,
            response.meeting_sequence,
            response.response_sent_at.as_deref(),
        ) {
            CalendarResponseWatermarkDecision::Superseded => {
                return Ok(MeetingResponseOutcome::Superseded);
            }
            CalendarResponseWatermarkDecision::Idempotent if response_matches_current => {
                return Ok(MeetingResponseOutcome::Idempotent);
            }
            CalendarResponseWatermarkDecision::Idempotent => {
                return Ok(MeetingResponseOutcome::Superseded);
            }
            CalendarResponseWatermarkDecision::Advance => {}
        }

        attendee.partstat = response.partstat.clone();
        attendee.counter_proposal = counter_proposal;
        attendee.proposed_start = response.proposed_start.clone();
        attendee.proposed_end = response.proposed_end.clone();
        response_state.insert(
            attendee_key,
            CalendarMeetingResponseWatermark {
                sequence: event_sequence,
                sent_at: response.response_sent_at.clone(),
            },
        );
        let attendees = calendar_attendee_labels(&participants);
        let attendees_json = serialize_calendar_participants_metadata(&participants);
        let response_state_json = serde_json::to_string(&response_state)?;
        sqlx::query(
            r#"
            UPDATE calendar_events
            SET attendees_json = $4::jsonb,
                source_payload_json = jsonb_set(
                    source_payload_json,
                    '{attendees}',
                    to_jsonb($5::text),
                    TRUE
                ),
                meeting_response_state_json = $6::jsonb
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(organizer_account_id)
        .bind(event_id)
        .bind(&attendees_json)
        .bind(attendees)
        .bind(response_state_json)
        .execute(&mut **tx)
        .await?;
        let modseq = self
            .allocate_account_modseq_in_tx(
                tx,
                tenant_id,
                organizer_account_id,
                CanonicalChangeCategory::Calendar.as_str(),
            )
            .await?;
        self.advance_calendar_event_version_in_tx(
            tx,
            tenant_id,
            organizer_account_id,
            event_id,
            modseq,
        )
        .await?;
        let affected_principals = Self::calendar_event_affected_principals_in_tx(
            tx,
            tenant_id,
            organizer_account_id,
            event_id,
        )
        .await?;
        Self::insert_mail_change_log_in_tx(
            tx,
            tenant_id,
            Some(organizer_account_id),
            None,
            "calendar_event",
            event_id,
            "updated",
            modseq,
            &affected_principals,
            serde_json::json!({
                "collectionId": calendar_id,
                "objectUid": uid,
                "meetingResponse": response.method,
                "counterProposal": counter_proposal,
            }),
        )
        .await?;
        Self::emit_collaboration_change(
            tx,
            tenant_id,
            CanonicalChangeCategory::Calendar,
            organizer_account_id,
        )
        .await?;
        Ok(MeetingResponseOutcome::Applied)
    }

    pub(super) async fn record_calendar_meeting_response_outcome_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        message_id: Uuid,
        outcome: MeetingResponseOutcome,
    ) -> Result<()> {
        self.insert_audit(
            tx,
            tenant_id,
            AuditEntryInput {
                actor: "lpe-core".to_string(),
                action: outcome.audit_action().to_string(),
                subject: format!("message:{message_id}"),
            },
        )
        .await
    }

    pub(super) async fn mark_inbound_calendar_response_processed_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        message_id: Uuid,
    ) -> Result<()> {
        let message = sqlx::query(
            r#"
            UPDATE messages
            SET calendar_response_processed = TRUE
            WHERE tenant_id = $1
              AND id = $2
              AND authorized_calendar_response_content_sha256 IS NOT NULL
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .execute(&mut **tx)
        .await?;
        let classification = sqlx::query(
            r#"
            UPDATE calendar_mail_classifications
            SET metadata_json = jsonb_set(
                    metadata_json,
                    '{response,server_processed}',
                    'true'::jsonb,
                    TRUE
                ),
                updated_at = NOW()
            WHERE tenant_id = $1
              AND message_id = $2
              AND classification = 'response'
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .execute(&mut **tx)
        .await?;
        if message.rows_affected() != 1 || classification.rows_affected() != 1 {
            bail!("processed calendar response lacks durable authorization or classification");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CalendarMeetingResponseWatermark {
    sequence: i32,
    #[serde(rename = "sentAt", default, skip_serializing_if = "Option::is_none")]
    sent_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CalendarResponseWatermarkDecision {
    Advance,
    Idempotent,
    Superseded,
}

fn calendar_response_watermark_decision(
    stored: Option<&CalendarMeetingResponseWatermark>,
    event_sequence: i32,
    response_sequence: Option<i32>,
    response_sent_at: Option<&str>,
) -> CalendarResponseWatermarkDecision {
    let Some(stored) = stored else {
        return CalendarResponseWatermarkDecision::Advance;
    };
    if event_sequence < stored.sequence
        || (response_sequence.is_none() && event_sequence != stored.sequence)
    {
        return CalendarResponseWatermarkDecision::Superseded;
    }
    if let Some(response_sequence) = response_sequence {
        if response_sequence < stored.sequence {
            return CalendarResponseWatermarkDecision::Superseded;
        }
        if response_sequence > stored.sequence {
            return CalendarResponseWatermarkDecision::Advance;
        }
    }
    match (stored.sent_at.as_deref(), response_sent_at) {
        (None, None) => CalendarResponseWatermarkDecision::Idempotent,
        (None, Some(_)) => CalendarResponseWatermarkDecision::Advance,
        (Some(stored_sent_at), Some(sent_at)) if sent_at > stored_sent_at => {
            CalendarResponseWatermarkDecision::Advance
        }
        (Some(stored_sent_at), Some(sent_at)) if sent_at == stored_sent_at => {
            CalendarResponseWatermarkDecision::Idempotent
        }
        _ => CalendarResponseWatermarkDecision::Superseded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meeting_response_outcomes_mark_only_handled_results_processed() {
        for outcome in [
            MeetingResponseOutcome::Applied,
            MeetingResponseOutcome::Superseded,
            MeetingResponseOutcome::Idempotent,
        ] {
            assert!(outcome.server_processed());
        }
        for outcome in [
            MeetingResponseOutcome::IgnoredOrganizerMismatch,
            MeetingResponseOutcome::IgnoredNoCandidate,
            MeetingResponseOutcome::IgnoredAmbiguousCandidate,
            MeetingResponseOutcome::IgnoredInvalidDurableState,
        ] {
            assert!(!outcome.server_processed());
        }
    }

    #[test]
    fn meeting_response_audit_is_bounded_and_opaque() {
        for outcome in [
            MeetingResponseOutcome::Applied,
            MeetingResponseOutcome::Superseded,
            MeetingResponseOutcome::Idempotent,
            MeetingResponseOutcome::IgnoredOrganizerMismatch,
            MeetingResponseOutcome::IgnoredNoCandidate,
            MeetingResponseOutcome::IgnoredAmbiguousCandidate,
            MeetingResponseOutcome::IgnoredInvalidDurableState,
        ] {
            let action = outcome.audit_action();
            assert!(action.starts_with("calendar.meeting-response."));
            assert!(!action.contains('@'));
            assert!(action.len() <= 64);
        }
    }

    #[test]
    fn meeting_response_watermark_distinguishes_advance_idempotent_and_superseded() {
        use CalendarResponseWatermarkDecision::{Advance, Idempotent, Superseded};

        assert_eq!(
            calendar_response_watermark_decision(None, 0, None, None),
            Advance
        );
        let sequence_zero_without_timestamp = CalendarMeetingResponseWatermark {
            sequence: 0,
            sent_at: None,
        };
        assert_eq!(
            calendar_response_watermark_decision(
                Some(&sequence_zero_without_timestamp),
                0,
                None,
                None,
            ),
            Idempotent
        );
        assert_eq!(
            calendar_response_watermark_decision(
                Some(&sequence_zero_without_timestamp),
                0,
                None,
                Some("2026-08-21T17:00:00Z"),
            ),
            Advance
        );
        let sequence_zero = CalendarMeetingResponseWatermark {
            sequence: 0,
            sent_at: Some("2026-08-21T17:00:00Z".to_string()),
        };
        assert_eq!(
            calendar_response_watermark_decision(
                Some(&sequence_zero),
                0,
                Some(0),
                Some("2026-08-21T17:01:00Z"),
            ),
            Advance
        );
        assert_eq!(
            calendar_response_watermark_decision(
                Some(&sequence_zero),
                0,
                Some(0),
                Some("2026-08-21T17:00:00Z"),
            ),
            Idempotent
        );
        for sent_at in [Some("2026-08-21T16:59:00Z"), None] {
            assert_eq!(
                calendar_response_watermark_decision(Some(&sequence_zero), 0, Some(0), sent_at,),
                Superseded
            );
        }
        assert_eq!(
            calendar_response_watermark_decision(Some(&sequence_zero), 1, Some(1), None,),
            Advance
        );
        let sequence_one = CalendarMeetingResponseWatermark {
            sequence: 1,
            sent_at: Some("2026-08-21T17:00:00Z".to_string()),
        };
        assert_eq!(
            calendar_response_watermark_decision(
                Some(&sequence_one),
                0,
                Some(0),
                Some("2026-08-21T18:00:00Z"),
            ),
            Superseded
        );
    }
}
