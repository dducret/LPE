use anyhow::Result;
use lpe_core::sieve::{parse_script, Action, MatchType, Statement, Test};
use sqlx::Row;

use crate::{EmailTraceResult, EmailTraceRow, MailFlowEntry, MailFlowRow};

pub(super) fn map_mail_flow_row(row: MailFlowRow) -> MailFlowEntry {
    MailFlowEntry {
        queue_id: row.queue_id,
        message_id: row.message_id,
        account_email: row.account_email,
        subject: row.subject,
        internet_message_id: row.internet_message_id,
        status: row.status,
        delivery_status: row.delivery_status,
        was_submitted: row.was_submitted,
        in_sent_mailbox: row.in_sent_mailbox,
        attempts: row.attempts.max(0) as u32,
        submitted_at: row.submitted_at,
        sent_at: row.sent_at,
        last_attempt_at: row.last_attempt_at,
        next_attempt_at: row.next_attempt_at,
        trace_id: row.trace_id,
        remote_message_ref: row.remote_message_ref,
        last_error: row.last_error,
        retry_after_seconds: row.retry_after_seconds,
        retry_policy: row.retry_policy,
        last_dsn_status: row.last_dsn_status,
        last_smtp_code: row.last_smtp_code,
        last_enhanced_status: row.last_enhanced_status,
    }
}

pub(super) fn map_email_trace_row(row: EmailTraceRow) -> EmailTraceResult {
    EmailTraceResult {
        message_id: row.message_id,
        internet_message_id: row.internet_message_id,
        subject: row.subject,
        sender: row.sender,
        account_email: row.account_email,
        mailbox: row.mailbox,
        delivery_status: row.delivery_status,
        was_submitted: row.was_submitted,
        in_sent_mailbox: row.in_sent_mailbox,
        sent_at: row.sent_at,
        queue_status: row.queue_status,
        latest_trace_id: row.latest_trace_id,
        remote_message_ref: row.remote_message_ref,
        last_attempt_at: row.last_attempt_at,
        next_attempt_at: row.next_attempt_at,
        last_error: row.last_error,
        last_dsn_status: row.last_dsn_status,
        last_smtp_code: row.last_smtp_code,
        last_enhanced_status: row.last_enhanced_status,
        received_at: row.received_at,
    }
}

pub(super) fn mailbox_rule_summaries(content: &str) -> (String, String) {
    match parse_script(content) {
        Ok(script) => (
            summarize_statements_conditions(&script.statements),
            summarize_statements_actions(&script.statements),
        ),
        Err(_) => (
            "unsupported sieve".to_string(),
            "unsupported sieve".to_string(),
        ),
    }
}

fn summarize_statements_conditions(statements: &[Statement]) -> String {
    let mut parts = Vec::new();
    collect_statement_conditions(statements, &mut parts);
    if parts.is_empty() {
        "always".to_string()
    } else {
        parts.join("; ")
    }
}

fn collect_statement_conditions(statements: &[Statement], parts: &mut Vec<String>) {
    for statement in statements {
        if let Statement::If {
            branches,
            else_block,
        } = statement
        {
            for (test, block) in branches {
                parts.push(summarize_test(test));
                collect_statement_conditions(block, parts);
            }
            if let Some(block) = else_block {
                collect_statement_conditions(block, parts);
            }
        }
    }
}

fn summarize_test(test: &Test) -> String {
    match test {
        Test::True => "always".to_string(),
        Test::False => "never".to_string(),
        Test::Header {
            match_type,
            fields,
            keys,
        } => format!(
            "header {} {} {}",
            fields.join(","),
            summarize_match_type(*match_type),
            keys.join(",")
        ),
        Test::Address {
            match_type,
            fields,
            keys,
        } => format!(
            "address {} {} {}",
            fields.join(","),
            summarize_match_type(*match_type),
            keys.join(",")
        ),
        Test::Envelope {
            match_type,
            parts,
            keys,
        } => format!(
            "envelope {} {} {}",
            parts.join(","),
            summarize_match_type(*match_type),
            keys.join(",")
        ),
        Test::AllOf(tests) => format!(
            "all of ({})",
            tests
                .iter()
                .map(summarize_test)
                .collect::<Vec<_>>()
                .join("; ")
        ),
        Test::AnyOf(tests) => format!(
            "any of ({})",
            tests
                .iter()
                .map(summarize_test)
                .collect::<Vec<_>>()
                .join("; ")
        ),
        Test::Not(test) => format!("not ({})", summarize_test(test)),
    }
}

fn summarize_match_type(match_type: MatchType) -> &'static str {
    match match_type {
        MatchType::Is => "is",
        MatchType::Contains => "contains",
    }
}

fn summarize_statements_actions(statements: &[Statement]) -> String {
    let mut parts = Vec::new();
    collect_statement_actions(statements, &mut parts);
    if parts.is_empty() {
        "keep".to_string()
    } else {
        parts.join("; ")
    }
}

fn collect_statement_actions(statements: &[Statement], parts: &mut Vec<String>) {
    for statement in statements {
        match statement {
            Statement::Action(action) => parts.push(summarize_action(action)),
            Statement::If {
                branches,
                else_block,
            } => {
                for (_, block) in branches {
                    collect_statement_actions(block, parts);
                }
                if let Some(block) = else_block {
                    collect_statement_actions(block, parts);
                }
            }
        }
    }
}

fn summarize_action(action: &Action) -> String {
    match action {
        Action::Keep => "keep".to_string(),
        Action::Discard => "discard".to_string(),
        Action::FileInto(mailbox) => format!("fileinto {mailbox}"),
        Action::Redirect(target) => format!("redirect {target}"),
        Action::Vacation { .. } => "vacation".to_string(),
        Action::Stop => "stop".to_string(),
    }
}

pub(super) fn unsupported_exchange_rule_features() -> Vec<String> {
    [
        "client_only_rules",
        "deferred_action_messages",
        "exchange_rule_action_blobs",
        "provider_specific_conditions",
        "delegate_rule_templates",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(super) fn unsupported_client_local_profile_state() -> Vec<String> {
    [
        "client_local_pst_files",
        "client_local_ost_cache",
        "windows_profile_registry",
        "full_exchange_profile_blobs",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(super) fn count_from_row(row: &sqlx::postgres::PgRow, column: &str) -> Result<u64> {
    Ok(row.try_get::<i64, _>(column)?.max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::{map_email_trace_row, map_mail_flow_row};
    use crate::{EmailTraceRow, MailFlowRow};
    use uuid::Uuid;

    #[test]
    fn mail_flow_mapping_keeps_explicit_submission_and_sent_signals() {
        let entry = map_mail_flow_row(MailFlowRow {
            queue_id: Uuid::nil(),
            message_id: Uuid::nil(),
            account_email: "alice@example.test".to_string(),
            subject: "Queued message".to_string(),
            internet_message_id: Some("<msg@example.test>".to_string()),
            status: "deferred".to_string(),
            delivery_status: "deferred".to_string(),
            was_submitted: true,
            in_sent_mailbox: true,
            attempts: -2,
            submitted_at: "2026-04-23T08:00:00Z".to_string(),
            sent_at: Some("2026-04-23T08:00:00Z".to_string()),
            last_attempt_at: Some("2026-04-23T08:05:00Z".to_string()),
            next_attempt_at: Some("2026-04-23T08:10:00Z".to_string()),
            trace_id: Some("trace-1".to_string()),
            remote_message_ref: Some("remote-1".to_string()),
            last_error: Some("temporary failure".to_string()),
            retry_after_seconds: Some(300),
            retry_policy: Some("deferred-backoff".to_string()),
            last_dsn_status: Some("4.4.1".to_string()),
            last_smtp_code: Some(451),
            last_enhanced_status: Some("4.4.1".to_string()),
        });

        assert!(entry.was_submitted);
        assert!(entry.in_sent_mailbox);
        assert_eq!(entry.attempts, 0);
        assert_eq!(entry.trace_id.as_deref(), Some("trace-1"));
    }

    #[test]
    fn email_trace_mapping_surfaces_latest_queue_state() {
        let entry = map_email_trace_row(EmailTraceRow {
            message_id: Uuid::nil(),
            internet_message_id: Some("<msg@example.test>".to_string()),
            subject: "Relay result".to_string(),
            sender: "alice@example.test".to_string(),
            account_email: "alice@example.test".to_string(),
            mailbox: "Sent".to_string(),
            delivery_status: "relayed".to_string(),
            was_submitted: true,
            in_sent_mailbox: true,
            sent_at: Some("2026-04-23T08:00:00Z".to_string()),
            queue_status: Some("relayed".to_string()),
            latest_trace_id: Some("trace-2".to_string()),
            remote_message_ref: Some("remote-2".to_string()),
            last_attempt_at: Some("2026-04-23T08:01:00Z".to_string()),
            next_attempt_at: None,
            last_error: None,
            last_dsn_status: None,
            last_smtp_code: Some(250),
            last_enhanced_status: Some("2.0.0".to_string()),
            received_at: "2026-04-23T08:00:00Z".to_string(),
        });

        assert_eq!(entry.queue_status.as_deref(), Some("relayed"));
        assert_eq!(entry.latest_trace_id.as_deref(), Some("trace-2"));
        assert!(entry.in_sent_mailbox);
    }
}
