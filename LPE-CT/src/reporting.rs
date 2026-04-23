use anyhow::{Context, Result};
use axum::extract::Query;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

use crate::smtp::{self, QuarantineSummary, RuntimeConfig, TraceDetails};

const DIGEST_REPORT_DIR: &str = "digest-reports";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReportingSettings {
    #[serde(default = "default_bool_true")]
    pub digest_enabled: bool,
    #[serde(default = "default_digest_interval_minutes")]
    pub digest_interval_minutes: u32,
    #[serde(default = "default_digest_max_items")]
    pub digest_max_items: u32,
    #[serde(default = "default_history_retention_days")]
    pub history_retention_days: u32,
    #[serde(default)]
    pub domain_defaults: Vec<DigestDomainDefault>,
    #[serde(default)]
    pub user_overrides: Vec<DigestUserOverride>,
    #[serde(default)]
    pub last_digest_run_at: Option<String>,
    #[serde(default)]
    pub next_digest_run_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DigestDomainDefault {
    pub domain: String,
    #[serde(default)]
    pub recipients: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DigestUserOverride {
    pub mailbox: String,
    pub recipient: String,
    #[serde(default = "default_bool_true")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct HistoryQuery {
    pub q: Option<String>,
    pub direction: Option<String>,
    pub queue: Option<String>,
    pub disposition: Option<String>,
    pub domain: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MailHistorySummary {
    pub trace_id: String,
    pub direction: String,
    pub queue: String,
    pub status: String,
    pub latest_event_at: String,
    pub mail_from: String,
    pub rcpt_to: Vec<String>,
    pub subject: String,
    pub internet_message_id: Option<String>,
    pub reason: Option<String>,
    pub route_target: Option<String>,
    pub remote_message_ref: Option<String>,
    pub spam_score: f32,
    pub security_score: f32,
    pub reputation_score: i32,
    pub event_count: usize,
    pub policy_tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MailHistoryResponse {
    pub total: usize,
    pub items: Vec<MailHistorySummary>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MailHistoryEvent {
    pub timestamp: String,
    pub trace_id: String,
    pub direction: String,
    pub queue: String,
    pub status: String,
    pub peer: String,
    pub mail_from: String,
    pub rcpt_to: Vec<String>,
    pub subject: String,
    pub internet_message_id: Option<String>,
    pub reason: Option<String>,
    pub route_target: Option<String>,
    pub remote_message_ref: Option<String>,
    pub spam_score: f32,
    pub security_score: f32,
    pub reputation_score: i32,
    pub dnsbl_hits: Vec<String>,
    pub auth_summary: Value,
    pub magika_summary: Option<String>,
    pub magika_decision: Option<String>,
    pub technical_status: Option<Value>,
    pub dsn: Option<Value>,
    pub throttle: Option<Value>,
    pub decision_trace: Vec<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TraceHistoryDetails {
    pub trace_id: String,
    pub current: Option<TraceDetails>,
    pub history: Vec<MailHistoryEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DigestReportSummary {
    pub report_id: String,
    pub generated_at: String,
    pub scope: String,
    pub scope_label: String,
    pub recipient: String,
    pub item_count: usize,
    pub top_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DigestReportDetails {
    pub summary: DigestReportSummary,
    pub content: String,
    pub items: Vec<QuarantineSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReportingSnapshot {
    pub settings: ReportingSettings,
    pub recent_reports: Vec<DigestReportSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DigestRunResponse {
    pub generated_at: String,
    pub generated_reports: Vec<DigestReportSummary>,
    pub next_digest_run_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct StoredMailHistoryEvent {
    timestamp: String,
    trace_id: String,
    direction: String,
    queue: String,
    status: String,
    peer: String,
    mail_from: String,
    rcpt_to: Vec<String>,
    subject: String,
    internet_message_id: Option<String>,
    reason: Option<String>,
    route_target: Option<String>,
    remote_message_ref: Option<String>,
    spam_score: f32,
    security_score: f32,
    reputation_score: i32,
    #[serde(default)]
    dnsbl_hits: Vec<String>,
    #[serde(default)]
    auth_summary: Value,
    #[serde(default)]
    magika_summary: Option<String>,
    #[serde(default)]
    magika_decision: Option<String>,
    #[serde(default)]
    technical_status: Option<Value>,
    #[serde(default)]
    dsn: Option<Value>,
    #[serde(default)]
    throttle: Option<Value>,
    #[serde(default)]
    decision_trace: Vec<Value>,
}

impl From<StoredMailHistoryEvent> for MailHistoryEvent {
    fn from(value: StoredMailHistoryEvent) -> Self {
        Self {
            timestamp: value.timestamp,
            trace_id: value.trace_id,
            direction: value.direction,
            queue: value.queue,
            status: value.status,
            peer: value.peer,
            mail_from: value.mail_from,
            rcpt_to: value.rcpt_to,
            subject: value.subject,
            internet_message_id: value.internet_message_id,
            reason: value.reason,
            route_target: value.route_target,
            remote_message_ref: value.remote_message_ref,
            spam_score: value.spam_score,
            security_score: value.security_score,
            reputation_score: value.reputation_score,
            dnsbl_hits: value.dnsbl_hits,
            auth_summary: value.auth_summary,
            magika_summary: value.magika_summary,
            magika_decision: value.magika_decision,
            technical_status: value.technical_status,
            dsn: value.dsn,
            throttle: value.throttle,
            decision_trace: value.decision_trace,
        }
    }
}

pub(crate) fn default_reporting_settings() -> ReportingSettings {
    ReportingSettings {
        digest_enabled: true,
        digest_interval_minutes: default_digest_interval_minutes(),
        digest_max_items: default_digest_max_items(),
        history_retention_days: default_history_retention_days(),
        domain_defaults: Vec::new(),
        user_overrides: Vec::new(),
        last_digest_run_at: None,
        next_digest_run_at: Some(timestamp_from_now(default_digest_interval_minutes() as u64 * 60)),
    }
}

pub(crate) fn default_digest_interval_minutes() -> u32 {
    360
}

pub(crate) fn default_digest_max_items() -> u32 {
    25
}

pub(crate) fn default_history_retention_days() -> u32 {
    30
}

pub(crate) fn normalize_reporting_settings(settings: &mut ReportingSettings) {
    if settings.digest_interval_minutes == 0 {
        settings.digest_interval_minutes = default_digest_interval_minutes();
    }
    if settings.digest_max_items == 0 {
        settings.digest_max_items = default_digest_max_items();
    }
    if settings.history_retention_days == 0 {
        settings.history_retention_days = default_history_retention_days();
    }

    settings.domain_defaults = normalize_domain_defaults(&settings.domain_defaults);
    settings.user_overrides = normalize_user_overrides(&settings.user_overrides);

    if settings.next_digest_run_at.is_none() {
        settings.next_digest_run_at =
            Some(timestamp_from_now(settings.digest_interval_minutes as u64 * 60));
    }
}

pub(crate) fn snapshot(
    spool_dir: &Path,
    settings: &ReportingSettings,
) -> Result<ReportingSnapshot> {
    Ok(ReportingSnapshot {
        settings: settings.clone(),
        recent_reports: list_recent_digest_reports(spool_dir, 10)?,
    })
}

pub(crate) fn run_due_digest_generation(
    spool_dir: &Path,
    settings: &mut ReportingSettings,
) -> Result<Vec<DigestReportSummary>> {
    normalize_reporting_settings(settings);
    if !settings.digest_enabled || !digest_is_due(settings) {
        return Ok(Vec::new());
    }
    run_digest_generation(spool_dir, settings)
}

pub(crate) fn run_digest_generation(
    spool_dir: &Path,
    settings: &mut ReportingSettings,
) -> Result<Vec<DigestReportSummary>> {
    normalize_reporting_settings(settings);
    let quarantine = smtp::list_quarantine_items(spool_dir, smtp::QuarantineQuery::default())?;
    let generated_at = current_timestamp();
    let mut reports = Vec::new();

    for digest in &settings.domain_defaults {
        let items = filter_quarantine_for_domain(&quarantine, &digest.domain, settings.digest_max_items);
        if items.is_empty() {
            continue;
        }
        for recipient in &digest.recipients {
            let detail = build_digest_report(
                spool_dir,
                &generated_at,
                "domain-default",
                &digest.domain,
                recipient,
                items.clone(),
            )?;
            reports.push(detail.summary);
        }
    }

    for override_entry in settings.user_overrides.iter().filter(|entry| entry.enabled) {
        let items =
            filter_quarantine_for_mailbox(&quarantine, &override_entry.mailbox, settings.digest_max_items);
        if items.is_empty() {
            continue;
        }
        let detail = build_digest_report(
            spool_dir,
            &generated_at,
            "user-override",
            &override_entry.mailbox,
            &override_entry.recipient,
            items,
        )?;
        reports.push(detail.summary);
    }

    settings.last_digest_run_at = Some(generated_at.clone());
    settings.next_digest_run_at =
        Some(timestamp_from_now(settings.digest_interval_minutes as u64 * 60));

    Ok(reports)
}

pub(crate) fn list_recent_digest_reports(
    spool_dir: &Path,
    limit: usize,
) -> Result<Vec<DigestReportSummary>> {
    let dir = digest_report_dir(spool_dir);
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut reports = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let detail: DigestReportDetails = serde_json::from_str(&fs::read_to_string(entry.path())?)?;
        reports.push(detail.summary);
    }
    reports.sort_by(|left, right| right.generated_at.cmp(&left.generated_at));
    reports.truncate(limit);
    Ok(reports)
}

pub(crate) fn load_digest_report(
    spool_dir: &Path,
    report_id: &str,
) -> Result<Option<DigestReportDetails>> {
    let path = digest_report_dir(spool_dir).join(format!("{report_id}.json"));
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str(&fs::read_to_string(path)?)?))
}

pub(crate) async fn search_mail_history(
    spool_dir: &Path,
    config: &RuntimeConfig,
    Query(query): Query<HistoryQuery>,
    retention_days: u32,
) -> Result<MailHistoryResponse> {
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    if let Some(response) = search_mail_history_from_db(config, &query, retention_days, limit).await? {
        return Ok(response);
    }
    let events = read_mail_history_events(spool_dir, config, retention_days).await?;
    let grouped = group_history(events);
    let mut items = grouped
        .into_values()
        .filter_map(|events| summarize_trace_history(events))
        .filter(|summary| history_matches(summary, &query))
        .collect::<Vec<_>>();
    items.sort_by(|left, right| right.latest_event_at.cmp(&left.latest_event_at));
    let total = items.len();
    items.truncate(limit);
    Ok(MailHistoryResponse { total, items })
}

pub(crate) async fn load_trace_history(
    spool_dir: &Path,
    config: &RuntimeConfig,
    trace_id: &str,
    retention_days: u32,
) -> Result<TraceHistoryDetails> {
    if let Some(details) =
        load_trace_history_from_db(spool_dir, config, trace_id, retention_days).await?
    {
        return Ok(details);
    }
    let history = read_mail_history_events(spool_dir, config, retention_days)
        .await?
        .into_iter()
        .filter(|event| event.trace_id == trace_id)
        .collect::<Vec<_>>();
    Ok(TraceHistoryDetails {
        trace_id: trace_id.to_string(),
        current: smtp::load_trace_details(spool_dir, trace_id)?,
        history,
    })
}

async fn read_mail_history_events(
    spool_dir: &Path,
    _config: &RuntimeConfig,
    retention_days: u32,
) -> Result<Vec<MailHistoryEvent>> {
    read_mail_history_events_from_jsonl(spool_dir, retention_days)
}

fn read_mail_history_events_from_jsonl(
    spool_dir: &Path,
    retention_days: u32,
) -> Result<Vec<MailHistoryEvent>> {
    let path = spool_dir.join("policy").join("transport-audit.jsonl");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let cutoff = current_unix_timestamp().saturating_sub(retention_days.max(1) as u64 * 86_400);
    let mut events = Vec::new();
    for line in fs::read_to_string(path)?.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let event: StoredMailHistoryEvent =
            serde_json::from_str(line).with_context(|| "unable to parse transport audit line")?;
        if parse_unix_timestamp(&event.timestamp).unwrap_or(0) < cutoff {
            continue;
        }
        events.push(event.into());
    }
    Ok(events)
}

async fn search_mail_history_from_db(
    config: &RuntimeConfig,
    query: &HistoryQuery,
    retention_days: u32,
    limit: usize,
) -> Result<Option<MailHistoryResponse>> {
    let Some(pool) = smtp::ensure_local_db_schema(config).await? else {
        return Ok(None);
    };

    let cutoff = history_cutoff(retention_days);
    let direction = normalized(query.direction.as_deref());
    let queue = normalized(query.queue.as_deref());
    let disposition = normalized(query.disposition.as_deref());
    let domain = normalized(query.domain.as_deref());
    let search_term = normalized(query.q.as_deref());
    let search_pattern = search_term.as_ref().map(|value| format!("%{value}%"));
    let total: i64 = sqlx::query_scalar(
        r#"
        WITH latest AS (
            SELECT DISTINCT ON (trace_id)
                   trace_id, event_unix, direction, queue, status, mail_from, rcpt_to
              FROM mail_flow_history
             WHERE event_unix >= $1
             ORDER BY trace_id, event_unix DESC
        )
        SELECT COUNT(*)
          FROM latest
         WHERE ($2::TEXT IS NULL OR LOWER(latest.direction) = $2)
           AND ($3::TEXT IS NULL OR LOWER(latest.queue) = $3)
           AND ($4::TEXT IS NULL OR LOWER(latest.status) = $4)
           AND (
                $5::TEXT IS NULL
                OR SPLIT_PART(LOWER(latest.mail_from), '@', 2) = $5
                OR EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements_text(latest.rcpt_to) AS recipient(value)
                     WHERE SPLIT_PART(LOWER(recipient.value), '@', 2) = $5
                )
           )
           AND (
                $6::TEXT IS NULL
                OR EXISTS (
                    SELECT 1
                      FROM mail_flow_history matched
                     WHERE matched.trace_id = latest.trace_id
                       AND matched.event_unix >= $1
                       AND (
                            matched.search_text LIKE $6
                            OR to_tsvector('simple', matched.search_text) @@ websearch_to_tsquery('simple', $7)
                       )
                )
           )
        "#,
    )
    .bind(cutoff)
    .bind(direction.clone())
    .bind(queue.clone())
    .bind(disposition.clone())
    .bind(domain.clone())
    .bind(search_pattern.clone())
    .bind(search_term.clone())
    .fetch_one(pool)
    .await?;

    let rows = sqlx::query(
        r#"
        WITH latest AS (
            SELECT DISTINCT ON (trace_id)
                   trace_id, event_unix, timestamp, direction, queue, status, mail_from, rcpt_to,
                   subject, internet_message_id, reason, route_target, remote_message_ref,
                   spam_score, security_score, reputation_score, dnsbl_hits, auth_summary,
                   magika_summary, magika_decision, technical_status, dsn, throttle,
                   decision_trace, search_text
              FROM mail_flow_history
             WHERE event_unix >= $1
             ORDER BY trace_id, event_unix DESC
        ),
        counts AS (
            SELECT trace_id, COUNT(*)::BIGINT AS event_count
              FROM mail_flow_history
             WHERE event_unix >= $1
             GROUP BY trace_id
        )
        SELECT latest.trace_id, latest.timestamp, latest.direction, latest.queue, latest.status,
               latest.mail_from, latest.rcpt_to, latest.subject, latest.internet_message_id,
               latest.reason, latest.route_target, latest.remote_message_ref, latest.spam_score,
               latest.security_score, latest.reputation_score, latest.dnsbl_hits,
               latest.auth_summary, latest.magika_summary, latest.magika_decision,
               latest.technical_status, latest.dsn, latest.throttle, latest.decision_trace,
               counts.event_count
          FROM latest
          JOIN counts ON counts.trace_id = latest.trace_id
         WHERE ($2::TEXT IS NULL OR LOWER(latest.direction) = $2)
           AND ($3::TEXT IS NULL OR LOWER(latest.queue) = $3)
           AND ($4::TEXT IS NULL OR LOWER(latest.status) = $4)
           AND (
                $5::TEXT IS NULL
                OR SPLIT_PART(LOWER(latest.mail_from), '@', 2) = $5
                OR EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements_text(latest.rcpt_to) AS recipient(value)
                     WHERE SPLIT_PART(LOWER(recipient.value), '@', 2) = $5
                )
           )
           AND (
                $6::TEXT IS NULL
                OR EXISTS (
                    SELECT 1
                      FROM mail_flow_history matched
                     WHERE matched.trace_id = latest.trace_id
                       AND matched.event_unix >= $1
                       AND (
                            matched.search_text LIKE $6
                            OR to_tsvector('simple', matched.search_text) @@ websearch_to_tsquery('simple', $7)
                       )
                )
           )
         ORDER BY latest.event_unix DESC
         LIMIT $8
        "#,
    )
    .bind(cutoff)
    .bind(direction)
    .bind(queue)
    .bind(disposition)
    .bind(domain)
    .bind(search_pattern)
    .bind(search_term)
    .bind(limit as i64)
    .fetch_all(pool)
    .await?;

    let items = rows
        .into_iter()
        .map(|row| {
            let event = mail_history_event_from_row(&row)?;
            let event_count = row.try_get::<i64, _>("event_count")? as usize;
            Ok::<MailHistorySummary, anyhow::Error>(MailHistorySummary {
                trace_id: event.trace_id.clone(),
                direction: event.direction.clone(),
                queue: event.queue.clone(),
                status: event.status.clone(),
                latest_event_at: event.timestamp.clone(),
                mail_from: event.mail_from.clone(),
                rcpt_to: event.rcpt_to.clone(),
                subject: event.subject.clone(),
                internet_message_id: event.internet_message_id.clone(),
                reason: event.reason.clone(),
                route_target: event.route_target.clone(),
                remote_message_ref: event.remote_message_ref.clone(),
                spam_score: event.spam_score,
                security_score: event.security_score,
                reputation_score: event.reputation_score,
                event_count,
                policy_tags: policy_tags_from_event(&event),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(MailHistoryResponse {
        total: total as usize,
        items,
    }))
}

async fn load_trace_history_from_db(
    spool_dir: &Path,
    config: &RuntimeConfig,
    trace_id: &str,
    retention_days: u32,
) -> Result<Option<TraceHistoryDetails>> {
    let Some(pool) = smtp::ensure_local_db_schema(config).await? else {
        return Ok(None);
    };

    let rows = sqlx::query(
        r#"
        SELECT timestamp, trace_id, direction, queue, status, peer, mail_from, rcpt_to, subject,
               internet_message_id, reason, route_target, remote_message_ref, spam_score,
               security_score, reputation_score, dnsbl_hits, auth_summary, magika_summary,
               magika_decision, technical_status, dsn, throttle, decision_trace
          FROM mail_flow_history
         WHERE trace_id = $1
           AND event_unix >= $2
         ORDER BY event_unix ASC
        "#,
    )
    .bind(trace_id)
    .bind(history_cutoff(retention_days))
    .fetch_all(pool)
    .await?;

    let history = rows
        .into_iter()
        .map(|row| mail_history_event_from_row(&row))
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(TraceHistoryDetails {
        trace_id: trace_id.to_string(),
        current: smtp::load_trace_details(spool_dir, trace_id)?,
        history,
    }))
}

fn mail_history_event_from_row(row: &sqlx::postgres::PgRow) -> Result<MailHistoryEvent> {
    Ok(MailHistoryEvent {
        timestamp: row.try_get("timestamp")?,
        trace_id: row.try_get("trace_id")?,
        direction: row.try_get("direction")?,
        queue: row.try_get("queue")?,
        status: row.try_get("status")?,
        peer: row.try_get("peer").unwrap_or_default(),
        mail_from: row.try_get("mail_from")?,
        rcpt_to: row.try_get::<sqlx::types::Json<Vec<String>>, _>("rcpt_to")?.0,
        subject: row.try_get("subject")?,
        internet_message_id: row.try_get("internet_message_id")?,
        reason: row.try_get("reason")?,
        route_target: row.try_get("route_target")?,
        remote_message_ref: row.try_get("remote_message_ref")?,
        spam_score: row.try_get("spam_score")?,
        security_score: row.try_get("security_score")?,
        reputation_score: row.try_get("reputation_score")?,
        dnsbl_hits: row.try_get::<sqlx::types::Json<Vec<String>>, _>("dnsbl_hits")?.0,
        auth_summary: row.try_get::<sqlx::types::Json<Value>, _>("auth_summary")?.0,
        magika_summary: row.try_get("magika_summary")?,
        magika_decision: row.try_get("magika_decision")?,
        technical_status: row
            .try_get::<Option<sqlx::types::Json<Value>>, _>("technical_status")?
            .map(|value| value.0),
        dsn: row
            .try_get::<Option<sqlx::types::Json<Value>>, _>("dsn")?
            .map(|value| value.0),
        throttle: row
            .try_get::<Option<sqlx::types::Json<Value>>, _>("throttle")?
            .map(|value| value.0),
        decision_trace: row.try_get::<sqlx::types::Json<Vec<Value>>, _>("decision_trace")?.0,
    })
}

fn history_cutoff(retention_days: u32) -> i64 {
    current_unix_timestamp().saturating_sub(retention_days.max(1) as u64 * 86_400) as i64
}

fn group_history(events: Vec<MailHistoryEvent>) -> BTreeMap<String, Vec<MailHistoryEvent>> {
    let mut grouped = BTreeMap::<String, Vec<MailHistoryEvent>>::new();
    for event in events {
        grouped.entry(event.trace_id.clone()).or_default().push(event);
    }
    for events in grouped.values_mut() {
        events.sort_by(|left, right| left.timestamp.cmp(&right.timestamp));
    }
    grouped
}

fn summarize_trace_history(events: Vec<MailHistoryEvent>) -> Option<MailHistorySummary> {
    let latest = events.last()?;
    Some(MailHistorySummary {
        trace_id: latest.trace_id.clone(),
        direction: latest.direction.clone(),
        queue: latest.queue.clone(),
        status: latest.status.clone(),
        latest_event_at: latest.timestamp.clone(),
        mail_from: latest.mail_from.clone(),
        rcpt_to: latest.rcpt_to.clone(),
        subject: latest.subject.clone(),
        internet_message_id: latest.internet_message_id.clone(),
        reason: latest.reason.clone(),
        route_target: latest.route_target.clone(),
        remote_message_ref: latest.remote_message_ref.clone(),
        spam_score: latest.spam_score,
        security_score: latest.security_score,
        reputation_score: latest.reputation_score,
        event_count: events.len(),
        policy_tags: policy_tags_from_event(latest),
    })
}

fn history_matches(item: &MailHistorySummary, query: &HistoryQuery) -> bool {
    if let Some(direction) = query.direction.as_deref() {
        if item.direction != direction {
            return false;
        }
    }
    if let Some(queue) = query.queue.as_deref() {
        if item.queue != queue {
            return false;
        }
    }
    if let Some(disposition) = query.disposition.as_deref() {
        if item.status != disposition {
            return false;
        }
    }
    if let Some(domain) = normalized(query.domain.as_deref()) {
        let sender_matches = domain_part(&item.mail_from).is_some_and(|value| value == domain);
        let recipient_matches = item
            .rcpt_to
            .iter()
            .filter_map(|value| domain_part(value))
            .any(|value| value == domain);
        if !sender_matches && !recipient_matches {
            return false;
        }
    }
    if let Some(q) = normalized(query.q.as_deref()) {
        let haystack = [
            item.trace_id.as_str(),
            item.mail_from.as_str(),
            item.subject.as_str(),
            item.reason.as_deref().unwrap_or(""),
            item.route_target.as_deref().unwrap_or(""),
            item.internet_message_id.as_deref().unwrap_or(""),
        ]
        .into_iter()
        .chain(item.rcpt_to.iter().map(String::as_str))
        .chain(item.policy_tags.iter().map(String::as_str))
        .map(|value| value.to_ascii_lowercase())
        .collect::<Vec<_>>();
        if !haystack.iter().any(|value| value.contains(&q)) {
            return false;
        }
    }
    true
}

fn build_digest_report(
    spool_dir: &Path,
    generated_at: &str,
    scope: &str,
    scope_label: &str,
    recipient: &str,
    items: Vec<QuarantineSummary>,
) -> Result<DigestReportDetails> {
    ensure_digest_dir(spool_dir)?;
    let report_id = format!("digest-{}", Uuid::new_v4());
    let content = render_digest_content(generated_at, scope, scope_label, recipient, &items);
    let summary = DigestReportSummary {
        report_id: report_id.clone(),
        generated_at: generated_at.to_string(),
        scope: scope.to_string(),
        scope_label: scope_label.to_string(),
        recipient: recipient.to_string(),
        item_count: items.len(),
        top_reason: items.iter().find_map(|item| item.reason.clone()),
    };
    let detail = DigestReportDetails {
        summary,
        content,
        items,
    };
    let path = digest_report_dir(spool_dir).join(format!("{report_id}.json"));
    fs::write(path, serde_json::to_vec_pretty(&detail)?)?;
    Ok(detail)
}

fn render_digest_content(
    generated_at: &str,
    scope: &str,
    scope_label: &str,
    recipient: &str,
    items: &[QuarantineSummary],
) -> String {
    let inbound = items.iter().filter(|item| item.direction == "inbound").count();
    let outbound = items.iter().filter(|item| item.direction == "outbound").count();
    let highest_spam = items
        .iter()
        .map(|item| item.spam_score)
        .fold(0.0_f32, f32::max);

    let mut lines = vec![
        format!("Quarantine digest generated {generated_at}"),
        format!("Scope: {scope} ({scope_label})"),
        format!("Recipient: {recipient}"),
        format!("Items: {}", items.len()),
        format!("Inbound: {inbound}"),
        format!("Outbound: {outbound}"),
        format!("Highest spam score: {highest_spam:.1}"),
        String::new(),
        "Quarantined items:".to_string(),
    ];

    for item in items {
        lines.push(format!(
            "- {} | {} | {} -> {} | {} | spam {:.1} security {:.1}",
            item.received_at,
            item.trace_id,
            item.mail_from,
            item.rcpt_to.join(", "),
            item.reason.clone().unwrap_or_else(|| item.status.clone()),
            item.spam_score,
            item.security_score
        ));
    }

    lines.join("\n")
}

fn filter_quarantine_for_domain(
    items: &[QuarantineSummary],
    domain: &str,
    max_items: u32,
) -> Vec<QuarantineSummary> {
    let expected = normalized(Some(domain));
    let mut filtered = items
        .iter()
        .filter(|item| {
            let sender_matches = expected
                .as_deref()
                .and_then(|value| domain_part(&item.mail_from).map(|item_domain| item_domain == value))
                .unwrap_or(false);
            let recipient_matches = item
                .rcpt_to
                .iter()
                .filter_map(|value| domain_part(value))
                .any(|value| Some(value) == expected);
            sender_matches || recipient_matches
        })
        .cloned()
        .collect::<Vec<_>>();
    filtered.sort_by(|left, right| right.received_at.cmp(&left.received_at));
    filtered.truncate(max_items as usize);
    filtered
}

fn filter_quarantine_for_mailbox(
    items: &[QuarantineSummary],
    mailbox: &str,
    max_items: u32,
) -> Vec<QuarantineSummary> {
    let expected = normalized(Some(mailbox));
    let mut filtered = items
        .iter()
        .filter(|item| {
            normalized(Some(&item.mail_from)) == expected
                || item
                    .rcpt_to
                    .iter()
                    .any(|value| normalized(Some(value)) == expected)
        })
        .cloned()
        .collect::<Vec<_>>();
    filtered.sort_by(|left, right| right.received_at.cmp(&left.received_at));
    filtered.truncate(max_items as usize);
    filtered
}

fn policy_tags_from_event(item: &MailHistoryEvent) -> Vec<String> {
    let mut tags = BTreeSet::new();
    if !item.queue.is_empty() {
        tags.insert(format!("queue:{}", item.queue));
    }
    if !item.status.is_empty() {
        tags.insert(format!("status:{}", item.status));
    }
    if let Some(route_target) = item.route_target.as_deref() {
        tags.insert(format!("route:{route_target}"));
    }
    if item.spam_score > 0.0 {
        tags.insert(format!("spam:{:.1}", item.spam_score));
    }
    if item.security_score > 0.0 {
        tags.insert(format!("security:{:.1}", item.security_score));
    }
    tags.into_iter().collect()
}

fn default_bool_true() -> bool {
    true
}

fn normalize_domain_defaults(items: &[DigestDomainDefault]) -> Vec<DigestDomainDefault> {
    let mut seen = BTreeSet::new();
    let mut normalized_items = Vec::new();
    for item in items {
        let domain = normalized(Some(&item.domain));
        let recipients = item
            .recipients
            .iter()
            .filter_map(|value| normalized(Some(value)))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let Some(domain) = domain else {
            continue;
        };
        if recipients.is_empty() || !seen.insert(domain.clone()) {
            continue;
        }
        normalized_items.push(DigestDomainDefault { domain, recipients });
    }
    normalized_items
}

fn normalize_user_overrides(items: &[DigestUserOverride]) -> Vec<DigestUserOverride> {
    let mut seen = BTreeSet::new();
    let mut normalized_items = Vec::new();
    for item in items {
        let mailbox = normalized(Some(&item.mailbox));
        let recipient = normalized(Some(&item.recipient));
        let (Some(mailbox), Some(recipient)) = (mailbox, recipient) else {
            continue;
        };
        let key = format!("{mailbox}->{recipient}");
        if !seen.insert(key) {
            continue;
        }
        normalized_items.push(DigestUserOverride {
            mailbox,
            recipient,
            enabled: item.enabled,
        });
    }
    normalized_items
}

fn ensure_digest_dir(spool_dir: &Path) -> Result<()> {
    fs::create_dir_all(digest_report_dir(spool_dir))?;
    Ok(())
}

fn digest_report_dir(spool_dir: &Path) -> PathBuf {
    spool_dir.join("policy").join(DIGEST_REPORT_DIR)
}

fn digest_is_due(settings: &ReportingSettings) -> bool {
    parse_unix_timestamp(settings.next_digest_run_at.as_deref().unwrap_or("unix:0"))
        .is_some_and(|timestamp| timestamp <= current_unix_timestamp())
}

fn normalized(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
}

fn domain_part(address: &str) -> Option<String> {
    address
        .rsplit_once('@')
        .map(|(_, domain)| domain.trim().to_ascii_lowercase())
        .filter(|domain| !domain.is_empty())
}

fn parse_unix_timestamp(value: &str) -> Option<u64> {
    value.strip_prefix("unix:")?.parse::<u64>().ok()
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn current_timestamp() -> String {
    format!("unix:{}", current_unix_timestamp())
}

fn timestamp_from_now(seconds: u64) -> String {
    format!("unix:{}", current_unix_timestamp().saturating_add(seconds))
}

#[cfg(test)]
mod tests {
    use super::{
        default_reporting_settings, filter_quarantine_for_domain, filter_quarantine_for_mailbox,
        normalize_reporting_settings,
    };
    use crate::smtp::QuarantineSummary;

    fn sample_item(trace_id: &str, mail_from: &str, rcpt_to: &[&str]) -> QuarantineSummary {
        QuarantineSummary {
            trace_id: trace_id.to_string(),
            queue: "quarantine".to_string(),
            direction: "inbound".to_string(),
            status: "quarantined".to_string(),
            received_at: "unix:10".to_string(),
            mail_from: mail_from.to_string(),
            rcpt_to: rcpt_to.iter().map(|value| (*value).to_string()).collect(),
            subject: "Subject".to_string(),
            internet_message_id: None,
            reason: None,
            spam_score: 5.5,
            security_score: 3.0,
            reputation_score: -2,
            route_target: None,
        }
    }

    #[test]
    fn reporting_defaults_are_normalized() {
        let mut settings = default_reporting_settings();
        settings.digest_interval_minutes = 0;
        settings.digest_max_items = 0;
        settings.history_retention_days = 0;
        normalize_reporting_settings(&mut settings);
        assert_eq!(settings.digest_interval_minutes, 360);
        assert_eq!(settings.digest_max_items, 25);
        assert_eq!(settings.history_retention_days, 30);
        assert!(settings.next_digest_run_at.is_some());
    }

    #[test]
    fn domain_filter_matches_sender_and_recipient_domains() {
        let items = vec![
            sample_item("a", "sender@example.com", &["dest@other.test"]),
            sample_item("b", "sender@other.test", &["dest@example.com"]),
            sample_item("c", "sender@else.test", &["dest@else.test"]),
        ];
        let filtered = filter_quarantine_for_domain(&items, "example.com", 25);
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn mailbox_filter_matches_sender_and_recipient_mailboxes() {
        let items = vec![
            sample_item("a", "alice@example.com", &["dest@other.test"]),
            sample_item("b", "sender@other.test", &["alice@example.com"]),
            sample_item("c", "sender@else.test", &["dest@else.test"]),
        ];
        let filtered = filter_quarantine_for_mailbox(&items, "alice@example.com", 25);
        assert_eq!(filtered.len(), 2);
    }
}
