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
    #[serde(default = "default_digest_report_retention_days")]
    pub digest_report_retention_days: u32,
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
    pub trace_id: Option<String>,
    pub sender: Option<String>,
    pub recipient: Option<String>,
    pub internet_message_id: Option<String>,
    pub peer: Option<String>,
    pub route_target: Option<String>,
    pub reason: Option<String>,
    pub direction: Option<String>,
    pub queue: Option<String>,
    pub disposition: Option<String>,
    pub domain: Option<String>,
    pub min_spam_score: Option<f32>,
    pub min_security_score: Option<f32>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MailHistorySummary {
    pub trace_id: String,
    pub direction: String,
    pub queue: String,
    pub status: String,
    pub latest_event_at: String,
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
    pub magika_decision: Option<String>,
    pub technical_status: Option<Value>,
    pub dsn: Option<Value>,
    pub latest_decision: Option<String>,
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
pub(crate) struct DigestMetricCount {
    pub key: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DigestReportSummary {
    pub report_id: String,
    pub generated_at: String,
    pub scope: String,
    pub scope_label: String,
    pub recipient: String,
    pub item_count: usize,
    pub inbound_count: usize,
    pub outbound_count: usize,
    pub highest_spam_score: f32,
    pub highest_security_score: f32,
    pub oldest_item_at: Option<String>,
    pub newest_item_at: Option<String>,
    pub top_reason: Option<String>,
    pub top_reasons: Vec<DigestMetricCount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DigestReportDetails {
    pub summary: DigestReportSummary,
    pub content: String,
    pub items: Vec<QuarantineSummary>,
    pub status_counts: Vec<DigestMetricCount>,
    pub domain_counts: Vec<DigestMetricCount>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        digest_report_retention_days: default_digest_report_retention_days(),
        domain_defaults: Vec::new(),
        user_overrides: Vec::new(),
        last_digest_run_at: None,
        next_digest_run_at: Some(timestamp_from_now(
            default_digest_interval_minutes() as u64 * 60,
        )),
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

pub(crate) fn default_digest_report_retention_days() -> u32 {
    14
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
    if settings.digest_report_retention_days == 0 {
        settings.digest_report_retention_days = default_digest_report_retention_days();
    }

    settings.domain_defaults = normalize_domain_defaults(&settings.domain_defaults);
    settings.user_overrides = normalize_user_overrides(&settings.user_overrides);

    if settings.next_digest_run_at.is_none() {
        settings.next_digest_run_at = Some(timestamp_from_now(
            settings.digest_interval_minutes as u64 * 60,
        ));
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
    prune_digest_reports(spool_dir, settings.digest_report_retention_days)?;
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
    prune_digest_reports(spool_dir, settings.digest_report_retention_days)?;
    let quarantine =
        smtp::list_quarantine_items_from_spool(spool_dir, smtp::QuarantineQuery::default())?;
    let generated_at = current_timestamp();
    let mut reports = Vec::new();

    for digest in &settings.domain_defaults {
        let items =
            filter_quarantine_for_domain(&quarantine, &digest.domain, settings.digest_max_items);
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
        let items = filter_quarantine_for_mailbox(
            &quarantine,
            &override_entry.mailbox,
            settings.digest_max_items,
        );
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
    settings.next_digest_run_at = Some(timestamp_from_now(
        settings.digest_interval_minutes as u64 * 60,
    ));

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
    if let Some(response) =
        search_mail_history_from_db(config, &query, retention_days, limit).await?
    {
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

pub(crate) async fn enforce_retention(
    spool_dir: &Path,
    config: &RuntimeConfig,
    settings: &ReportingSettings,
) -> Result<()> {
    prune_transport_audit_jsonl(spool_dir, settings.history_retention_days)?;
    prune_digest_reports(spool_dir, settings.digest_report_retention_days)?;
    prune_retained_rows_from_db(config, settings).await?;
    Ok(())
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
    let trace_id = normalized(query.trace_id.as_deref());
    let sender = normalized(query.sender.as_deref());
    let recipient = normalized(query.recipient.as_deref());
    let internet_message_id = normalized(query.internet_message_id.as_deref());
    let peer = normalized(query.peer.as_deref());
    let route_target = normalized(query.route_target.as_deref());
    let reason = normalized(query.reason.as_deref());
    let search_term = normalized(query.q.as_deref());
    let search_pattern = search_term.as_ref().map(|value| format!("%{value}%"));
    let total: i64 = sqlx::query_scalar(
        r#"
        WITH latest AS (
            SELECT DISTINCT ON (trace_id)
                   trace_id, event_unix, direction, queue, status, peer, mail_from, rcpt_to,
                   internet_message_id, reason, route_target, spam_score, security_score
              FROM mail_flow_history
             WHERE event_unix >= $1
             ORDER BY trace_id, event_unix DESC
        )
        SELECT COUNT(*)
          FROM latest
         WHERE ($2::TEXT IS NULL OR LOWER(latest.direction) = $2)
           AND ($3::TEXT IS NULL OR LOWER(latest.queue) = $3)
           AND ($4::TEXT IS NULL OR LOWER(latest.status) = $4)
           AND ($5::TEXT IS NULL OR LOWER(latest.trace_id) = $5)
           AND (
                $6::TEXT IS NULL
                OR SPLIT_PART(LOWER(latest.mail_from), '@', 2) = $6
                OR EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements_text(latest.rcpt_to) AS recipient(value)
                     WHERE SPLIT_PART(LOWER(recipient.value), '@', 2) = $6
                )
           )
           AND ($7::TEXT IS NULL OR LOWER(latest.mail_from) LIKE $7)
           AND (
                $8::TEXT IS NULL
                OR EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements_text(latest.rcpt_to) AS recipient(value)
                     WHERE LOWER(recipient.value) LIKE $8
                )
           )
           AND ($9::TEXT IS NULL OR LOWER(COALESCE(latest.internet_message_id, '')) LIKE $9)
           AND ($10::TEXT IS NULL OR LOWER(latest.peer) LIKE $10)
           AND ($11::TEXT IS NULL OR LOWER(COALESCE(latest.route_target, '')) LIKE $11)
           AND ($12::TEXT IS NULL OR LOWER(COALESCE(latest.reason, '')) LIKE $12)
           AND ($13::REAL IS NULL OR latest.spam_score >= $13)
           AND ($14::REAL IS NULL OR latest.security_score >= $14)
           AND (
                $15::TEXT IS NULL
                OR EXISTS (
                    SELECT 1
                      FROM mail_flow_history matched
                     WHERE matched.trace_id = latest.trace_id
                       AND matched.event_unix >= $1
                       AND (
                            matched.search_text LIKE $15
                            OR to_tsvector('simple', matched.search_text) @@ websearch_to_tsquery('simple', $16)
                       )
                )
           )
        "#,
    )
    .bind(cutoff)
    .bind(direction.clone())
    .bind(queue.clone())
    .bind(disposition.clone())
    .bind(trace_id.clone())
    .bind(domain.clone())
    .bind(sender.as_ref().map(|value| format!("%{value}%")))
    .bind(recipient.as_ref().map(|value| format!("%{value}%")))
    .bind(internet_message_id.as_ref().map(|value| format!("%{value}%")))
    .bind(peer.as_ref().map(|value| format!("%{value}%")))
    .bind(route_target.as_ref().map(|value| format!("%{value}%")))
    .bind(reason.as_ref().map(|value| format!("%{value}%")))
    .bind(query.min_spam_score)
    .bind(query.min_security_score)
    .bind(search_pattern.clone())
    .bind(search_term.clone())
    .fetch_one(pool)
    .await?;

    let rows = sqlx::query(
        r#"
        WITH latest AS (
            SELECT DISTINCT ON (trace_id)
                   trace_id, event_unix, timestamp, direction, queue, status, peer, mail_from, rcpt_to,
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
               latest.peer, latest.mail_from, latest.rcpt_to, latest.subject, latest.internet_message_id,
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
           AND ($5::TEXT IS NULL OR LOWER(latest.trace_id) = $5)
           AND (
                $6::TEXT IS NULL
                OR SPLIT_PART(LOWER(latest.mail_from), '@', 2) = $6
                OR EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements_text(latest.rcpt_to) AS recipient(value)
                     WHERE SPLIT_PART(LOWER(recipient.value), '@', 2) = $6
                )
           )
           AND ($7::TEXT IS NULL OR LOWER(latest.mail_from) LIKE $7)
           AND (
                $8::TEXT IS NULL
                OR EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements_text(latest.rcpt_to) AS recipient(value)
                     WHERE LOWER(recipient.value) LIKE $8
                )
           )
           AND ($9::TEXT IS NULL OR LOWER(COALESCE(latest.internet_message_id, '')) LIKE $9)
           AND ($10::TEXT IS NULL OR LOWER(latest.peer) LIKE $10)
           AND ($11::TEXT IS NULL OR LOWER(COALESCE(latest.route_target, '')) LIKE $11)
           AND ($12::TEXT IS NULL OR LOWER(COALESCE(latest.reason, '')) LIKE $12)
           AND ($13::REAL IS NULL OR latest.spam_score >= $13)
           AND ($14::REAL IS NULL OR latest.security_score >= $14)
           AND (
                $15::TEXT IS NULL
                OR EXISTS (
                    SELECT 1
                      FROM mail_flow_history matched
                     WHERE matched.trace_id = latest.trace_id
                       AND matched.event_unix >= $1
                       AND (
                            matched.search_text LIKE $15
                            OR to_tsvector('simple', matched.search_text) @@ websearch_to_tsquery('simple', $16)
                       )
                )
           )
         ORDER BY latest.event_unix DESC
         LIMIT $17
        "#,
    )
    .bind(cutoff)
    .bind(direction)
    .bind(queue)
    .bind(disposition)
    .bind(trace_id)
    .bind(domain)
    .bind(sender.as_ref().map(|value| format!("%{value}%")))
    .bind(recipient.as_ref().map(|value| format!("%{value}%")))
    .bind(internet_message_id.as_ref().map(|value| format!("%{value}%")))
    .bind(peer.as_ref().map(|value| format!("%{value}%")))
    .bind(route_target.as_ref().map(|value| format!("%{value}%")))
    .bind(reason.as_ref().map(|value| format!("%{value}%")))
    .bind(query.min_spam_score)
    .bind(query.min_security_score)
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
                peer: event.peer.clone(),
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
                dnsbl_hits: event.dnsbl_hits.clone(),
                auth_summary: event.auth_summary.clone(),
                magika_decision: event.magika_decision.clone(),
                technical_status: event.technical_status.clone(),
                dsn: event.dsn.clone(),
                latest_decision: latest_decision(&event),
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
        rcpt_to: row
            .try_get::<sqlx::types::Json<Vec<String>>, _>("rcpt_to")?
            .0,
        subject: row.try_get("subject")?,
        internet_message_id: row.try_get("internet_message_id")?,
        reason: row.try_get("reason")?,
        route_target: row.try_get("route_target")?,
        remote_message_ref: row.try_get("remote_message_ref")?,
        spam_score: row.try_get("spam_score")?,
        security_score: row.try_get("security_score")?,
        reputation_score: row.try_get("reputation_score")?,
        dnsbl_hits: row
            .try_get::<sqlx::types::Json<Vec<String>>, _>("dnsbl_hits")?
            .0,
        auth_summary: row
            .try_get::<sqlx::types::Json<Value>, _>("auth_summary")?
            .0,
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
        decision_trace: row
            .try_get::<sqlx::types::Json<Vec<Value>>, _>("decision_trace")?
            .0,
    })
}

fn history_cutoff(retention_days: u32) -> i64 {
    current_unix_timestamp().saturating_sub(retention_days.max(1) as u64 * 86_400) as i64
}

fn group_history(events: Vec<MailHistoryEvent>) -> BTreeMap<String, Vec<MailHistoryEvent>> {
    let mut grouped = BTreeMap::<String, Vec<MailHistoryEvent>>::new();
    for event in events {
        grouped
            .entry(event.trace_id.clone())
            .or_default()
            .push(event);
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
        peer: latest.peer.clone(),
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
        dnsbl_hits: latest.dnsbl_hits.clone(),
        auth_summary: latest.auth_summary.clone(),
        magika_decision: latest.magika_decision.clone(),
        technical_status: latest.technical_status.clone(),
        dsn: latest.dsn.clone(),
        latest_decision: latest_decision(latest),
        event_count: events.len(),
        policy_tags: policy_tags_from_event(latest),
    })
}

fn history_matches(item: &MailHistorySummary, query: &HistoryQuery) -> bool {
    if let Some(trace_id) = normalized(query.trace_id.as_deref()) {
        if item.trace_id != trace_id {
            return false;
        }
    }
    if let Some(sender) = normalized(query.sender.as_deref()) {
        if !item.mail_from.to_ascii_lowercase().contains(&sender) {
            return false;
        }
    }
    if let Some(recipient) = normalized(query.recipient.as_deref()) {
        if !item
            .rcpt_to
            .iter()
            .any(|value| value.to_ascii_lowercase().contains(&recipient))
        {
            return false;
        }
    }
    if let Some(internet_message_id) = normalized(query.internet_message_id.as_deref()) {
        if !item
            .internet_message_id
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase()
            .contains(&internet_message_id)
        {
            return false;
        }
    }
    if let Some(peer) = normalized(query.peer.as_deref()) {
        if !item.peer.to_ascii_lowercase().contains(&peer) {
            return false;
        }
    }
    if let Some(route_target) = normalized(query.route_target.as_deref()) {
        if !item
            .route_target
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase()
            .contains(&route_target)
        {
            return false;
        }
    }
    if let Some(reason) = normalized(query.reason.as_deref()) {
        if !item
            .reason
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase()
            .contains(&reason)
        {
            return false;
        }
    }
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
    if let Some(min_spam_score) = query.min_spam_score {
        if item.spam_score < min_spam_score {
            return false;
        }
    }
    if let Some(min_security_score) = query.min_security_score {
        if item.security_score < min_security_score {
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
    let top_reasons = summarize_digest_counts(
        items
            .iter()
            .filter_map(|item| item.reason.clone())
            .collect::<Vec<_>>(),
        5,
    );
    let summary = DigestReportSummary {
        report_id: report_id.clone(),
        generated_at: generated_at.to_string(),
        scope: scope.to_string(),
        scope_label: scope_label.to_string(),
        recipient: recipient.to_string(),
        item_count: items.len(),
        inbound_count: items
            .iter()
            .filter(|item| item.direction == "inbound")
            .count(),
        outbound_count: items
            .iter()
            .filter(|item| item.direction == "outbound")
            .count(),
        highest_spam_score: items.iter().map(|item| item.spam_score).fold(0.0, f32::max),
        highest_security_score: items
            .iter()
            .map(|item| item.security_score)
            .fold(0.0, f32::max),
        oldest_item_at: items.last().map(|item| item.received_at.clone()),
        newest_item_at: items.first().map(|item| item.received_at.clone()),
        top_reason: items.iter().find_map(|item| item.reason.clone()),
        top_reasons,
    };
    let detail = DigestReportDetails {
        summary,
        content,
        items,
        status_counts: Vec::new(),
        domain_counts: Vec::new(),
    };
    let detail = enrich_digest_detail(detail);
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
    let inbound = items
        .iter()
        .filter(|item| item.direction == "inbound")
        .count();
    let outbound = items
        .iter()
        .filter(|item| item.direction == "outbound")
        .count();
    let highest_spam = items
        .iter()
        .map(|item| item.spam_score)
        .fold(0.0_f32, f32::max);
    let highest_security = items
        .iter()
        .map(|item| item.security_score)
        .fold(0.0_f32, f32::max);
    let oldest_item_at = items.last().map(|item| item.received_at.clone());
    let newest_item_at = items.first().map(|item| item.received_at.clone());
    let reason_counts = summarize_digest_counts(
        items
            .iter()
            .filter_map(|item| item.reason.clone())
            .collect::<Vec<_>>(),
        6,
    );
    let status_counts = summarize_digest_counts(
        items
            .iter()
            .map(|item| item.status.clone())
            .collect::<Vec<_>>(),
        6,
    );

    let mut lines = vec![
        format!("Quarantine digest generated {generated_at}"),
        format!("Scope: {scope} ({scope_label})"),
        format!("Recipient: {recipient}"),
        format!("Items: {}", items.len()),
        format!("Inbound: {inbound}"),
        format!("Outbound: {outbound}"),
        format!("Highest spam score: {highest_spam:.1}"),
        format!("Highest security score: {highest_security:.1}"),
        format!(
            "Coverage: {} -> {}",
            oldest_item_at.as_deref().unwrap_or("n/a"),
            newest_item_at.as_deref().unwrap_or("n/a")
        ),
        format!(
            "Top reasons: {}",
            render_metric_counts(&reason_counts, "none")
        ),
        format!("Statuses: {}", render_metric_counts(&status_counts, "none")),
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
                .and_then(|value| {
                    domain_part(&item.mail_from).map(|item_domain| item_domain == value)
                })
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
    if let Some(value) = item.magika_decision.as_deref() {
        tags.insert(format!("magika:{value}"));
    }
    if !item.dnsbl_hits.is_empty() {
        tags.insert(format!("dnsbl:{}hit", item.dnsbl_hits.len()));
    }
    if let Some(action) = item
        .dsn
        .as_ref()
        .and_then(|dsn| dsn.get("action"))
        .and_then(Value::as_str)
    {
        tags.insert(format!("dsn:{action}"));
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

fn latest_decision(event: &MailHistoryEvent) -> Option<String> {
    event.decision_trace.last().and_then(|value| {
        let stage = value.get("stage")?.as_str()?;
        let outcome = value.get("outcome")?.as_str()?;
        Some(format!("{stage}:{outcome}"))
    })
}

fn summarize_digest_counts(values: Vec<String>, limit: usize) -> Vec<DigestMetricCount> {
    let mut counts = BTreeMap::<String, usize>::new();
    for value in values.into_iter().filter(|value| !value.trim().is_empty()) {
        *counts.entry(value).or_default() += 1;
    }
    let mut items = counts
        .into_iter()
        .map(|(key, count)| DigestMetricCount { key, count })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.key.cmp(&right.key))
    });
    items.truncate(limit);
    items
}

fn render_metric_counts(values: &[DigestMetricCount], empty: &str) -> String {
    if values.is_empty() {
        return empty.to_string();
    }
    values
        .iter()
        .map(|value| format!("{} ({})", value.key, value.count))
        .collect::<Vec<_>>()
        .join(", ")
}

fn enrich_digest_detail(mut detail: DigestReportDetails) -> DigestReportDetails {
    detail.status_counts = summarize_digest_counts(
        detail
            .items
            .iter()
            .map(|item| item.status.clone())
            .collect::<Vec<_>>(),
        6,
    );
    detail.domain_counts = summarize_digest_counts(
        detail
            .items
            .iter()
            .flat_map(|item| {
                let mut values = Vec::new();
                if let Some(domain) = domain_part(&item.mail_from) {
                    values.push(domain);
                }
                values.extend(item.rcpt_to.iter().filter_map(|value| domain_part(value)));
                values
            })
            .collect::<Vec<_>>(),
        8,
    );
    detail
}

fn prune_transport_audit_jsonl(spool_dir: &Path, retention_days: u32) -> Result<()> {
    let path = spool_dir.join("policy").join("transport-audit.jsonl");
    if !path.exists() {
        return Ok(());
    }
    let cutoff = history_cutoff(retention_days) as u64;
    let retained = fs::read_to_string(&path)?
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| {
            let event: StoredMailHistoryEvent = serde_json::from_str(line).ok()?;
            (parse_unix_timestamp(&event.timestamp).unwrap_or(0) >= cutoff)
                .then(|| serde_json::to_string(&event).ok())
                .flatten()
        })
        .collect::<Vec<_>>();
    let output = if retained.is_empty() {
        String::new()
    } else {
        format!("{}\n", retained.join("\n"))
    };
    fs::write(path, output)?;
    Ok(())
}

fn prune_digest_reports(spool_dir: &Path, retention_days: u32) -> Result<()> {
    let dir = digest_report_dir(spool_dir);
    if !dir.exists() {
        return Ok(());
    }
    let cutoff = current_unix_timestamp().saturating_sub(retention_days.max(1) as u64 * 86_400);
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let detail: DigestReportDetails = serde_json::from_str(&fs::read_to_string(&path)?)?;
        if parse_unix_timestamp(&detail.summary.generated_at).unwrap_or(0) < cutoff {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

async fn prune_retained_rows_from_db(
    config: &RuntimeConfig,
    settings: &ReportingSettings,
) -> Result<()> {
    let Some(pool) = smtp::ensure_local_db_schema(config).await? else {
        return Ok(());
    };
    sqlx::query("DELETE FROM mail_flow_history WHERE event_unix < $1")
        .bind(history_cutoff(settings.history_retention_days))
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        build_digest_report, default_reporting_settings, filter_quarantine_for_domain,
        filter_quarantine_for_mailbox, load_digest_report, normalize_reporting_settings,
        DigestDomainDefault, DigestUserOverride,
    };
    use crate::smtp::QuarantineSummary;
    use serde_json::Value;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_dir(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("lpe-ct-reporting-{name}-{suffix}"));
        fs::create_dir_all(path.join("policy")).unwrap();
        path
    }

    fn sample_item(trace_id: &str, mail_from: &str, rcpt_to: &[&str]) -> QuarantineSummary {
        QuarantineSummary {
            trace_id: trace_id.to_string(),
            queue: "quarantine".to_string(),
            direction: "inbound".to_string(),
            status: "quarantined".to_string(),
            received_at: "unix:10".to_string(),
            peer: "203.0.113.10".to_string(),
            helo: "mx.example.test".to_string(),
            mail_from: mail_from.to_string(),
            rcpt_to: rcpt_to.iter().map(|value| (*value).to_string()).collect(),
            subject: "Subject".to_string(),
            internet_message_id: None,
            reason: None,
            spam_score: 5.5,
            security_score: 3.0,
            reputation_score: -2,
            dnsbl_hits: Vec::new(),
            auth_summary: Value::Null,
            magika_summary: None,
            magika_decision: None,
            remote_message_ref: None,
            route_target: None,
            decision_summary: None,
        }
    }

    #[test]
    fn reporting_defaults_are_normalized() {
        let mut settings = default_reporting_settings();
        settings.digest_interval_minutes = 0;
        settings.digest_max_items = 0;
        settings.history_retention_days = 0;
        settings.digest_report_retention_days = 0;
        normalize_reporting_settings(&mut settings);
        assert_eq!(settings.digest_interval_minutes, 360);
        assert_eq!(settings.digest_max_items, 25);
        assert_eq!(settings.history_retention_days, 30);
        assert_eq!(settings.digest_report_retention_days, 14);
        assert!(settings.next_digest_run_at.is_some());
    }

    #[test]
    fn reporting_normalization_deduplicates_domain_defaults_and_overrides() {
        let mut settings = default_reporting_settings();
        settings.domain_defaults = vec![
            DigestDomainDefault {
                domain: "Example.com".to_string(),
                recipients: vec!["Ops@example.com".to_string(), "ops@example.com".to_string()],
            },
            DigestDomainDefault {
                domain: "example.com".to_string(),
                recipients: vec!["audit@example.com".to_string()],
            },
        ];
        settings.user_overrides = vec![
            DigestUserOverride {
                mailbox: "Alice@example.com".to_string(),
                recipient: "Ops@example.com".to_string(),
                enabled: true,
            },
            DigestUserOverride {
                mailbox: "alice@example.com".to_string(),
                recipient: "ops@example.com".to_string(),
                enabled: false,
            },
        ];

        normalize_reporting_settings(&mut settings);

        assert_eq!(settings.domain_defaults.len(), 1);
        assert_eq!(settings.domain_defaults[0].domain, "example.com");
        assert_eq!(
            settings.domain_defaults[0].recipients,
            vec!["ops@example.com"]
        );
        assert_eq!(settings.user_overrides.len(), 1);
        assert_eq!(settings.user_overrides[0].mailbox, "alice@example.com");
        assert_eq!(settings.user_overrides[0].recipient, "ops@example.com");
        assert!(settings.user_overrides[0].enabled);
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

    #[test]
    fn digest_report_enriches_status_and_domain_counts_and_persists_artifact() {
        let spool = temp_dir("digest");
        let mut inbound = sample_item("a", "alice@example.com", &["dest@other.test"]);
        inbound.reason = Some("dmarc reject".to_string());
        inbound.status = "quarantined".to_string();
        inbound.received_at = "unix:20".to_string();

        let mut outbound = sample_item("b", "sender@other.test", &["dest@example.com"]);
        outbound.direction = "outbound".to_string();
        outbound.status = "failed".to_string();
        outbound.received_at = "unix:30".to_string();
        outbound.reason = Some("blocked attachment".to_string());

        let detail = build_digest_report(
            &spool,
            "unix:40",
            "domain-default",
            "example.com",
            "ops@example.com",
            vec![outbound.clone(), inbound.clone()],
        )
        .unwrap();

        assert_eq!(detail.summary.item_count, 2);
        assert_eq!(detail.summary.inbound_count, 1);
        assert_eq!(detail.summary.outbound_count, 1);
        assert!(detail
            .status_counts
            .iter()
            .any(|entry| entry.key == "failed" && entry.count == 1));
        assert!(detail
            .status_counts
            .iter()
            .any(|entry| entry.key == "quarantined" && entry.count == 1));
        assert!(detail
            .domain_counts
            .iter()
            .any(|entry| entry.key == "example.com"));

        let persisted = load_digest_report(&spool, &detail.summary.report_id)
            .unwrap()
            .unwrap();
        assert_eq!(persisted.summary.report_id, detail.summary.report_id);
        assert_eq!(persisted.items.len(), 2);
    }
}
