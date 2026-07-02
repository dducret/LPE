use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuarantineMetadata {
    trace_id: String,
    direction: String,
    status: String,
    received_at: String,
    received_unix: i64,
    peer: String,
    helo: String,
    mail_from: String,
    sender_domain: Option<String>,
    rcpt_to: Vec<String>,
    recipient_domains: Vec<String>,
    subject: String,
    internet_message_id: Option<String>,
    spool_path: String,
    reason: Option<String>,
    spam_score: f32,
    security_score: f32,
    reputation_score: i32,
    dnsbl_hits: Vec<String>,
    auth_summary: AuthSummary,
    decision_trace: Vec<DecisionTraceEntry>,
    magika_summary: Option<String>,
    magika_decision: Option<String>,
    remote_message_ref: Option<String>,
    route_target: Option<String>,
    search_text: String,
}

pub(crate) async fn list_quarantine_items(
    spool_dir: &Path,
    config: &RuntimeConfig,
    query: QuarantineQuery,
) -> Result<Vec<QuarantineSummary>> {
    if let Some(items) = list_quarantine_items_from_db(config, &query).await? {
        return Ok(items);
    }
    list_quarantine_items_from_spool(spool_dir, query)
}

pub(crate) fn list_quarantine_items_from_spool(
    spool_dir: &Path,
    query: QuarantineQuery,
) -> Result<Vec<QuarantineSummary>> {
    let mut items = Vec::new();
    for entry in fs::read_dir(spool_dir.join("quarantine"))? {
        let entry = entry?;
        if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let message = load_message_from_path(&entry.path())?;
        items.push(quarantine_summary_from_message(&message));
    }
    items.sort_by(|left, right| right.received_at.cmp(&left.received_at));
    items.retain(|item| quarantine_matches(item, &query));
    items.truncate(query.limit.unwrap_or(50).clamp(1, 200));
    Ok(items)
}

async fn list_quarantine_items_from_db(
    config: &RuntimeConfig,
    query: &QuarantineQuery,
) -> Result<Option<Vec<QuarantineSummary>>> {
    let Some(pool) = ensure_local_db_schema(config).await? else {
        return Ok(None);
    };

    let limit = query.limit.unwrap_or(50).clamp(1, 200) as i64;
    let direction = normalized(query.direction.as_deref());
    let status = normalized(query.status.as_deref());
    let domain = normalized(query.domain.as_deref());
    let trace_id = normalized(query.trace_id.as_deref());
    let sender = normalized(query.sender.as_deref()).map(|value| format!("%{value}%"));
    let recipient = normalized(query.recipient.as_deref()).map(|value| format!("%{value}%"));
    let internet_message_id =
        normalized(query.internet_message_id.as_deref()).map(|value| format!("%{value}%"));
    let route_target = normalized(query.route_target.as_deref()).map(|value| format!("%{value}%"));
    let reason = normalized(query.reason.as_deref()).map(|value| format!("%{value}%"));
    let search_term = normalized(query.q.as_deref());
    let search_pattern = search_term.as_ref().map(|value| format!("%{value}%"));

    let rows = sqlx::query(
        r#"
        SELECT trace_id, direction, status, received_at, peer, helo, mail_from, rcpt_to,
               subject, internet_message_id, reason, spam_score, security_score,
               reputation_score, dnsbl_hits, auth_summary, magika_summary,
               magika_decision, remote_message_ref, route_target, decision_trace
          FROM quarantine_messages
         WHERE ($1::TEXT IS NULL OR LOWER(direction) = $1)
           AND ($2::TEXT IS NULL OR LOWER(status) = $2)
           AND ($3::TEXT IS NULL OR LOWER(trace_id) = $3)
           AND (
                $4::TEXT IS NULL
                OR SPLIT_PART(LOWER(mail_from), '@', 2) = $4
                OR EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements_text(recipient_domains) AS recipient_domain(value)
                     WHERE LOWER(recipient_domain.value) = $4
                )
           )
           AND ($5::TEXT IS NULL OR LOWER(mail_from) LIKE $5)
           AND (
                $6::TEXT IS NULL
                OR EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements_text(rcpt_to) AS recipient_value(value)
                     WHERE LOWER(recipient_value.value) LIKE $6
                )
           )
           AND ($7::TEXT IS NULL OR LOWER(COALESCE(internet_message_id, '')) LIKE $7)
           AND ($8::TEXT IS NULL OR LOWER(COALESCE(route_target, '')) LIKE $8)
           AND ($9::TEXT IS NULL OR LOWER(COALESCE(reason, '')) LIKE $9)
           AND ($10::REAL IS NULL OR spam_score >= $10)
           AND ($11::REAL IS NULL OR security_score >= $11)
           AND (
                $12::TEXT IS NULL
                OR search_text LIKE $12
                OR to_tsvector('simple', search_text) @@ websearch_to_tsquery('simple', $13)
           )
         ORDER BY received_unix DESC, updated_at DESC
         LIMIT $14
        "#,
    )
    .bind(direction)
    .bind(status)
    .bind(trace_id)
    .bind(domain)
    .bind(sender)
    .bind(recipient)
    .bind(internet_message_id)
    .bind(route_target)
    .bind(reason)
    .bind(query.min_spam_score)
    .bind(query.min_security_score)
    .bind(search_pattern)
    .bind(search_term)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let items = rows
        .into_iter()
        .map(|row| {
            Ok::<QuarantineSummary, anyhow::Error>(QuarantineSummary {
                trace_id: row.try_get("trace_id")?,
                queue: "quarantine".to_string(),
                direction: row.try_get("direction")?,
                status: row.try_get("status")?,
                received_at: row.try_get("received_at")?,
                peer: row.try_get("peer")?,
                helo: row.try_get("helo")?,
                mail_from: row.try_get("mail_from")?,
                rcpt_to: row.try_get::<Json<Vec<String>>, _>("rcpt_to")?.0,
                subject: row.try_get("subject")?,
                internet_message_id: row.try_get("internet_message_id")?,
                reason: row.try_get("reason")?,
                spam_score: row.try_get("spam_score")?,
                security_score: row.try_get("security_score")?,
                reputation_score: row.try_get("reputation_score")?,
                dnsbl_hits: row.try_get::<Json<Vec<String>>, _>("dnsbl_hits")?.0,
                auth_summary: row.try_get::<Json<Value>, _>("auth_summary")?.0,
                magika_summary: row.try_get("magika_summary")?,
                magika_decision: row.try_get("magika_decision")?,
                remote_message_ref: row.try_get("remote_message_ref")?,
                route_target: row.try_get("route_target")?,
                decision_summary: row
                    .try_get::<Json<Vec<Value>>, _>("decision_trace")?
                    .0
                    .last()
                    .and_then(|value| {
                        let stage = value.get("stage")?.as_str()?;
                        let outcome = value.get("outcome")?.as_str()?;
                        Some(format!("{stage}:{outcome}"))
                    }),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(items))
}

fn quarantine_metadata(spool_dir: &Path, message: &QueuedMessage) -> QuarantineMetadata {
    let sender_domain = domain_part(&message.mail_from);
    let recipient_domains = message
        .rcpt_to
        .iter()
        .filter_map(|value| domain_part(value))
        .collect::<Vec<_>>();
    QuarantineMetadata {
        trace_id: message.id.clone(),
        direction: message.direction.clone(),
        status: message.status.clone(),
        received_at: message.received_at.clone(),
        received_unix: parse_unix_timestamp(&message.received_at).unwrap_or(0) as i64,
        peer: message.peer.clone(),
        helo: message.helo.clone(),
        mail_from: message.mail_from.clone(),
        sender_domain,
        rcpt_to: message.rcpt_to.clone(),
        recipient_domains,
        subject: parse_rfc822_header_value(&message.data, "subject").unwrap_or_default(),
        internet_message_id: parse_rfc822_header_value(&message.data, "message-id"),
        spool_path: spool_path(spool_dir, "quarantine", &message.id)
            .display()
            .to_string(),
        reason: message.relay_error.clone(),
        spam_score: message.spam_score,
        security_score: message.security_score,
        reputation_score: message.reputation_score,
        dnsbl_hits: message.dnsbl_hits.clone(),
        auth_summary: message.auth_summary.clone(),
        decision_trace: message.decision_trace.clone(),
        magika_summary: message.magika_summary.clone(),
        magika_decision: message.magika_decision.clone(),
        remote_message_ref: message.remote_message_ref.clone(),
        route_target: message
            .route
            .as_ref()
            .and_then(|route| route.relay_target.clone()),
        search_text: quarantine_search_text(message),
    }
}

pub(in crate::smtp) async fn persist_quarantine_metadata(
    spool_dir: &Path,
    config: &RuntimeConfig,
    message: &QueuedMessage,
) -> Result<()> {
    let Some(pool) = ensure_local_db_schema(config).await? else {
        return Ok(());
    };

    let metadata = quarantine_metadata(spool_dir, message);
    sqlx::query(
        r#"
        INSERT INTO quarantine_messages (
            trace_id, direction, status, received_at, received_unix, peer, helo, mail_from,
            sender_domain, rcpt_to, recipient_domains, subject, internet_message_id, spool_path,
            reason, spam_score, security_score, reputation_score, dnsbl_hits, auth_summary,
            decision_trace, magika_summary, magika_decision, remote_message_ref, route_target,
            search_text
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13, $14, $15,
            $16, $17, $18, $19, $20, $21, $22,
            $23, $24, $25, $26
        )
        ON CONFLICT (trace_id) DO UPDATE SET
            status = EXCLUDED.status,
            received_at = EXCLUDED.received_at,
            received_unix = EXCLUDED.received_unix,
            peer = EXCLUDED.peer,
            helo = EXCLUDED.helo,
            mail_from = EXCLUDED.mail_from,
            sender_domain = EXCLUDED.sender_domain,
            rcpt_to = EXCLUDED.rcpt_to,
            recipient_domains = EXCLUDED.recipient_domains,
            subject = EXCLUDED.subject,
            internet_message_id = EXCLUDED.internet_message_id,
            spool_path = EXCLUDED.spool_path,
            reason = EXCLUDED.reason,
            spam_score = EXCLUDED.spam_score,
            security_score = EXCLUDED.security_score,
            reputation_score = EXCLUDED.reputation_score,
            dnsbl_hits = EXCLUDED.dnsbl_hits,
            auth_summary = EXCLUDED.auth_summary,
            decision_trace = EXCLUDED.decision_trace,
            magika_summary = EXCLUDED.magika_summary,
            magika_decision = EXCLUDED.magika_decision,
            remote_message_ref = EXCLUDED.remote_message_ref,
            route_target = EXCLUDED.route_target,
            search_text = EXCLUDED.search_text,
            updated_at = NOW()
        "#,
    )
    .bind(&metadata.trace_id)
    .bind(&metadata.direction)
    .bind(&metadata.status)
    .bind(&metadata.received_at)
    .bind(metadata.received_unix)
    .bind(&metadata.peer)
    .bind(&metadata.helo)
    .bind(&metadata.mail_from)
    .bind(&metadata.sender_domain)
    .bind(Json(metadata.rcpt_to))
    .bind(Json(metadata.recipient_domains))
    .bind(&metadata.subject)
    .bind(&metadata.internet_message_id)
    .bind(&metadata.spool_path)
    .bind(&metadata.reason)
    .bind(metadata.spam_score)
    .bind(metadata.security_score)
    .bind(metadata.reputation_score)
    .bind(Json(metadata.dnsbl_hits))
    .bind(Json(metadata.auth_summary))
    .bind(Json(metadata.decision_trace))
    .bind(&metadata.magika_summary)
    .bind(&metadata.magika_decision)
    .bind(&metadata.remote_message_ref)
    .bind(&metadata.route_target)
    .bind(&metadata.search_text)
    .execute(pool)
    .await?;

    Ok(())
}

pub(in crate::smtp) async fn persist_quarantine_metadata_or_warn(
    spool_dir: &Path,
    config: &RuntimeConfig,
    message: &QueuedMessage,
) {
    if let Err(error) = persist_quarantine_metadata(spool_dir, config, message).await {
        warn!(
            trace_id = %message.id,
            error = %error,
            "unable to persist quarantine metadata in local PostgreSQL"
        );
    }
}

async fn remove_quarantine_metadata(config: &RuntimeConfig, trace_id: &str) -> Result<()> {
    let Some(pool) = ensure_local_db_schema(config).await? else {
        return Ok(());
    };
    sqlx::query("DELETE FROM quarantine_messages WHERE trace_id = $1")
        .bind(trace_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub(in crate::smtp) async fn remove_quarantine_metadata_or_warn(
    config: &RuntimeConfig,
    trace_id: &str,
) {
    if let Err(error) = remove_quarantine_metadata(config, trace_id).await {
        warn!(trace_id = trace_id, error = %error, "unable to remove quarantine metadata");
    }
}
