use axum::{
    extract::{MatchedPath, Request},
    http::{header::CONTENT_TYPE, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{
    collections::BTreeMap,
    env,
    path::Path,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};
use tracing::info;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const TRACE_HEADER: &str = "x-trace-id";
const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

static METRICS: OnceLock<Mutex<CtMetrics>> = OnceLock::new();

#[derive(Default)]
struct CtMetrics {
    http_requests_total: BTreeMap<(String, String, u16), u64>,
    http_request_duration: BTreeMap<(String, String, u16), DurationAggregate>,
    outbound_handoffs_total: BTreeMap<String, u64>,
    inbound_delivery_total: BTreeMap<String, u64>,
    smtp_sessions_total: BTreeMap<String, u64>,
    security_events_total: BTreeMap<String, u64>,
}

#[derive(Default)]
struct DurationAggregate {
    count: u64,
    sum_seconds: f64,
}

fn metrics() -> &'static Mutex<CtMetrics> {
    METRICS.get_or_init(|| Mutex::new(CtMetrics::default()))
}

pub fn init_tracing(service_name: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let format = env::var("LPE_CT_LOG_FORMAT").unwrap_or_else(|_| "plain".to_string());

    match format.trim().to_ascii_lowercase().as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .with_current_span(false)
                .with_span_list(false)
                .with_target(false)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(false)
                .init();
        }
    }

    info!(service = service_name, log_format = %format, "observability initialized");
}

pub fn trace_id_from_headers(headers: &axum::http::HeaderMap) -> String {
    headers
        .get(TRACE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

pub async fn observe_http(mut request: Request, next: Next) -> Response {
    let started_at = Instant::now();
    let method = request.method().as_str().to_string();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or_else(|| request.uri().path())
        .to_string();
    let trace_id = trace_id_from_headers(request.headers());
    if let Ok(value) = HeaderValue::from_str(&trace_id) {
        request.headers_mut().insert(TRACE_HEADER, value);
    }

    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&trace_id) {
        response.headers_mut().insert(TRACE_HEADER, value);
    }

    let status = response.status().as_u16();
    let elapsed = started_at.elapsed();
    record_http_request(&method, &route, status, elapsed);
    info!(
        trace_id = %trace_id,
        method = %method,
        route = %route,
        status,
        duration_ms = elapsed.as_secs_f64() * 1000.0,
        "http request completed"
    );

    response
}

pub async fn metrics_endpoint(
    spool_dir: std::sync::Arc<std::path::PathBuf>,
) -> impl IntoResponse {
    if !metrics_enabled() {
        return StatusCode::NOT_FOUND.into_response();
    }

    (
        [(CONTENT_TYPE, HeaderValue::from_static(METRICS_CONTENT_TYPE))],
        render_metrics(spool_dir.as_ref()),
    )
        .into_response()
}

pub fn record_outbound_handoff(status: &str) {
    if let Ok(mut guard) = metrics().lock() {
        *guard
            .outbound_handoffs_total
            .entry(status.to_string())
            .or_insert(0) += 1;
    }
}

pub fn record_inbound_delivery(status: &str) {
    if let Ok(mut guard) = metrics().lock() {
        *guard
            .inbound_delivery_total
            .entry(status.to_string())
            .or_insert(0) += 1;
    }
}

pub fn record_smtp_session(result: &str) {
    if let Ok(mut guard) = metrics().lock() {
        *guard.smtp_sessions_total.entry(result.to_string()).or_insert(0) += 1;
    }
}

pub fn record_security_event(event: &str) {
    if let Ok(mut guard) = metrics().lock() {
        *guard
            .security_events_total
            .entry(event.to_string())
            .or_insert(0) += 1;
    }
}

fn record_http_request(method: &str, route: &str, status: u16, elapsed: Duration) {
    if let Ok(mut guard) = metrics().lock() {
        let key = (method.to_string(), route.to_string(), status);
        *guard.http_requests_total.entry(key.clone()).or_insert(0) += 1;
        let aggregate = guard.http_request_duration.entry(key).or_default();
        aggregate.count += 1;
        aggregate.sum_seconds += elapsed.as_secs_f64();
    }
}

fn render_metrics(spool_dir: &Path) -> String {
    let guard = match metrics().lock() {
        Ok(guard) => guard,
        Err(_) => return "# observability unavailable\n".to_string(),
    };

    let mut output = String::new();
    output.push_str("# HELP lpe_ct_http_requests_total Total HTTP requests handled by LPE-CT.\n");
    output.push_str("# TYPE lpe_ct_http_requests_total counter\n");
    for ((method, route, status), value) in &guard.http_requests_total {
        output.push_str(&format!(
            "lpe_ct_http_requests_total{{method=\"{}\",route=\"{}\",status=\"{}\"}} {}\n",
            escape_label(method),
            escape_label(route),
            status,
            value
        ));
    }

    output.push_str("# HELP lpe_ct_http_request_duration_seconds_sum Total HTTP request duration in seconds.\n");
    output.push_str("# TYPE lpe_ct_http_request_duration_seconds_sum counter\n");
    for ((method, route, status), value) in &guard.http_request_duration {
        output.push_str(&format!(
            "lpe_ct_http_request_duration_seconds_sum{{method=\"{}\",route=\"{}\",status=\"{}\"}} {:.6}\n",
            escape_label(method),
            escape_label(route),
            status,
            value.sum_seconds
        ));
    }

    output.push_str("# HELP lpe_ct_http_request_duration_seconds_count Total observed HTTP requests for duration tracking.\n");
    output.push_str("# TYPE lpe_ct_http_request_duration_seconds_count counter\n");
    for ((method, route, status), value) in &guard.http_request_duration {
        output.push_str(&format!(
            "lpe_ct_http_request_duration_seconds_count{{method=\"{}\",route=\"{}\",status=\"{}\"}} {}\n",
            escape_label(method),
            escape_label(route),
            status,
            value.count
        ));
    }

    output.push_str("# HELP lpe_ct_outbound_handoffs_total Outbound relay handoff results handled by LPE-CT.\n");
    output.push_str("# TYPE lpe_ct_outbound_handoffs_total counter\n");
    for (status, value) in &guard.outbound_handoffs_total {
        output.push_str(&format!(
            "lpe_ct_outbound_handoffs_total{{status=\"{}\"}} {}\n",
            escape_label(status),
            value
        ));
    }

    output.push_str("# HELP lpe_ct_inbound_delivery_total Results of final delivery calls from LPE-CT to LPE.\n");
    output.push_str("# TYPE lpe_ct_inbound_delivery_total counter\n");
    for (status, value) in &guard.inbound_delivery_total {
        output.push_str(&format!(
            "lpe_ct_inbound_delivery_total{{status=\"{}\"}} {}\n",
            escape_label(status),
            value
        ));
    }

    output.push_str("# HELP lpe_ct_smtp_sessions_total SMTP session outcomes observed at the DMZ edge.\n");
    output.push_str("# TYPE lpe_ct_smtp_sessions_total counter\n");
    for (result, value) in &guard.smtp_sessions_total {
        output.push_str(&format!(
            "lpe_ct_smtp_sessions_total{{result=\"{}\"}} {}\n",
            escape_label(result),
            value
        ));
    }

    output.push_str("# HELP lpe_ct_security_events_total Security-significant decisions observed by LPE-CT.\n");
    output.push_str("# TYPE lpe_ct_security_events_total counter\n");
    for (event, value) in &guard.security_events_total {
        output.push_str(&format!(
            "lpe_ct_security_events_total{{event=\"{}\"}} {}\n",
            escape_label(event),
            value
        ));
    }

    output.push_str("# HELP lpe_ct_spool_messages Current number of queued message files per spool queue.\n");
    output.push_str("# TYPE lpe_ct_spool_messages gauge\n");
    for queue in ["incoming", "outbound", "deferred", "quarantine", "held", "bounces", "sent"] {
        output.push_str(&format!(
            "lpe_ct_spool_messages{{queue=\"{}\"}} {}\n",
            queue,
            count_queue(spool_dir, queue)
        ));
    }

    output
}

fn count_queue(spool_dir: &Path, queue: &str) -> u64 {
    std::fs::read_dir(spool_dir.join(queue))
        .ok()
        .into_iter()
        .flat_map(|entries| entries.filter_map(Result::ok))
        .filter(|entry| entry.path().extension().and_then(|value| value.to_str()) == Some("json"))
        .count() as u64
}

fn metrics_enabled() -> bool {
    env::var("LPE_CT_METRICS_ENABLED")
        .ok()
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(true)
}

fn escape_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
