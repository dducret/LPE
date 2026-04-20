use axum::{
    extract::{MatchedPath, Request},
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{
    collections::BTreeMap,
    env,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::info;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const TRACE_HEADER: &str = "x-trace-id";
const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

static METRICS: OnceLock<Mutex<CoreMetrics>> = OnceLock::new();

#[derive(Default)]
struct CoreMetrics {
    http_requests_total: BTreeMap<(String, String, u16), u64>,
    http_request_duration: BTreeMap<(String, String, u16), DurationAggregate>,
    mail_submissions_total: BTreeMap<String, u64>,
    inbound_deliveries_total: BTreeMap<String, u64>,
    outbound_worker_dispatch_total: BTreeMap<String, u64>,
    security_events_total: BTreeMap<String, u64>,
    outbound_worker_batch_size_last: i64,
    outbound_worker_last_poll_timestamp_seconds: u64,
}

#[derive(Default)]
struct DurationAggregate {
    count: u64,
    sum_seconds: f64,
}

fn metrics() -> &'static Mutex<CoreMetrics> {
    METRICS.get_or_init(|| Mutex::new(CoreMetrics::default()))
}

pub fn init_tracing(service_name: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let format = env::var("LPE_LOG_FORMAT").unwrap_or_else(|_| "plain".to_string());

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

pub fn trace_id_from_headers(headers: &HeaderMap) -> String {
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

pub async fn metrics_endpoint() -> impl IntoResponse {
    if !metrics_enabled() {
        return StatusCode::NOT_FOUND.into_response();
    }

    (
        [(CONTENT_TYPE, HeaderValue::from_static(METRICS_CONTENT_TYPE))],
        render_metrics(),
    )
        .into_response()
}

pub fn record_mail_submission(source: &str) {
    if let Ok(mut guard) = metrics().lock() {
        *guard
            .mail_submissions_total
            .entry(source.to_string())
            .or_insert(0) += 1;
    }
}

pub fn record_inbound_delivery(status: &str) {
    if let Ok(mut guard) = metrics().lock() {
        *guard
            .inbound_deliveries_total
            .entry(status.to_string())
            .or_insert(0) += 1;
    }
}

pub fn record_outbound_dispatch(status: &str) {
    if let Ok(mut guard) = metrics().lock() {
        *guard
            .outbound_worker_dispatch_total
            .entry(status.to_string())
            .or_insert(0) += 1;
    }
}

pub fn record_outbound_worker_poll(batch_size: usize) {
    if let Ok(mut guard) = metrics().lock() {
        guard.outbound_worker_batch_size_last = batch_size as i64;
        guard.outbound_worker_last_poll_timestamp_seconds = unix_timestamp_seconds();
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

fn render_metrics() -> String {
    let guard = match metrics().lock() {
        Ok(guard) => guard,
        Err(_) => return "# observability unavailable\n".to_string(),
    };

    let mut output = String::new();
    output.push_str("# HELP lpe_http_requests_total Total HTTP requests handled by LPE.\n");
    output.push_str("# TYPE lpe_http_requests_total counter\n");
    for ((method, route, status), value) in &guard.http_requests_total {
        output.push_str(&format!(
            "lpe_http_requests_total{{method=\"{}\",route=\"{}\",status=\"{}\"}} {}\n",
            escape_label(method),
            escape_label(route),
            status,
            value
        ));
    }

    output.push_str(
        "# HELP lpe_http_request_duration_seconds_sum Total HTTP request duration in seconds.\n",
    );
    output.push_str("# TYPE lpe_http_request_duration_seconds_sum counter\n");
    for ((method, route, status), value) in &guard.http_request_duration {
        output.push_str(&format!(
            "lpe_http_request_duration_seconds_sum{{method=\"{}\",route=\"{}\",status=\"{}\"}} {:.6}\n",
            escape_label(method),
            escape_label(route),
            status,
            value.sum_seconds
        ));
    }

    output.push_str(
        "# HELP lpe_http_request_duration_seconds_count Total observed HTTP requests for duration tracking.\n",
    );
    output.push_str("# TYPE lpe_http_request_duration_seconds_count counter\n");
    for ((method, route, status), value) in &guard.http_request_duration {
        output.push_str(&format!(
            "lpe_http_request_duration_seconds_count{{method=\"{}\",route=\"{}\",status=\"{}\"}} {}\n",
            escape_label(method),
            escape_label(route),
            status,
            value.count
        ));
    }

    output.push_str(
        "# HELP lpe_mail_submissions_total Canonical submitted messages accepted by LPE.\n",
    );
    output.push_str("# TYPE lpe_mail_submissions_total counter\n");
    for (source, value) in &guard.mail_submissions_total {
        output.push_str(&format!(
            "lpe_mail_submissions_total{{source=\"{}\"}} {}\n",
            escape_label(source),
            value
        ));
    }

    output.push_str(
        "# HELP lpe_mail_inbound_deliveries_total Final inbound deliveries accepted from LPE-CT.\n",
    );
    output.push_str("# TYPE lpe_mail_inbound_deliveries_total counter\n");
    for (status, value) in &guard.inbound_deliveries_total {
        output.push_str(&format!(
            "lpe_mail_inbound_deliveries_total{{status=\"{}\"}} {}\n",
            escape_label(status),
            value
        ));
    }

    output.push_str("# HELP lpe_outbound_worker_dispatch_total Outbound worker dispatch results toward LPE-CT.\n");
    output.push_str("# TYPE lpe_outbound_worker_dispatch_total counter\n");
    for (status, value) in &guard.outbound_worker_dispatch_total {
        output.push_str(&format!(
            "lpe_outbound_worker_dispatch_total{{status=\"{}\"}} {}\n",
            escape_label(status),
            value
        ));
    }

    output
        .push_str("# HELP lpe_outbound_worker_batch_size_last Last outbound worker batch size.\n");
    output.push_str("# TYPE lpe_outbound_worker_batch_size_last gauge\n");
    output.push_str(&format!(
        "lpe_outbound_worker_batch_size_last {}\n",
        guard.outbound_worker_batch_size_last
    ));

    output.push_str("# HELP lpe_outbound_worker_last_poll_timestamp_seconds Unix timestamp of the last outbound worker poll.\n");
    output.push_str("# TYPE lpe_outbound_worker_last_poll_timestamp_seconds gauge\n");
    output.push_str(&format!(
        "lpe_outbound_worker_last_poll_timestamp_seconds {}\n",
        guard.outbound_worker_last_poll_timestamp_seconds
    ));

    output.push_str(
        "# HELP lpe_security_events_total Security-significant events observed by LPE.\n",
    );
    output.push_str("# TYPE lpe_security_events_total counter\n");
    for (event, value) in &guard.security_events_total {
        output.push_str(&format!(
            "lpe_security_events_total{{event=\"{}\"}} {}\n",
            escape_label(event),
            value
        ));
    }

    output
}

fn metrics_enabled() -> bool {
    env::var("LPE_METRICS_ENABLED")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(true)
}

fn escape_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn unix_timestamp_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
