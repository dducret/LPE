use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::body::Bytes;

#[cfg(not(test))]
use super::RPC_PROXY_CONNECTION_TIMEOUT_MS;

type OutChannelKey = (String, Option<[u8; 16]>);
type OutChannelSender = tokio::sync::mpsc::UnboundedSender<Bytes>;
type PendingOutChannelResponse = (Instant, [u8; 16], Vec<u8>);

#[cfg(not(test))]
pub(super) fn rpc_proxy_channel_hold_ms() -> u64 {
    std::env::var("LPE_RPC_PROXY_OUT_CHANNEL_HOLD_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(u64::from(RPC_PROXY_CONNECTION_TIMEOUT_MS))
        .min(14_400_000)
}

#[cfg(test)]
pub(super) fn rpc_proxy_channel_hold_ms() -> u64 {
    1
}

pub(crate) fn mark_rpc_proxy_out_endpoint_bind_ack(query: &str) {
    let mut pending = rpc_proxy_out_endpoint_bind_acks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let count = pending.entry(query.to_string()).or_insert(0);
    *count = count.saturating_add(1);
}

pub(super) fn consume_rpc_proxy_out_endpoint_bind_ack(query: &str) -> bool {
    let mut pending = rpc_proxy_out_endpoint_bind_acks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(count) = pending.get_mut(query) else {
        return false;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        pending.remove(query);
    }
    true
}

pub(super) fn rpc_proxy_should_send_synthetic_rts_connect(query: &str) -> bool {
    query.contains(":6004")
}

pub(super) fn register_rpc_proxy_out_channel(
    query: &str,
    virtual_connection_cookie: Option<[u8; 16]>,
    sender: OutChannelSender,
) {
    let mut channels = rpc_proxy_out_channels()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    channels.insert(
        (query.to_string(), virtual_connection_cookie),
        sender.clone(),
    );
    channels.insert((query.to_string(), None), sender);
}

pub(super) fn send_rpc_proxy_out_channel(
    query: &str,
    virtual_connection_cookie: Option<[u8; 16]>,
    bytes: Vec<u8>,
) -> bool {
    let sender = {
        let channels = rpc_proxy_out_channels()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if virtual_connection_cookie.is_some() {
            channels
                .get(&(query.to_string(), virtual_connection_cookie))
                .cloned()
        } else {
            channels.get(&(query.to_string(), None)).cloned()
        }
    };
    if let Some(sender) = sender {
        return sender.send(Bytes::from(bytes)).is_ok();
    }
    false
}

pub(super) fn queue_pending_rpc_proxy_out_channel_response(
    query: &str,
    virtual_connection_cookie: [u8; 16],
    bytes: Vec<u8>,
) {
    let now = Instant::now();
    let ttl = Duration::from_millis(rpc_proxy_channel_hold_ms());
    let mut pending = pending_rpc_proxy_out_channel_responses()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    pending.retain(|_, entries| {
        entries.retain(|(first_seen, _, _)| now.duration_since(*first_seen) <= ttl);
        !entries.is_empty()
    });
    let entries = pending.entry(query.to_string()).or_default();
    if entries.len() < 8 {
        entries.push((now, virtual_connection_cookie, bytes));
    }
}

pub(super) fn consume_pending_rpc_proxy_out_channel_responses(
    query: &str,
    virtual_connection_cookie: Option<[u8; 16]>,
) -> Vec<u8> {
    let now = Instant::now();
    let ttl = Duration::from_millis(rpc_proxy_channel_hold_ms());
    let mut pending = pending_rpc_proxy_out_channel_responses()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(entries) = pending.get_mut(query) else {
        return Vec::new();
    };
    let mut matched = Vec::new();
    entries.retain(|(first_seen, cookie, bytes)| {
        let fresh = now.duration_since(*first_seen) <= ttl;
        if fresh && virtual_connection_cookie.is_some_and(|expected| expected == *cookie) {
            matched.extend_from_slice(bytes);
            false
        } else {
            fresh
        }
    });
    if entries.is_empty() {
        pending.remove(query);
    }
    matched
}

pub(super) fn mark_rpc_proxy_out_endpoint_rts_connect(query: &str) {
    let mut pending = rpc_proxy_out_endpoint_rts_connects()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let count = pending.entry(query.to_string()).or_insert(0);
    *count = count.saturating_add(1);
}

pub(super) fn consume_rpc_proxy_out_endpoint_rts_connect(query: &str) -> bool {
    let mut pending = rpc_proxy_out_endpoint_rts_connects()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(count) = pending.get_mut(query) else {
        return false;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        pending.remove(query);
    }
    true
}

pub(super) fn remove_rpc_proxy_out_channel(
    query: &str,
    virtual_connection_cookie: Option<[u8; 16]>,
) {
    let mut channels = rpc_proxy_out_channels()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    channels.remove(&(query.to_string(), virtual_connection_cookie));
    channels.remove(&(query.to_string(), None));
}

fn rpc_proxy_out_endpoint_bind_acks() -> &'static Mutex<HashMap<String, usize>> {
    static BIND_ACKS: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    BIND_ACKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn rpc_proxy_out_endpoint_rts_connects() -> &'static Mutex<HashMap<String, usize>> {
    static PENDING: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    PENDING.get_or_init(|| Mutex::new(HashMap::new()))
}

fn pending_rpc_proxy_out_channel_responses(
) -> &'static Mutex<HashMap<String, Vec<PendingOutChannelResponse>>> {
    static PENDING: OnceLock<Mutex<HashMap<String, Vec<PendingOutChannelResponse>>>> =
        OnceLock::new();
    PENDING.get_or_init(|| Mutex::new(HashMap::new()))
}

fn rpc_proxy_out_channels() -> &'static Mutex<HashMap<OutChannelKey, OutChannelSender>> {
    static CHANNELS: OnceLock<Mutex<HashMap<OutChannelKey, OutChannelSender>>> = OnceLock::new();
    CHANNELS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_proxy_out_channels_are_scoped_by_virtual_connection_cookie() {
        let query = "mail.cookie-scope.example.test:6004";
        let cookie_a = [0x0a; 16];
        let cookie_b = [0x0b; 16];
        let (sender_a, mut receiver_a) = tokio::sync::mpsc::unbounded_channel();
        let (sender_b, mut receiver_b) = tokio::sync::mpsc::unbounded_channel();

        register_rpc_proxy_out_channel(query, Some(cookie_a), sender_a);
        register_rpc_proxy_out_channel(query, Some(cookie_b), sender_b);

        assert!(send_rpc_proxy_out_channel(query, Some(cookie_a), vec![1]));
        assert!(send_rpc_proxy_out_channel(query, Some(cookie_b), vec![2]));

        assert_eq!(receiver_a.try_recv().unwrap(), Bytes::from_static(&[1]));
        assert_eq!(receiver_b.try_recv().unwrap(), Bytes::from_static(&[2]));

        remove_rpc_proxy_out_channel(query, Some(cookie_a));
        remove_rpc_proxy_out_channel(query, Some(cookie_b));
    }

    #[test]
    fn rpc_proxy_cookie_scoped_response_does_not_fall_back_to_unscoped_out_channel() {
        let query = "mail.stale-unscoped.example.test:6002";
        let stale_cookie = [0x0a; 16];
        let current_cookie = [0x0b; 16];
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        register_rpc_proxy_out_channel(query, Some(stale_cookie), sender);

        assert!(!send_rpc_proxy_out_channel(
            query,
            Some(current_cookie),
            vec![1]
        ));
        assert!(receiver.try_recv().is_err());

        assert!(send_rpc_proxy_out_channel(query, None, vec![2]));
        assert_eq!(receiver.try_recv().unwrap(), Bytes::from_static(&[2]));

        remove_rpc_proxy_out_channel(query, Some(stale_cookie));
    }
}
