//! 收到 bucket / event 变更后，best-effort 转发到远程 aw-server（所有 watcher 统一由此同步）。
//! 远程地址优先从 Web 设置读取（aw_server.forward_remote_*），其次为 config.toml [forward_remote]。

use std::thread;
use std::time::Duration;

use aw_datastore::Datastore;
use aw_models::Bucket;
use aw_models::Event;
use log::debug;
use rocket::State;

use crate::config::AWConfig;
use crate::endpoints::ServerState;

const KEY_ENABLED: &str = "settings.aw_server.forward_remote_enabled";
const KEY_HOSTNAME: &str = "settings.aw_server.forward_remote_hostname";
const KEY_PORT: &str = "settings.aw_server.forward_remote_port";

/// 解析 settings 中的 JSON 值，失败返回 None
fn parse_setting<T: serde::de::DeserializeOwned>(s: &str) -> Option<T> {
    serde_json::from_str(s).ok()
}

/// 从 datastore 的 settings 或 config 解析出 (enabled, hostname, port)。优先 settings。
fn resolve_forward_remote_from_datastore(
    datastore: &Datastore,
    config: &AWConfig,
) -> Option<(bool, String, u16)> {
    let enabled: Option<bool> = datastore
        .get_key_value(KEY_ENABLED)
        .ok()
        .and_then(|s| parse_setting(&s));
    let hostname: Option<String> = datastore
        .get_key_value(KEY_HOSTNAME)
        .ok()
        .and_then(|s| parse_setting(&s));
    let port: Option<u16> = datastore
        .get_key_value(KEY_PORT)
        .ok()
        .and_then(|s| parse_setting(&s));

    // 若 Web 设置里至少填了 hostname，则用 Web 设置（enabled 缺省为 true 便于开箱即用）
    if let Some(h) = hostname {
        if !h.is_empty() {
            return Some((
                enabled.unwrap_or(true),
                h,
                port.unwrap_or(5600),
            ));
        }
    }

    // 否则用 config.toml
    let fr = config.forward_remote.as_ref()?;
    if !fr.enabled || fr.hostname.is_empty() {
        return None;
    }
    Some((fr.enabled, fr.hostname.clone(), fr.port))
}

/// 从带锁的 ServerState 中解析远程转发配置。
fn resolve_forward_remote(
    state: &State<ServerState>,
    config: &State<AWConfig>,
) -> Option<(bool, String, u16)> {
    let datastore = match state.datastore.lock() {
        Ok(guard) => guard,
        Err(_) => return None,
    };
    resolve_forward_remote_from_datastore(&datastore, config.inner())
}

/// 统一封装 POST 转发，避免 create_bucket / events / heartbeat 分别重复拼接请求。
fn spawn_forward_post(base: String, path: String, content_label: &'static str, body: Vec<u8>) {
    thread::spawn(move || {
        let url = format!("{}{}", base, path);
        let client = match reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                debug!("forward_remote {}: client build failed: {}", content_label, e);
                return;
            }
        };
        if let Err(e) = client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
        {
            debug!("forward_remote {} {}: {}", content_label, url, e);
        }
    });
}

/// 统一封装 DELETE 转发，覆盖删除 bucket / 单条事件场景。
fn spawn_forward_delete(base: String, path: String, content_label: &'static str) {
    thread::spawn(move || {
        let url = format!("{}{}", base, path);
        let client = match reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                debug!("forward_remote {}: client build failed: {}", content_label, e);
                return;
            }
        };
        if let Err(e) = client.delete(&url).send() {
            debug!("forward_remote {} {}: {}", content_label, url, e);
        }
    });
}

/// 若配置了 forward_remote 且 enabled，则后台线程转发 create_bucket 到远程
pub fn maybe_forward_create_bucket(
    state: &State<ServerState>,
    config: &State<AWConfig>,
    bucket_id: &str,
    bucket: &Bucket,
) {
    let Some((enabled, hostname, port)) = resolve_forward_remote(state, config) else {
        return;
    };
    if !enabled || hostname.is_empty() {
        return;
    }
    maybe_forward_create_bucket_to_remote(hostname, port, bucket_id, bucket);
}

/// 供不经过 Rocket 路由、直接持有 Datastore 的调用方复用。
#[cfg_attr(not(target_os = "android"), allow(dead_code))]
pub fn maybe_forward_create_bucket_for_datastore(
    datastore: &Datastore,
    config: &AWConfig,
    bucket_id: &str,
    bucket: &Bucket,
) {
    let Some((enabled, hostname, port)) = resolve_forward_remote_from_datastore(datastore, config)
    else {
        return;
    };
    if !enabled || hostname.is_empty() {
        return;
    }
    maybe_forward_create_bucket_to_remote(hostname, port, bucket_id, bucket);
}

fn maybe_forward_create_bucket_to_remote(hostname: String, port: u16, bucket_id: &str, bucket: &Bucket) {
    let base = format!("http://{}:{}", hostname, port);
    let path = format!("/api/0/buckets/{}", bucket_id);
    let body = match serde_json::to_vec(bucket) {
        Ok(b) => b,
        Err(e) => {
            debug!("forward_remote: serialize bucket failed: {}", e);
            return;
        }
    };
    spawn_forward_post(base, path, "create_bucket", body);
}

/// 若配置了 forward_remote 且 enabled，则后台线程转发 heartbeat 到远程
pub fn maybe_forward_heartbeat(
    state: &State<ServerState>,
    config: &State<AWConfig>,
    bucket_id: &str,
    pulsetime: f64,
    event: &Event,
) {
    let Some((enabled, hostname, port)) = resolve_forward_remote(state, config) else {
        return;
    };
    if !enabled || hostname.is_empty() {
        return;
    }
    maybe_forward_heartbeat_to_remote(hostname, port, bucket_id, pulsetime, event);
}

/// 供不经过 Rocket 路由、直接持有 Datastore 的调用方复用。
#[cfg_attr(not(target_os = "android"), allow(dead_code))]
pub fn maybe_forward_heartbeat_for_datastore(
    datastore: &Datastore,
    config: &AWConfig,
    bucket_id: &str,
    pulsetime: f64,
    event: &Event,
) {
    let Some((enabled, hostname, port)) = resolve_forward_remote_from_datastore(datastore, config)
    else {
        return;
    };
    if !enabled || hostname.is_empty() {
        return;
    }
    maybe_forward_heartbeat_to_remote(hostname, port, bucket_id, pulsetime, event);
}

fn maybe_forward_heartbeat_to_remote(
    hostname: String,
    port: u16,
    bucket_id: &str,
    pulsetime: f64,
    event: &Event,
) {
    let base = format!("http://{}:{}", hostname, port);
    let path = format!("/api/0/buckets/{}/heartbeat?pulsetime={}", bucket_id, pulsetime);
    let body = match serde_json::to_vec(event) {
        Ok(b) => b,
        Err(e) => {
            debug!("forward_remote: serialize heartbeat failed: {}", e);
            return;
        }
    };
    spawn_forward_post(base, path, "heartbeat", body);
}

/// 若配置了 forward_remote 且 enabled，则后台线程转发 events 批量写入到远程。
pub fn maybe_forward_insert_events(
    state: &State<ServerState>,
    config: &State<AWConfig>,
    bucket_id: &str,
    events: &[Event],
) {
    let Some((enabled, hostname, port)) = resolve_forward_remote(state, config) else {
        return;
    };
    if !enabled || hostname.is_empty() || events.is_empty() {
        return;
    }
    maybe_forward_insert_events_to_remote(hostname, port, bucket_id, events);
}

/// 供不经过 Rocket 路由、直接持有 Datastore 的调用方复用。
#[allow(dead_code)]
pub fn maybe_forward_insert_events_for_datastore(
    datastore: &Datastore,
    config: &AWConfig,
    bucket_id: &str,
    events: &[Event],
) {
    let Some((enabled, hostname, port)) = resolve_forward_remote_from_datastore(datastore, config)
    else {
        return;
    };
    if !enabled || hostname.is_empty() || events.is_empty() {
        return;
    }
    maybe_forward_insert_events_to_remote(hostname, port, bucket_id, events);
}

fn maybe_forward_insert_events_to_remote(
    hostname: String,
    port: u16,
    bucket_id: &str,
    events: &[Event],
) {
    let base = format!("http://{}:{}", hostname, port);
    let path = format!("/api/0/buckets/{}/events", bucket_id);
    let body = match serde_json::to_vec(events) {
        Ok(b) => b,
        Err(e) => {
            debug!("forward_remote: serialize events failed: {}", e);
            return;
        }
    };
    spawn_forward_post(base, path, "insert_events", body);
}

/// 若配置了 forward_remote 且 enabled，则后台线程转发删除单条 event 到远程。
pub fn maybe_forward_delete_event(
    state: &State<ServerState>,
    config: &State<AWConfig>,
    bucket_id: &str,
    event_id: i64,
) {
    let Some((enabled, hostname, port)) = resolve_forward_remote(state, config) else {
        return;
    };
    if !enabled || hostname.is_empty() {
        return;
    }
    let base = format!("http://{}:{}", hostname, port);
    let path = format!("/api/0/buckets/{}/events/{}", bucket_id, event_id);
    spawn_forward_delete(base, path, "delete_event");
}

/// 若配置了 forward_remote 且 enabled，则后台线程转发删除 bucket 到远程。
pub fn maybe_forward_delete_bucket(
    state: &State<ServerState>,
    config: &State<AWConfig>,
    bucket_id: &str,
) {
    let Some((enabled, hostname, port)) = resolve_forward_remote(state, config) else {
        return;
    };
    if !enabled || hostname.is_empty() {
        return;
    }
    let base = format!("http://{}:{}", hostname, port);
    let path = format!("/api/0/buckets/{}", bucket_id);
    spawn_forward_delete(base, path, "delete_bucket");
}
