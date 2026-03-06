//! 收到 create_bucket / heartbeat 后，best-effort 转发到远程 aw-server（所有 watcher 统一由此同步）。
//! 远程地址优先从 Web 设置读取（aw_server.forward_remote_*），其次为 config.toml [forward_remote]。

use std::thread;
use std::time::Duration;

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
fn resolve_forward_remote(
    state: &State<ServerState>,
    config: &State<AWConfig>,
) -> Option<(bool, String, u16)> {
    let datastore = match state.datastore.lock() {
        Ok(guard) => guard,
        Err(_) => return None,
    };
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
    let base = format!("http://{}:{}", hostname, port);
    let path = format!("/api/0/buckets/{}", bucket_id);
    let body = match serde_json::to_vec(bucket) {
        Ok(b) => b,
        Err(e) => {
            debug!("forward_remote: serialize bucket failed: {}", e);
            return;
        }
    };
    thread::spawn(move || {
        let url = format!("{}{}", base, path);
        let client = match reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                debug!("forward_remote create_bucket: client build failed: {}", e);
                return;
            }
        };
        if let Err(e) = client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
        {
            debug!("forward_remote create_bucket {}: {}", url, e);
        }
    });
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
    let base = format!("http://{}:{}", hostname, port);
    let path = format!("/api/0/buckets/{}/heartbeat?pulsetime={}", bucket_id, pulsetime);
    let body = match serde_json::to_vec(event) {
        Ok(b) => b,
        Err(e) => {
            debug!("forward_remote: serialize heartbeat failed: {}", e);
            return;
        }
    };
    thread::spawn(move || {
        let url = format!("{}{}", base, path);
        let client = match reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                debug!("forward_remote heartbeat: client build failed: {}", e);
                return;
            }
        };
        if let Err(e) = client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
        {
            debug!("forward_remote heartbeat {}: {}", url, e);
        }
    });
}
