//! 测试远程 aw-server 连通性（供 Web Settings「测试连接」按钮调用）

use rocket::serde::json::Json;
use std::collections::HashMap;
use std::time::Duration;


/// GET /api/0/test-remote-server?hostname=xxx&port=yyy
/// 对 http://hostname:port/api/0/info 发 GET，超时 3s；成功返回 {"ok": true}，失败返回 {"ok": false, "error": "..."}
#[get("/?<hostname>&<port>")]
pub fn test_remote_server(
    hostname: Option<String>,
    port: Option<String>,
) -> Json<HashMap<&'static str, serde_json::Value>> {
    let mut out: HashMap<&'static str, serde_json::Value> = HashMap::new();
    let hostname = match hostname.as_deref().filter(|s| !s.is_empty()) {
        Some(h) => h,
        None => {
            out.insert("ok", serde_json::json!(false));
            out.insert("error", serde_json::json!("hostname required"));
            return Json(out);
        }
    };
    let port: u16 = match port.as_deref().and_then(|s| s.parse().ok()) {
        Some(p) => p,
        None => {
            out.insert("ok", serde_json::json!(false));
            out.insert("error", serde_json::json!("port required or invalid"));
            return Json(out);
        }
    };
    let url = format!("http://{}:{}/api/0/info", hostname, port);
    let client = match reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            out.insert("ok", serde_json::json!(false));
            out.insert("error", serde_json::json!(e.to_string()));
            return Json(out);
        }
    };
    match client.get(&url).send() {
        Ok(resp) => {
            if resp.status().is_success() {
                out.insert("ok", serde_json::json!(true));
            } else {
                out.insert("ok", serde_json::json!(false));
                out.insert(
                    "error",
                    serde_json::json!(format!("HTTP {}", resp.status())),
                );
            }
        }
        Err(e) => {
            out.insert("ok", serde_json::json!(false));
            out.insert("error", serde_json::json!(e.to_string()));
        }
    }
    Json(out)
}
