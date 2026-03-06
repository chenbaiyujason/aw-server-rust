use std::error::Error;

use crate::sync::{sync_run, SyncMode, SyncSpec};
use aw_client_rust::blocking::AwClient;

pub fn pull_all(client: &AwClient) -> Result<(), Box<dyn Error>> {
    let hostnames = crate::util::get_remotes()?;
    for host in hostnames {
        // Skip the current host to avoid pulling back the data we just staged locally.
        if host == client.hostname {
            info!("Skipping local host during pull_all: {}", host);
            continue;
        }
        pull(&host, client)?
    }
    Ok(())
}

pub fn pull(host: &str, client: &AwClient) -> Result<(), Box<dyn Error>> {
    client.wait_for_start()?;

    // Guard explicit pulls as well, so a self-host request does not create a sync loop.
    if host == client.hostname {
        info!("Skipping pull for local host: {}", host);
        return Ok(());
    }

    // 同步目录结构: ./{hostname}/{device_id}/test.db
    // 对该 host 下所有逻辑设备（所有 UUID 的 db）都执行 pull，由 sync_run 内 find_remotes 发现全部，
    // sync_one 按事件身份 (timestamp+data) 合并去重，同身份保留更长 duration。
    let sync_root_dir = crate::dirs::get_sync_dir().map_err(|_| "Could not get sync dir")?;
    let sync_dir = sync_root_dir.join(host);

    let sync_spec = SyncSpec {
        path: sync_dir,
        path_db: None, // 不指定单个 db，让 sync_run 对该 host 下所有 device 的 db 都拉取并合并
        buckets: None,
        start: None,
    };
    sync_run(client, &sync_spec, SyncMode::Pull)?;

    Ok(())
}

pub fn push(client: &AwClient) -> Result<(), Box<dyn Error>> {
    push_with_hostname(client, &client.hostname)
}

pub fn push_with_hostname(client: &AwClient, hostname: &str) -> Result<(), Box<dyn Error>> {
    let sync_dir = crate::dirs::get_sync_dir()
        .map_err(|_| "Could not get sync dir")?
        .join(hostname);

    let sync_spec = SyncSpec {
        path: sync_dir,
        path_db: None,
        buckets: None, // Sync all buckets by default
        start: None,
    };
    sync_run(client, &sync_spec, SyncMode::Push)?;

    Ok(())
}
