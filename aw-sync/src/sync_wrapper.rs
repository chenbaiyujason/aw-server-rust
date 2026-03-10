use std::error::Error;
use std::path::{Path, PathBuf};

use crate::sync::{sync_run, SyncMode, SyncSpec};
use aw_client_rust::blocking::AwClient;

pub fn pull_all(client: &AwClient) -> Result<(), Box<dyn Error>> {
    let sync_root_dir = crate::dirs::get_sync_dir().map_err(|_| "Could not get sync dir")?;
    let sync_spec = SyncSpec {
        path: sync_root_dir,
        path_db: None,
        buckets: None,
        start: None,
    };
    pull_all_with_spec(client, &sync_spec)
}

pub fn pull(host: &str, client: &AwClient) -> Result<(), Box<dyn Error>> {
    client.wait_for_start()?;

    // 同步目录结构: ./{hostname}/{device_id}/test.db
    // 对该 host 下所有逻辑设备（所有 UUID 的 db）都执行 pull，由 sync_run 内 find_remotes 发现全部，
    // sync_one 按事件身份 (timestamp+data) 合并去重，同身份保留更长 duration。
    // 当前设备自己的 staging db 会在 find_remotes_nonlocal 中按精确路径排除。
    let sync_root_dir = crate::dirs::get_sync_dir().map_err(|_| "Could not get sync dir")?;
    let sync_dir = host_sync_dir(sync_root_dir.as_path(), host);

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
    let sync_root_dir = crate::dirs::get_sync_dir().map_err(|_| "Could not get sync dir")?;
    let sync_spec = SyncSpec {
        path: sync_root_dir,
        path_db: None,
        buckets: None, // Sync all buckets by default
        start: None,
    };
    push_with_hostname_and_spec(client, hostname, &sync_spec)
}

/// 构建某个 host 对应的同步目录路径。
fn host_sync_dir(sync_root_dir: &Path, hostname: &str) -> PathBuf {
    sync_root_dir.join(hostname)
}

/// 使用统一的多层目录布局执行 push。
pub fn push_with_hostname_and_spec(
    client: &AwClient,
    hostname: &str,
    sync_spec: &SyncSpec,
) -> Result<(), Box<dyn Error>> {
    let scoped_sync_spec = SyncSpec {
        path: host_sync_dir(sync_spec.path.as_path(), hostname),
        path_db: sync_spec.path_db.clone(),
        buckets: sync_spec.buckets.clone(),
        start: sync_spec.start,
    };

    sync_run(client, &scoped_sync_spec, SyncMode::Push)?;
    Ok(())
}

/// 从同步根目录下读取所有来源。
///
/// 读取时兼容：
/// - 新布局：`./{host}/{device_id}/test.db`
/// - 旧布局：`./{device_id}/test.db`
pub fn pull_all_with_spec(client: &AwClient, sync_spec: &SyncSpec) -> Result<(), Box<dyn Error>> {
    client.wait_for_start()?;

    let sync_source_dirs = if let Some(sync_db_path) = &sync_spec.path_db {
        vec![crate::util::infer_sync_directory_for_db(
            sync_spec.path.as_path(),
            sync_db_path.as_path(),
        )?]
    } else {
        crate::util::get_remote_sync_dirs(sync_spec.path.as_path())?
    };

    for sync_source_dir in sync_source_dirs {
        let scoped_sync_spec = SyncSpec {
            path: sync_source_dir,
            path_db: sync_spec.path_db.clone(),
            buckets: sync_spec.buckets.clone(),
            start: sync_spec.start,
        };
        sync_run(client, &scoped_sync_spec, SyncMode::Pull)?;
    }

    Ok(())
}
