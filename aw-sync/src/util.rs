use std::error::Error;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

/// Returns the port of the local aw-server instance
#[cfg(not(target_os = "android"))]
pub fn get_server_port(testing: bool) -> Result<u16, Box<dyn Error>> {
    // TODO: get aw-server config more reliably
    let aw_server_conf = crate::dirs::get_server_config_path(testing)
        .map_err(|_| "Could not get aw-server config path")?;
    let fallback: u16 = if testing { 5666 } else { 5600 };
    let port = if aw_server_conf.exists() {
        let mut file = File::open(&aw_server_conf)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let value: toml::Value = toml::from_str(&contents)?;
        value
            .get("port")
            .and_then(|v| v.as_integer())
            .map(|v| v as u16)
            .unwrap_or(fallback)
    } else {
        fallback
    };
    Ok(port)
}

/// Check if a directory contains a .db file
fn contains_db_file(dir: &std::path::Path) -> bool {
    fs::read_dir(dir)
        .ok()
        .map(|entries| {
            entries.filter_map(Result::ok).any(|entry| {
                entry
                    .path()
                    .extension()
                    .map(|ext| ext == "db")
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

/// Check if a directory contains a subdirectory that contains a .db file
fn contains_subdir_with_db_file(dir: &std::path::Path) -> bool {
    fs::read_dir(dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .any(|entry| entry.path().is_dir() && contains_db_file(&entry.path()))
        })
        .unwrap_or(false)
}

/// Return all remotes in the sync folder
/// Only returns folders that match ./{host}/{device_id}/*.db
// TODO: share logic with find_remotes and find_remotes_nonlocal
pub fn get_remotes() -> Result<Vec<String>, Box<dyn Error>> {
    let sync_root_dir = crate::dirs::get_sync_dir()?;
    fs::create_dir_all(&sync_root_dir)?;
    let hostnames = fs::read_dir(sync_root_dir)?
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_dir() && contains_subdir_with_db_file(&entry.path()))
        .filter_map(|entry| {
            entry
                .path()
                .file_name()
                .and_then(|os_str| os_str.to_str().map(String::from))
        })
        .collect();
    info!("Found remotes: {:?}", hostnames);
    Ok(hostnames)
}

/// Returns a list of all remote dbs
fn find_remotes(sync_directory: &Path) -> std::io::Result<Vec<PathBuf>> {
    let dbs = fs::read_dir(sync_directory)?
        .map(|res| res.ok().unwrap().path())
        .filter(|p| p.is_dir())
        .flat_map(|d| fs::read_dir(d).unwrap())
        .map(|res| res.ok().unwrap().path())
        .filter(|path| path.extension().unwrap_or_else(|| OsStr::new("")) == "db")
        .collect();
    Ok(dbs)
}

/// Compares database paths using both the raw path and canonical path when available.
fn is_same_db_path(path: &Path, local_db_path: &Path) -> bool {
    if path == local_db_path {
        return true;
    }

    match (fs::canonicalize(path), fs::canonicalize(local_db_path)) {
        (Ok(path_canonical), Ok(local_canonical)) => path_canonical == local_canonical,
        _ => false,
    }
}

/// Returns a list of all remotes, excluding local ones
pub fn find_remotes_nonlocal(
    sync_directory: &Path,
    local_db_path: &Path,
    sync_db: Option<&PathBuf>,
) -> Vec<PathBuf> {
    let remotes_all = find_remotes(sync_directory).unwrap();
    remotes_all
        .into_iter()
        // Filter out the local staging database using an exact path comparison.
        .filter(|path| !is_same_db_path(path, local_db_path))
        // If sync_db is Some, return only remotes in that path
        .filter(|path| {
            if let Some(sync_db) = sync_db {
                path.starts_with(sync_db)
            } else {
                true
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::find_remotes_nonlocal;
    use std::fs::{self, File};
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Creates a unique temporary directory for filesystem-based tests.
    fn create_temp_dir(test_name: &str) -> PathBuf {
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let temp_dir = std::env::temp_dir().join(format!(
            "aw-sync-{test_name}-{}-{timestamp_ns}",
            std::process::id()
        ));
        fs::create_dir_all(&temp_dir).unwrap();
        temp_dir
    }

    /// Creates a fake sync database file in the expected `{device_id}/test.db` layout.
    fn create_sync_db(sync_root: &Path, device_id: &str) -> PathBuf {
        let device_dir = sync_root.join(device_id);
        fs::create_dir_all(&device_dir).unwrap();
        let db_path = device_dir.join("test.db");
        File::create(&db_path).unwrap();
        db_path
    }

    #[test]
    fn test_find_remotes_nonlocal_excludes_local_db_path() {
        let sync_root = create_temp_dir("exclude-local-db");
        let local_db_path = create_sync_db(&sync_root, "local-device");
        let remote_db_path = create_sync_db(&sync_root, "remote-device");

        let remote_paths = find_remotes_nonlocal(&sync_root, &local_db_path, None);

        assert_eq!(remote_paths, vec![remote_db_path]);

        fs::remove_dir_all(sync_root).unwrap();
    }
}
