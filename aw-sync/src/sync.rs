/// Basic syncing for ActivityWatch
/// Based on: https://github.com/ActivityWatch/aw-server/pull/50
///
/// This does not handle any direct peer interaction/connections/networking, it works as a "bring your own folder synchronizer".
///
/// It manages a sync-folder by syncing the aw-server datastore with a copy/staging datastore in the folder (one for each host).
/// The sync folder is then synced with remotes using Syncthing/Dropbox/whatever.
extern crate chrono;
extern crate reqwest;
extern crate serde_json;

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use aw_client_rust::blocking::AwClient;
use chrono::{DateTime, Utc};

use aw_datastore::{Datastore, DatastoreError};
use aw_models::{Bucket, Event};
use serde_json::{Map, Value};

#[cfg(feature = "cli")]
use clap::ValueEnum;

use crate::accessmethod::AccessMethod;

#[derive(PartialEq, Eq, Copy, Clone)]
#[cfg_attr(feature = "cli", derive(ValueEnum))]
pub enum SyncMode {
    Push,
    Pull,
    Both,
}

#[derive(Debug)]
pub struct SyncSpec {
    /// Path of sync folder
    pub path: PathBuf,
    /// Path of sync db
    /// If None, will use all
    pub path_db: Option<PathBuf>,
    /// Bucket IDs to sync
    pub buckets: Option<Vec<String>>,
    /// Start of time range to sync
    pub start: Option<DateTime<Utc>>,
}

impl Default for SyncSpec {
    fn default() -> Self {
        // TODO: Better default path
        let path = Path::new("/tmp/aw-sync").to_path_buf();
        SyncSpec {
            path,
            path_db: None,
            buckets: None,
            start: None,
        }
    }
}

/// Performs a single sync pass
pub fn sync_run(
    client: &AwClient,
    sync_spec: &SyncSpec,
    mode: SyncMode,
) -> Result<(), Box<dyn Error>> {
    let info = client.get_info()?;

    // FIXME: Here it is assumed that the device_id for the local server is the one used by
    // aw-server-rust, which is not necessarily true (aw-server-python has seperate device_id).
    // Therefore, this may sometimes fail to pick up the correct local datastore.
    let device_id = info.device_id.as_str();
    // Keep the exact local staging database path so we only exclude our own sync cache.
    let local_db_path = local_remote_db_path(sync_spec.path.as_path(), device_id);

    // FIXME: Bad device_id assumption?
    let ds_localremote = setup_local_remote(sync_spec.path.as_path(), device_id)?;
    let remote_dbfiles = crate::util::find_remotes_nonlocal(
        sync_spec.path.as_path(),
        local_db_path.as_path(),
        sync_spec.path_db.as_ref(),
    );

    // Log if remotes found
    // TODO: Only log remotes of interest
    if !remote_dbfiles.is_empty() {
        info!(
            "Found {} remote db files: {:?}",
            remote_dbfiles.len(),
            remote_dbfiles
        );
    }

    // TODO: Check for compatible remote db version before opening
    let ds_remotes: Vec<Datastore> = remote_dbfiles
        .iter()
        .map(|p| p.as_path())
        .map(create_datastore)
        .collect();

    if !ds_remotes.is_empty() {
        info!(
            "Found {} remote datastores: {:?}",
            ds_remotes.len(),
            ds_remotes
        );
    }

    // Pull
    if mode == SyncMode::Pull || mode == SyncMode::Both {
        info!("Pulling...");
        for ds_from in &ds_remotes {
            sync_datastores(ds_from, client, false, None, sync_spec);
        }
    }

    // Push local server buckets to sync folder
    if mode == SyncMode::Push || mode == SyncMode::Both {
        info!("Pushing...");
        sync_datastores(client, &ds_localremote, true, Some(device_id), sync_spec);
    }

    // Close open database connections
    for ds_from in &ds_remotes {
        ds_from.close();
    }
    ds_localremote.close();

    // Dropping also works to close the database connections, weirdly enough.
    // Probably because once the database is dropped, the thread will stop,
    // and then the Connection will be dropped, which closes the connection.
    std::mem::drop(ds_remotes);
    std::mem::drop(ds_localremote);

    // NOTE: Will fail if db connections not closed (as it will open them again)
    //list_buckets(&client, sync_spec.path.as_path());

    Ok(())
}

#[allow(dead_code)]
pub fn list_buckets(client: &AwClient) -> Result<(), Box<dyn Error>> {
    let sync_root_dir = crate::dirs::get_sync_dir().map_err(|_| "Could not get sync dir")?;
    let local_host_sync_dir = sync_root_dir.join(&client.hostname);
    let info = client.get_info()?;

    // FIXME: Incorrect device_id assumption?
    let device_id = info.device_id.as_str();
    // Reuse the same exact-path filtering as sync mode to avoid listing our own staging DB.
    let local_db_path = local_remote_db_path(local_host_sync_dir.as_path(), device_id);
    let ds_localremote = setup_local_remote(local_host_sync_dir.as_path(), device_id)?;

    // 同时列出新旧两种布局下的远端数据库，方便排查兼容性问题。
    let mut remote_dbfiles = Vec::<PathBuf>::new();
    for sync_source_dir in crate::util::get_remote_sync_dirs(sync_root_dir.as_path())? {
        remote_dbfiles.extend(crate::util::find_remotes_nonlocal(
            sync_source_dir.as_path(),
            local_db_path.as_path(),
            None,
        ));
    }
    info!("Found remotes: {:?}", remote_dbfiles);

    // TODO: Check for compatible remote db version before opening
    let ds_remotes: Vec<Datastore> = remote_dbfiles
        .iter()
        .map(|p| p.as_path())
        .map(create_datastore)
        .collect();

    log_buckets(client);
    log_buckets(&ds_localremote);
    for ds_from in &ds_remotes {
        log_buckets(ds_from);
    }

    Ok(())
}

/// Returns the staging database path for the current device inside the host sync directory.
fn local_remote_db_path(path: &Path, device_id: &str) -> PathBuf {
    path.join(device_id).join("test.db")
}

fn setup_local_remote(path: &Path, device_id: &str) -> Result<Datastore, Box<dyn Error>> {
    // FIXME: Don't run twice if already exists
    fs::create_dir_all(path)?;

    let remotedir = path.join(device_id);
    fs::create_dir_all(&remotedir)?;

    let dbfile = local_remote_db_path(path, device_id);

    // Print a message if dbfile doesn't already exist
    if !dbfile.exists() {
        info!("Creating new database file: {}", dbfile.display());
    }

    let ds_localremote = create_datastore(&dbfile);
    Ok(ds_localremote)
}

pub fn create_datastore(path: &Path) -> Datastore {
    let pathstr = path.as_os_str().to_str().unwrap();
    Datastore::new(pathstr.to_string(), false)
}

/// Returns the sync-destination bucket for a given bucket, creates it if it doesn't exist.
/// Buckets now keep the same ID on all devices to avoid "-synced-from-*" fanout.
fn get_or_create_sync_bucket(bucket_from: &Bucket, ds_to: &dyn AccessMethod) -> Bucket {
    let bucket_id = bucket_from.id.clone();
    match ds_to.get_bucket(bucket_id.as_str()) {
        Ok(bucket) => bucket,
        Err(DatastoreError::NoSuchBucket(_)) => {
            let mut bucket_new = bucket_from.clone();
            bucket_new.id = bucket_id.clone();
            ds_to.create_bucket(&bucket_new).unwrap();
            match ds_to.get_bucket(bucket_id.as_str()) {
                Ok(bucket) => bucket,
                Err(e) => panic!("{e:?}"),
            }
        }
        Err(e) => panic!("{e:?}"),
    }
}

/// Syncs all buckets from `ds_from` to `ds_to` using the same bucket ID on both sides.
///
/// is_push: a bool indicating if we're pushing local buckets to the sync dir
///          (as opposed to pulling from remotes)
/// src_did: source device ID
pub fn sync_datastores(
    ds_from: &dyn AccessMethod,
    ds_to: &dyn AccessMethod,
    is_push: bool,
    src_did: Option<&str>,
    sync_spec: &SyncSpec,
) {
    info!("Syncing {:?} to {:?}", ds_from, ds_to);

    let mut buckets_from: Vec<Bucket> = ds_from
        .get_buckets()
        .unwrap()
        .iter_mut()
        // Only filter buckets if specific bucket IDs are provided
        .filter(|tup| {
            let bucket = &tup.1;
            if let Some(buckets) = &sync_spec.buckets {
                // If "*" is in the buckets list or no buckets specified, sync all buckets
                if buckets.iter().any(|b_id| b_id == "*") || buckets.is_empty() {
                    true
                } else {
                    buckets.iter().any(|b_id| b_id == &bucket.id)
                }
            } else {
                // By default, sync all buckets
                true
            }
        })
        .map(|tup| {
            // TODO: Refuse to sync buckets without hostname/device ID set, or if set to 'unknown'
            if tup.1.hostname == "unknown" {
                warn!(" ! Bucket hostname/device ID was invalid, setting to device ID/hostname");
                tup.1.hostname = src_did.unwrap().to_string();
            }
            tup.1.clone()
        })
        .collect();

    // Log warning for buckets requested but not found
    if let Some(buckets) = &sync_spec.buckets {
        for b_id in buckets {
            if !buckets_from.iter().any(|b| b.id == *b_id) {
                error!(" ! Bucket \"{}\" not found in source datastore", b_id);
            }
        }
    }

    // Sync buckets in order of most recently updated
    buckets_from.sort_by_key(|b| b.metadata.end);

    for bucket_from in buckets_from {
        let bucket_to = get_or_create_sync_bucket(&bucket_from, ds_to);
        sync_one(ds_from, ds_to, bucket_from, bucket_to);
    }
}

/// Create a deterministic JSON string by sorting object keys recursively.
fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted = BTreeMap::<String, Value>::new();
            for (key, child_value) in map {
                sorted.insert(key.clone(), canonicalize_json(child_value));
            }
            let mut output = Map::<String, Value>::new();
            for (key, child_value) in sorted {
                output.insert(key, child_value);
            }
            Value::Object(output)
        }
        Value::Array(items) => {
            let mapped_items = items.iter().map(canonicalize_json).collect::<Vec<Value>>();
            Value::Array(mapped_items)
        }
        _ => value.clone(),
    }
}

/// Build event identity using `timestamp + data` (duration intentionally excluded).
fn event_identity(event: &Event) -> String {
    let timestamp = event.timestamp.to_rfc3339();
    let canonical_data = canonicalize_json(&Value::Object(event.data.clone()));
    let data_json = serde_json::to_string(&canonical_data).unwrap();
    format!("{timestamp}|{data_json}")
}

/// Syncs a single bucket from one datastore to another
fn sync_one(
    ds_from: &dyn AccessMethod,
    ds_to: &dyn AccessMethod,
    bucket_from: Bucket,
    bucket_to: Bucket,
) {
    info!(" ⟳  Syncing bucket '{}'", bucket_to.id);

    // Fetch all source and destination events so we can perform identity-based union merge.
    let source_events: Vec<Event> = ds_from
        .get_events(bucket_from.id.as_str(), None, None, None)
        .unwrap()
        .into_iter()
        .map(|mut event| {
            event.id = None;
            event
        })
        .collect();
    let destination_events = ds_to
        .get_events(bucket_to.id.as_str(), None, None, None)
        .unwrap();

    // Build an index of destination events by identity to support union merge.
    let mut destination_index = HashMap::<String, Event>::new();
    for event in destination_events {
        let identity = event_identity(&event);
        destination_index.insert(identity, event);
    }

    let mut inserted_events = Vec::<Event>::new();
    let mut replaced_events_count = 0_i64;
    for source_event in source_events {
        let identity = event_identity(&source_event);
        if let Some(existing_event) = destination_index.get(&identity) {
            if source_event.duration > existing_event.duration {
                // We treat this as the same event and keep the longer duration.
                if let Some(existing_event_id) = existing_event.id {
                    ds_to
                        .delete_event(bucket_to.id.as_str(), existing_event_id)
                        .unwrap();
                    let mut updated_event = source_event.clone();
                    updated_event.id = None;
                    ds_to
                        .insert_events(bucket_to.id.as_str(), vec![updated_event.clone()])
                        .unwrap();
                    destination_index.insert(identity, updated_event);
                    replaced_events_count += 1;
                }
            }
            continue;
        }

        let mut new_event = source_event.clone();
        new_event.id = None;
        inserted_events.push(new_event.clone());
        destination_index.insert(identity, new_event);
    }

    if !inserted_events.is_empty() {
        ds_to
            .insert_events(bucket_to.id.as_str(), inserted_events.clone())
            .unwrap();
    }

    if !inserted_events.is_empty() || replaced_events_count > 0 {
        info!(
            "  = Synced {} new events, updated {} existing events",
            inserted_events.len(),
            replaced_events_count
        );
    } else {
        info!("  ✓ Already up to date!");
    }
}

fn log_buckets(ds: &dyn AccessMethod) {
    // Logs all buckets and some metadata for a given datastore
    let buckets = ds.get_buckets().unwrap();
    info!("Buckets in {:?}:", ds);
    for bucket in buckets.values() {
        info!(" - {}", bucket.id.as_str());
        info!(
            "   eventcount: {:?}",
            ds.get_event_count(bucket.id.as_str()).unwrap()
        );
    }
}
