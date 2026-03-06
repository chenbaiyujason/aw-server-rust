#[macro_use]
extern crate log;
extern crate aw_sync;

#[cfg(test)]
mod sync_tests {
    use chrono::{DateTime, Duration, Utc};

    use aw_datastore::{Datastore, DatastoreError};
    use aw_models::{Bucket, Event};
    use aw_sync::SyncSpec;

    struct TestState {
        ds_a: Datastore,
        ds_b: Datastore,
    }

    fn init_teststate() -> TestState {
        TestState {
            ds_a: Datastore::new_in_memory(false),
            ds_b: Datastore::new_in_memory(false),
        }
    }

    fn ensure_bucket(ds: &Datastore, bucket_id: &str, hostname: &str) {
        let bucket_jsonstr = format!(
            r#"{{
            "id": "{bucket_id}",
            "type": "test",
            "hostname": "{hostname}",
            "client": "test"
        }}"#
        );
        let bucket: Bucket = serde_json::from_str(&bucket_jsonstr).unwrap();
        match ds.create_bucket(&bucket) {
            Ok(()) => (),
            Err(e) => match e {
                DatastoreError::BucketAlreadyExists(_) => {
                    debug!("bucket already exists, skipping");
                }
                e => panic!("woops! {e:?}"),
            },
        };
    }

    fn make_event(timestamp: DateTime<Utc>, duration_secs: i64, value: i64) -> Event {
        let json = serde_json::json!({
            "timestamp": timestamp.to_rfc3339(),
            "duration": duration_secs,
            "data": {"test": value}
        });
        serde_json::from_value(json).unwrap()
    }

    fn insert_events(ds: &Datastore, bucket_id: &str, events: Vec<Event>) {
        ds.insert_events(bucket_id, &events).unwrap();
        ds.force_commit().unwrap();
    }

    fn get_bucket_events(ds: &Datastore, bucket_id: &str) -> Vec<Event> {
        ds.get_events(bucket_id, None, None, None).unwrap()
    }

    fn sort_events(mut events: Vec<Event>) -> Vec<Event> {
        events.sort_by_key(|event| event.timestamp);
        events
    }

    #[test]
    fn test_same_bucket_id_no_synced_suffix() {
        let state = init_teststate();
        ensure_bucket(&state.ds_a, "bucket-shared", "device-a");

        aw_sync::sync_datastores(
            &state.ds_a,
            &state.ds_b,
            false,
            None,
            &SyncSpec::default(),
        );

        let buckets_b = state.ds_b.get_buckets().unwrap();
        assert!(buckets_b.contains_key("bucket-shared"));
        assert!(
            !buckets_b
                .keys()
                .any(|bucket_id| bucket_id.contains("-synced-from-"))
        );
    }

    #[test]
    fn test_union_merge_recovers_deleted_event() {
        let state = init_teststate();
        let bucket_id = "bucket-shared";
        ensure_bucket(&state.ds_a, bucket_id, "device-a");
        ensure_bucket(&state.ds_b, bucket_id, "device-b");

        let now = Utc::now();
        let event_one = make_event(now, 5, 1);
        let event_two = make_event(now + Duration::seconds(10), 5, 2);

        insert_events(&state.ds_a, bucket_id, vec![event_one.clone(), event_two.clone()]);
        insert_events(&state.ds_b, bucket_id, vec![event_one.clone(), event_two.clone()]);

        // Simulate accidental deletion on device A.
        let existing_events = get_bucket_events(&state.ds_a, bucket_id);
        let event_two_id = existing_events
            .iter()
            .find(|event| event.data.get("test") == Some(&serde_json::json!(2)))
            .and_then(|event| event.id)
            .unwrap();
        state
            .ds_a
            .delete_events_by_id(bucket_id, vec![event_two_id])
            .unwrap();
        state.ds_a.force_commit().unwrap();

        // Pull from device B to recover missing events in device A.
        aw_sync::sync_datastores(&state.ds_b, &state.ds_a, false, None, &SyncSpec::default());
        let merged_events = sort_events(get_bucket_events(&state.ds_a, bucket_id));
        assert_eq!(merged_events.len(), 2);

        let recovered_values = merged_events
            .iter()
            .map(|event| event.data.get("test").unwrap().as_i64().unwrap())
            .collect::<Vec<i64>>();
        assert_eq!(recovered_values, vec![1, 2]);
    }

    #[test]
    fn test_union_merge_combines_events_from_both_sides() {
        let state = init_teststate();
        let bucket_id = "bucket-shared";
        ensure_bucket(&state.ds_a, bucket_id, "device-a");
        ensure_bucket(&state.ds_b, bucket_id, "device-b");

        let now = Utc::now();
        let event_a = make_event(now, 5, 11);
        let event_b = make_event(now + Duration::seconds(20), 5, 22);

        insert_events(&state.ds_a, bucket_id, vec![event_a.clone()]);
        insert_events(&state.ds_b, bucket_id, vec![event_b.clone()]);

        aw_sync::sync_datastores(&state.ds_a, &state.ds_b, false, None, &SyncSpec::default());
        aw_sync::sync_datastores(&state.ds_b, &state.ds_a, false, None, &SyncSpec::default());

        let events_a = sort_events(get_bucket_events(&state.ds_a, bucket_id));
        let events_b = sort_events(get_bucket_events(&state.ds_b, bucket_id));
        assert_eq!(events_a, events_b);
        assert_eq!(events_a.len(), 2);
    }

    #[test]
    fn test_same_identity_uses_longer_duration() {
        let state = init_teststate();
        let bucket_id = "bucket-shared";
        ensure_bucket(&state.ds_a, bucket_id, "device-a");
        ensure_bucket(&state.ds_b, bucket_id, "device-b");

        let timestamp = Utc::now();
        let short_event = make_event(timestamp, 10, 99);
        let long_event = make_event(timestamp, 60, 99);

        insert_events(&state.ds_a, bucket_id, vec![short_event.clone()]);
        insert_events(&state.ds_b, bucket_id, vec![long_event.clone()]);

        // Pull from B -> A, the same identity should be upgraded to longer duration.
        aw_sync::sync_datastores(&state.ds_b, &state.ds_a, false, None, &SyncSpec::default());

        let merged_events = get_bucket_events(&state.ds_a, bucket_id);
        assert_eq!(merged_events.len(), 1);
        assert_eq!(merged_events[0].duration, Duration::seconds(60));
    }
}
