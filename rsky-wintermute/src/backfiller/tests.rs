#[cfg(test)]
mod backfiller_tests {
    use base64::Engine as _;

    use crate::backfiller::{BackfillerManager, convert_record_to_ipld};
    use crate::storage::Storage;
    use crate::types::{BackfillJob, WintermuteError};
    use serde_json::json;
    use serial_test::serial;
    use tempfile::TempDir;

    fn setup_test_storage() -> (Storage, TempDir) {
        let temp_dir = TempDir::with_prefix("backfiller_test_").unwrap();
        let db_path = temp_dir.path().join("test_db");
        let storage = Storage::new(Some(db_path)).unwrap();
        (storage, temp_dir)
    }

    /// Serializes tests that read or mutate the global `crate::SHUTDOWN` flag.
    /// Tests run in parallel in one process, so a test flipping the flag would
    /// otherwise tear down pipelines started by concurrently running tests.
    static SHUTDOWN_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[test]
    fn test_convert_record_preserves_objects() {
        let input = json!({"text": "hello", "nested": {"key": "value"}});
        let output = convert_record_to_ipld(&input);
        assert_eq!(output, input);
    }

    #[test]
    fn test_convert_record_converts_cid_bytes() {
        let cid_bytes = vec![1u8, 113, 18, 32];
        let mut full_bytes = cid_bytes;
        full_bytes.extend_from_slice(&[0u8; 28]);

        let input = json!({"data": full_bytes});
        let output = convert_record_to_ipld(&input);

        assert!(output.get("data").is_some());
    }

    #[test]
    fn test_convert_record_preserves_regular_arrays() {
        let input = json!({"tags": ["one", "two", "three"]});
        let output = convert_record_to_ipld(&input);
        assert_eq!(output, input);
    }

    #[test]
    fn test_convert_record_handles_nested_structures() {
        let input = json!({
            "text": "post",
            "facets": [
                {"features": [{"$type": "mention"}]}
            ]
        });
        let output = convert_record_to_ipld(&input);
        assert_eq!(output["text"], "post");
    }

    #[tokio::test]
    async fn test_process_car_bytes_with_fixture_repo() {
        let (storage, _dir) = setup_test_storage();

        let car_bytes = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../rsky-repo/resources/test/valid_repo.car"
        ))
        .unwrap();

        let enqueued = BackfillerManager::process_car_bytes(
            &storage,
            "did:plc:r7fdhqmw3h2cifeakw5hmvy6",
            &car_bytes,
            false,
            None,
        )
        .await
        .unwrap();

        // valid_repo.car's MST references exactly 6 live records
        let queue_len = storage.firehose_backfill_len().unwrap();
        assert_eq!(enqueued, queue_len);
        assert_eq!(enqueued, 6);
    }

    #[tokio::test]
    async fn test_process_car_bytes_rejects_did_mismatch() {
        let (storage, _dir) = setup_test_storage();

        let car_bytes = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../rsky-repo/resources/test/valid_repo.car"
        ))
        .unwrap();

        let result = BackfillerManager::process_car_bytes(
            &storage,
            "did:plc:someotherdidentirely00000",
            &car_bytes,
            false,
            None,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(storage.firehose_backfill_len().unwrap(), 0);
    }

    #[tokio::test]
    #[ignore = "hits the live network; run manually with --ignored"]
    async fn test_process_job_with_real_repo() {
        let (storage, _dir) = setup_test_storage();

        let job = BackfillJob {
            did: "did:plc:w4xbfzo7kqfes5zb7r6qv3rw".to_owned(),
            retry_count: 0,
            priority: false,
        };

        let http_client = rsky_identity::safe_fetch::SafeClient::new(
            rsky_identity::safe_fetch::NetworkPolicy::PERMISSIVE,
            std::time::Duration::from_secs(60),
        )
        .unwrap();

        BackfillerManager::process_job(&storage, &http_client, &dashmap::DashMap::new(), &job)
            .await
            .unwrap();

        assert!(storage.firehose_backfill_len().unwrap() > 0);
    }

    // Enqueues BACKFILLER_OUTPUT_HIGH_WATER_MARK records one at a time, so its
    // runtime scales with a production tuning constant that defaults to 100k.
    // Ignored by default and run in CI with a small mark, the same way the video
    // crate handles its ffmpeg-dependent tests.
    #[tokio::test]
    #[ignore = "runtime scales with BACKFILLER_OUTPUT_HIGH_WATER_MARK; set a small mark and run with --ignored"]
    async fn test_backpressure_metric_tracking() {
        use crate::config::BACKFILLER_OUTPUT_HIGH_WATER_MARK;
        use crate::types::IndexJob;
        use crate::types::WriteAction;

        let (storage, _dir) = setup_test_storage();

        // Fill output stream beyond high water mark
        for i in 0..*BACKFILLER_OUTPUT_HIGH_WATER_MARK + 100 {
            let job = IndexJob {
                uri: format!("at://did:plc:test/app.bsky.feed.post/test{i}"),
                cid: "bafytest".to_owned(),
                action: WriteAction::Create,
                record: Some(json!({"text": "test"})),
                indexed_at: "2024-01-01T00:00:00Z".to_owned(),
                rev: "test".to_owned(),
                provenance: None,
            };
            storage.enqueue_firehose_backfill(&job).unwrap();
        }

        // Verify output stream length exceeds high water mark
        let output_len = storage.firehose_backfill_len().unwrap();
        let high_water_mark = *BACKFILLER_OUTPUT_HIGH_WATER_MARK;
        assert!(
            output_len > high_water_mark,
            "output stream should exceed high water mark: {output_len} > {high_water_mark}"
        );
    }

    #[tokio::test]
    async fn test_process_job_with_invalid_did() {
        let (storage, _dir) = setup_test_storage();

        let job = BackfillJob {
            did: "did:plc:nonexistent123456789".to_owned(),
            retry_count: 0,
            priority: false,
        };

        let http_client = rsky_identity::safe_fetch::SafeClient::new(
            rsky_identity::safe_fetch::NetworkPolicy::PERMISSIVE,
            std::time::Duration::from_secs(10),
        )
        .unwrap();

        let result =
            BackfillerManager::process_job(&storage, &http_client, &dashmap::DashMap::new(), &job)
                .await;

        // Should fail with DID resolution error
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("did resolution failed") || err_msg.contains("no pds found"),
            "unexpected error: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_process_job_with_http_error() {
        let (storage, _dir) = setup_test_storage();

        // Use a DID that should resolve but point to a non-existent PDS
        let job = BackfillJob {
            did: "did:web:example.invalid".to_owned(),
            retry_count: 0,
            priority: false,
        };

        let http_client = rsky_identity::safe_fetch::SafeClient::new(
            rsky_identity::safe_fetch::NetworkPolicy::PERMISSIVE,
            std::time::Duration::from_secs(5),
        )
        .unwrap();

        let result =
            BackfillerManager::process_job(&storage, &http_client, &dashmap::DashMap::new(), &job)
                .await;

        // Should fail with HTTP or DID resolution error
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_convert_record_with_cid_link() {
        // Test that CID bytes are properly converted to $link format
        let cid_bytes = vec![
            1, 113, 18, 32, // CID prefix for dag-cbor sha256
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];

        let input = json!({"embed": {"cid": cid_bytes}});
        let output = convert_record_to_ipld(&input);

        // Verify nested CID was converted
        assert!(output["embed"]["cid"].get("$link").is_some());
    }

    #[tokio::test]
    async fn test_convert_record_with_mixed_content() {
        let input = json!({
            "text": "Hello world",
            "facets": [
                {
                    "index": {"byteStart": 0, "byteEnd": 5},
                    "features": [{"$type": "app.bsky.richtext.facet#mention"}]
                }
            ],
            "tags": ["rust", "atproto"]
        });

        let output = convert_record_to_ipld(&input);

        // Verify structure is preserved
        assert_eq!(output["text"], "Hello world");
        assert_eq!(output["tags"][0], "rust");
        assert_eq!(output["tags"][1], "atproto");
        assert_eq!(
            output["facets"][0]["features"][0]["$type"],
            "app.bsky.richtext.facet#mention"
        );
    }

    #[test]
    fn test_backfiller_manager_new() {
        let (storage, _dir) = setup_test_storage();
        let manager = BackfillerManager::new(std::sync::Arc::new(storage));
        assert!(manager.is_ok(), "BackfillerManager::new should succeed");
    }

    #[test]
    fn test_convert_record_empty_object() {
        let input = json!({});
        let output = convert_record_to_ipld(&input);
        assert_eq!(output, json!({}));
    }

    #[test]
    fn test_convert_record_empty_array() {
        let input = json!({"items": []});
        let output = convert_record_to_ipld(&input);
        assert_eq!(output, json!({"items": []}));
    }

    #[test]
    fn test_convert_record_numbers_and_booleans() {
        let input = json!({
            "count": 42,
            "score": 2.5,
            "enabled": true,
            "disabled": false
        });
        let output = convert_record_to_ipld(&input);
        assert_eq!(output, input);
    }

    #[test]
    fn test_convert_record_null_values() {
        let input = json!({"field": null});
        let output = convert_record_to_ipld(&input);
        assert_eq!(output, input);
    }

    #[test]
    fn test_convert_record_invalid_cid_bytes_becomes_bytes() {
        // Byte array that's not a valid CID - should be encoded as $bytes
        let input = json!({"data": [1, 2, 3, 4, 5]});
        let output = convert_record_to_ipld(&input);
        // Should have $bytes since it's a byte array but not a valid CID
        assert!(output["data"].is_object());
        assert!(output["data"]["$bytes"].is_string());
        assert_eq!(output["data"]["$bytes"], "AQIDBAU="); // base64 of [1,2,3,4,5]
    }

    #[test]
    fn test_convert_record_germ_key_bytes() {
        // Simulate a germ declaration record with crypto key bytes
        let key_bytes: Vec<u8> = (0..32).collect();
        let input = json!({
            "$type": "com.germnetwork.declaration",
            "currentKey": key_bytes,
            "keyPackage": key_bytes
        });
        let output = convert_record_to_ipld(&input);

        // Both key fields should be $bytes encoded
        assert!(output["currentKey"]["$bytes"].is_string());
        assert!(output["keyPackage"]["$bytes"].is_string());
        // Verify round-trip: decode and check
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(output["currentKey"]["$bytes"].as_str().unwrap())
            .unwrap();
        assert_eq!(decoded, (0u8..32).collect::<Vec<u8>>());
    }

    #[test]
    fn test_convert_record_mixed_byte_array() {
        // Array with numbers > 255 should not be treated as byte array
        let input = json!({"data": [1, 2, 256, 4]});
        let output = convert_record_to_ipld(&input);
        assert_eq!(output, input);
    }

    #[test]
    fn test_convert_record_deeply_nested() {
        let input = json!({
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "value": "deep"
                        }
                    }
                }
            }
        });
        let output = convert_record_to_ipld(&input);
        assert_eq!(
            output["level1"]["level2"]["level3"]["level4"]["value"],
            "deep"
        );
    }

    #[test]
    fn test_convert_record_array_of_objects() {
        let input = json!({
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25}
            ]
        });
        let output = convert_record_to_ipld(&input);
        assert_eq!(output["users"][0]["name"], "Alice");
        assert_eq!(output["users"][1]["age"], 25);
    }

    // Tests for the old batch-and-barrier methods (update_metrics, check_backpressure,
    // dequeue_batch, spawn_job_tasks, handle_task_results) were removed when the
    // backfiller was rewritten to use a continuous channel-based pipeline.

    #[tokio::test]
    async fn test_process_job_invalid_did() {
        let (storage, _dir) = setup_test_storage();

        let job = BackfillJob {
            did: "did:plc:nonexistent123456789".to_owned(),
            retry_count: 0,
            priority: false,
        };

        let http_client = rsky_identity::safe_fetch::SafeClient::new(
            rsky_identity::safe_fetch::NetworkPolicy::PERMISSIVE,
            std::time::Duration::from_secs(10),
        )
        .unwrap();
        let result =
            BackfillerManager::process_job(&storage, &http_client, &dashmap::DashMap::new(), &job)
                .await;

        assert!(result.is_err());
        match result {
            Err(WintermuteError::Other(msg)) => {
                assert!(msg.contains("did resolution failed"));
            }
            _ => panic!("expected DID resolution error"),
        }
    }

    // Mutates the process-global SHUTDOWN flag, so it must not run beside the
    // other tests that do: their reset lands while this one's run() is still
    // looping on the flag, and that loop then never exits.
    #[test]
    #[serial(shutdown_flag)]
    fn test_run_creates_runtime() {
        use std::sync::Arc;

        let _shutdown_guard = SHUTDOWN_LOCK.blocking_lock();
        let (storage, _dir) = setup_test_storage();
        let manager = BackfillerManager::new(Arc::new(storage)).unwrap();

        // Set shutdown flag immediately so process_loop exits
        crate::SHUTDOWN.store(true, std::sync::atomic::Ordering::Relaxed);

        // run() should complete successfully
        let result = manager.run();
        assert!(result.is_ok());

        // Reset shutdown flag for other tests
        crate::SHUTDOWN.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    #[tokio::test]
    #[serial(shutdown_flag)]
    async fn test_process_loop_exits_on_shutdown() {
        use std::sync::Arc;

        let _shutdown_guard = SHUTDOWN_LOCK.lock().await;
        let (storage, _dir) = setup_test_storage();
        let manager = BackfillerManager::new(Arc::new(storage)).unwrap();

        // Set shutdown flag
        crate::SHUTDOWN.store(true, std::sync::atomic::Ordering::Relaxed);

        // process_loop should exit immediately
        manager.process_loop().await;

        // Reset for other tests
        crate::SHUTDOWN.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    #[tokio::test]
    #[serial(shutdown_flag)]
    async fn test_process_loop_handles_empty_queue() {
        use std::sync::Arc;
        use std::time::Duration;

        let _shutdown_guard = SHUTDOWN_LOCK.lock().await;
        let (storage, _dir) = setup_test_storage();
        let manager = BackfillerManager::new(Arc::new(storage)).unwrap();

        // Spawn process_loop in background
        let manager_arc = Arc::new(manager);
        let handle = {
            let m = manager_arc.clone();
            tokio::spawn(async move {
                m.process_loop().await;
            })
        };

        // Let it run for a bit
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Set shutdown
        crate::SHUTDOWN.store(true, std::sync::atomic::Ordering::Relaxed);

        // Wait for completion
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("process_loop should exit on shutdown")
            .expect("task should complete");

        // Reset
        crate::SHUTDOWN.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    // HTTP Mocking Tests - These actually test error paths in process_job()
    // Note: These tests primarily exercise DID resolution error paths since mocking
    // the full CAR fetch flow would require extensive infrastructure

    #[tokio::test]
    async fn test_process_job_did_resolution_failure() {
        let (storage, _dir) = setup_test_storage();

        // Test with invalid DID that will fail resolution
        let job = BackfillJob {
            did: "did:plc:invalidtestdid123".to_owned(),
            retry_count: 0,
            priority: false,
        };

        let http_client = rsky_identity::safe_fetch::SafeClient::new(
            rsky_identity::safe_fetch::NetworkPolicy::PERMISSIVE,
            std::time::Duration::from_secs(10),
        )
        .unwrap();
        let result =
            BackfillerManager::process_job(&storage, &http_client, &dashmap::DashMap::new(), &job)
                .await;

        // Should fail with DID resolution error
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("did resolution failed") || err_msg.contains("DID"));
    }

    #[tokio::test]
    async fn test_process_job_invalid_did_format() {
        let (storage, _dir) = setup_test_storage();

        // Test with completely malformed DID
        let job = BackfillJob {
            did: "not-a-valid-did".to_owned(),
            retry_count: 0,
            priority: false,
        };

        let http_client = rsky_identity::safe_fetch::SafeClient::new(
            rsky_identity::safe_fetch::NetworkPolicy::PERMISSIVE,
            std::time::Duration::from_secs(10),
        )
        .unwrap();
        let result =
            BackfillerManager::process_job(&storage, &http_client, &dashmap::DashMap::new(), &job)
                .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_new_http_client_creation() {
        use std::sync::Arc;

        let (storage, _dir) = setup_test_storage();

        // Test that new() successfully creates HTTP client with timeout
        let result = BackfillerManager::new(Arc::new(storage));
        assert!(result.is_ok());

        let manager = result.unwrap();
        assert_eq!(manager.workers, *crate::config::WORKERS_BACKFILLER);

        // HTTP client should be configured with timeout
        // We can't directly test the timeout config, but we know it was created
    }

    // =========================================================================
    // Pipeline stall regression tests
    //
    // The continuous pipeline in `run_pipeline` historically had two silent
    // total-stall modes (seen in production as a repo_backfill queue frozen at
    // 127K for days):
    //
    //   1. Workers wedged on an upstream that accepts the connection but never
    //      responds: the bounded channel filled, the dequeue task blocked
    //      forever in `tx.send()`, and the queue froze.
    //   2. A panic in job processing (e.g. rsky_repo parsing a malformed repo)
    //      permanently killed a worker task; workers were never revived, so
    //      after N panics the pipeline was dead while the process stayed
    //      "healthy".
    //
    // Both are now fixed by the per-job timeout + panic containment in the
    // worker loop, which route wedged/panicking jobs into the existing
    // retry/dead-letter path. These tests assert the FIXED behavior: the
    // queue always drains and the pipeline keeps making progress. If either
    // regresses, they fail by timing out waiting for the drain.
    // =========================================================================

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::{Duration, Instant};

    /// TCP server that accepts connections and never responds, wedging any
    /// HTTP client on it until the client-side timeout (120s in production).
    /// Returns the endpoint and a counter of accepted connections: a truly
    /// wedged worker never opens a second connection, while a worker that
    /// times out and retries keeps opening new ones.
    fn spawn_hang_server() -> (String, Arc<AtomicUsize>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let connections = Arc::new(AtomicUsize::new(0));
        let connections_srv = Arc::clone(&connections);
        std::thread::spawn(move || {
            while let Ok((stream, _)) = listener.accept() {
                connections_srv.fetch_add(1, AtomicOrdering::Relaxed);
                // Keep the socket open forever without responding.
                std::mem::forget(stream);
            }
        });
        (format!("http://{addr}"), connections)
    }

    /// Polls `probe` every 100ms until it returns true or the deadline passes.
    /// Returns the final probe result.
    async fn wait_for(deadline: Duration, mut probe: impl FnMut() -> bool) -> bool {
        let end = Instant::now() + deadline;
        while Instant::now() < end {
            if probe() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        probe()
    }

    /// Regression test for stall mode 1: workers wedged on unresponsive
    /// upstreams must recover via the per-job timeout.
    ///
    /// Uses the REAL `process_loop` and `process_job`. The PDS cache is
    /// pre-populated so no DID resolution happens; every repo fetch goes to a
    /// server that accepts and never responds (the HTTP client's own timeout
    /// is far beyond the test window, so only the job timeout can save the
    /// workers). Each attempt must time out, retry twice, and dead-letter,
    /// fully draining the queue. Before the fix, workers hung, the channel
    /// filled, the dequeue task blocked, and the queue froze forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_pipeline_recovers_from_wedged_workers() {
        const JOBS: usize = 6;

        // These tests need SHUTDOWN to stay false for their whole duration.
        let _shutdown_guard = SHUTDOWN_LOCK.lock().await;
        let (storage, _dir) = setup_test_storage();
        let storage = Arc::new(storage);
        let (endpoint, connections) = spawn_hang_server();

        let pds_cache = Arc::new(dashmap::DashMap::new());
        for i in 0..JOBS {
            let did = format!("did:plc:wedge{i:03}");
            pds_cache.insert(did.clone(), endpoint.clone());
            storage
                .enqueue_backfill(&BackfillJob {
                    did,
                    retry_count: 0,
                    priority: false,
                })
                .unwrap();
        }
        assert_eq!(storage.repo_backfill_len().unwrap(), JOBS);

        let manager = BackfillerManager {
            workers: 2,
            storage: Arc::clone(&storage),
            // PERMISSIVE so the loopback hang server is reachable; the client
            // timeout is far beyond the test window, so recovery must come
            // from the pipeline's job timeout, not from the transport.
            http_client: rsky_identity::safe_fetch::SafeClient::new(
                rsky_identity::safe_fetch::NetworkPolicy::PERMISSIVE,
                Duration::from_secs(120),
            )
            .unwrap(),
            pds_cache,
            generations: None,
            // Short job timeout so the 18 attempts (6 jobs x 3 tries) finish
            // quickly; production uses backfiller_job_timeout().
            job_timeout: Duration::from_millis(300),
        };

        let pipeline = tokio::spawn(async move { manager.process_loop().await });

        // Every attempt (6 jobs x 3 tries) opens a fresh upstream connection.
        // 18 attempts / 2 workers x 300ms ~= 3s; allow generous slack. Before
        // the fix, the count froze at one connection per worker (2).
        // Note: queue length alone is NOT a progress signal here -- the
        // dequeue task empties Fjall into the channel without processing.
        let all_attempted = wait_for(Duration::from_secs(30), || {
            connections.load(AtomicOrdering::Relaxed) >= JOBS * 3
        })
        .await;
        assert!(
            all_attempted,
            "expected {} upstream connection attempts, got {} -- workers are \
             not recovering from wedged requests",
            JOBS * 3,
            connections.load(AtomicOrdering::Relaxed)
        );

        // Once the final attempts time out and dead-letter, the queue must be
        // fully drained: no strays, no unfinished retries.
        let drained = wait_for(Duration::from_secs(10), || {
            storage.repo_backfill_len().unwrap() == 0
        })
        .await;
        assert!(
            drained,
            "queue should fully drain once all attempts are dead-lettered"
        );

        // The pipeline survives: it idles on the empty queue, ready for more.
        assert!(
            !pipeline.is_finished(),
            "pipeline should stay alive after draining the queue"
        );

        pipeline.abort();
    }
}
