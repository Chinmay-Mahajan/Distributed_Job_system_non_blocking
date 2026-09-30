use std::thread;
use std::time::Duration;

use distributed_jobv2::model::{Job, JobState};
use distributed_jobv2::store::ResultStore;

#[test]
fn test_result_store_lifecycle() {
    let store = ResultStore::new(Duration::from_secs(60));

    let job = Job::new(
        "job-100".into(),
        "client-1".into(),
        "python".into(),
        "print('hi')".into(),
        None,
        10,
    );

    // 1. Pending
    store.insert_pending(&job);
    let res = store.get("job-100").expect("should exist");
    assert_eq!(res.status, JobState::Pending);
    assert_eq!(res.client_id, "client-1");
    assert_eq!(res.job.code, "print('hi')");

    // 2. Running
    store.set_running("job-100");
    let res2 = store.get("job-100").expect("should exist");
    assert_eq!(res2.status, JobState::Running);

    // 3. Completed (Done)
    store.set_completed(
        "job-100",
        distributed_jobv2::store::ExecutionOutcome {
            status: JobState::Done,
            stdout: "hi\n".to_string(),
            stderr: "".to_string(),
            exit_code: Some(0),
            error_message: None,
            duration_ms: 42,
        },
    );
    let res3 = store.get("job-100").expect("should exist");
    assert_eq!(res3.status, JobState::Done);
    assert_eq!(res3.stdout, "hi\n");
    assert_eq!(res3.exit_code, Some(0));
    assert_eq!(res3.execution_duration_ms, Some(42));
    assert!(res3.completed_at_ms.is_some());
}

#[test]
fn test_result_store_ttl_expiry() {
    // Very short TTL for test
    let store = ResultStore::new(Duration::from_millis(50));

    let job = Job::new(
        "job-exp".into(),
        "client-1".into(),
        "sh".into(),
        "echo 1".into(),
        None,
        5,
    );

    store.insert_pending(&job);
    assert!(store.get("job-exp").is_some());

    // Wait past TTL
    thread::sleep(Duration::from_millis(70));

    // Should return None because it expired
    assert!(store.get("job-exp").is_none());
}

#[test]
fn test_result_store_sharding_distribution() {
    let shard_count = 16;
    let store = ResultStore::with_shards(Duration::from_secs(60), shard_count);
    assert_eq!(store.num_shards(), shard_count);

    // Insert 128 jobs across different clients
    for i in 0..128 {
        let job = Job::new(
            format!("job-shard-{i}"),
            format!("client-{}", i % 8),
            "python".into(),
            format!("print({i})"),
            None,
            10,
        );
        store.insert_pending(&job);
    }

    assert_eq!(store.count(), 128);

    // Verify all jobs can be retrieved
    for i in 0..128 {
        let job_id = format!("job-shard-{i}");
        let res = store.get(&job_id).expect("job should exist in store");
        assert_eq!(res.status, JobState::Pending);
    }

    // Inspect individual shard distribution
    let counts = store.shard_counts();
    assert_eq!(counts.len(), shard_count);
    let total: usize = counts.iter().sum();
    assert_eq!(total, 128);

    // Verify hash distribution: entries should be spread across shards,
    // not clustered exclusively into a single bucket.
    let populated_shards = counts.iter().filter(|&&c| c > 0).count();
    assert!(
        populated_shards >= 10,
        "Expected at least 10/16 shards populated, got {populated_shards}"
    );
}

#[test]
fn test_result_store_concurrent_sharded_access() {
    let store = std::sync::Arc::new(ResultStore::with_shards(Duration::from_secs(60), 32));
    let num_threads = 8;
    let jobs_per_thread = 25;

    let mut handles = Vec::new();
    for thread_idx in 0..num_threads {
        let store_clone = std::sync::Arc::clone(&store);
        let handle = thread::spawn(move || {
            for j in 0..jobs_per_thread {
                let job_id = format!("concurrent-t{thread_idx}-j{j}");
                let job = Job::new(
                    job_id.clone(),
                    format!("client-{thread_idx}"),
                    "python".into(),
                    "x = 1".into(),
                    None,
                    10,
                );

                // Insert pending
                store_clone.insert_pending(&job);

                // Transition to running
                store_clone.set_running(&job_id);

                // Transition to completed
                store_clone.set_completed(
                    &job_id,
                    distributed_jobv2::store::ExecutionOutcome {
                        status: JobState::Done,
                        stdout: format!("result {thread_idx}-{j}"),
                        stderr: String::new(),
                        exit_code: Some(0),
                        error_message: None,
                        duration_ms: 10,
                    },
                );

                // Read back
                let res = store_clone.get(&job_id).expect("should find result");
                assert_eq!(res.status, JobState::Done);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().expect("thread panicked");
    }

    assert_eq!(store.count(), num_threads * jobs_per_thread);
}

#[test]
fn test_result_store_sharded_cleanup() {
    let store = ResultStore::with_shards(Duration::from_millis(50), 8);

    for i in 0..40 {
        let job = Job::new(
            format!("purge-job-{i}"),
            "purger".into(),
            "python".into(),
            "pass".into(),
            None,
            5,
        );
        store.insert_pending(&job);
    }

    assert_eq!(store.count(), 40);

    // Wait for TTL to expire
    thread::sleep(Duration::from_millis(70));

    // Cleanup should prune all 40 expired entries across the 8 shards
    let removed = store.cleanup_expired();
    assert_eq!(removed, 40);
    assert_eq!(store.count(), 0);

    let counts = store.shard_counts();
    assert!(counts.iter().all(|&c| c == 0));
}
