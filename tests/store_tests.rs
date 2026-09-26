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
