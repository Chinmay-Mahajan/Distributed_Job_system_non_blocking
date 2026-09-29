use distributed_jobv2::model::Job;
use distributed_jobv2::scheduler::TwoLevelScheduler;

#[test]
fn test_fifo_within_single_client() {
    let scheduler = TwoLevelScheduler::new(5);

    let j1 = Job::new("1".into(), "clientA".into(), "python".into(), "code1".into(), None, 10);
    let j2 = Job::new("2".into(), "clientA".into(), "python".into(), "code2".into(), None, 10);
    let j3 = Job::new("3".into(), "clientA".into(), "python".into(), "code3".into(), None, 10);

    scheduler.submit(j1).unwrap();
    scheduler.submit(j2).unwrap();
    scheduler.submit(j3).unwrap();

    // Popping should return exactly in FIFO order
    let p1 = scheduler.pop_next_job().expect("expected j1");
    assert_eq!(p1.id, "1");

    let p2 = scheduler.pop_next_job().expect("expected j2");
    assert_eq!(p2.id, "2");

    let p3 = scheduler.pop_next_job().expect("expected j3");
    assert_eq!(p3.id, "3");
}

#[test]
fn test_per_client_in_flight_cap() {
    // Max in-flight = 2
    let scheduler = TwoLevelScheduler::new(2);

    for i in 1..=4 {
        let job = Job::new(
            format!("{}", i),
            "clientA".into(),
            "python".into(),
            format!("code{}", i),
            None,
            10,
        );
        scheduler.submit(job).unwrap();
    }

    // First two jobs should be dispatched
    let p1 = scheduler.pop_next_job().expect("job 1");
    assert_eq!(p1.id, "1");

    let p2 = scheduler.pop_next_job().expect("job 2");
    assert_eq!(p2.id, "2");

    // Client has 2 in flight (cap reached). Stats should reflect this.
    let stats = scheduler.stats();
    assert_eq!(stats.in_flight_jobs, 2);
    assert_eq!(stats.queued_jobs, 2);

    // A third pop without completing should NOT dispatch clientA's 3rd job.
    // In another thread or with completion, completing job 1 allows job 3 to be dispatched.
    scheduler.on_job_completed("clientA");

    let p3 = scheduler.pop_next_job().expect("job 3");
    assert_eq!(p3.id, "3");
}

#[test]
fn test_round_robin_across_clients() {
    // Max in-flight = 2, but clientA submits 3 jobs, clientB submits 1 job, clientC submits 1 job
    let scheduler = TwoLevelScheduler::new(2);

    scheduler.submit(Job::new("A1".into(), "clientA".into(), "python".into(), "".into(), None, 10)).unwrap();
    scheduler.submit(Job::new("A2".into(), "clientA".into(), "python".into(), "".into(), None, 10)).unwrap();
    scheduler.submit(Job::new("A3".into(), "clientA".into(), "python".into(), "".into(), None, 10)).unwrap();

    scheduler.submit(Job::new("B1".into(), "clientB".into(), "python".into(), "".into(), None, 10)).unwrap();
    scheduler.submit(Job::new("C1".into(), "clientC".into(), "python".into(), "".into(), None, 10)).unwrap();

    // Pop 1: Client A
    let p1 = scheduler.pop_next_job().expect("A1");
    assert_eq!(p1.id, "A1");

    // Pop 2: Round-robin should move to Client B (not A again immediately!)
    let p2 = scheduler.pop_next_job().expect("B1");
    assert_eq!(p2.id, "B1");

    // Pop 3: Round-robin should move to Client C
    let p3 = scheduler.pop_next_job().expect("C1");
    assert_eq!(p3.id, "C1");

    // Pop 4: Moves back to Client A
    let p4 = scheduler.pop_next_job().expect("A2");
    assert_eq!(p4.id, "A2");
}

#[test]
fn test_two_queue_o1_dispatch_and_waiting_promotion() {
    // max_in_flight = 2, two_queue = true, queue_limit = true, multiplier = 5
    let scheduler = TwoLevelScheduler::with_config(2, true, true, 5);

    // Client A submits 4 jobs
    for i in 1..=4 {
        let job = Job::new(
            format!("job-{}", i),
            "clientA".into(),
            "python".into(),
            "pass".into(),
            None,
            10,
        );
        scheduler.submit(job).expect("submit ok");
    }

    // Initially: Client A is in dispatchable_queue
    let s0 = scheduler.stats();
    assert_eq!(s0.dispatchable_clients, 1);
    assert_eq!(s0.waiting_clients, 0);

    // Pop 1: in_flight becomes 1 (< 2 cap). Client A remains dispatchable!
    let _ = scheduler.pop_next_job().expect("pop 1");
    let s1 = scheduler.stats();
    assert_eq!(s1.in_flight_jobs, 1);
    assert_eq!(s1.dispatchable_clients, 1);
    assert_eq!(s1.waiting_clients, 0);

    // Pop 2: in_flight becomes 2 (== 2 cap, saturated!).
    // Client A is moved from dispatchable_queue to waiting_queue!
    let _ = scheduler.pop_next_job().expect("pop 2");
    let s2 = scheduler.stats();
    assert_eq!(s2.in_flight_jobs, 2);
    assert_eq!(s2.dispatchable_clients, 0);
    assert_eq!(s2.waiting_clients, 1);

    // When a job completes, Client A is promoted from waiting_queue back to dispatchable_queue
    scheduler.on_job_completed("clientA");
    let s3 = scheduler.stats();
    assert_eq!(s3.in_flight_jobs, 1);
    assert_eq!(s3.dispatchable_clients, 1);
    assert_eq!(s3.waiting_clients, 0);

    // Worker can immediately dispatch the 3rd job in O(1)
    let p3 = scheduler.pop_next_job().expect("pop 3");
    assert_eq!(p3.id, "job-3");
}

#[test]
fn test_queue_limit_rate_limiting_enforcement() {
    // max_in_flight = 2, queue_limit = true, multiplier = 2
    // Maximum queued jobs allowed per client = 2 * 2 = 4
    let scheduler = TwoLevelScheduler::with_config(2, true, true, 2);

    // Submitting 4 jobs should succeed
    for i in 1..=4 {
        let job = Job::new(
            format!("job-{}", i),
            "clientA".into(),
            "python".into(),
            "pass".into(),
            None,
            10,
        );
        assert!(scheduler.submit(job).is_ok());
    }

    // Submitting a 5th job while 4 are queued must be rejected with rate limit error!
    let overflow_job = Job::new(
        "job-5".into(),
        "clientA".into(),
        "python".into(),
        "pass".into(),
        None,
        10,
    );
    let res = scheduler.submit(overflow_job);
    assert!(res.is_err());
    let err_msg = res.unwrap_err();
    assert!(err_msg.contains("Rate limit exceeded"));
    assert!(err_msg.contains("client 'clientA' reached maximum queue capacity of 4"));

    // Other clients should still be able to submit their own quota independently!
    let client_b_job = Job::new(
        "b-1".into(),
        "clientB".into(),
        "python".into(),
        "pass".into(),
        None,
        10,
    );
    assert!(scheduler.submit(client_b_job).is_ok(), "Client B quota must be isolated");
}

#[test]
fn test_queue_limit_toggle_off() {
    // enable_queue_limit = false
    let scheduler = TwoLevelScheduler::with_config(2, true, false, 1);

    // Saturated client submitting more than multiplier * in_flight must be allowed when toggled off
    for i in 1..=10 {
        let job = Job::new(
            format!("job-{}", i),
            "clientA".into(),
            "python".into(),
            "pass".into(),
            None,
            10,
        );
        assert!(scheduler.submit(job).is_ok(), "Queue limit is turned off, submission should succeed");
    }
}

#[test]
fn test_legacy_single_queue_fallback() {
    // enable_two_queue_scheduler = false (single-queue mode)
    let scheduler = TwoLevelScheduler::with_config(2, false, false, 5);

    let j1 = Job::new("1".into(), "clientA".into(), "python".into(), "".into(), None, 10);
    let j2 = Job::new("2".into(), "clientB".into(), "python".into(), "".into(), None, 10);

    scheduler.submit(j1).unwrap();
    scheduler.submit(j2).unwrap();

    let p1 = scheduler.pop_next_job().expect("pop 1");
    let p2 = scheduler.pop_next_job().expect("pop 2");

    assert_eq!(p1.client_id, "clientA");
    assert_eq!(p2.client_id, "clientB");
}
