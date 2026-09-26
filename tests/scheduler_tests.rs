use distributed_jobv2::model::Job;
use distributed_jobv2::scheduler::TwoLevelScheduler;

#[test]
fn test_fifo_within_single_client() {
    let scheduler = TwoLevelScheduler::new(5);

    let j1 = Job::new("1".into(), "clientA".into(), "python".into(), "code1".into(), None, 10);
    let j2 = Job::new("2".into(), "clientA".into(), "python".into(), "code2".into(), None, 10);
    let j3 = Job::new("3".into(), "clientA".into(), "python".into(), "code3".into(), None, 10);

    scheduler.submit(j1);
    scheduler.submit(j2);
    scheduler.submit(j3);

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
        scheduler.submit(job);
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

    scheduler.submit(Job::new("A1".into(), "clientA".into(), "python".into(), "".into(), None, 10));
    scheduler.submit(Job::new("A2".into(), "clientA".into(), "python".into(), "".into(), None, 10));
    scheduler.submit(Job::new("A3".into(), "clientA".into(), "python".into(), "".into(), None, 10));

    scheduler.submit(Job::new("B1".into(), "clientB".into(), "python".into(), "".into(), None, 10));
    scheduler.submit(Job::new("C1".into(), "clientC".into(), "python".into(), "".into(), None, 10));

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
