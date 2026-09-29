use distributed_jobv2::model::{Job, JobState};
use distributed_jobv2::sandbox::{LocalProcessSandbox, MockSandbox, Sandbox};

#[test]
fn test_local_sandbox_python_success() {
    let sandbox = LocalProcessSandbox;
    let job = Job::new(
        "test-py".into(),
        "client1".into(),
        "python".into(),
        "print('sandboxed python')".into(),
        None,
        5,
    );

    let res = sandbox.execute(&job);
    assert_eq!(res.status, JobState::Done);
    assert!(res.stdout.contains("sandboxed python"));
    assert_eq!(res.exit_code, Some(0));
}

#[test]
fn test_local_sandbox_python_with_stdin() {
    let sandbox = LocalProcessSandbox;
    let job = Job::new(
        "test-stdin".into(),
        "client1".into(),
        "python".into(),
        "import sys; data = sys.stdin.read(); print(f'got: {data.strip()}')".into(),
        Some("hello from input".into()),
        5,
    );

    let res = sandbox.execute(&job);
    assert_eq!(res.status, JobState::Done);
    assert!(res.stdout.contains("got: hello from input"));
}

#[test]
fn test_local_sandbox_failure() {
    let sandbox = LocalProcessSandbox;
    let job = Job::new(
        "test-fail".into(),
        "client1".into(),
        "python".into(),
        "import sys; sys.exit(42)".into(),
        None,
        5,
    );

    let res = sandbox.execute(&job);
    assert_eq!(res.status, JobState::Failed);
    assert_eq!(res.exit_code, Some(42));
}

#[test]
fn test_local_sandbox_timeout() {
    let sandbox = LocalProcessSandbox;
    // Job with 1 second timeout running 5 second sleep
    let job = Job::new(
        "test-timeout".into(),
        "client1".into(),
        "python".into(),
        "import time; time.sleep(5)".into(),
        None,
        1,
    );

    let res = sandbox.execute(&job);
    assert_eq!(res.status, JobState::TimedOut);
    assert!(res.stderr.contains("timed out"));
}

#[test]
fn test_mock_sandbox() {
    let sandbox = MockSandbox;

    let job_ok = Job::new("1".into(), "c1".into(), "py".into(), "print(1)".into(), None, 10);
    assert_eq!(sandbox.execute(&job_ok).status, JobState::Done);

    let job_fail = Job::new("2".into(), "c1".into(), "py".into(), "FAIL".into(), None, 10);
    assert_eq!(sandbox.execute(&job_fail).status, JobState::Failed);

    let job_timeout = Job::new("3".into(), "c1".into(), "py".into(), "SLEEP_TIMEOUT".into(), None, 1);
    assert_eq!(sandbox.execute(&job_timeout).status, JobState::TimedOut);
}

#[test]
fn test_docker_sandbox_config_and_pool_wiring() {
    use distributed_jobv2::config::Config;
    use distributed_jobv2::sandbox::DockerSandbox;

    let mut config = Config::default();
    config.enable_prewarmed_pool = false;
    config.docker_python_image = "distjob-python:latest".to_string();

    let sandbox = DockerSandbox::new(&config);
    assert!(sandbox.prewarmed_pool().is_none());

    let mut warm_config = Config::default();
    warm_config.enable_prewarmed_pool = true;
    warm_config.prewarmed_pool_size = 3;
    warm_config.docker_python_image = "custom-runner:v2".to_string();

    let warm_sandbox = DockerSandbox::new(&warm_config);
    assert!(warm_sandbox.prewarmed_pool().is_some());
    let pool = warm_sandbox.prewarmed_pool().unwrap();
    assert_eq!(pool.capacity(), 3);
}

#[test]
fn test_prewarmed_pool_lifecycle_and_shutdown() {
    use distributed_jobv2::config::Config;
    use distributed_jobv2::sandbox::PrewarmedContainerPool;

    let mut config = Config::default();
    config.prewarmed_pool_size = 2;
    config.docker_python_image = "python:3.11-slim".to_string();

    let pool = PrewarmedContainerPool::start(&config);
    assert_eq!(pool.capacity(), 2);
    // Pool shutdown should be clean and not hang
    pool.shutdown();
}
