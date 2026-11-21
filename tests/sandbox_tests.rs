use distributed_jobv2::config::Config;
use distributed_jobv2::sandbox::{DockerSandbox, PrewarmedContainerPool};

#[test]
fn test_docker_sandbox_config_and_pool_wiring() {
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
    pool.shutdown();
}

#[test]
fn test_prewarmed_pool_lifecycle_and_shutdown() {
    let mut config = Config::default();
    config.prewarmed_pool_size = 2;
    config.docker_python_image = "python:3.11-slim".to_string();

    let pool = PrewarmedContainerPool::start(&config);
    assert_eq!(pool.capacity(), 2);
    // Pool shutdown should be clean and not hang
    pool.shutdown();
}

#[test]
fn test_nsjail_sandbox_config_and_unsupported_language() {
    use distributed_jobv2::model::Job;
    use distributed_jobv2::sandbox::{NsjailSandbox, Sandbox};

    let mut config = Config::default();
    config.nsjail_bin = "nonexistent_nsjail_binary_123".to_string();
    config.nsjail_rootfs = "/tmp/fake_rootfs".to_string();

    let sandbox = NsjailSandbox::new(&config);
    assert_eq!(sandbox.nsjail_bin, "nonexistent_nsjail_binary_123");
    assert_eq!(sandbox.rootfs_path, "/tmp/fake_rootfs");

    // Unsupported language should fast-fail
    let job = Job {
        id: uuid::Uuid::new_v4().to_string(),
        client_id: "test-client".to_string(),
        language: "rust_unsupported".to_string(),
        code: "fn main() {}".to_string(),
        stdin: None,
        timeout_secs: 5,
        created_at_ms: 0,
    };
    let res = sandbox.execute(&job);
    assert_eq!(res.status, distributed_jobv2::model::JobState::Failed);
    assert!(res.stderr.contains("Unsupported language"));
}

