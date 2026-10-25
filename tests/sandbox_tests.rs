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

