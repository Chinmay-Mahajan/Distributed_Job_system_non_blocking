use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use distributed_jobv2::auth::AuthManager;
use distributed_jobv2::config::{Config, SandboxType};
use distributed_jobv2::model::{JobResult, JobState, ServerResponse};
use distributed_jobv2::server::BrokerServer;
use distributed_jobv2::server_async::AsyncBrokerServer;

fn start_test_server(sandbox_type: SandboxType, max_in_flight: usize) -> (String, Arc<BrokerServer>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr").to_string();

    let config = Config {
        bind_address: addr.clone(),
        worker_count: 2,
        max_in_flight_per_client: max_in_flight,
        sandbox_type,
        default_timeout_secs: 2,
        result_ttl: Duration::from_secs(60),
        ..Config::default()
    };

    let server = Arc::new(BrokerServer::new(config));
    let server_clone = Arc::clone(&server);

    thread::spawn(move || {
        let _ = server_clone.run_listener(listener);
    });

    // Give listener a moment to start
    thread::sleep(Duration::from_millis(50));

    (addr, server)
}

fn register_client_json(stream: &mut TcpStream, reader: &mut BufReader<TcpStream>) -> String {
    let reg_req = serde_json::json!({ "action": "REGISTER" });
    writeln!(stream, "{}", reg_req).unwrap();

    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let resp: ServerResponse = serde_json::from_str(line.trim()).expect("parse register response");
    match resp {
        ServerResponse::RegisterOk { secret_key, .. } => secret_key,
        other => panic!("Expected RegisterOk, got {:?}", other),
    }
}

#[test]
fn test_e2e_json_submit_and_poll() {
    let (addr, _server) = start_test_server(SandboxType::Local, 2);

    let mut stream = TcpStream::connect(&addr).expect("connect to broker");
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));

    // 0. Register to obtain server-issued secret_key
    let secret_key = register_client_json(&mut stream, &mut reader);
    let expected_hash = AuthManager::hash_key(&secret_key);

    // 1. Submit python code via JSON
    let submit_req = serde_json::json!({
        "action": "SUBMIT",
        "secret_key": secret_key,
        "language": "python",
        "code": "print('hello from e2e test')",
        "timeout_secs": 5
    });
    writeln!(stream, "{}", submit_req).unwrap();

    let mut response_line = String::new();
    reader.read_line(&mut response_line).unwrap();

    let submit_resp: ServerResponse = serde_json::from_str(response_line.trim()).expect("parse submit response");
    let job_id = match submit_resp {
        ServerResponse::SubmitOk { job_id, .. } => job_id,
        other => panic!("Unexpected response: {:?}", other),
    };

    assert!(!job_id.is_empty());

    // 2. Poll until Done
    let mut completed_result: Option<JobResult> = None;
    for _ in 0..50 {
        thread::sleep(Duration::from_millis(50));
        let poll_req = serde_json::json!({
            "action": "POLL",
            "secret_key": secret_key,
            "job_id": job_id
        });
        writeln!(stream, "{}", poll_req).unwrap();

        response_line.clear();
        reader.read_line(&mut response_line).unwrap();

        if let Ok(res) = serde_json::from_str::<JobResult>(response_line.trim())
            && res.status == JobState::Done
        {
            completed_result = Some(res);
            break;
        }
    }

    let final_res = completed_result.expect("job should complete within 2.5s");
    assert_eq!(final_res.status, JobState::Done);
    assert!(final_res.stdout.contains("hello from e2e test"));
    assert_eq!(final_res.job.client_id, expected_hash);
}

#[test]
fn test_e2e_text_submit_and_poll() {
    let (addr, _server) = start_test_server(SandboxType::Local, 2);

    let mut stream = TcpStream::connect(&addr).expect("connect");
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));

    // 0. Register via text command
    writeln!(stream, "REGISTER").unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.starts_with("+sk_"), "Expected +sk_<uuid>, got {}", line);
    let secret_key = line.trim()[1..].to_string();

    // 1. Submit via text command
    writeln!(stream, "SUBMIT {} python print(100 + 200)", secret_key).unwrap();

    line.clear();
    reader.read_line(&mut line).unwrap();
    assert!(line.starts_with(':'), "Expected :<job_id>, got {}", line);
    let job_id = line.trim()[1..].to_string();

    // 2. Poll via text command
    let mut done = false;
    for _ in 0..50 {
        thread::sleep(Duration::from_millis(50));
        writeln!(stream, "STATUS {} {}", secret_key, job_id).unwrap();

        line.clear();
        reader.read_line(&mut line).unwrap();
        if line.trim() == "+Done" {
            done = true;
            break;
        }
    }

    assert!(done, "Expected +Done status");

    // Fetch full result via POLL
    writeln!(stream, "POLL {} {}", secret_key, job_id).unwrap();
    line.clear();
    reader.read_line(&mut line).unwrap();
    assert!(line.starts_with("+Done {"));
    assert!(line.contains("300"));
}

#[test]
fn test_e2e_wall_clock_timeout() {
    let (addr, _server) = start_test_server(SandboxType::Local, 2);

    let mut stream = TcpStream::connect(&addr).expect("connect");
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));

    let secret_key = register_client_json(&mut stream, &mut reader);

    // Submit job with 1 second timeout that attempts to sleep for 10 seconds
    let submit_req = serde_json::json!({
        "action": "SUBMIT",
        "secret_key": secret_key,
        "language": "python",
        "code": "import time; time.sleep(10)",
        "timeout_secs": 1
    });
    writeln!(stream, "{}", submit_req).unwrap();

    let mut response_line = String::new();
    reader.read_line(&mut response_line).unwrap();
    let submit_resp: ServerResponse = serde_json::from_str(response_line.trim()).unwrap();
    let job_id = match submit_resp {
        ServerResponse::SubmitOk { job_id, .. } => job_id,
        _ => panic!(),
    };

    // Poll until TimedOut
    let mut timed_out = false;
    for _ in 0..60 {
        thread::sleep(Duration::from_millis(100));
        writeln!(stream, "STATUS {} {}", secret_key, job_id).unwrap();
        response_line.clear();
        reader.read_line(&mut response_line).unwrap();
        if response_line.trim() == "+TimedOut" {
            timed_out = true;
            break;
        }
    }

    assert!(timed_out, "Expected job to be killed and marked as TimedOut");
}

#[test]
fn test_e2e_multi_client_fairness_under_contention() {
    let (addr, _server) = start_test_server(SandboxType::Local, 2);

    // Client A registers and submits 6 jobs that sleep for 200ms each
    let mut stream_a = TcpStream::connect(&addr).unwrap();
    let mut reader_a = BufReader::new(stream_a.try_clone().unwrap());
    let sk_a = register_client_json(&mut stream_a, &mut reader_a);

    for i in 0..6 {
        let req = serde_json::json!({
            "action": "SUBMIT",
            "secret_key": sk_a,
            "language": "python",
            "code": format!("import time; time.sleep(0.2); print('A{}')", i),
            "timeout_secs": 5
        });
        writeln!(stream_a, "{}", req).unwrap();
        let mut line = String::new();
        reader_a.read_line(&mut line).unwrap();
    }

    // Now Client B connects, registers, and submits 1 quick job
    let mut stream_b = TcpStream::connect(&addr).unwrap();
    let mut reader_b = BufReader::new(stream_b.try_clone().unwrap());
    let sk_b = register_client_json(&mut stream_b, &mut reader_b);

    let req_b = serde_json::json!({
        "action": "SUBMIT",
        "secret_key": sk_b,
        "language": "python",
        "code": "print('B-done')",
        "timeout_secs": 5
    });
    writeln!(stream_b, "{}", req_b).unwrap();
    let mut line_b = String::new();
    reader_b.read_line(&mut line_b).unwrap();
    let resp: ServerResponse = serde_json::from_str(line_b.trim()).unwrap();
    let job_id_b = match resp {
        ServerResponse::SubmitOk { job_id, .. } => job_id,
        _ => panic!(),
    };

    // Client B's job should complete promptly because round-robin gives Client B an immediate turn
    // and Client A is throttled to 2 in-flight.
    let mut b_completed = false;
    for _ in 0..50 {
        thread::sleep(Duration::from_millis(50));
        let poll = serde_json::json!({ "action": "POLL", "secret_key": sk_b, "job_id": job_id_b });
        writeln!(stream_b, "{}", poll).unwrap();
        let mut resp_line = String::new();
        reader_b.read_line(&mut resp_line).unwrap();

        if let Ok(res) = serde_json::from_str::<JobResult>(resp_line.trim())
            && res.status == JobState::Done
        {
            assert!(res.stdout.contains("B-done"));
            b_completed = true;
            break;
        }
    }

    assert!(b_completed, "Client B should not be starved by Client A's batch");
}

#[test]
fn test_e2e_rate_limiting_rejection() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr").to_string();

    let config = Config {
        bind_address: addr.clone(),
        worker_count: 1,
        max_in_flight_per_client: 1,
        enable_queue_limit: true,
        queue_limit_multiplier: 2, // max queued = 1 * 2 = 2
        sandbox_type: SandboxType::Local,
        default_timeout_secs: 5,
        result_ttl: Duration::from_secs(60),
        cleanup_interval: Duration::from_millis(50),
        ..Config::default()
    };

    let server = Arc::new(BrokerServer::new(config));
    let server_clone = Arc::clone(&server);

    thread::spawn(move || {
        let _ = server_clone.run_listener(listener);
    });

    thread::sleep(Duration::from_millis(50));

    let mut stream = TcpStream::connect(&addr).expect("connect");
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let sk = register_client_json(&mut stream, &mut reader);
    let expected_hash = AuthManager::hash_key(&sk);

    // Submit 3 jobs quickly:
    // Job 1 is popped and runs (sleep 0.5s) -> in_flight = 1
    // Job 2 is queued -> queued = 1
    // Job 3 is queued -> queued = 2 (reaches capacity limit of 1 * 2 = 2)
    for i in 1..=3 {
        let req = serde_json::json!({
            "action": "SUBMIT",
            "secret_key": sk,
            "language": "python",
            "code": "import time; time.sleep(0.5)",
        });
        writeln!(stream, "{}", req).unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let resp: ServerResponse = serde_json::from_str(line.trim()).expect("parse ok");
        match resp {
            ServerResponse::SubmitOk { .. } => {}
            other => panic!("Expected SubmitOk for job {}, got {:?}", i, other),
        }
    }

    // Submit 4th job immediately: while 2 are queued and 1 in-flight, must be rejected!
    let req4 = serde_json::json!({
        "action": "SUBMIT",
        "secret_key": sk,
        "language": "python",
        "code": "import time; time.sleep(0.5)",
    });
    writeln!(stream, "{}", req4).unwrap();
    let mut line4 = String::new();
    reader.read_line(&mut line4).unwrap();

    let resp4: ServerResponse = serde_json::from_str(line4.trim()).expect("parse error resp");
    match resp4 {
        ServerResponse::Error { error, .. } => {
            assert!(error.contains("Rate limit exceeded"));
            assert!(error.contains(&expected_hash));
        }
        other => panic!("Expected Error response for job 4, got {:?}", other),
    }

    server.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_e2e_async_server_submit_and_poll() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
    use tokio::net::TcpStream as TokioTcpStream;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr").to_string();

    let config = Config {
        bind_address: addr.clone(),
        worker_count: 2,
        max_in_flight_per_client: 2,
        sandbox_type: SandboxType::Local,
        default_timeout_secs: 2,
        result_ttl: Duration::from_secs(60),
        enable_async_server: true,
        ..Config::default()
    };

    let server = Arc::new(AsyncBrokerServer::new(config));
    let server_clone = Arc::clone(&server);

    tokio::spawn(async move {
        let _ = server_clone.run_listener(listener).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let stream = TokioTcpStream::connect(&addr).await.expect("connect to async broker");
    let (reader, mut writer) = stream.into_split();
    let mut reader = TokioBufReader::new(reader);

    // 0. Register via JSON
    let reg_req = serde_json::json!({ "action": "REGISTER" });
    writer.write_all(format!("{}\n", reg_req).as_bytes()).await.unwrap();
    writer.flush().await.unwrap();

    let mut reg_line = String::new();
    reader.read_line(&mut reg_line).await.unwrap();
    let reg_resp: ServerResponse = serde_json::from_str(reg_line.trim()).expect("parse register response");
    let secret_key = match reg_resp {
        ServerResponse::RegisterOk { secret_key, .. } => secret_key,
        other => panic!("Expected RegisterOk, got {:?}", other),
    };

    // 1. Submit python code via JSON
    let submit_req = serde_json::json!({
        "action": "SUBMIT",
        "secret_key": secret_key,
        "language": "python",
        "code": "print('hello from async server')",
        "timeout_secs": 5
    });
    writer.write_all(format!("{}\n", submit_req).as_bytes()).await.unwrap();
    writer.flush().await.unwrap();

    let mut response_line = String::new();
    reader.read_line(&mut response_line).await.unwrap();

    let submit_resp: ServerResponse = serde_json::from_str(response_line.trim()).expect("parse submit response");
    let job_id = match submit_resp {
        ServerResponse::SubmitOk { job_id, .. } => job_id,
        other => panic!("Expected SubmitOk, got {:?}", other),
    };

    // 2. Poll until DONE
    let mut result_done = None;
    for _ in 0..50 {
        let poll_req = serde_json::json!({
            "action": "POLL",
            "secret_key": secret_key,
            "job_id": job_id.clone()
        });
        writer.write_all(format!("{}\n", poll_req).as_bytes()).await.unwrap();
        writer.flush().await.unwrap();

        let mut poll_line = String::new();
        reader.read_line(&mut poll_line).await.unwrap();

        if let Ok(res) = serde_json::from_str::<JobResult>(poll_line.trim())
            && res.status == JobState::Done
        {
            result_done = Some(res);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let final_res = result_done.expect("Job should finish within timeout");
    assert_eq!(final_res.status, JobState::Done);
    assert_eq!(final_res.exit_code, Some(0));
    assert!(final_res.stdout.contains("hello from async server"));

    server.shutdown();
}

#[test]
fn test_e2e_auth_ownership_and_unauthorized_rejection() {
    let (addr, server) = start_test_server(SandboxType::Mock, 2);

    let mut stream = TcpStream::connect(&addr).expect("connect");
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));

    // 1. Submitting with an invalid/unregistered secret_key must be rejected
    let bad_submit = serde_json::json!({
        "action": "SUBMIT",
        "secret_key": "sk_forged_key_does_not_exist",
        "language": "python",
        "code": "print('hack')"
    });
    writeln!(stream, "{}", bad_submit).unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.contains("Unauthorized"), "Expected Unauthorized error, got: {}", line);

    // 2. Register Alice and Bob
    let sk_alice = register_client_json(&mut stream, &mut reader);
    let sk_bob = register_client_json(&mut stream, &mut reader);

    // 3. Alice submits a job
    let alice_submit = serde_json::json!({
        "action": "SUBMIT",
        "secret_key": sk_alice,
        "language": "python",
        "code": "print('alice secret output')"
    });
    writeln!(stream, "{}", alice_submit).unwrap();
    line.clear();
    reader.read_line(&mut line).unwrap();
    let alice_job_id = match serde_json::from_str::<ServerResponse>(line.trim()).unwrap() {
        ServerResponse::SubmitOk { job_id, .. } => job_id,
        other => panic!("Expected SubmitOk, got {:?}", other),
    };

    // 4. Bob tries to POLL Alice's job -> must be rejected!
    let bob_poll = serde_json::json!({
        "action": "POLL",
        "secret_key": sk_bob,
        "job_id": alice_job_id
    });
    writeln!(stream, "{}", bob_poll).unwrap();
    line.clear();
    reader.read_line(&mut line).unwrap();
    assert!(
        line.contains("Unauthorized: job belongs to another client"),
        "Bob should not be allowed to poll Alice's job, got: {}",
        line
    );

    // 5. Bob tries to STATUS Alice's job -> must also be rejected!
    let bob_status = serde_json::json!({
        "action": "STATUS",
        "secret_key": sk_bob,
        "job_id": alice_job_id
    });
    writeln!(stream, "{}", bob_status).unwrap();
    line.clear();
    reader.read_line(&mut line).unwrap();
    assert!(
        line.contains("Unauthorized: job belongs to another client"),
        "Bob should not be allowed to check status of Alice's job, got: {}",
        line
    );

    server.shutdown();
}

#[test]
fn test_e2e_l1_l2_redis_cache_and_poll_rate_limit_demotion() {
    let config = Config {
        l1_max_capacity: 128,
        l1_ttl: Duration::from_millis(150), // Short L1 TTL (150ms) to test L1 expiry -> L2 Redis promotion
        l2_redis_ttl_secs: 60,
        max_polls_per_sec: 3, // Max 3 polls/sec
        max_poll_strikes: 2,  // 2 strikes -> evict from L1 & set penalty in L2 Redis
        l2_penalty_ttl_secs: 10,
        ..Config::default()
    };

    let auth = AuthManager::new(&config);

    // 1. Register user -> inserted into both L1 (Heap) and L2 (Redis)
    let sk = auth.register();
    let key_hash = AuthManager::hash_key(&sk);
    assert!(auth.is_in_l1(&key_hash), "Newly registered key must be in L1");

    // 2. Let L1 TTL expire (wait 200ms > 150ms)
    thread::sleep(Duration::from_millis(200));
    assert!(!auth.is_in_l1(&key_hash), "Key should be expired from L1 after 200ms");

    // 3. Genuine user polls after L1 TTL expiry -> L1 Miss, L2 (Redis) Hit -> Promoted back to L1!
    let verified = auth.verify_poll(&sk).expect("L2 hit should succeed");
    assert_eq!(verified, key_hash);
    assert!(
        auth.is_in_l1(&key_hash),
        "Genuine user must be re-promoted from L2 (Redis) to L1 on poll"
    );

    // 4. Now simulate polling spammer exceeding max_polls_per_sec (3 polls/sec):
    // Poll #1 was the promotion above. Poll #2 and Poll #3 are within the 3/sec quota:
    assert!(auth.verify_poll(&sk).is_ok(), "Poll 2/3 should succeed");
    assert!(auth.verify_poll(&sk).is_ok(), "Poll 3/3 should succeed");

    // Poll #4 in the same 1s window -> Violation Strike 1/2!
    let strike1 = auth.verify_poll(&sk).unwrap_err();
    assert!(strike1.contains("strike 1/2"), "Expected strike 1/2, got: {}", strike1);
    assert!(auth.is_in_l1(&key_hash), "Still in L1 after strike 1");

    // Poll #5 in the same 1s window -> Violation Strike 2/2 -> Evicted from L1 + Demoted to L2 Redis penalty!
    let strike2 = auth.verify_poll(&sk).unwrap_err();
    assert!(
        strike2.contains("evicted from L1 cache to L2 (Redis) penalty probation"),
        "Expected L1 eviction message, got: {}",
        strike2
    );
    assert!(
        !auth.is_in_l1(&key_hash),
        "Spammer must be evicted from L1 cache after reaching max_poll_strikes"
    );

    // 5. Subsequent poll while on L2 penalty probation:
    // Hits L2 (Redis), sees penalty:<key_hash>, and is NOT promoted back to L1!
    assert!(auth.verify_poll(&sk).is_ok(), "L2 lookup still authenticates valid user");
    assert!(
        !auth.is_in_l1(&key_hash),
        "Penalized user must NOT be re-promoted to L1 while penalty TTL is active in L2 Redis"
    );
}
