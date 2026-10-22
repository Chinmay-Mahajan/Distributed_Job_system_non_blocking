use distributed_jobv2::model::{ClientCommand, JobRequest, JobResult, JobState};
use distributed_jobv2::protocol::{
    format_poll_ok, format_pong, format_register_ok, format_status_ok, format_submit_ok,
    parse_line, ProtocolFormat,
};

#[test]
fn test_parse_json_register_and_submit() {
    let reg_raw = r#"{"action": "REGISTER"}"#;
    let reg_req = parse_line(reg_raw, "default-client").expect("parse register ok");
    assert_eq!(reg_req.format, ProtocolFormat::Json);
    assert_eq!(reg_req.command, ClientCommand::Register);

    let raw = r#"{"action": "SUBMIT", "secret_key": "sk_alice_123", "language": "python", "code": "print(123)", "stdin": "input", "timeout_secs": 5}"#;
    let req = parse_line(raw, "default-client").expect("parse ok");

    assert_eq!(req.format, ProtocolFormat::Json);
    match req.command {
        ClientCommand::Submit {
            secret_key,
            language,
            code,
            stdin,
            timeout_secs,
        } => {
            assert_eq!(secret_key, "sk_alice_123");
            assert_eq!(language, "python");
            assert_eq!(code, "print(123)");
            assert_eq!(stdin.as_deref(), Some("input"));
            assert_eq!(timeout_secs, Some(5));
        }
        _ => panic!("Expected submit command"),
    }
}

#[test]
fn test_parse_text_commands() {
    // REGISTER
    let reg = parse_line("REGISTER", "default").expect("ok");
    assert_eq!(reg.format, ProtocolFormat::Text);
    assert_eq!(reg.command, ClientCommand::Register);

    // SUBMIT <secret_key> <language> <code...>
    let req = parse_line("SUBMIT sk_bob_999 python print('hello world')", "default").expect("ok");
    assert_eq!(req.format, ProtocolFormat::Text);
    match req.command {
        ClientCommand::Submit {
            secret_key,
            language,
            code,
            ..
        } => {
            assert_eq!(secret_key, "sk_bob_999");
            assert_eq!(language, "python");
            assert_eq!(code, "print('hello world')");
        }
        _ => panic!("Expected submit"),
    }

    // POLL <secret_key> <job_id>
    let poll_req = parse_line("POLL sk_bob_999 12345", "default").expect("ok");
    assert_eq!(poll_req.format, ProtocolFormat::Text);
    match poll_req.command {
        ClientCommand::Poll { secret_key, job_id } => {
            assert_eq!(secret_key, "sk_bob_999");
            assert_eq!(job_id, "12345");
        }
        _ => panic!("Expected poll"),
    }

    // STATUS <secret_key> <job_id>
    let status_req = parse_line("STATUS sk_bob_999 12345", "default").expect("ok");
    assert_eq!(status_req.format, ProtocolFormat::Text);
    match status_req.command {
        ClientCommand::Status { secret_key, job_id } => {
            assert_eq!(secret_key, "sk_bob_999");
            assert_eq!(job_id, "12345");
        }
        _ => panic!("Expected status"),
    }

    // PING
    let ping_req = parse_line("PING", "default").expect("ok");
    assert_eq!(ping_req.format, ProtocolFormat::Text);
    assert_eq!(ping_req.command, ClientCommand::Ping);
}

#[test]
fn test_formatters() {
    let reg_json = format_register_ok("sk_test_123", ProtocolFormat::Json);
    assert!(reg_json.contains(r#""secret_key":"sk_test_123""#));

    let reg_text = format_register_ok("sk_test_123", ProtocolFormat::Text);
    assert_eq!(reg_text, "+sk_test_123\r\n");

    let submit_json = format_submit_ok("abc-123", ProtocolFormat::Json);
    assert!(submit_json.contains(r#""job_id":"abc-123""#));

    let submit_text = format_submit_ok("abc-123", ProtocolFormat::Text);
    assert_eq!(submit_text, ":abc-123\r\n");

    let status_text = format_status_ok("abc-123", JobState::Done, ProtocolFormat::Text);
    assert_eq!(status_text, "+Done\r\n");

    let pong_text = format_pong(ProtocolFormat::Text);
    assert_eq!(pong_text, "+PONG\r\n");

    let res = JobResult {
        job_id: "abc-123".into(),
        client_id: "alice".into(),
        status: JobState::Done,
        stdout: "out\n".into(),
        stderr: "".into(),
        exit_code: Some(0),
        error_message: None,
        job: JobRequest {
            client_id: "alice".into(),
            language: "python".into(),
            code: "pass".into(),
            stdin: None,
            timeout_secs: Some(10),
        },
        created_at_ms: 1000,
        completed_at_ms: Some(1050),
        execution_duration_ms: Some(50),
    };

    let poll_text = format_poll_ok(&res, ProtocolFormat::Text);
    assert!(poll_text.starts_with("+Done {"));
}
