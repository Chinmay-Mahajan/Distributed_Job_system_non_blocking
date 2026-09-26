use distributed_jobv2::model::{ClientCommand, JobRequest, JobResult, JobState};
use distributed_jobv2::protocol::{
    format_poll_ok, format_pong, format_status_ok, format_submit_ok, parse_line, ProtocolFormat,
};

#[test]
fn test_parse_json_submit() {
    let raw = r#"{"action": "SUBMIT", "client_id": "alice", "language": "python", "code": "print(123)", "stdin": "input", "timeout_secs": 5}"#;
    let req = parse_line(raw, "default-client").expect("parse ok");

    assert_eq!(req.format, ProtocolFormat::Json);
    match req.command {
        ClientCommand::Submit {
            client_id,
            language,
            code,
            stdin,
            timeout_secs,
        } => {
            assert_eq!(client_id.as_deref(), Some("alice"));
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
    // SUBMIT
    let req = parse_line("SUBMIT bob python print('hello world')", "default").expect("ok");
    assert_eq!(req.format, ProtocolFormat::Text);
    match req.command {
        ClientCommand::Submit {
            client_id,
            language,
            code,
            ..
        } => {
            assert_eq!(client_id.as_deref(), Some("bob"));
            assert_eq!(language, "python");
            assert_eq!(code, "print('hello world')");
        }
        _ => panic!("Expected submit"),
    }

    // POLL
    let poll_req = parse_line("POLL 12345", "default").expect("ok");
    assert_eq!(poll_req.format, ProtocolFormat::Text);
    match poll_req.command {
        ClientCommand::Poll { job_id } => assert_eq!(job_id, "12345"),
        _ => panic!("Expected poll"),
    }

    // STATUS
    let status_req = parse_line("STATUS 12345", "default").expect("ok");
    assert_eq!(status_req.format, ProtocolFormat::Text);
    match status_req.command {
        ClientCommand::Status { job_id } => assert_eq!(job_id, "12345"),
        _ => panic!("Expected status"),
    }

    // PING
    let ping_req = parse_line("PING", "default").expect("ok");
    assert_eq!(ping_req.format, ProtocolFormat::Text);
    assert_eq!(ping_req.command, ClientCommand::Ping);
}

#[test]
fn test_formatters() {
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
