use crate::model::{ClientCommand, JobResult, JobState, ServerResponse};
use crate::scheduler::SchedulerStats;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolFormat {
    Json,
    Text,
}

pub struct ParsedRequest {
    pub command: ClientCommand,
    pub format: ProtocolFormat,
}

pub fn parse_line(line: &str, default_client_id: &str) -> Result<ParsedRequest, String> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err("Empty command".to_string());
    }

    if trimmed.starts_with('{') {
        let mut cmd: ClientCommand =
            serde_json::from_str(trimmed).map_err(|e| format!("Invalid JSON request: {}", e))?;

        // If client_id is omitted in Submit, fill in default_client_id
        if let ClientCommand::Submit { ref mut client_id, .. } = cmd
            && client_id.as_ref().map(|s| s.is_empty()).unwrap_or(true)
        {
            *client_id = Some(default_client_id.to_string());
        }

        return Ok(ParsedRequest {
            command: cmd,
            format: ProtocolFormat::Json,
        });
    }

    // Text-based protocol parser
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    let verb = parts[0].to_uppercase();

    match verb.as_str() {
        "SUBMIT" => {
            // Syntax: SUBMIT <client_id> <language> <code...>
            // Or fallback: SUBMIT <language> <code...> (client_id defaulted)
            if parts.len() < 3 {
                return Err("Syntax: SUBMIT <client_id> <language> <code>".to_string());
            }

            let (client_id, language, code_start_idx) = if parts.len() >= 4 {
                (parts[1].to_string(), parts[2].to_string(), 3)
            } else {
                (default_client_id.to_string(), parts[1].to_string(), 2)
            };

            let code = parts[code_start_idx..].join(" ");
            Ok(ParsedRequest {
                command: ClientCommand::Submit {
                    client_id: Some(client_id),
                    language,
                    code,
                    stdin: None,
                    timeout_secs: None,
                },
                format: ProtocolFormat::Text,
            })
        }
        "POLL" => {
            if parts.len() < 2 {
                return Err("Syntax: POLL <job_id>".to_string());
            }
            Ok(ParsedRequest {
                command: ClientCommand::Poll {
                    job_id: parts[1].to_string(),
                },
                format: ProtocolFormat::Text,
            })
        }
        "STATUS" => {
            if parts.len() < 2 {
                return Err("Syntax: STATUS <job_id>".to_string());
            }
            Ok(ParsedRequest {
                command: ClientCommand::Status {
                    job_id: parts[1].to_string(),
                },
                format: ProtocolFormat::Text,
            })
        }
        "PING" => Ok(ParsedRequest {
            command: ClientCommand::Ping,
            format: ProtocolFormat::Text,
        }),
        "STATS" => Ok(ParsedRequest {
            command: ClientCommand::Stats,
            format: ProtocolFormat::Text,
        }),
        _ => Err(format!("Unknown command: '{}'", verb)),
    }
}

pub fn format_submit_ok(job_id: &str, format: ProtocolFormat) -> String {
    match format {
        ProtocolFormat::Json => {
            let resp = ServerResponse::SubmitOk {
                status: "ok".to_string(),
                job_id: job_id.to_string(),
            };
            format!("{}\n", serde_json::to_string(&resp).unwrap())
        }
        ProtocolFormat::Text => format!(":{}\r\n", job_id),
    }
}

pub fn format_poll_ok(result: &JobResult, format: ProtocolFormat) -> String {
    match format {
        ProtocolFormat::Json => format!("{}\n", serde_json::to_string(result).unwrap()),
        ProtocolFormat::Text => {
            let json_summary = serde_json::to_string(result).unwrap();
            format!("+{} {}\r\n", result.status, json_summary)
        }
    }
}

pub fn format_status_ok(job_id: &str, status: JobState, format: ProtocolFormat) -> String {
    match format {
        ProtocolFormat::Json => {
            let resp = ServerResponse::StatusOk {
                job_id: job_id.to_string(),
                status,
            };
            format!("{}\n", serde_json::to_string(&resp).unwrap())
        }
        ProtocolFormat::Text => format!("+{}\r\n", status),
    }
}

pub fn format_pong(format: ProtocolFormat) -> String {
    match format {
        ProtocolFormat::Json => {
            let resp = ServerResponse::Pong {
                message: "PONG".to_string(),
            };
            format!("{}\n", serde_json::to_string(&resp).unwrap())
        }
        ProtocolFormat::Text => "+PONG\r\n".to_string(),
    }
}

pub fn format_stats(
    stats: &SchedulerStats,
    stored_results: usize,
    format: ProtocolFormat,
) -> String {
    match format {
        ProtocolFormat::Json => {
            let resp = ServerResponse::StatsOk {
                active_clients: stats.active_clients,
                queued_jobs: stats.queued_jobs,
                in_flight_jobs: stats.in_flight_jobs,
                stored_results,
            };
            format!("{}\n", serde_json::to_string(&resp).unwrap())
        }
        ProtocolFormat::Text => format!(
            "+OK active_clients={} queued={} in_flight={} stored_results={}\r\n",
            stats.active_clients, stats.queued_jobs, stats.in_flight_jobs, stored_results
        ),
    }
}

pub fn format_error(err: &str, format: ProtocolFormat) -> String {
    match format {
        ProtocolFormat::Json => {
            let resp = ServerResponse::Error {
                status: "error".to_string(),
                error: err.to_string(),
            };
            format!("{}\n", serde_json::to_string(&resp).unwrap())
        }
        ProtocolFormat::Text => format!("-ERR {}\r\n", err),
    }
}
