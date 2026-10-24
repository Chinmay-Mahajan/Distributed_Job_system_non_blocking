use serde::{Deserialize, Serialize};

/// The lifecycle state of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    /// Queued in client's FIFO queue, waiting for a worker.
    Pending,
    /// Currently executing in an isolated sandbox.
    Running,
    /// Finished execution with exit code 0.
    Done,
    /// Finished execution with non-zero exit code or execution error.
    Failed,
    /// Terminated forcibly by broker because it exceeded the wall-clock timeout.
    TimedOut,
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobState::Pending => write!(f, "Pending"),
            JobState::Running => write!(f, "Running"),
            JobState::Done => write!(f, "Done"),
            JobState::Failed => write!(f, "Failed"),
            JobState::TimedOut => write!(f, "TimedOut"),
        }
    }
}

/// A request to execute code in a sandbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRequest {
    pub client_id: String, // we wanna make sure we keep a track of whom this request belongs too . 
    pub language: String,
    pub code: String,
    #[serde(default)]
    pub stdin: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

/// An internal job descriptor held in the scheduler and result store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub client_id: String, // storing the client id inside the job along with the job id , had we not done this any client who is aware of the job id of a job submitted by another clinet would be able to poll the job !!! (security risk)
    pub language: String,
    pub code: String,
    pub stdin: Option<String>,
    pub timeout_secs: u64,
    pub created_at_ms: u64,
}

impl Job {
    pub fn new(
        id: String,
        client_id: String,
        language: String,
        code: String,
        stdin: Option<String>,
        timeout_secs: u64,
    ) -> Self {
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            id,
            client_id,
            language,
            code,
            stdin,
            timeout_secs,
            created_at_ms,
        }
    }

    pub fn to_request(&self) -> JobRequest {
        JobRequest {
            client_id: self.client_id.clone(),
            language: self.language.clone(),
            code: self.code.clone(),
            stdin: self.stdin.clone(),
            timeout_secs: Some(self.timeout_secs),
        }
    }
}

/// The result of a job execution stored in the ResultStore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobResult {
    pub job_id: String,
    pub client_id: String, // this !!! prevents people from veiwing the results of otheer peoples code . 
    pub status: JobState,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub error_message: Option<String>,
    pub created_at_ms: u64,
    pub completed_at_ms: Option<u64>,
    pub execution_duration_ms: Option<u64>,
}

impl JobResult {
    pub fn new_pending(job: &Job) -> Self {
        Self {
            job_id: job.id.clone(),
            client_id: job.client_id.clone(),
            status: JobState::Pending,
            stdout: String::new(),
            stderr: String::new(),
            exit_code: None,
            error_message: None,
            created_at_ms: job.created_at_ms,
            completed_at_ms: None,
            execution_duration_ms: None,
        }
    }
}

/// Parsed client request commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum ClientCommand {
    #[serde(rename = "REGISTER", alias = "AUTH")]
    Register,
    #[serde(rename = "SUBMIT")]
    Submit {
        #[serde(default)]
        secret_key: String,
        language: String,
        code: String,
        #[serde(default)]
        stdin: Option<String>,
        #[serde(default)]
        timeout_secs: Option<u64>,
    },
    #[serde(rename = "POLL")]
    Poll {
        #[serde(default)]
        secret_key: String,
        job_id: String,
    },
    #[serde(rename = "STATUS")]
    Status {
        #[serde(default)]
        secret_key: String,
        job_id: String,
    },
    #[serde(rename = "PING")]
    Ping,
    #[serde(rename = "STATS")]
    Stats,
}

/// Responses sent back to clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ServerResponse {
    RegisterOk {
        status: String,
        secret_key: String,
    },
    SubmitOk {
        status: String,
        job_id: String,
    },
    PollOk(Box<JobResult>),
    StatusOk {
        job_id: String,
        status: JobState,
    },
    Pong {
        message: String,
    },
    StatsOk {
        active_clients: usize,
        queued_jobs: usize,
        in_flight_jobs: usize,
        stored_results: usize,
        #[serde(default)]
        dispatchable_clients: usize,
        #[serde(default)]
        waiting_clients: usize,
    },
    Error {
        status: String,
        error: String,
    },
}
