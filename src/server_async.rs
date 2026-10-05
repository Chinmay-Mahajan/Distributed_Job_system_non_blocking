use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

use crate::config::{Config, SandboxType};
use crate::model::{ClientCommand, Job};
use crate::protocol::{
    format_error, format_poll_ok, format_pong, format_stats, format_status_ok, format_submit_ok,
    parse_line, ProtocolFormat,
};
use crate::sandbox::{DockerSandbox, LocalProcessSandbox, MockSandbox, Sandbox};
use crate::scheduler::TwoLevelScheduler;
use crate::store::ResultStore;
use crate::worker::WorkerPool;

/// Asynchronous Broker Server implementation using Tokio.
///
/// Unlike the synchronous `BrokerServer` (which dedicates one OS thread per connection),
/// this server utilizes an async event-driven reactor (epoll on Linux / kqueue on macOS).
/// Idle connections, polling clients, and network waits yield execution back to Tokio's
/// runtime, consuming zero dedicated OS threads and only ~300 bytes of memory per client.
pub struct AsyncBrokerServer {
    pub config: Config,
    pub scheduler: Arc<TwoLevelScheduler>,
    pub result_store: Arc<ResultStore>,
    worker_pool: std::sync::Mutex<Option<WorkerPool>>,
    reaper_handle: std::sync::Mutex<Option<JoinHandle<()>>>,
    pub shutdown: Arc<AtomicBool>,
    pub sandbox: Arc<dyn Sandbox>,
}

impl AsyncBrokerServer {
    pub fn new(config: Config) -> Self {
        let sandbox: Arc<dyn Sandbox> = match config.sandbox_type {
            SandboxType::Docker => Arc::new(DockerSandbox::new(&config)),
            SandboxType::Local => Arc::new(LocalProcessSandbox),
            SandboxType::Mock => Arc::new(MockSandbox),
        };

        let scheduler = Arc::new(TwoLevelScheduler::from_config(&config));
        let result_store = Arc::new(ResultStore::with_shards(config.result_ttl, config.store_shards));
        let shutdown = Arc::new(AtomicBool::new(false));

        // Start background result reaper
        let reaper_store = Arc::clone(&result_store);
        let reaper_shutdown = Arc::clone(&shutdown);
        let reaper_handle = reaper_store.start_cleanup_thread(
            config.cleanup_interval,
            reaper_shutdown,
        );

        // Start worker pool (fixed number of dedicated OS threads for sandbox execution)
        let worker_pool = WorkerPool::start(
            config.worker_count,
            Arc::clone(&scheduler),
            Arc::clone(&result_store),
            Arc::clone(&sandbox),
        );

        Self {
            config,
            scheduler,
            result_store,
            worker_pool: std::sync::Mutex::new(Some(worker_pool)),
            reaper_handle: std::sync::Mutex::new(Some(reaper_handle)),
            shutdown,
            sandbox,
        }
    }

    /// Run the async server loop accepting connections via Tokio.
    /// Spawns a lightweight asynchronous green task per incoming connection.
    pub async fn run_listener(&self, listener: TcpListener) -> std::io::Result<()> {
        println!(
            "Sandboxed Job Execution Broker (v2) [ASYNC TOKIO MODEL] listening on {}",
            listener.local_addr()?
        );
        println!(
            "Configuration: connection_model=Async(Tokio/epoll), workers={}, max_in_flight={}, queue_limit={:?} (mult={}), two_queue={}, sandbox={:?}, ttl={:?}",
            self.config.worker_count,
            self.config.max_in_flight_per_client,
            self.config.enable_queue_limit,
            self.config.queue_limit_multiplier,
            self.config.enable_two_queue_scheduler,
            self.config.sandbox_type,
            self.config.result_ttl
        );

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            let (stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    if !self.shutdown.load(Ordering::Relaxed) {
                        eprintln!("Failed to accept incoming async connection: {}", e);
                    }
                    continue;
                }
            };

            let scheduler = Arc::clone(&self.scheduler);
            let result_store = Arc::clone(&self.result_store);
            let config = self.config.clone();

            // Spawn lightweight Tokio task instead of a 256KB OS thread!
            tokio::spawn(async move {
                if let Err(e) = handle_connection_async(stream, scheduler, result_store, config).await {
                    if e.kind() != std::io::ErrorKind::UnexpectedEof
                        && e.kind() != std::io::ErrorKind::ConnectionReset
                    {
                        eprintln!("Async connection error: {}", e);
                    }
                }
            });
        }

        Ok(())
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(pool) = self.worker_pool.lock().ok().and_then(|mut l| l.take()) {
            pool.shutdown();
        }
        if let Some(reaper) = self.reaper_handle.lock().ok().and_then(|mut l| l.take()) {
            let _ = reaper.join();
        }
        self.sandbox.shutdown();
    }
}

impl Drop for AsyncBrokerServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Asynchronous connection handler loop.
/// Yields execution back to Tokio whenever waiting for client socket bytes.
async fn handle_connection_async(
    stream: TcpStream,
    scheduler: Arc<TwoLevelScheduler>,
    result_store: Arc<ResultStore>,
    config: Config,
) -> std::io::Result<()> {
    let peer_addr = stream.peer_addr()?;
    let default_client_id = format!("client-{}", peer_addr);

    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            // Client closed connection cleanly
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parsed = match parse_line(trimmed, &default_client_id) {
            Ok(p) => p,
            Err(err_msg) => {
                let err_resp = format_error(&err_msg, ProtocolFormat::Text);
                writer.write_all(err_resp.as_bytes()).await?;
                writer.flush().await?;
                continue;
            }
        };

        match parsed.command {
            ClientCommand::Submit {
                client_id,
                language,
                code,
                stdin,
                timeout_secs,
            } => {
                let job_id = Uuid::new_v4().to_string();
                let client = client_id.unwrap_or_else(|| default_client_id.clone());
                let effective_timeout = timeout_secs
                    .unwrap_or(config.default_timeout_secs)
                    .min(config.max_timeout_secs);

                let job = Job::new(
                    job_id.clone(),
                    client,
                    language,
                    code,
                    stdin,
                    effective_timeout,
                );

                match scheduler.submit(job.clone()) {
                    Ok(()) => {
                        result_store.insert_pending(&job);
                        let resp = format_submit_ok(&job_id, parsed.format);
                        writer.write_all(resp.as_bytes()).await?;
                    }
                    Err(err_msg) => {
                        let resp = format_error(&err_msg, parsed.format);
                        writer.write_all(resp.as_bytes()).await?;
                    }
                }
            }

            ClientCommand::Poll { job_id } => {
                let resp = match result_store.get(&job_id) {
                    Some(result) => format_poll_ok(&result, parsed.format),
                    None => format_error("Job not found or expired", parsed.format),
                };
                writer.write_all(resp.as_bytes()).await?;
            }

            ClientCommand::Status { job_id } => {
                let resp = match result_store.get(&job_id) {
                    Some(result) => format_status_ok(&job_id, result.status, parsed.format),
                    None => format_error("Job not found or expired", parsed.format),
                };
                writer.write_all(resp.as_bytes()).await?;
            }

            ClientCommand::Ping => {
                let resp = format_pong(parsed.format);
                writer.write_all(resp.as_bytes()).await?;
            }

            ClientCommand::Stats => {
                let stats = scheduler.stats();
                let stored_count = result_store.count();
                let resp = format_stats(&stats, stored_count, parsed.format);
                writer.write_all(resp.as_bytes()).await?;
            }
        }

        writer.flush().await?;
    }

    Ok(())
}
