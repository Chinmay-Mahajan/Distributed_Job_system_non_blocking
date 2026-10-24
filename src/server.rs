use std::io::{BufRead, BufReader, Write , Read};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::auth::AuthManager;
use crate::config::{Config, SandboxType};
use crate::model::{ClientCommand, Job};
use crate::protocol::{
    format_error, format_poll_ok, format_pong, format_register_ok, format_stats,
    format_status_ok, format_submit_ok, parse_line, ProtocolFormat,
};
use crate::sandbox::{DockerSandbox, LocalProcessSandbox, MockSandbox, Sandbox};
use crate::scheduler::TwoLevelScheduler;
use crate::store::ResultStore;
use crate::worker::WorkerPool;

const MAX_LINE_BYTES:usize = 64*1024;

pub struct BrokerServer {
    pub config: Config,
    pub scheduler: Arc<TwoLevelScheduler>,
    pub result_store: Arc<ResultStore>,
    pub auth_manager: Arc<AuthManager>,
    worker_pool: std::sync::Mutex<Option<WorkerPool>>,
    reaper_handle: std::sync::Mutex<Option<JoinHandle<()>>>,
    pub shutdown: Arc<AtomicBool>,
    pub sandbox: Arc<dyn Sandbox>,
}

impl BrokerServer {
    pub fn new(config: Config) -> Self {
        let sandbox: Arc<dyn Sandbox> = match config.sandbox_type {
            SandboxType::Docker => Arc::new(DockerSandbox::new(&config)),
            SandboxType::Local => Arc::new(LocalProcessSandbox),
            SandboxType::Mock => Arc::new(MockSandbox),
        };

        let scheduler = Arc::new(TwoLevelScheduler::from_config(&config));
        let result_store = Arc::new(ResultStore::with_shards(config.result_ttl, config.store_shards));
        let auth_manager = Arc::new(AuthManager::new(&config));
        let shutdown = Arc::new(AtomicBool::new(false));

        // Start background result reaper
        let reaper_store = Arc::clone(&result_store);
        let reaper_shutdown = Arc::clone(&shutdown);
        let reaper_handle = reaper_store.start_cleanup_thread(
            config.cleanup_interval,
            reaper_shutdown,
        );

        // Start worker pool
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
            auth_manager,
            worker_pool: std::sync::Mutex::new(Some(worker_pool)),
            reaper_handle: std::sync::Mutex::new(Some(reaper_handle)),
            shutdown,
            sandbox,
        }
    }

    /// Run the server loop accepting connections.
    /// Spawns one OS thread per connection.
    pub fn run_listener(&self, listener: TcpListener) -> std::io::Result<()> {
        println!(
            "Sandboxed Job Execution Broker (v2) listening on {}",
            listener.local_addr()?
        );
        println!(
            "Configuration: workers={}, max_in_flight={}, queue_limit={:?} (mult={}), two_queue={}, sandbox={:?}, ttl={:?}",
            self.config.worker_count,
            self.config.max_in_flight_per_client,
            self.config.enable_queue_limit,
            self.config.queue_limit_multiplier,
            self.config.enable_two_queue_scheduler,
            self.config.sandbox_type,
            self.config.result_ttl
        );

        for stream in listener.incoming() {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            match stream {
                Ok(stream) => {
                    let scheduler = Arc::clone(&self.scheduler);
                    let result_store = Arc::clone(&self.result_store);
                    let auth_manager = Arc::clone(&self.auth_manager);
                    let config = self.config.clone();

                    let builder = std::thread::Builder::new()
                        .name("broker-conn".into())
                        .stack_size(256 * 1024);

                    if let Err(e) = builder.spawn(move || {
                        if let Err(e) =
                            handle_connection(stream, scheduler, result_store, auth_manager, config)
                        {
                            eprintln!("Connection error: {}", e);
                        }
                    }) {
                        eprintln!("Failed to spawn connection thread: {}", e);
                    }
                }
                Err(e) => {
                    if !self.shutdown.load(Ordering::Relaxed) {
                        eprintln!("Failed to accept incoming connection: {}", e);
                    }
                }
            }
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

impl Drop for BrokerServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct ConnectionCleanupGuard {
    auth_manager: Arc<AuthManager>,
    default_client_id: String,
}

impl Drop for ConnectionCleanupGuard {
    fn drop(&mut self) {
        self.auth_manager.remove_connection(&self.default_client_id);
    }
}

fn handle_connection(
    stream: TcpStream,
    scheduler: Arc<TwoLevelScheduler>,
    result_store: Arc<ResultStore>,
    auth_manager: Arc<AuthManager>,
    config: Config,
) -> std::io::Result<()> {
    let peer_addr = stream.peer_addr()?; // addr IP+port of the client connecting to the server
    let default_client_id = format!("client-{}", peer_addr);
    auth_manager.init_connection(&default_client_id);
    let _cleanup_guard = ConnectionCleanupGuard {
        auth_manager: Arc::clone(&auth_manager),
        default_client_id: default_client_id.clone(),
    };

    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);
    let mut line = String::with_capacity(1024);
    let mut key_expires_at: Option<Instant> = None;
    let mut active_key_hash: Option<String> = None;

    loop {
        // SO Earlier we wrote the data in string line ,  since a String in rust is a wrapper around a Vec<u8> when this Vec grows bigger than the capacity rust requests the OS for a 2xCapcity sized chunk of memoery on the heap. So had we not bounded the data being read then we would have an evergrowing heap structure . Which could trigger a SIGKILL on the server itself , taking the entire server down. 
        if line.capacity()>64*1024{ 
            line = String::with_capacity(1024);
        }
        else{
            line.clear();
        }

        // Enforce both the per-connection idle timeout and the issued secret_key lifetime deadline
        let mut wait_duration = config.client_idle_timeout;
        if let Some(deadline) = key_expires_at {
            let remaining_key_ttl = deadline.saturating_duration_since(Instant::now());
            if remaining_key_ttl.is_zero() {
                if let Some(ref kh) = active_key_hash {
                    result_store.remove_client_results(kh);
                }
                let err_resp = format_error(
                    "Session expired: secret_key TTL elapsed, disconnecting",
                    ProtocolFormat::Text,
                );
                let _ = writer.write_all(err_resp.as_bytes());
                let _ = writer.flush();
                break;
            }
            wait_duration = wait_duration.min(remaining_key_ttl);
        }

        writer.set_read_timeout(Some(wait_duration))?;
        let bytes_read = match (&mut reader).take((MAX_LINE_BYTES+1) as u64).read_line(&mut line) {
            Ok(n) => {
                if n>MAX_LINE_BYTES{
                    let err = format_error("Too big payload", ProtocolFormat::Text);
                    let _ =writer.write_all(err.as_bytes()); 
                    let _ = writer.flush(); 
                    break;

                }

                n 
                
            },
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if let Some(ref kh) = active_key_hash {
                    result_store.remove_client_results(kh);
                }
                let reason = if key_expires_at.is_some_and(|d| Instant::now() >= d) {
                    "Session expired: secret_key TTL elapsed, disconnecting"
                } else {
                    "Connection idle timeout exceeded, disconnecting"
                };
                let err_resp = format_error(reason, ProtocolFormat::Text);
                let _ = writer.write_all(err_resp.as_bytes());
                let _ = writer.flush();
                break;
            }
            Err(e) => return Err(e),
        };
        if bytes_read == 0 {
            // Client closed connection
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
                writer.write_all(err_resp.as_bytes())?;
                writer.flush()?;
                continue;
            }
        };

        match parsed.command {
            ClientCommand::Register => {
                let resp = match auth_manager.register_for_client(&default_client_id) {
                    Ok(secret_key) => {
                        key_expires_at = Some(
                            Instant::now() + Duration::from_secs(config.l2_redis_ttl_secs),
                        );
                        active_key_hash = Some(AuthManager::hash_key(&secret_key));
                        format_register_ok(&secret_key, parsed.format)
                    }
                    Err(err_msg) => format_error(&err_msg, parsed.format),
                };
                writer.write_all(resp.as_bytes())?;
            }

            ClientCommand::Submit {
                secret_key,
                language,
                code,
                stdin,
                timeout_secs,
            } => {
                let key_hash = match auth_manager.verify_submit(&secret_key) {
                    Ok(h) => h,
                    Err(err_msg) => {
                        let resp = format_error(&err_msg, parsed.format);
                        writer.write_all(resp.as_bytes())?;
                        writer.flush()?;
                        continue;
                    }
                };
                active_key_hash = Some(key_hash.clone());

                let timeout = timeout_secs
                    .unwrap_or(config.default_timeout_secs)
                    .min(config.max_timeout_secs);

                let job_id = Uuid::new_v4().to_string();
                let job = Job::new(job_id.clone(), key_hash, language, code, stdin, timeout);

                match scheduler.submit(job.clone()) {
                    Ok(()) => {
                        // Insert into result store as Pending only if admitted
                        result_store.insert_pending(&job);

                        // Return opaque job_id immediately without blocking!
                        let resp = format_submit_ok(&job_id, parsed.format);
                        writer.write_all(resp.as_bytes())?;
                    }
                    Err(err_msg) => {
                        let resp = format_error(&err_msg, parsed.format);
                        writer.write_all(resp.as_bytes())?;
                    }
                }
            }

            ClientCommand::Poll { secret_key, job_id } => {
                let key_hash = match auth_manager.verify_poll(&secret_key) {
                    Ok(h) => h,
                    Err(err_msg) => {
                        let resp = format_error(&err_msg, parsed.format);
                        writer.write_all(resp.as_bytes())?;
                        writer.flush()?;
                        continue;
                    }
                };

                let resp = match result_store.poll_for_client(&job_id, &key_hash) {
                    Ok(Some(result)) => format_poll_ok(&result, parsed.format),
                    Ok(None) => format_error("Job not found or expired", parsed.format),
                    Err(err_msg) => format_error(err_msg, parsed.format),
                };
                writer.write_all(resp.as_bytes())?;
            }

            ClientCommand::Status { secret_key, job_id } => {
                let key_hash = match auth_manager.verify_poll(&secret_key) {
                    Ok(h) => h,
                    Err(err_msg) => {
                        let resp = format_error(&err_msg, parsed.format);
                        writer.write_all(resp.as_bytes())?;
                        writer.flush()?;
                        continue;
                    }
                };

                let resp = match result_store.get(&job_id) {
                    Some(result) => {
                        if result.client_id != key_hash {
                            format_error(
                                "Unauthorized: job belongs to another client",
                                parsed.format,
                            )
                        } else {
                            format_status_ok(&job_id, result.status, parsed.format)
                        }
                    }
                    None => format_error("Job not found or expired", parsed.format),
                };
                writer.write_all(resp.as_bytes())?;
            }

            ClientCommand::Ping => {
                let resp = format_pong(parsed.format);
                writer.write_all(resp.as_bytes())?;
            }

            ClientCommand::Stats => {
                let stats = scheduler.stats();
                let stored_count = result_store.count();
                let resp = format_stats(&stats, stored_count, parsed.format);
                writer.write_all(resp.as_bytes())?;
            }
        }

        writer.flush()?;
    }

    auth_manager.remove_connection(&default_client_id);
    Ok(())
}
