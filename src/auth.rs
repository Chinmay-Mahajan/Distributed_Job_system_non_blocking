use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use redis::AsyncCommands;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config::Config;

const DEFAULT_L1_SHARDS: usize = 32;

/// Per-user session metadata stored in the fast L1 (in-process heap) cache.
#[derive(Debug, Clone)]
pub struct L1Session {
    pub last_active: Instant,
    pub window_start: Instant,
    pub polls_in_window: u32,
    pub violation_strikes: u32,
}

impl L1Session {
    fn new(initial_polls: u32) -> Self {
        let now = Instant::now();
        Self {
            last_active: now,
            window_start: now,
            polls_in_window: initial_polls,
            violation_strikes: 0,
        }
    }
}

/// Fallback L2 entry used only if external Redis is unreachable in isolated unit test environments.
#[derive(Debug, Clone)]
struct FallbackL2Entry {
    expires_at: Instant,
    penalty_until: Option<Instant>,
}

/// Two-tier Authentication, Authorization, and Poll Rate-Limiting Manager.
///
/// - **L1 Cache (In-Process Heap)**: Sharded `RwLock<HashMap<String, L1Session>>` storing SHA-256
///   hashed secret keys (`key_hash`), bounded by `l1_max_capacity` and `l1_ttl`. Tracks per-user
///   polling rates (`polls_in_window`) and strike counts (`violation_strikes`).
/// - **L2 Cache (Redis)**: External Redis instance storing `auth:<key_hash>` (with `l2_redis_ttl_secs`)
///   and `penalty:<key_hash>` (with `l2_penalty_ttl_secs`).
/// - **Penalty Demotion (Option A)**: Users who repeatedly violate `max_polls_per_sec` (`>= max_poll_strikes`)
///   are evicted from L1 and marked with `penalty:<key_hash>` in L2 Redis, preventing L1 re-promotion
///   until their probation TTL expires.
pub struct AuthManager {
    l1_shards: Vec<RwLock<HashMap<String, L1Session>>>,
    shard_capacity: usize,
    l1_ttl: Duration,
    l2_redis_ttl_secs: u64,
    max_polls_per_sec: u32,
    max_poll_strikes: u32,
    l2_penalty_ttl_secs: u64,
    redis_client: Option<redis::Client>,
    async_redis_conn: tokio::sync::RwLock<Option<redis::aio::MultiplexedConnection>>,
    fallback_l2: RwLock<HashMap<String, FallbackL2Entry>>,
}

impl AuthManager {
    pub fn new(config: &Config) -> Self {
        let num_shards = DEFAULT_L1_SHARDS.max(1);
        let shard_capacity = (config.l1_max_capacity / num_shards).max(1);
        let mut l1_shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            l1_shards.push(RwLock::new(HashMap::new()));
        }

        let redis_client = redis::Client::open(config.redis_url.as_str()).ok();

        Self {
            l1_shards,
            shard_capacity,
            l1_ttl: config.l1_ttl,
            l2_redis_ttl_secs: config.l2_redis_ttl_secs,
            max_polls_per_sec: config.max_polls_per_sec,
            max_poll_strikes: config.max_poll_strikes,
            l2_penalty_ttl_secs: config.l2_penalty_ttl_secs,
            redis_client,
            async_redis_conn: tokio::sync::RwLock::new(None),
            fallback_l2: RwLock::new(HashMap::new()),
        }
    }

    /// Deterministically hashes a raw `secret_key` using SHA-256 into a 64-character hex string.
    /// The server never stores the raw `secret_key` in L1 or L2—only its SHA-256 hash.
    pub fn hash_key(secret_key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(secret_key.as_bytes());
        let digest = hasher.finalize();
        let mut hex = String::with_capacity(64);
        for byte in digest {
            use std::fmt::Write;
            let _ = write!(&mut hex, "{:02x}", byte);
        }
        hex
    }

    #[inline]
    fn get_shard(&self, key_hash: &str) -> &RwLock<HashMap<String, L1Session>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key_hash.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % self.l1_shards.len();
        &self.l1_shards[idx]
    }

    /// Inserts or updates a session in the L1 heap cache, enforcing the shard capacity cap.
    fn insert_l1(&self, key_hash: String, initial_polls: u32) {
        let mut shard = self.get_shard(&key_hash).write().unwrap();
        if !shard.contains_key(&key_hash) && shard.len() >= self.shard_capacity {
            // 1. Try evicting an expired L1 entry first
            let ttl = self.l1_ttl;
            let expired_key = shard
                .iter()
                .find(|(_, session)| session.last_active.elapsed() > ttl)
                .map(|(k, _)| k.clone());

            if let Some(k) = expired_key {
                shard.remove(&k);
            } else if let Some(lru_key) = shard
                .iter()
                .min_by_key(|(_, session)| session.last_active)
                .map(|(k, _)| k.clone())
            {
                // 2. Otherwise evict the least-recently-active entry in the shard
                shard.remove(&lru_key);
            }
        }
        shard.insert(key_hash, L1Session::new(initial_polls));
    }

    /// Checks if `key_hash` is currently present and unexpired in L1.
    pub fn is_in_l1(&self, key_hash: &str) -> bool {
        let shard = self.get_shard(key_hash).read().unwrap();
        shard
            .get(key_hash)
            .map(|s| s.last_active.elapsed() <= self.l1_ttl)
            .unwrap_or(false)
    }

    /// Returns total active (unexpired) sessions across all L1 shards.
    pub fn l1_count(&self) -> usize {
        let ttl = self.l1_ttl;
        self.l1_shards
            .iter()
            .map(|s| {
                s.read()
                    .unwrap()
                    .values()
                    .filter(|v| v.last_active.elapsed() <= ttl)
                    .count()
            })
            .sum()
    }

    // =========================================================================
    // SYNCHRONOUS API (for thread-per-connection BrokerServer)
    // =========================================================================

    /// Issues a brand-new `secret_key` (`sk_<uuid>`), stores `SHA-256(secret_key)` in
    /// both L1 (Heap) and L2 (Redis), and returns the raw `secret_key` to the client.
    pub fn register(&self) -> String {
        let secret_key = format!("sk_{}", Uuid::new_v4().simple());
        let key_hash = Self::hash_key(&secret_key);

        // 1. Write to L1 (Heap)
        self.insert_l1(key_hash.clone(), 0);

        // 2. Write to L2 (Redis) with TTL
        self.l2_store_sync(&key_hash);

        secret_key
    }

    /// Verifies `secret_key` for `SUBMIT` requests (L1 -> L2 read-through) and returns
    /// the authenticated `key_hash` (used as the internal `client_id`).
    pub fn verify_submit(&self, secret_key: &str) -> Result<String, String> {
        let trimmed = secret_key.trim();
        if trimmed.is_empty() {
            return Err("Missing secret_key: register first via REGISTER command".to_string());
        }

        let key_hash = Self::hash_key(trimmed);

        // 1. Check L1 Cache (Heap)
        {
            let mut shard = self.get_shard(&key_hash).write().unwrap();
            if let Some(session) = shard.get_mut(&key_hash) {
                if session.last_active.elapsed() <= self.l1_ttl {
                    session.last_active = Instant::now();
                    return Ok(key_hash);
                } else {
                    // Expired in L1
                    shard.remove(&key_hash);
                }
            }
        }

        // 2. L1 Miss -> Check L2 Cache (Redis)
        let (auth_valid, on_penalty) = self.l2_lookup_sync(&key_hash);
        if !auth_valid {
            return Err("Unauthorized: invalid or expired secret_key".to_string());
        }

        // 3. Promote back to L1 only if not serving an L2 penalty probation
        if !on_penalty {
            self.insert_l1(key_hash.clone(), 0);
        }

        Ok(key_hash)
    }

    /// Verifies `secret_key` and enforces per-user poll rate limits for `POLL` and `STATUS`.
    /// Repeated poll quota violations evict the user from L1 to L2 (Redis) penalty probation.
    pub fn verify_poll(&self, secret_key: &str) -> Result<String, String> {
        let trimmed = secret_key.trim();
        if trimmed.is_empty() {
            return Err("Missing secret_key: register first via REGISTER command".to_string());
        }

        let key_hash = Self::hash_key(trimmed);

        // 1. Check L1 Cache (Heap)
        let mut demote_to_l2 = false;
        {
            let mut shard = self.get_shard(&key_hash).write().unwrap();
            if let Some(session) = shard.get_mut(&key_hash) {
                if session.last_active.elapsed() <= self.l1_ttl {
                    let now = Instant::now();
                    session.last_active = now;

                    if now.duration_since(session.window_start) >= Duration::from_secs(1) {
                        session.window_start = now;
                        session.polls_in_window = 1;
                    } else {
                        session.polls_in_window += 1;
                    }

                    if session.polls_in_window > self.max_polls_per_sec {
                        session.violation_strikes += 1;
                        let strikes = session.violation_strikes;

                        if strikes >= self.max_poll_strikes {
                            // Evict from L1 and demote to L2 penalty probation
                            shard.remove(&key_hash);
                            demote_to_l2 = true;
                        } else {
                            return Err(format!(
                                "Poll rate limit exceeded: max {} polls/sec allowed (strike {}/{})",
                                self.max_polls_per_sec, strikes, self.max_poll_strikes
                            ));
                        }
                    } else {
                        return Ok(key_hash);
                    }
                } else {
                    // Expired in L1
                    shard.remove(&key_hash);
                }
            }
        }

        if demote_to_l2 {
            self.l2_set_penalty_sync(&key_hash);
            return Err(format!(
                "Poll rate limit exceeded ({}/s). Repeated violations: evicted from L1 cache to L2 (Redis) penalty probation for {}s",
                self.max_polls_per_sec, self.l2_penalty_ttl_secs
            ));
        }

        // 2. L1 Miss -> Check L2 Cache (Redis)
        let (auth_valid, on_penalty) = self.l2_lookup_sync(&key_hash);
        if !auth_valid {
            return Err("Unauthorized: invalid or expired secret_key".to_string());
        }

        // 3. Promote to L1 only if not on penalty probation
        if !on_penalty {
            self.insert_l1(key_hash.clone(), 1);
        }

        Ok(key_hash)
    }

    fn l2_store_sync(&self, key_hash: &str) {
        let auth_key = format!("auth:{}", key_hash);
        let ttl = self.l2_redis_ttl_secs;

        if let Some(ref client) = self.redis_client
            && let Ok(mut conn) = client.get_connection()
        {
            let res: redis::RedisResult<()> = redis::cmd("SET")
                .arg(&auth_key)
                .arg("1")
                .arg("EX")
                .arg(ttl)
                .query(&mut conn);
            if res.is_ok() {
                return;
            }
        }

        // Fallback in-memory L2 if Redis is unreachable
        let mut fb = self.fallback_l2.write().unwrap();
        fb.insert(
            key_hash.to_string(),
            FallbackL2Entry {
                expires_at: Instant::now() + Duration::from_secs(ttl),
                penalty_until: None,
            },
        );
    }

    fn l2_lookup_sync(&self, key_hash: &str) -> (bool, bool) {
        let auth_key = format!("auth:{}", key_hash);
        let penalty_key = format!("penalty:{}", key_hash);

        if let Some(ref client) = self.redis_client
            && let Ok(mut conn) = client.get_connection()
        {
            let res: redis::RedisResult<(Option<String>, Option<String>)> = redis::cmd("MGET")
                .arg(&auth_key)
                .arg(&penalty_key)
                .query(&mut conn);
            if let Ok((auth_val, penalty_val)) = res {
                return (auth_val.is_some(), penalty_val.is_some());
            }
        }

        // Fallback in-memory L2
        let now = Instant::now();
        let fb = self.fallback_l2.read().unwrap();
        if let Some(entry) = fb.get(key_hash)
            && now <= entry.expires_at
        {
            let on_penalty = entry.penalty_until.map(|t| now <= t).unwrap_or(false);
            return (true, on_penalty);
        }
        (false, false)
    }

    fn l2_set_penalty_sync(&self, key_hash: &str) {
        let penalty_key = format!("penalty:{}", key_hash);
        let penalty_ttl = self.l2_penalty_ttl_secs;

        if let Some(ref client) = self.redis_client
            && let Ok(mut conn) = client.get_connection()
        {
            let res: redis::RedisResult<()> = redis::cmd("SET")
                .arg(&penalty_key)
                .arg("1")
                .arg("EX")
                .arg(penalty_ttl)
                .query(&mut conn);
            if res.is_ok() {
                return;
            }
        }

        let mut fb = self.fallback_l2.write().unwrap();
        if let Some(entry) = fb.get_mut(key_hash) {
            entry.penalty_until = Some(Instant::now() + Duration::from_secs(penalty_ttl));
        }
    }

    // =========================================================================
    // ASYNCHRONOUS API (for Tokio AsyncBrokerServer)
    // =========================================================================

    async fn get_async_conn(&self) -> Option<redis::aio::MultiplexedConnection> {
        {
            let guard = self.async_redis_conn.read().await;
            if let Some(ref conn) = *guard {
                return Some(conn.clone());
            }
        }

        let client = self.redis_client.as_ref()?;
        if let Ok(conn) = client.get_multiplexed_async_connection().await {
            let mut guard = self.async_redis_conn.write().await;
            *guard = Some(conn.clone());
            return Some(conn);
        }
        None
    }

    /// Asynchronously issues a brand-new `secret_key` (`sk_<uuid>`), stores `SHA-256(secret_key)`
    /// in both L1 (Heap) and L2 (Redis), and returns the raw `secret_key`.
    pub async fn register_async(&self) -> String {
        let secret_key = format!("sk_{}", Uuid::new_v4().simple());
        let key_hash = Self::hash_key(&secret_key);

        // 1. Write to L1 (Heap)
        self.insert_l1(key_hash.clone(), 0);

        // 2. Write to L2 (Redis)
        self.l2_store_async(&key_hash).await;

        secret_key
    }

    /// Asynchronously verifies `secret_key` for `SUBMIT` requests (L1 -> L2 read-through).
    pub async fn verify_submit_async(&self, secret_key: &str) -> Result<String, String> {
        let trimmed = secret_key.trim();
        if trimmed.is_empty() {
            return Err("Missing secret_key: register first via REGISTER command".to_string());
        }

        let key_hash = Self::hash_key(trimmed);

        // 1. Check L1 Cache (Heap)
        {
            let mut shard = self.get_shard(&key_hash).write().unwrap();
            if let Some(session) = shard.get_mut(&key_hash) {
                if session.last_active.elapsed() <= self.l1_ttl {
                    session.last_active = Instant::now();
                    return Ok(key_hash);
                } else {
                    shard.remove(&key_hash);
                }
            }
        }

        // 2. L1 Miss -> Check L2 Cache (Redis)
        let (auth_valid, on_penalty) = self.l2_lookup_async(&key_hash).await;
        if !auth_valid {
            return Err("Unauthorized: invalid or expired secret_key".to_string());
        }

        // 3. Promote back to L1 only if not on penalty probation
        if !on_penalty {
            self.insert_l1(key_hash.clone(), 0);
        }

        Ok(key_hash)
    }

    /// Asynchronously verifies `secret_key` and enforces per-user poll rate limits for `POLL` and `STATUS`.
    pub async fn verify_poll_async(&self, secret_key: &str) -> Result<String, String> {
        let trimmed = secret_key.trim();
        if trimmed.is_empty() {
            return Err("Missing secret_key: register first via REGISTER command".to_string());
        }

        let key_hash = Self::hash_key(trimmed);

        // 1. Check L1 Cache (Heap)
        let mut demote_to_l2 = false;
        {
            let mut shard = self.get_shard(&key_hash).write().unwrap();
            if let Some(session) = shard.get_mut(&key_hash) {
                if session.last_active.elapsed() <= self.l1_ttl {
                    let now = Instant::now();
                    session.last_active = now;

                    if now.duration_since(session.window_start) >= Duration::from_secs(1) {
                        session.window_start = now;
                        session.polls_in_window = 1;
                    } else {
                        session.polls_in_window += 1;
                    }

                    if session.polls_in_window > self.max_polls_per_sec {
                        session.violation_strikes += 1;
                        let strikes = session.violation_strikes;

                        if strikes >= self.max_poll_strikes {
                            shard.remove(&key_hash);
                            demote_to_l2 = true;
                        } else {
                            return Err(format!(
                                "Poll rate limit exceeded: max {} polls/sec allowed (strike {}/{})",
                                self.max_polls_per_sec, strikes, self.max_poll_strikes
                            ));
                        }
                    } else {
                        return Ok(key_hash);
                    }
                } else {
                    shard.remove(&key_hash);
                }
            }
        }

        if demote_to_l2 {
            self.l2_set_penalty_async(&key_hash).await;
            return Err(format!(
                "Poll rate limit exceeded ({}/s). Repeated violations: evicted from L1 cache to L2 (Redis) penalty probation for {}s",
                self.max_polls_per_sec, self.l2_penalty_ttl_secs
            ));
        }

        // 2. L1 Miss -> Check L2 Cache (Redis)
        let (auth_valid, on_penalty) = self.l2_lookup_async(&key_hash).await;
        if !auth_valid {
            return Err("Unauthorized: invalid or expired secret_key".to_string());
        }

        // 3. Promote to L1 only if not on penalty probation
        if !on_penalty {
            self.insert_l1(key_hash.clone(), 1);
        }

        Ok(key_hash)
    }

    async fn l2_store_async(&self, key_hash: &str) {
        let auth_key = format!("auth:{}", key_hash);
        let ttl = self.l2_redis_ttl_secs;

        if let Some(mut conn) = self.get_async_conn().await {
            let res: redis::RedisResult<()> = conn.set_ex(&auth_key, "1", ttl).await;
            if res.is_ok() {
                return;
            }
        }

        let mut fb = self.fallback_l2.write().unwrap();
        fb.insert(
            key_hash.to_string(),
            FallbackL2Entry {
                expires_at: Instant::now() + Duration::from_secs(ttl),
                penalty_until: None,
            },
        );
    }

    async fn l2_lookup_async(&self, key_hash: &str) -> (bool, bool) {
        let auth_key = format!("auth:{}", key_hash);
        let penalty_key = format!("penalty:{}", key_hash);

        if let Some(mut conn) = self.get_async_conn().await {
            let res: redis::RedisResult<(Option<String>, Option<String>)> = redis::cmd("MGET")
                .arg(&auth_key)
                .arg(&penalty_key)
                .query_async(&mut conn)
                .await;
            if let Ok((auth_val, penalty_val)) = res {
                return (auth_val.is_some(), penalty_val.is_some());
            }
        }

        let now = Instant::now();
        let fb = self.fallback_l2.read().unwrap();
        if let Some(entry) = fb.get(key_hash)
            && now <= entry.expires_at
        {
            let on_penalty = entry.penalty_until.map(|t| now <= t).unwrap_or(false);
            return (true, on_penalty);
        }
        (false, false)
    }

    async fn l2_set_penalty_async(&self, key_hash: &str) {
        let penalty_key = format!("penalty:{}", key_hash);
        let penalty_ttl = self.l2_penalty_ttl_secs;

        if let Some(mut conn) = self.get_async_conn().await {
            let res: redis::RedisResult<()> = conn.set_ex(&penalty_key, "1", penalty_ttl).await;
            if res.is_ok() {
                return;
            }
        }

        let mut fb = self.fallback_l2.write().unwrap();
        if let Some(entry) = fb.get_mut(key_hash) {
            entry.penalty_until = Some(Instant::now() + Duration::from_secs(penalty_ttl));
        }
    }
}

pub type SharedAuthManager = Arc<AuthManager>;
