use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::env;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::PathBuf;
use std::process;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use dotenvy::dotenv;

use chrono::{DateTime, Local, Timelike};
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::{SinkExt, StreamExt};
use mongodb::{
    bson::{doc, Document, Bson},
    Client,
    IndexModel,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Number, Value};
use tokio::fs;
use tokio::process::Command;
use tokio::sync::{broadcast, RwLock};
use tokio::time::{sleep, Duration, interval};
use warp::http::StatusCode;
use warp::ws::{Message, WebSocket};
use warp::Filter;

// =============================================================================
// TIPOS GLOBALES
// =============================================================================

type Broadcaster = broadcast::Sender<String>;
type MongoState = Arc<MongoConnectionInfo>;
type RequestCache = Arc<RequestCacheState>;

// =============================================================================
// CONSTANTES DE CONFIGURACIÓN
// =============================================================================

const DEFAULT_PORT: u16 = 3329;
const DEFAULT_REQUEST_CACHE_TTL_SECS: u64 = 5 * 60;
const DEFAULT_DOC_CACHE_TTL_SECS: u64 = 5 * 60;
const DEFAULT_AUTOINDEX_THRESHOLD: u64 = 50;
const DEFAULT_AUTOINDEX_WINDOW_SECS: u64 = 600;
const DEFAULT_AUTOINDEX_DROP_IDLE_SECS: u64 = 86400;
const ENV_BACKUP_DIR: &str = "MICROSERVICEDB_BACKUP_DIR";
const ENV_BACKUP_INTERVAL_MINS: &str = "MICROSERVICEDB_BACKUP_INTERVAL_MINS";
const ENV_DRAGONFLY_URL: &str = "MICROSERVICEDB_DRAGONFLY_URL";
const ENV_LOG_LEVEL: &str = "MICROSERVICEDB_LOG_LEVEL";
const ENV_MONGODB_URI: &str = "MICROSERVICEDB_MONGODB_URI";
const ENV_PORT: &str = "MICROSERVICEDB_PORT";
const ENV_REQUEST_CACHE_TTL_SECS: &str = "REQUEST_CACHE_TTL_SECS";
const ENV_DOC_CACHE_TTL_SECS: &str = "DOC_CACHE_TTL_SECS";
const ENV_SSL_CERT_PATH: &str = "MICROSERVICEDB_SSL_CERT";
const ENV_SSL_KEY_PATH: &str = "MICROSERVICEDB_SSL_KEY";
const REQUEST_CACHE_PREFIX: &str = "microservicedb:request_cache";
const DOC_CACHE_PREFIX: &str = "microservicedb:doc";
const INDEX_PREFIX: &str = "microservicedb:idx";
const INDEX_STATS_PREFIX: &str = "microservicedb:idx_stats";

const BOOTSTRAP_COLLECTION: &str = "__mdb_placeholder_empty__";

// =============================================================================
// LOGGING
// =============================================================================

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum LogLevel {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

struct LogConfig {
    level: LogLevel,
}

static LOG_CONFIG: OnceLock<LogConfig> = OnceLock::new();

fn parse_log_level(raw: &str) -> Option<LogLevel> {
    match raw.trim().to_lowercase().as_str() {
        "error" => Some(LogLevel::Error),
        "warn" | "warning" => Some(LogLevel::Warn),
        "info" => Some(LogLevel::Info),
        "debug" => Some(LogLevel::Debug),
        "trace" => Some(LogLevel::Trace),
        _ => None,
    }
}

fn get_log_config() -> &'static LogConfig {
    LOG_CONFIG.get_or_init(|| {
        let level = env::var(ENV_LOG_LEVEL)
            .ok()
            .and_then(|s| parse_log_level(&s))
            .unwrap_or(LogLevel::Info);
        LogConfig { level }
    })
}

fn log_enabled(level: LogLevel) -> bool {
    level <= get_log_config().level
}

fn log_line(level: LogLevel, message: &str, fields: Option<Value>) {
    if !log_enabled(level) {
        return;
    }
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let level_str = match level {
        LogLevel::Error => "ERROR",
        LogLevel::Warn => "WARN",
        LogLevel::Info => "INFO",
        LogLevel::Debug => "DEBUG",
        LogLevel::Trace => "TRACE",
    };
    let mut obj = Map::new();
    obj.insert("ts".into(), Value::Number(Number::from(ts)));
    obj.insert("level".into(), Value::String(level_str.to_string()));
    obj.insert("msg".into(), Value::String(message.to_string()));
    if let Some(Value::Object(map)) = fields {
        for (k, v) in map {
            obj.insert(k, v);
        }
    } else if let Some(other) = fields {
        obj.insert("data".into(), other);
    }
    let line = Value::Object(obj).to_string();
    match level {
        LogLevel::Error | LogLevel::Warn => eprintln!("{line}"),
        _ => println!("{line}"),
    }
}

// =============================================================================
// UTILIDADES
// =============================================================================

fn truncate_string(value: String, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...[truncated]")
    } else {
        truncated
    }
}

fn preview_json(value: &Value, max_chars: usize) -> Value {
    Value::String(truncate_string(value.to_string(), max_chars))
}

fn preview_text(value: &str, max_chars: usize) -> Value {
    Value::String(truncate_string(value.to_string(), max_chars))
}

fn env_string(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn env_path(name: &str) -> Option<PathBuf> {
    env_string(name).map(PathBuf::from)
}

fn parse_port(raw: &str, source: &str) -> Result<u16, String> {
    raw.parse::<u16>()
        .map_err(|_| format!("Invalid port in {source}: '{raw}'"))
}

fn parse_u64(raw: &str, source: &str) -> Result<u64, String> {
    raw.parse::<u64>()
        .map_err(|_| format!("Invalid u64 in {source}: '{raw}'"))
}

fn parse_bool(raw: &str, source: &str) -> Result<bool, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(format!("Invalid boolean in {source}: '{raw}'")),
    }
}

struct CliArgs {
    port: Option<u16>,
    backup_enabled: bool,
}

fn parse_cli_args() -> Result<CliArgs, String> {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut cli = CliArgs {
        port: None,
        backup_enabled: false,
    };
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--port" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Missing value for --port".to_string())?;
                cli.port = Some(parse_port(value, "--port")?);
            }
            "--backup" => {
                if let Some(value) = args.get(index + 1) {
                    if !value.starts_with("--") {
                        cli.backup_enabled = parse_bool(value, "--backup")?;
                        index += 1;
                    } else {
                        cli.backup_enabled = true;
                    }
                } else {
                    cli.backup_enabled = true;
                }
            }
            value if !value.starts_with("--") && cli.port.is_none() => {
                cli.port = Some(parse_port(value, "CLI argument")?);
            }
            other => return Err(format!("Unknown CLI argument: '{other}'")),
        }
        index += 1;
    }

    Ok(cli)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn json_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        _ => v.to_string(),
    }
}

fn is_reserved_database(name: &str) -> bool {
    matches!(name, "admin" | "config" | "local")
}

fn is_reserved_collection(name: &str) -> bool {
    name == BOOTSTRAP_COLLECTION || name.starts_with("system.")
}

fn value_to_document(value: &Value) -> Result<Document, String> {
    serde_json::from_value::<Document>(value.clone())
        .map_err(|e| format!("Invalid JSON document for MongoDB: {e}"))
}

fn document_to_value(document: Document) -> Result<Value, String> {
    serde_json::to_value(&document)
        .map_err(|e| format!("Could not serialize MongoDB document: {e}"))
}

fn cache_key_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() * 2);
    for byte in raw.as_bytes() {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

fn get_local_ip() -> String {
    local_ip_address::local_ip()
        .map(|ip| ip.to_string())
        .unwrap_or_else(|_| "127.0.0.1".to_string())
}

// =============================================================================
// CONFIGURACIÓN DE LA APLICACIÓN
// =============================================================================

#[derive(Clone)]
struct AppConfig {
    port: u16,
    mongodb_uri: Option<String>,
    dragonfly_url: Option<String>,
    backup_enabled: bool,
    request_cache_ttl_secs: u64,
    doc_cache_ttl_secs: u64,
    backup_dir: Option<PathBuf>,
    backup_interval_mins: Option<u64>,
    autoindex_threshold: u64,
    autoindex_window_secs: u64,
    autoindex_drop_idle_secs: u64,
}

fn load_app_config() -> Result<AppConfig, String> {
    let cli = parse_cli_args()?;
    let port = match cli.port {
        Some(value) => value,
        None => match env_string(ENV_PORT) {
            Some(value) => parse_port(&value, ENV_PORT)?,
            None => DEFAULT_PORT,
        },
    };

    let requested_request_cache_ttl_secs = match env_string(ENV_REQUEST_CACHE_TTL_SECS) {
        Some(value) => parse_u64(&value, ENV_REQUEST_CACHE_TTL_SECS)?,
        None => DEFAULT_REQUEST_CACHE_TTL_SECS,
    };
    let request_cache_ttl_secs =
        requested_request_cache_ttl_secs.min(DEFAULT_REQUEST_CACHE_TTL_SECS);

    let doc_cache_ttl_secs = match env_string(ENV_DOC_CACHE_TTL_SECS) {
        Some(value) => parse_u64(&value, ENV_DOC_CACHE_TTL_SECS)?,
        None => DEFAULT_DOC_CACHE_TTL_SECS,
    };

    let autoindex_threshold = env_string("MICROSERVICEDB_AUTOINDEX_THRESHOLD")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_AUTOINDEX_THRESHOLD);
    let autoindex_window_secs = env_string("MICROSERVICEDB_AUTOINDEX_WINDOW_SECS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_AUTOINDEX_WINDOW_SECS);
    let autoindex_drop_idle_secs = env_string("MICROSERVICEDB_AUTOINDEX_DROP_IDLE_SECS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_AUTOINDEX_DROP_IDLE_SECS);

    let backup_interval_mins = match env_string(ENV_BACKUP_INTERVAL_MINS) {
        Some(value) => {
            let parsed = parse_u64(&value, ENV_BACKUP_INTERVAL_MINS)?;
            if parsed == 0 {
                return Err(format!("{ENV_BACKUP_INTERVAL_MINS} must be greater than 0"));
            }
            Some(parsed)
        }
        None => None,
    };

    let mongodb_uri = env_string(ENV_MONGODB_URI);
    let dragonfly_url = env_string(ENV_DRAGONFLY_URL);
    let backup_dir = env_path(ENV_BACKUP_DIR);

    if cli.backup_enabled {
        if backup_dir.is_none() {
            return Err(format!(
                "--backup requires {ENV_BACKUP_DIR} environment variable to be set"
            ));
        }
        if backup_interval_mins.is_none() {
            return Err(format!(
                "--backup requires {ENV_BACKUP_INTERVAL_MINS} environment variable to be set"
            ));
        }
    }

    Ok(AppConfig {
        port,
        mongodb_uri,
        dragonfly_url,
        backup_enabled: cli.backup_enabled,
        request_cache_ttl_secs,
        doc_cache_ttl_secs,
        backup_dir,
        backup_interval_mins,
        autoindex_threshold,
        autoindex_window_secs,
        autoindex_drop_idle_secs,
    })
}

fn validate_tls_paths(
    cert_path: PathBuf,
    key_path: PathBuf,
) -> Result<Option<(String, String)>, String> {
    if !cert_path.exists() {
        return Err(format!(
            "SSL certificate not found at '{}'",
            cert_path.display()
        ));
    }
    if !key_path.exists() {
        return Err(format!("SSL key not found at '{}'", key_path.display()));
    }
    Ok(Some((
        cert_path.to_string_lossy().to_string(),
        key_path.to_string_lossy().to_string(),
    )))
}

fn tls_config() -> Result<Option<(String, String)>, String> {
    match (env_path(ENV_SSL_CERT_PATH), env_path(ENV_SSL_KEY_PATH)) {
        (Some(cert), Some(key)) => validate_tls_paths(cert, key),
        (Some(_), None) | (None, Some(_)) => Err(format!(
            "{ENV_SSL_CERT_PATH} and {ENV_SSL_KEY_PATH} must be set together"
        )),
        (None, None) => Ok(None),
    }
}

// =============================================================================
// MODELOS
// =============================================================================

#[derive(Clone, Serialize, Deserialize, Default)]
struct Database {
    collections: HashMap<String, Vec<Value>>,
}

#[derive(Deserialize, Clone)]
struct WsRequest {
    #[serde(rename = "type")]
    msg_type: String,
    category: Option<String>,
    function_name: Option<String>,
    payload: Option<Value>,
    database_name: Option<String>,
    collections: Option<Vec<String>>,
}

#[derive(Serialize, Clone)]
struct WsResponse {
    success: bool,
    response_json: String,
    message: String,
}

// =============================================================================
// CONEXIONES
// =============================================================================

struct MongoConnectionInfo {
    client: Client,
}

#[derive(Clone)]
struct BackupLayout {
    date_dir: String,
    hour_dir: String,
    slot_dir: String,
    timestamp: String,
}

fn sanitize_file_component(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|ch| match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();

    let trimmed = sanitized.trim().trim_matches('.');
    if trimmed.is_empty() {
        "database".to_string()
    } else {
        trimmed.to_string()
    }
}

fn backup_layout(now: DateTime<Local>) -> BackupLayout {
    let slot_dir = if now.minute() < 30 {
        "backup_00".to_string()
    } else {
        "backup_30".to_string()
    };

    BackupLayout {
        date_dir: now.format("%Y-%m-%d").to_string(),
        hour_dir: now.format("%H").to_string(),
        slot_dir,
        timestamp: now.format("%Y-%m-%d_%H-%M-%S").to_string(),
    }
}

fn time_until_next_backup_slot(now: DateTime<Local>) -> Duration {
    let minute = now.minute();
    let second = now.second();
    let nanos = now.nanosecond();
    let minutes_until_next = if minute < 30 {
        30 - minute
    } else {
        60 - minute
    };

    let mut delay =
        Duration::from_secs((minutes_until_next as u64 * 60).saturating_sub(second as u64));
    if nanos > 0 {
        delay += Duration::from_nanos((1_000_000_000_u32 - nanos) as u64);
    }
    delay
}

async fn try_write_7z_archive(
    temp_json_path: &PathBuf,
    archive_path: &PathBuf,
) -> Result<(), String> {
    let temp_json = temp_json_path.to_string_lossy().to_string();
    let archive = archive_path.to_string_lossy().to_string();

    for executable in ["7z", "7za"] {
        let output = match Command::new(executable)
            .args([
                "a",
                "-t7z",
                "-mx=9",
                "-m0=LZMA2",
                "-mfb=273",
                "-md=64m",
                "-ms=on",
                &archive,
                &temp_json,
            ])
            .output()
            .await
        {
            Ok(output) => output,
            Err(_) => continue,
        };

        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        return Err(format!(
            "{executable} failed: {}",
            if !stderr.is_empty() { stderr } else { stdout }
        ));
    }

    Err("7z executable not found in PATH".to_string())
}

async fn write_gzip_archive(archive_path: PathBuf, payload: Vec<u8>) -> Result<(), String> {
    let gzip_bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, String> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        encoder
            .write_all(&payload)
            .map_err(|e| format!("Failed to write gzip payload: {e}"))?;
        encoder
            .finish()
            .map_err(|e| format!("Failed to finalize gzip payload: {e}"))
    })
    .await
    .map_err(|e| format!("Backup gzip task failed: {e}"))??;

    fs::write(&archive_path, gzip_bytes).await.map_err(|e| {
        format!(
            "Failed to write gzip archive '{}': {e}",
            archive_path.display()
        )
    })
}

async fn write_backup_archive(
    backup_dir: &PathBuf,
    database_name: &str,
    payload: Vec<u8>,
) -> Result<PathBuf, String> {
    let file_stem = sanitize_file_component(database_name);
    let temp_json_path = backup_dir.join(format!("{file_stem}.json"));
    let archive_7z_path = backup_dir.join(format!("{file_stem}.7z"));
    let archive_gz_path = backup_dir.join(format!("{file_stem}.json.gz"));

    fs::write(&temp_json_path, &payload).await.map_err(|e| {
        format!(
            "Failed to write backup temp file '{}': {e}",
            temp_json_path.display()
        )
    })?;

    let _ = fs::remove_file(&archive_7z_path).await;
    let _ = fs::remove_file(&archive_gz_path).await;

    let result = match try_write_7z_archive(&temp_json_path, &archive_7z_path).await {
        Ok(()) => Ok(archive_7z_path),
        Err(error) => {
            log_line(
                LogLevel::Warn,
                "backup_7z_unavailable_fallback_gzip",
                Some(json!({
                    "database_name": database_name,
                    "error": error,
                    "fallback_path": archive_gz_path.display().to_string()
                })),
            );
            write_gzip_archive(archive_gz_path.clone(), payload).await?;
            Ok(archive_gz_path)
        }
    };

    let _ = fs::remove_file(&temp_json_path).await;
    result
}

async fn backup_database_snapshot(
    mongo: &MongoConnectionInfo,
    backup_root: &PathBuf,
    layout: &BackupLayout,
    database_name: &str,
) -> Result<PathBuf, String> {
    let database = load_database(mongo, database_name).await?;
    let collections = database.collections.len();
    let documents: usize = database.collections.values().map(|docs| docs.len()).sum();
    let backup_dir = backup_root
        .join(&layout.date_dir)
        .join(&layout.hour_dir)
        .join(&layout.slot_dir);

    fs::create_dir_all(&backup_dir).await.map_err(|e| {
        format!(
            "Failed to create backup directory '{}': {e}",
            backup_dir.display()
        )
    })?;

    let payload = serde_json::to_vec(&json!({
        "database_name": database_name,
        "created_at_local": layout.timestamp,
        "collections": database.collections,
    }))
    .map_err(|e| format!("Failed to serialize backup payload for '{database_name}': {e}"))?;

    let archive_path = write_backup_archive(&backup_dir, database_name, payload).await?;

    log_line(
        LogLevel::Info,
        "backup_database_written",
        Some(json!({
            "database_name": database_name,
            "collections": collections,
            "documents": documents,
            "archive_path": archive_path.display().to_string()
        })),
    );

    Ok(archive_path)
}

async fn run_backup_cycle(mongo: &MongoConnectionInfo, config: &AppConfig) -> Result<(), String> {
    let databases = list_database_names(mongo).await?;
    let layout = backup_layout(Local::now());
    let backup_root = config
        .backup_dir
        .as_ref()
        .expect("backup_dir validated when backup_enabled=true");
    let target_dir = backup_root
        .join(&layout.date_dir)
        .join(&layout.hour_dir)
        .join(&layout.slot_dir);

    log_line(
        LogLevel::Info,
        "backup_cycle_start",
        Some(json!({
            "database_count": databases.len(),
            "target_dir": target_dir.display().to_string(),
            "timestamp": layout.timestamp
        })),
    );

    if databases.is_empty() {
        log_line(
            LogLevel::Info,
            "backup_cycle_no_databases",
            Some(json!({ "target_dir": target_dir.display().to_string() })),
        );
        return Ok(());
    }

    let mut completed = 0usize;
    let mut failed = 0usize;
    let mut errors = Vec::new();

    for database_name in databases {
        match backup_database_snapshot(mongo, backup_root, &layout, &database_name).await {
            Ok(_) => completed += 1,
            Err(error) => {
                failed += 1;
                errors.push(format!("{database_name}: {error}"));
                log_line(
                    LogLevel::Warn,
                    "backup_database_failed",
                    Some(json!({ "database_name": database_name, "error": error })),
                );
            }
        }
    }

    log_line(
        LogLevel::Info,
        "backup_cycle_done",
        Some(json!({
            "completed": completed,
            "failed": failed,
            "target_dir": target_dir.display().to_string()
        })),
    );

    if failed > 0 {
        Err(format!(
            "Backup cycle completed with failures: {}",
            errors.join(" | ")
        ))
    } else {
        Ok(())
    }
}

async fn backup_agent_loop(mongo: MongoState, config: AppConfig) {
    let backup_dir = config
        .backup_dir
        .as_ref()
        .expect("backup_dir validated when backup_enabled=true");
    let backup_interval_mins = config
        .backup_interval_mins
        .expect("backup_interval_mins validated when backup_enabled=true");
    log_line(
        LogLevel::Info,
        "backup_agent_started",
        Some(json!({
            "backup_dir": backup_dir.display().to_string(),
            "interval_minutes": backup_interval_mins
        })),
    );

    if let Err(error) = run_backup_cycle(&mongo, &config).await {
        log_line(
            LogLevel::Warn,
            "backup_cycle_failed",
            Some(json!({ "error": error })),
        );
    }

    loop {
        let delay = time_until_next_backup_slot(Local::now());
        log_line(
            LogLevel::Debug,
            "backup_agent_sleep_until_next_slot",
            Some(json!({ "sleep_ms": delay.as_millis() as u64 })),
        );
        sleep(delay).await;

        if let Err(error) = run_backup_cycle(&mongo, &config).await {
            log_line(
                LogLevel::Warn,
                "backup_cycle_failed",
                Some(json!({ "error": error })),
            );
        }
    }
}

async fn connect_mongodb(config: &AppConfig) -> Result<MongoConnectionInfo, String> {
    let uri = config
        .mongodb_uri
        .as_deref()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| format!("{ENV_MONGODB_URI} environment variable is required"))?;

    log_line(
        LogLevel::Info,
        "mongodb_connect_start",
        Some(json!({ "uri": uri })),
    );

    let client = Client::with_uri_str(uri)
        .await
        .map_err(|e| format!("Failed to create MongoDB client: {e}"))?;

    client
        .database("admin")
        .run_command(doc! { "ping": 1_i32 })
        .await
        .map_err(|e| format!("Failed to connect to MongoDB: {e}"))?;

    log_line(
        LogLevel::Info,
        "mongodb_connect_ok",
        Some(json!({ "uri": uri })),
    );

    Ok(MongoConnectionInfo { client })
}

// =============================================================================
// REDIS DOCUMENT CACHE — Cache de documentos individuales (NO colecciones completas)
// =============================================================================

#[derive(Clone)]
struct RedisDocumentCache {
    connection: redis::aio::MultiplexedConnection,
    ttl_seconds: u64,
    prefix: String,
}

impl RedisDocumentCache {
    fn doc_key(&self, db: &str, collection: &str, doc_id: &str) -> String {
        format!("{}:{}:{}:{}:{}", self.prefix, cache_key_segment(db), cache_key_segment(collection), cache_key_segment(doc_id))
    }

    /// Obtiene un documento individual por su _id desde Redis.
    async fn get_doc(&self, db: &str, collection: &str, doc_id: &str) -> Option<Value> {
        let mut conn = self.connection.clone();
        let key = self.doc_key(db, collection, doc_id);
        match conn.hgetall::<_, HashMap<String, String>>(&key).await {
            Ok(map) if !map.is_empty() => {
                let mut obj = Map::new();
                for (k, v) in map {
                    if let Ok(val) = serde_json::from_str::<Value>(&v) {
                        obj.insert(k, val);
                    } else {
                        obj.insert(k, Value::String(v));
                    }
                }
                Some(Value::Object(obj))
            }
            _ => None,
        }
    }

    /// Guarda un documento individual en Redis como Hash.
    async fn set_doc(&self, db: &str, collection: &str, doc_id: &str, doc: &Value) {
        let mut conn = self.connection.clone();
        let key = self.doc_key(db, collection, doc_id);
        if let Some(obj) = doc.as_object() {
            let mut pipe = redis::pipe();
            pipe.atomic();
            for (k, v) in obj {
                if let Ok(json_str) = serde_json::to_string(v) {
                    pipe.hset(&key, k, json_str);
                }
            }
            pipe.expire(&key, self.ttl_seconds as i64);
            if let Err(e) = pipe.query_async::<()>(&mut conn).await {
                log_line(LogLevel::Warn, "doc_cache_set_failed", Some(json!({"key": key, "error": e.to_string()})));
            }
        }
    }

    /// Elimina un documento del cache.
    async fn del_doc(&self, db: &str, collection: &str, doc_id: &str) {
        let mut conn = self.connection.clone();
        let key = self.doc_key(db, collection, doc_id);
        let _: Result<(), _> = conn.del(&key).await;
    }

    /// Obtiene múltiples documentos por sus IDs (pipeline optimizado).
    async fn mget_docs(&self, db: &str, collection: &str, doc_ids: &[String]) -> Vec<Option<Value>> {
        if doc_ids.is_empty() {
            return vec![];
        }
        let mut conn = self.connection.clone();
        let mut pipe = redis::pipe();
        for id in doc_ids {
            let key = self.doc_key(db, collection, id);
            pipe.hgetall(&key);
        }
        match pipe.query_async::<Vec<HashMap<String, String>>>(&mut conn).await {
            Ok(results) => results.into_iter().map(|map| {
                if map.is_empty() {
                    None
                } else {
                    let mut obj = Map::new();
                    for (k, v) in map {
                        if let Ok(val) = serde_json::from_str::<Value>(&v) {
                            obj.insert(k, val);
                        } else {
                            obj.insert(k, Value::String(v));
                        }
                    }
                    Some(Value::Object(obj))
                }
            }).collect(),
            Err(e) => {
                log_line(LogLevel::Warn, "doc_cache_mget_failed", Some(json!({"error": e.to_string()})));
                vec![None; doc_ids.len()]
            }
        }
    }

    /// Invalida todos los documentos de una colección.
    async fn invalidate_collection(&self, db: &str, collection: &str) {
        let mut conn = self.connection.clone();
        let pattern = format!("{}:{}:{}:*", self.prefix, cache_key_segment(db), cache_key_segment(collection));
        if let Ok(keys) = redis::cmd("SCAN").arg("0").arg("MATCH").arg(&pattern).arg("COUNT").arg(1000).query_async::<(String, Vec<String>)>(&mut conn).await {
            let mut all_keys = keys.1;
            let mut cursor = keys.0;
            while cursor != "0" {
                if let Ok((next, more)) = redis::cmd("SCAN").arg(&cursor).arg("MATCH").arg(&pattern).arg("COUNT").arg(1000).query_async::<(String, Vec<String>)>(&mut conn).await {
                    all_keys.extend(more);
                    cursor = next;
                } else {
                    break;
                }
            }
            if !all_keys.is_empty() {
                for chunk in all_keys.chunks(500) {
                    let _: Result<(), _> = redis::cmd("DEL").arg(chunk).query_async(&mut conn).await;
                }
            }
        }
    }

    /// Invalida todos los documentos de una base de datos.
    async fn invalidate_database(&self, db: &str) {
        let mut conn = self.connection.clone();
        let pattern = format!("{}:{}:*", self.prefix, cache_key_segment(db));
        if let Ok(keys) = redis::cmd("SCAN").arg("0").arg("MATCH").arg(&pattern).arg("COUNT").arg(1000).query_async::<(String, Vec<String>)>(&mut conn).await {
            let mut all_keys = keys.1;
            let mut cursor = keys.0;
            while cursor != "0" {
                if let Ok((next, more)) = redis::cmd("SCAN").arg(&cursor).arg("MATCH").arg(&pattern).arg("COUNT").arg(1000).query_async::<(String, Vec<String>)>(&mut conn).await {
                    all_keys.extend(more);
                    cursor = next;
                } else {
                    break;
                }
            }
            if !all_keys.is_empty() {
                for chunk in all_keys.chunks(500) {
                    let _: Result<(), _> = redis::cmd("DEL").arg(chunk).query_async(&mut conn).await;
                }
            }
        }
    }
}

// =============================================================================
// REDIS INDEX MANAGER — Índices secundarios en Redis (ZSET, SET, STRING)
// =============================================================================

#[derive(Clone, Debug)]
enum IndexType {
    Numeric,   // Sorted Set: score = valor numérico
    Tag,       // Set: SADD idx:field:tag:valor docId
    String,    // String: SET idx:field:string:valor docId (único)
    Compound,  // Sorted Set: score = hash de múltiples campos
}

#[derive(Clone)]
struct RedisIndexManager {
    connection: redis::aio::MultiplexedConnection,
    prefix: String,
}

impl RedisIndexManager {
    fn idx_key_numeric(&self, db: &str, collection: &str, field: &str) -> String {
        format!("{}:{}:{}:{}:numeric", self.prefix, cache_key_segment(db), cache_key_segment(collection), cache_key_segment(field))
    }
    fn idx_key_tag(&self, db: &str, collection: &str, field: &str, value: &str) -> String {
        format!("{}:{}:{}:{}:tag:{}", self.prefix, cache_key_segment(db), cache_key_segment(collection), cache_key_segment(field), cache_key_segment(value))
    }
    fn idx_key_string(&self, db: &str, collection: &str, field: &str, value: &str) -> String {
        format!("{}:{}:{}:{}:string:{}", self.prefix, cache_key_segment(db), cache_key_segment(collection), cache_key_segment(field), cache_key_segment(value))
    }
    fn idx_key_compound(&self, db: &str, collection: &str, fields: &[String]) -> String {
        let fields_hash = fields.join(":");
        format!("{}:{}:{}:{}:compound", self.prefix, cache_key_segment(db), cache_key_segment(collection), cache_key_segment(&fields_hash))
    }

    /// Crea/actualiza los índices de un documento en Redis.
    async fn index_document(&self, db: &str, collection: &str, doc: &Value, index_defs: &[(String, IndexType)]) {
        let mut conn = self.connection.clone();
        let doc_id = match doc.get("_id") {
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
            None => return,
        };

        let mut pipe = redis::pipe();
        pipe.atomic();

        for (field, idx_type) in index_defs {
            if let Some(val) = get_nested_value(doc, field) {
                match idx_type {
                    IndexType::Numeric => {
                        if let Some(num) = val.as_f64() {
                            let key = self.idx_key_numeric(db, collection, field);
                            pipe.zadd(&key, &doc_id, num);
                        }
                    }
                    IndexType::Tag => {
                        let values: Vec<String> = if let Some(arr) = val.as_array() {
                            arr.iter().map(json_to_string).collect()
                        } else {
                            vec![json_to_string(&val)]
                        };
                        for v in values {
                            let key = self.idx_key_tag(db, collection, field, &v);
                            pipe.sadd(&key, &doc_id);
                        }
                    }
                    IndexType::String => {
                        let s = json_to_string(&val);
                        let key = self.idx_key_string(db, collection, field, &s);
                        pipe.set(&key, &doc_id);
                    }
                    IndexType::Compound => {
                        // Compound usa hash de los valores como score
                        let score = Self::hash_value(&val);
                        let key = self.idx_key_compound(db, collection, &[field.clone()]);
                        pipe.zadd(&key, &doc_id, score);
                    }
                }
            }
        }

        if let Err(e) = pipe.query_async::<()>(&mut conn).await {
            log_line(LogLevel::Warn, "index_doc_failed", Some(json!({"error": e.to_string()})));
        }
    }

    /// Elimina un documento de todos los índices.
    async fn unindex_document(&self, db: &str, collection: &str, doc: &Value, index_defs: &[(String, IndexType)]) {
        let mut conn = self.connection.clone();
        let doc_id = match doc.get("_id") {
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
            None => return,
        };

        let mut pipe = redis::pipe();
        pipe.atomic();

        for (field, idx_type) in index_defs {
            if let Some(val) = get_nested_value(doc, field) {
                match idx_type {
                    IndexType::Numeric => {
                        let key = self.idx_key_numeric(db, collection, field);
                        pipe.zrem(&key, &doc_id);
                    }
                    IndexType::Tag => {
                        let values: Vec<String> = if let Some(arr) = val.as_array() {
                            arr.iter().map(json_to_string).collect()
                        } else {
                            vec![json_to_string(&val)]
                        };
                        for v in values {
                            let key = self.idx_key_tag(db, collection, field, &v);
                            pipe.srem(&key, &doc_id);
                        }
                    }
                    IndexType::String => {
                        let s = json_to_string(&val);
                        let key = self.idx_key_string(db, collection, field, &s);
                        pipe.del(&key);
                    }
                    IndexType::Compound => {
                        let key = self.idx_key_compound(db, collection, &[field.clone()]);
                        pipe.zrem(&key, &doc_id);
                    }
                }
            }
        }

        let _: Result<(), _> = pipe.query_async(&mut conn).await;
    }

    /// Query por rango numérico. Retorna IDs.
    async fn query_numeric_range(&self, db: &str, collection: &str, field: &str, min: f64, max: f64) -> Vec<String> {
        let mut conn = self.connection.clone();
        let key = self.idx_key_numeric(db, collection, field);
        match conn.zrangebyscore(&key, min, max).await {
            Ok(ids) => ids,
            Err(e) => {
                log_line(LogLevel::Warn, "index_query_numeric_failed", Some(json!({"error": e.to_string()})));
                vec![]
            }
        }
    }

    /// Query por tag exacto. Retorna IDs.
    async fn query_tag(&self, db: &str, collection: &str, field: &str, value: &str) -> Vec<String> {
        let mut conn = self.connection.clone();
        let key = self.idx_key_tag(db, collection, field, value);
        match conn.smembers(&key).await {
            Ok(ids) => ids,
            Err(e) => {
                log_line(LogLevel::Warn, "index_query_tag_failed", Some(json!({"error": e.to_string()})));
                vec![]
            }
        }
    }

    /// Query por string exacto. Retorna un ID.
    async fn query_string(&self, db: &str, collection: &str, field: &str, value: &str) -> Option<String> {
        let mut conn = self.connection.clone();
        let key = self.idx_key_string(db, collection, field, value);
        match conn.get(&key).await {
            Ok(id) => Some(id),
            Err(_) => None,
        }
    }

    /// Query por múltiples tags (AND lógico con SINTER).
    async fn query_tags_and(&self, db: &str, collection: &str, field: &str, values: &[String]) -> Vec<String> {
        if values.is_empty() {
            return vec![];
        }
        let mut conn = self.connection.clone();
        let keys: Vec<String> = values.iter()
            .map(|v| self.idx_key_tag(db, collection, field, v))
            .collect();
        match redis::cmd("SINTER").arg(&keys).query_async::<Vec<String>>(&mut conn).await {
            Ok(ids) => ids,
            Err(e) => {
                log_line(LogLevel::Warn, "index_query_tags_and_failed", Some(json!({"error": e.to_string()})));
                vec![]
            }
        }
    }

    /// Elimina todos los índices de una colección.
    async fn drop_collection_indexes(&self, db: &str, collection: &str) {
        let mut conn = self.connection.clone();
        let pattern = format!("{}:{}:{}:*", self.prefix, cache_key_segment(db), cache_key_segment(collection));
        if let Ok((_, keys)) = redis::cmd("SCAN").arg("0").arg("MATCH").arg(&pattern).arg("COUNT").arg(1000).query_async::<(String, Vec<String>)>(&mut conn).await {
            if !keys.is_empty() {
                for chunk in keys.chunks(500) {
                    let _: Result<(), _> = redis::cmd("DEL").arg(chunk).query_async(&mut conn).await;
                }
            }
        }
    }

    fn hash_value(val: &Value) -> f64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        val.to_string().hash(&mut hasher);
        let hash = hasher.finish();
        (hash as f64) / (u64::MAX as f64)
    }
}

// =============================================================================
// QUERY PLAN ANALYZER — Decide si una query puede usar índices Redis
// =============================================================================

#[derive(Clone, Debug)]
struct QueryPlan {
    can_use_redis: bool,
    index_strategy: IndexStrategy,
    fallback_to_mongo: bool,
}

#[derive(Clone, Debug)]
enum IndexStrategy {
    PrimaryKeyLookup { id: String },
    NumericRange { field: String, min: f64, max: f64 },
    TagExact { field: String, value: String },
    StringExact { field: String, value: String },
    TagsAnd { field: String, values: Vec<String> },
    Composite(Vec<IndexStrategy>),
    None,
}

struct QueryPlanAnalyzer;

impl QueryPlanAnalyzer {
    /// Analiza un filter JSON y determina si puede resolverse con índices Redis.
    fn analyze(filter: &Value) -> QueryPlan {
        let Some(obj) = filter.as_object() else {
            return QueryPlan { can_use_redis: false, index_strategy: IndexStrategy::None, fallback_to_mongo: true };
        };

        // Caso 1: lookup por _id exacto
        if let Some(id_val) = obj.get("_id") {
            if !is_operator_object(id_val) {
                let id = json_to_string(id_val);
                return QueryPlan {
                    can_use_redis: true,
                    index_strategy: IndexStrategy::PrimaryKeyLookup { id },
                    fallback_to_mongo: false,
                };
            }
        }

        // Caso 2: campo único con operador $eq o valor directo
        if obj.len() == 1 {
            for (k, v) in obj {
                if k.starts_with('$') {
                    continue;
                }
                if let Some(strategy) = Self::analyze_field_condition(k, v) {
                    return QueryPlan {
                        can_use_redis: true,
                        index_strategy: strategy,
                        fallback_to_mongo: false,
                    };
                }
            }
        }

        // Caso 3: múltiples condiciones $and que podrían intersectarse
        if let Some(and_arr) = obj.get("$and").and_then(|v| v.as_array()) {
            let mut strategies = Vec::new();
            for cond in and_arr {
                if let Some(cond_obj) = cond.as_object() {
                    for (k, v) in cond_obj {
                        if let Some(s) = Self::analyze_field_condition(k, v) {
                            strategies.push(s);
                        }
                    }
                }
            }
            if !strategies.is_empty() {
                return QueryPlan {
                    can_use_redis: true,
                    index_strategy: IndexStrategy::Composite(strategies),
                    fallback_to_mongo: false,
                };
            }
        }

        QueryPlan {
            can_use_redis: false,
            index_strategy: IndexStrategy::None,
            fallback_to_mongo: true,
        }
    }

    fn analyze_field_condition(field: &str, value: &Value) -> Option<IndexStrategy> {
        if field.starts_with('$') || field == "_id" {
            return None;
        }

        // Valor directo (implica $eq)
        if !is_operator_object(value) {
            if let Some(num) = value.as_f64() {
                return Some(IndexStrategy::NumericRange {
                    field: field.to_string(),
                    min: num,
                    max: num,
                });
            }
            return Some(IndexStrategy::TagExact {
                field: field.to_string(),
                value: json_to_string(value),
            });
        }

        // Operador object
        if let Some(op_obj) = value.as_object() {
            for (op, op_val) in op_obj {
                match op.as_str() {
                    "$eq" => {
                        if let Some(num) = op_val.as_f64() {
                            return Some(IndexStrategy::NumericRange {
                                field: field.to_string(),
                                min: num,
                                max: num,
                            });
                        }
                        return Some(IndexStrategy::TagExact {
                            field: field.to_string(),
                            value: json_to_string(op_val),
                        });
                    }
                    "$gt" => {
                        if let Some(min) = op_val.as_f64() {
                            return Some(IndexStrategy::NumericRange {
                                field: field.to_string(),
                                min: min + f64::EPSILON,
                                max: f64::INFINITY,
                            });
                        }
                    }
                    "$gte" => {
                        if let Some(min) = op_val.as_f64() {
                            return Some(IndexStrategy::NumericRange {
                                field: field.to_string(),
                                min,
                                max: f64::INFINITY,
                            });
                        }
                    }
                    "$lt" => {
                        if let Some(max) = op_val.as_f64() {
                            return Some(IndexStrategy::NumericRange {
                                field: field.to_string(),
                                min: f64::NEG_INFINITY,
                                max: max - f64::EPSILON,
                            });
                        }
                    }
                    "$lte" => {
                        if let Some(max) = op_val.as_f64() {
                            return Some(IndexStrategy::NumericRange {
                                field: field.to_string(),
                                min: f64::NEG_INFINITY,
                                max,
                            });
                        }
                    }
                    "$in" => {
                        if let Some(arr) = op_val.as_array() {
                            let values: Vec<String> = arr.iter().map(json_to_string).collect();
                            return Some(IndexStrategy::TagsAnd {
                                field: field.to_string(),
                                values,
                            });
                        }
                    }
                    _ => {}
                }
            }
        }

        None
    }

    /// Extrae los campos que podrían beneficiarse de un índice.
    fn extract_indexable_fields(filter: &Value) -> Vec<String> {
        let mut fields = Vec::new();
        let Some(obj) = filter.as_object() else { return fields; };

        for (k, v) in obj {
            match k.as_str() {
                "$and" | "$or" | "$nor" => {
                    if let Some(arr) = v.as_array() {
                        for item in arr {
                            fields.extend(Self::extract_indexable_fields(item));
                        }
                    }
                }
                "$not" => {
                    fields.extend(Self::extract_indexable_fields(v));
                }
                _ if !k.starts_with('$') => {
                    fields.push(k.clone());
                }
                _ => {}
            }
        }
        fields
    }
}

// =============================================================================
// AUTO INDEX MANAGER — Autocrea y autodestruye índices basado en patrones de uso
// =============================================================================

#[derive(Clone)]
struct AutoIndexManager {
    connection: redis::aio::MultiplexedConnection,
    threshold: u64,
    window_secs: u64,
    drop_idle_secs: u64,
    prefix: String,
}

impl AutoIndexManager {
    fn stats_key(&self, db: &str, collection: &str, field: &str) -> String {
        format!("{}:{}:{}:{}:{}", self.prefix, cache_key_segment(db), cache_key_segment(collection), cache_key_segment(field), "queries")
    }

    fn last_used_key(&self, db: &str, collection: &str, field: &str) -> String {
        format!("{}:{}:{}:{}:{}", self.prefix, cache_key_segment(db), cache_key_segment(collection), cache_key_segment(field), "last_used")
    }

    fn index_exists_key(&self, db: &str, collection: &str, field: &str) -> String {
        format!("{}:{}:{}:{}:{}", self.prefix, cache_key_segment(db), cache_key_segment(collection), cache_key_segment(field), "exists")
    }

    /// Registra que un campo fue usado en una query (para análisis de patrones).
    async fn record_query_field(&self, db: &str, collection: &str, field: &str) {
        let mut conn = self.connection.clone();
        let stats_key = self.stats_key(db, collection, field);
        let last_key = self.last_used_key(db, collection, field);
        let now = now_ms() / 1000;

        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.incr(&stats_key, 1);
        pipe.expire(&stats_key, self.window_secs as i64);
        pipe.set(&last_key, now.to_string());
        pipe.expire(&last_key, self.drop_idle_secs as i64);
        let _: Result<(), _> = pipe.query_async(&mut conn).await;
    }

    /// Verifica si algún campo ha superado el umbral y necesita un índice.
    async fn check_and_create_indexes(
        &self,
        mongo: &MongoConnectionInfo,
        index_mgr: &RedisIndexManager,
        db: &str,
        collection: &str,
    ) {
        let mut conn = self.connection.clone();
        let pattern = format!("{}:{}:{}:*:queries", self.prefix, cache_key_segment(db), cache_key_segment(collection));

        let mut cursor = "0".to_string();
        let mut to_create = Vec::new();

        loop {
            let (next, keys): (String, Vec<String>) = match redis::cmd("SCAN")
                .arg(&cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await
            {
                Ok(r) => r,
                Err(_) => break,
            };

            for key in keys {
                if let Ok(count) = conn.get::<_, u64>(&key).await {
                    if count >= self.threshold {
                        // Extraer field del key
                        let parts: Vec<&str> = key.split(':').collect();
                        if parts.len() >= 5 {
                            let field_hex = parts[parts.len() - 2];
                            // Decodificar hex a string (simplificado)
                            if let Ok(field_bytes) = (0..field_hex.len())
                                .step_by(2)
                                .map(|i| u8::from_str_radix(&field_hex[i..i+2], 16))
                                .collect::<Result<Vec<u8>, _>>()
                            {
                                if let Ok(field) = String::from_utf8(field_bytes) {
                                    let exists_key = self.index_exists_key(db, collection, &field);
                                    if let Ok(exists) = conn.exists::<_, bool>(&exists_key).await {
                                        if !exists {
                                            to_create.push(field);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if next == "0" {
                break;
            }
            cursor = next;
        }

        for field in to_create {
            self.create_index(mongo, index_mgr, db, collection, &field).await;
        }
    }

    /// Crea un índice tanto en MongoDB como en Redis.
    async fn create_index(
        &self,
        mongo: &MongoConnectionInfo,
        index_mgr: &RedisIndexManager,
        db: &str,
        collection: &str,
        field: &str,
    ) {
        log_line(
            LogLevel::Info,
            "autoindex_create",
            Some(json!({
                "database_name": db,
                "collection": collection,
                "field": field
            })),
        );

        // 1. Crear índice en MongoDB (background)
        let mongo_db = mongo.client.database(db);
        let idx_model = IndexModel::builder()
            .keys(doc! { field: 1 })
            .build();
        if let Err(e) = mongo_db.collection::<Document>(collection).create_index(idx_model).await {
            log_line(LogLevel::Warn, "autoindex_mongo_failed", Some(json!({"error": e.to_string()})));
        }

        // 2. Marcar como existente en Redis
        let mut conn = self.connection.clone();
        let exists_key = self.index_exists_key(db, collection, field);
        let _: Result<(), _> = conn.set(&exists_key, "1").await;

        // 3. Indexar documentos existentes en Redis
        if let Ok(database) = load_database(mongo, db).await {
            if let Some(docs) = database.collections.get(collection) {
                let idx_type = IndexType::Tag; // Default a Tag por flexibilidad
                for doc in docs {
                    index_mgr.index_document(db, collection, doc, &[(field.to_string(), idx_type.clone())]).await;
                }
            }
        }
    }

    /// Elimina índices que no se han usado en el período configurado.
    async fn drop_idle_indexes(
        &self,
        mongo: &MongoConnectionInfo,
        index_mgr: &RedisIndexManager,
        db: &str,
        collection: &str,
    ) {
        let mut conn = self.connection.clone();
        let pattern = format!("{}:{}:{}:*:last_used", self.prefix, cache_key_segment(db), cache_key_segment(collection));
        let now = now_ms() / 1000;
        let mut to_drop = Vec::new();

        let mut cursor = "0".to_string();
        loop {
            let (next, keys): (String, Vec<String>) = match redis::cmd("SCAN")
                .arg(&cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await
            {
                Ok(r) => r,
                Err(_) => break,
            };

            for key in keys {
                if let Ok(last_used_str) = conn.get::<_, String>(&key).await {
                    if let Ok(last_used) = last_used_str.parse::<u64>() {
                        if now.saturating_sub(last_used) > self.drop_idle_secs {
                            let parts: Vec<&str> = key.split(':').collect();
                            if parts.len() >= 5 {
                                let field_hex = parts[parts.len() - 2];
                                if let Ok(field_bytes) = (0..field_hex.len())
                                    .step_by(2)
                                    .map(|i| u8::from_str_radix(&field_hex[i..i+2], 16))
                                    .collect::<Result<Vec<u8>, _>>()
                                {
                                    if let Ok(field) = String::from_utf8(field_bytes) {
                                        to_drop.push(field);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if next == "0" {
                break;
            }
            cursor = next;
        }

        for field in to_drop {
            self.drop_index(mongo, index_mgr, db, collection, &field).await;
        }
    }

    async fn drop_index(
        &self,
        mongo: &MongoConnectionInfo,
        index_mgr: &RedisIndexManager,
        db: &str,
        collection: &str,
        field: &str,
    ) {
        log_line(
            LogLevel::Info,
            "autoindex_drop",
            Some(json!({
                "database_name": db,
                "collection": collection,
                "field": field
            })),
        );

        // 1. Drop en MongoDB
        let mongo_db = mongo.client.database(db);
        let _ = mongo_db.collection::<Document>(collection).drop_index(format!("{}_1", field)).await;

        // 2. Limpiar keys de Redis
        index_mgr.drop_collection_indexes(db, collection).await;

        // 3. Limpiar metadatos
        let mut conn = self.connection.clone();
        let exists_key = self.index_exists_key(db, collection, field);
        let stats_key = self.stats_key(db, collection, field);
        let last_key = self.last_used_key(db, collection, field);
        let _: Result<(), _> = redis::cmd("DEL").arg(&exists_key).arg(&stats_key).arg(&last_key).query_async(&mut conn).await;
    }
}

// =============================================================================
// CACHÉ DE REQUESTS (DRAGONFLY / REDIS) — Mantenido para compatibilidad
// =============================================================================

struct RequestCacheState {
    connection: redis::aio::MultiplexedConnection,
    ttl_seconds: u64,
    entry_prefix: String,
}

impl RequestCacheState {
    async fn build_key(&self, req: &WsRequest) -> Option<String> {
        let scope = cacheable_request_scope(req)?;
        let fingerprint = cacheable_request_fingerprint(req)?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        fingerprint.hash(&mut hasher);
        let hash = hasher.finish();
        Some(format!("{}:{}:{:016x}", self.entry_prefix, scope, hash))
    }

    async fn get_response(&self, cache_key: &str) -> Option<String> {
        let mut conn = self.connection.clone();
        match conn.get::<_, Option<String>>(cache_key).await {
            Ok(value) => value,
            Err(error) => {
                log_line(
                    LogLevel::Warn,
                    "cache_get_failed",
                    Some(json!({ "cache_key": cache_key, "error": error.to_string() })),
                );
                None
            }
        }
    }

    async fn set_response(&self, cache_key: &str, response_json: &str) {
        let mut conn = self.connection.clone();
        if let Err(error) = redis::cmd("SETEX")
            .arg(cache_key)
            .arg(self.ttl_seconds)
            .arg(response_json)
            .query_async::<()>(&mut conn)
            .await
        {
            log_line(
                LogLevel::Warn,
                "cache_set_failed",
                Some(json!({ "cache_key": cache_key, "error": error.to_string() })),
            );
        }
    }

    async fn invalidate_list_databases(&self) {
        let mut conn = self.connection.clone();
        let pattern = format!("{}:scope:list_databases:*", self.entry_prefix);
        match clear_keys(&mut conn, &pattern).await {
            Ok(deleted) => {
                log_line(
                    LogLevel::Info,
                    "cache_invalidate_list_databases",
                    Some(json!({ "deleted_keys": deleted })),
                );
            }
            Err(error) => {
                log_line(
                    LogLevel::Warn,
                    "cache_invalidate_list_databases_failed",
                    Some(json!({ "error": error.to_string() })),
                );
            }
        }
    }

    async fn invalidate_database(&self, db_name: &str) {
        let mut conn = self.connection.clone();
        let pattern = format!("{}:db:{}:*", self.entry_prefix, cache_key_segment(db_name));
        match clear_keys(&mut conn, &pattern).await {
            Ok(deleted) => {
                log_line(
                    LogLevel::Info,
                    "cache_invalidate_db",
                    Some(json!({ "database_name": db_name, "deleted_keys": deleted })),
                );
            }
            Err(error) => {
                log_line(
                    LogLevel::Warn,
                    "cache_invalidate_db_failed",
                    Some(json!({ "database_name": db_name, "error": error.to_string() })),
                );
            }
        }
    }
}

fn cacheable_request_scope(req: &WsRequest) -> Option<String> {
    if !is_cacheable_request(req) {
        return None;
    }

    match req.msg_type.as_str() {
        "metadata" => Some("scope:metadata".to_string()),
        "list_databases" => Some("scope:list_databases".to_string()),
        "list_collections" => {
            let db_name = req.database_name.clone().unwrap_or_default();
            Some(format!(
                "db:{}:list_collections",
                cache_key_segment(&db_name)
            ))
        }
        "action" => {
            let category = req.category.clone().unwrap_or_default();
            let function_name = req.function_name.clone().unwrap_or_default();
            let payload = req.payload.as_ref();
            let db_name = payload
                .and_then(|value| value.get("database_name"))
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .or(req.database_name.as_deref())
                .unwrap_or_default();

            match category.as_str() {
                "collection" => {
                    let collection_name = payload
                        .and_then(|value| value.get("collectionName"))
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    Some(format!(
                        "db:{}:collection:{}:function:{}",
                        cache_key_segment(db_name),
                        cache_key_segment(collection_name),
                        cache_key_segment(&function_name),
                    ))
                }
                "database" => Some(format!(
                    "db:{}:database:function:{}",
                    cache_key_segment(db_name),
                    cache_key_segment(&function_name),
                )),
                _ => None,
            }
        }
        _ => None,
    }
}

async fn clear_keys(
    connection: &mut redis::aio::MultiplexedConnection,
    pattern: &str,
) -> Result<u64, redis::RedisError> {
    let mut cursor = "0".to_string();
    let mut keys_to_delete: Vec<String> = Vec::new();
    loop {
        let (next_cursor, keys): (String, Vec<String>) = redis::cmd("SCAN")
            .arg(&cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(1000_u32)
            .query_async(connection)
            .await?;
        if !keys.is_empty() {
            keys_to_delete.extend(keys);
        }
        if next_cursor == "0" {
            break;
        }
        cursor = next_cursor;
    }

    let mut deleted: u64 = 0;
    for chunk in keys_to_delete.chunks(500) {
        let removed: i64 = redis::cmd("DEL").arg(chunk).query_async(connection).await?;
        if removed > 0 {
            deleted = deleted.saturating_add(removed as u64);
        }
    }

    Ok(deleted)
}

async fn connect_request_cache(config: &AppConfig) -> Option<RequestCache> {
    let url = match config.dragonfly_url.as_deref() {
        Some(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!("Request cache disabled: {ENV_DRAGONFLY_URL} environment variable not set");
            return None;
        }
    };
    log_line(
        LogLevel::Info,
        "cache_connect_start",
        Some(json!({ "url": url })),
    );

    let client = match redis::Client::open(url) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Request cache disabled: invalid URL: {e}");
            return None;
        }
    };

    let mut connection = match client.get_multiplexed_tokio_connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Request cache disabled: connection failed: {e}");
            return None;
        }
    };

    if let Err(e) = redis::cmd("PING")
        .query_async::<String>(&mut connection)
        .await
    {
        eprintln!("Request cache disabled: ping failed: {e}");
        return None;
    }

    let entry_prefix = format!("{REQUEST_CACHE_PREFIX}:entry");
    let clear_pattern = format!("{REQUEST_CACHE_PREFIX}:*");
    let deleted = match clear_keys(&mut connection, &clear_pattern).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Request cache disabled: namespace cleanup failed: {e}");
            return None;
        }
    };

    log_line(
        LogLevel::Info,
        "cache_connect_ok",
        Some(json!({ "ttl_seconds": config.request_cache_ttl_secs, "cleared_keys": deleted })),
    );

    Some(Arc::new(RequestCacheState {
        connection,
        ttl_seconds: config.request_cache_ttl_secs,
        entry_prefix,
    }))
}

// =============================================================================
// LÓGICA DE CACHÉABLES / MUTACIONES
// =============================================================================

fn is_cacheable_collection_function(func: &str) -> bool {
    matches!(func, "find" | "findOne" | "exportCollection")
}

fn is_cacheable_database_function(func: &str) -> bool {
    matches!(func, "exportDatabase")
}

fn is_cacheable_request(req: &WsRequest) -> bool {
    match req.msg_type.as_str() {
        "metadata" | "list_databases" | "list_collections" => true,
        "action" => {
            let category = req.category.as_deref().unwrap_or_default();
            let function_name = req.function_name.as_deref().unwrap_or_default();
            match category {
                "collection" => is_cacheable_collection_function(function_name),
                "database" => is_cacheable_database_function(function_name),
                _ => false,
            }
        }
        _ => false,
    }
}

fn is_mutating_request(req: &WsRequest) -> bool {
    if req.msg_type != "action" {
        return false;
    }
    let category = req.category.as_deref().unwrap_or_default();
    let function_name = req.function_name.as_deref().unwrap_or_default();
    match category {
        "collection" => matches!(
            function_name,
            "insertOne"
                | "updateOne"
                | "replaceOne"
                | "deleteOne"
                | "deleteMany"
                | "updateCollection"
                | "importCollection"
        ),
        "database" => matches!(
            function_name,
            "createDatabase"
                | "deleteDatabase"
                | "renameDatabase"
                | "createCollection"
                | "deleteCollection"
                | "renameCollection"
                | "importDatabase"
        ),
        _ => false,
    }
}

fn response_indicates_mutation(req: &WsRequest, response: &WsResponse) -> bool {
    if !response.success || req.msg_type != "action" {
        return false;
    }
    let category = req.category.as_deref().unwrap_or_default();
    let function_name = req.function_name.as_deref().unwrap_or_default();

    let parsed = serde_json::from_str::<Value>(&response.response_json).ok();
    match category {
        "collection" => match function_name {
            "insertOne" => true,
            "updateOne" | "replaceOne" => {
                let modified = parsed
                    .as_ref()
                    .and_then(|v| v.get("modifiedCount").and_then(Value::as_i64))
                    .unwrap_or(0);
                let upserted = parsed
                    .as_ref()
                    .and_then(|v| v.get("upsertedCount").and_then(Value::as_i64))
                    .unwrap_or(0);
                modified > 0 || upserted > 0
            }
            "deleteOne" | "deleteMany" => {
                parsed
                    .as_ref()
                    .and_then(|v| v.get("deletedCount").and_then(Value::as_i64))
                    .unwrap_or(0)
                    > 0
            }
            "updateCollection" => {
                let inserted = parsed
                    .as_ref()
                    .and_then(|v| v.get("insertedCount").and_then(Value::as_i64))
                    .unwrap_or(0);
                let modified = parsed
                    .as_ref()
                    .and_then(|v| v.get("modifiedCount").and_then(Value::as_i64))
                    .unwrap_or(0);
                let matched = parsed
                    .as_ref()
                    .and_then(|v| v.get("matchedCount").and_then(Value::as_i64))
                    .unwrap_or(0);
                inserted > 0 || modified > 0 || matched > 0
            }
            "importCollection" => true,
            _ => true,
        },
        "database" => match function_name {
            "deleteCollection" | "deleteDatabase" => parsed
                .as_ref()
                .and_then(|v| v.get("deleted").and_then(Value::as_bool))
                .unwrap_or(true),
            "importDatabase" => true,
            _ => true,
        },
        _ => true,
    }
}

fn cacheable_request_fingerprint(req: &WsRequest) -> Option<String> {
    if !is_cacheable_request(req) {
        return None;
    }
    match req.msg_type.as_str() {
        "metadata" => serde_json::to_string(&json!({ "type": "metadata" })).ok(),
        "list_databases" => serde_json::to_string(&json!({ "type": "list_databases" })).ok(),
        "list_collections" => serde_json::to_string(&json!({
            "type": "list_collections",
            "database_name": req.database_name.clone().unwrap_or_default()
        }))
        .ok(),
        "action" => serde_json::to_string(&json!({
            "type": "action",
            "category": req.category.clone().unwrap_or_default(),
            "function_name": req.function_name.clone().unwrap_or_default(),
            "payload": req.payload.clone().unwrap_or(Value::Null)
        }))
        .ok(),
        _ => None,
    }
}

// =============================================================================
// ACCESO A MONGODB (ABSTRACCIÓN)
// =============================================================================

async fn all_database_names(mongo: &MongoConnectionInfo) -> Result<Vec<String>, String> {
    mongo
        .client
        .list_database_names()
        .await
        .map_err(|e| format!("Failed to list MongoDB databases: {e}"))
}

async fn list_database_names(mongo: &MongoConnectionInfo) -> Result<Vec<String>, String> {
    all_database_names(mongo).await.map(|names| {
        names
            .into_iter()
            .filter(|n| !is_reserved_database(n))
            .collect()
    })
}

async fn database_exists(mongo: &MongoConnectionInfo, db_name: &str) -> Result<bool, String> {
    Ok(all_database_names(mongo)
        .await?
        .into_iter()
        .any(|n| n == db_name))
}

async fn list_collection_names(
    mongo: &MongoConnectionInfo,
    db_name: &str,
) -> Result<Vec<String>, String> {
    mongo
        .client
        .database(db_name)
        .list_collection_names()
        .await
        .map(|cols| {
            cols.into_iter()
                .filter(|n| !is_reserved_collection(n))
                .collect()
        })
        .map_err(|e| format!("Failed to list collections of '{db_name}': {e}"))
}

async fn ensure_database_exists(mongo: &MongoConnectionInfo, db_name: &str) -> Result<(), String> {
    let raw_collections = mongo
        .client
        .database(db_name)
        .list_collection_names()
        .await
        .map_err(|e| format!("Could not inspect '{db_name}' in MongoDB: {e}"))?;

    if raw_collections.iter().any(|n| n == BOOTSTRAP_COLLECTION) {
        return Ok(());
    }
    if raw_collections.is_empty() {
        mongo
            .client
            .database(db_name)
            .create_collection(BOOTSTRAP_COLLECTION)
            .await
            .map_err(|e| format!("Could not create database '{db_name}' in MongoDB: {e}"))?;
    }
    Ok(())
}

async fn load_database(mongo: &MongoConnectionInfo, name: &str) -> Result<Database, String> {
    log_line(
        LogLevel::Debug,
        "mongodb_load_start",
        Some(json!({ "database_name": name })),
    );

    if !database_exists(mongo, name).await? {
        return Err(format!("Database '{}' not found", name));
    }

    let mut collections = HashMap::new();
    let mut total_docs = 0usize;

    for collection_name in list_collection_names(mongo, name).await? {
        let mut docs = Vec::new();
        let mut cursor = mongo
            .client
            .database(name)
            .collection::<Document>(&collection_name)
            .find(doc! {})
            .await
            .map_err(|e| format!("Could not read '{name}.{collection_name}' from MongoDB: {e}"))?;

        while let Some(result) = cursor.next().await {
            let document = result.map_err(|e| {
                format!("Could not iterate '{name}.{collection_name}' in MongoDB: {e}")
            })?;
            docs.push(document_to_value(document)?);
        }

        total_docs += docs.len();
        log_line(
            LogLevel::Debug,
            "mongodb_collection_loaded",
            Some(json!({
                "database_name": name,
                "collectionName": collection_name,
                "documents": docs.len()
            })),
        );
        collections.insert(collection_name, docs);
    }

    log_line(
        LogLevel::Info,
        "mongodb_load_done",
        Some(json!({
            "database_name": name,
            "collections": collections.len(),
            "documents": total_docs
        })),
    );

    Ok(Database { collections })
}

async fn save_database(
    mongo: &MongoConnectionInfo,
    db_name: &str,
    database: &Database,
) -> Result<(), String> {
    let mongo_db = mongo.client.database(db_name);

    let existing_collections: Vec<String> = mongo_db
        .list_collection_names()
        .await
        .map(|cols| {
            cols.into_iter()
                .filter(|n| !is_reserved_collection(n))
                .collect()
        })
        .map_err(|e| format!("Could not list collections of '{db_name}': {e}"))?;

    for collection_name in &existing_collections {
        if !database.collections.contains_key(collection_name) {
            mongo_db
                .collection::<Document>(collection_name)
                .drop()
                .await
                .map_err(|e| {
                    format!("Could not drop collection '{db_name}.{collection_name}': {e}")
                })?;
        }
    }

    if database.collections.is_empty() {
        return ensure_database_exists(mongo, db_name).await;
    }

    if existing_collections
        .iter()
        .any(|n| n == BOOTSTRAP_COLLECTION)
    {
        mongo_db
            .collection::<Document>(BOOTSTRAP_COLLECTION)
            .drop()
            .await
            .map_err(|e| format!("Could not clean bootstrap collection of '{db_name}': {e}"))?;
    }

    for (collection_name, docs) in &database.collections {
        if !existing_collections.iter().any(|n| n == collection_name) {
            mongo_db
                .create_collection(collection_name)
                .await
                .map_err(|e| {
                    format!("Could not create collection '{db_name}.{collection_name}': {e}")
                })?;
        }
        let collection = mongo_db.collection::<Document>(collection_name);
        collection.delete_many(doc! {}).await.map_err(|e| {
            format!("Could not clean collection '{db_name}.{collection_name}': {e}")
        })?;
        if !docs.is_empty() {
            let mongo_docs = docs
                .iter()
                .map(value_to_document)
                .collect::<Result<Vec<_>, _>>()?;
            collection.insert_many(mongo_docs).await.map_err(|e| {
                format!("Could not insert documents into '{db_name}.{collection_name}': {e}")
            })?;
        }
    }

    Ok(())
}

// =============================================================================
// QUERY ENGINE (find, filter, sort, projection, update operators)
// =============================================================================

fn get_nested_value(doc: &Value, path: &str) -> Option<Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut cur = doc;
    for (i, p) in parts.iter().enumerate() {
        match cur {
            Value::Object(obj) => {
                cur = obj.get(*p)?;
            }
            Value::Array(arr) => {
                if let Ok(idx) = p.parse::<usize>() {
                    cur = arr.get(idx)?;
                } else {
                    let remaining = parts[i..].join(".");
                    let collected: Vec<Value> = arr
                        .iter()
                        .filter_map(|item| get_nested_value(item, &remaining))
                        .collect();
                    return if collected.is_empty() {
                        None
                    } else {
                        Some(Value::Array(collected))
                    };
                }
            }
            _ => return None,
        }
    }
    Some(cur.clone())
}

fn set_nested_value(doc: &mut Value, path: &str, value: Value) {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() {
        return;
    }
    let mut cur = doc;
    for p in &parts[..parts.len() - 1] {
        match cur {
            Value::Object(ref mut obj) => {
                let next = obj
                    .entry(p.to_string())
                    .or_insert_with(|| Value::Object(Map::new()));
                cur = next;
            }
            Value::Array(ref mut arr) => {
                if let Ok(idx) = p.parse::<usize>() {
                    while arr.len() <= idx {
                        arr.push(Value::Null);
                    }
                    cur = &mut arr[idx];
                } else {
                    return;
                }
            }
            _ => return,
        }
    }
    let last = parts.last().unwrap();
    if let Some(obj) = cur.as_object_mut() {
        obj.insert(last.to_string(), value);
    } else if let Some(arr) = cur.as_array_mut() {
        if let Ok(idx) = last.parse::<usize>() {
            while arr.len() <= idx {
                arr.push(Value::Null);
            }
            arr[idx] = value;
        }
    }
}

fn unset_nested_value(doc: &mut Value, path: &str) {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() {
        return;
    }
    let mut cur = doc;
    for p in &parts[..parts.len() - 1] {
        match cur {
            Value::Object(ref mut obj) => {
                if let Some(next) = obj.get_mut(*p) {
                    cur = next;
                } else {
                    return;
                }
            }
            Value::Array(ref mut arr) => {
                if let Ok(idx) = p.parse::<usize>() {
                    if let Some(next) = arr.get_mut(idx) {
                        cur = next;
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }
            _ => return,
        }
    }
    let last = parts.last().unwrap();
    if let Some(obj) = cur.as_object_mut() {
        obj.remove(*last);
    } else if let Some(arr) = cur.as_array_mut() {
        if let Ok(idx) = last.parse::<usize>() {
            if idx < arr.len() {
                arr.remove(idx);
            }
        }
    }
}

fn parse_object_id_text(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    let inner = if trimmed.starts_with("ObjectId(\"") && trimmed.ends_with("\")") {
        &trimmed[10..trimmed.len().saturating_sub(2)]
    } else if trimmed.starts_with("ObjectId('") && trimmed.ends_with("')") {
        &trimmed[10..trimmed.len().saturating_sub(2)]
    } else {
        trimmed
    };
    let candidate = inner.trim();
    if candidate.len() == 24 && candidate.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Some(candidate.to_ascii_lowercase())
    } else {
        None
    }
}

fn value_as_object_id_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => parse_object_id_text(text),
        Value::Object(obj) => obj
            .get("$oid")
            .and_then(Value::as_str)
            .and_then(parse_object_id_text),
        _ => None,
    }
}

fn value_eq(a: Option<&Value>, b: Option<&Value>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(va), Some(vb)) => {
            if va == vb {
                return true;
            }
            if let (Some(oa), Some(ob)) = (value_as_object_id_text(va), value_as_object_id_text(vb))
            {
                return oa == ob;
            }
            false
        }
        _ => false,
    }
}

fn compare_values(a: Option<&Value>, b: Option<&Value>) -> i32 {
    match (a, b) {
        (None, None) => 0,
        (None, Some(_)) => -1,
        (Some(_), None) => 1,
        (Some(va), Some(vb)) => {
            if let (Some(na), Some(nb)) = (va.as_f64(), vb.as_f64()) {
                return if na < nb {
                    -1
                } else if na > nb {
                    1
                } else {
                    0
                };
            }
            if let (Some(sa), Some(sb)) = (va.as_str(), vb.as_str()) {
                return match sa.cmp(sb) {
                    Ordering::Less => -1,
                    Ordering::Equal => 0,
                    Ordering::Greater => 1,
                };
            }
            let sa = json_to_string(va);
            let sb = json_to_string(vb);
            match sa.cmp(&sb) {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            }
        }
    }
}

fn is_operator_object(v: &Value) -> bool {
    v.as_object()
        .map(|obj| !obj.is_empty() && obj.keys().all(|k| k.starts_with('$')))
        .unwrap_or(false)
}

fn match_value(doc_val: Option<&Value>, query_val: &Value) -> bool {
    if value_eq(doc_val, Some(query_val)) {
        return true;
    }
    match query_val {
        Value::Object(obj) if is_operator_object(query_val) => {
            for (op, op_val) in obj {
                match op.as_str() {
                    "$eq" => {
                        if !value_eq(doc_val, Some(op_val)) {
                            return false;
                        }
                    }
                    "$ne" => {
                        if value_eq(doc_val, Some(op_val)) {
                            return false;
                        }
                    }
                    "$gt" => {
                        if compare_values(doc_val, Some(op_val)) <= 0 {
                            return false;
                        }
                    }
                    "$gte" => {
                        if compare_values(doc_val, Some(op_val)) < 0 {
                            return false;
                        }
                    }
                    "$lt" => {
                        if compare_values(doc_val, Some(op_val)) >= 0 {
                            return false;
                        }
                    }
                    "$lte" => {
                        if compare_values(doc_val, Some(op_val)) > 0 {
                            return false;
                        }
                    }
                    "$in" => {
                        let arr = op_val.as_array();
                        let dv = doc_val;
                        if arr.is_none() || dv.is_none() {
                            return false;
                        }
                        let arr = arr.unwrap();
                        let dv = dv.unwrap();
                        if let Some(darr) = dv.as_array() {
                            if !arr
                                .iter()
                                .any(|v| darr.iter().any(|d| value_eq(Some(d), Some(v))))
                            {
                                return false;
                            }
                        } else {
                            if !arr.iter().any(|v| value_eq(Some(dv), Some(v))) {
                                return false;
                            }
                        }
                    }
                    "$nin" => {
                        let arr = op_val.as_array();
                        let dv = doc_val;
                        if arr.is_none() || dv.is_none() {
                            return false;
                        }
                        let arr = arr.unwrap();
                        let dv = dv.unwrap();
                        if let Some(darr) = dv.as_array() {
                            if arr
                                .iter()
                                .any(|v| darr.iter().any(|d| value_eq(Some(d), Some(v))))
                            {
                                return false;
                            }
                        } else {
                            if arr.iter().any(|v| value_eq(Some(dv), Some(v))) {
                                return false;
                            }
                        }
                    }
                    "$exists" => {
                        let exists = doc_val.is_some() && !matches!(doc_val, Some(Value::Null));
                        if op_val.as_bool().unwrap_or(true) != exists {
                            return false;
                        }
                    }
                    "$regex" => {
                        let Some(dv) = doc_val else {
                            return false;
                        };
                        let s = json_to_string(dv);
                        let pat = json_to_string(op_val);
                        if let Ok(re) = regex::Regex::new(&pat) {
                            if !re.is_match(&s) {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$size" => {
                        let Some(dv) = doc_val else {
                            return false;
                        };
                        let Some(arr) = dv.as_array() else {
                            return false;
                        };
                        if arr.len() != op_val.as_u64().unwrap_or(0) as usize {
                            return false;
                        }
                    }
                    "$all" => {
                        let Some(dv) = doc_val else {
                            return false;
                        };
                        let Some(darr) = dv.as_array() else {
                            return false;
                        };
                        let Some(arr) = op_val.as_array() else {
                            return false;
                        };
                        if !arr
                            .iter()
                            .all(|v| darr.iter().any(|d| value_eq(Some(d), Some(v))))
                        {
                            return false;
                        }
                    }
                    _ => return false,
                }
            }
            true
        }
        _ => value_eq(doc_val, Some(query_val)),
    }
}

fn match_filter(doc: &Value, filter: &Value) -> bool {
    let Some(obj) = filter.as_object() else {
        return true;
    };
    for (k, v) in obj {
        match k.as_str() {
            "$and" => {
                let Some(arr) = v.as_array() else {
                    return false;
                };
                if !arr.iter().all(|f| match_filter(doc, f)) {
                    return false;
                }
            }
            "$or" => {
                let Some(arr) = v.as_array() else {
                    return false;
                };
                if arr.is_empty() {
                    return false;
                }
                if !arr.iter().any(|f| match_filter(doc, f)) {
                    return false;
                }
            }
            "$nor" => {
                let Some(arr) = v.as_array() else {
                    return false;
                };
                if arr.iter().any(|f| match_filter(doc, f)) {
                    return false;
                }
            }
            "$not" => {
                if match_filter(doc, v) {
                    return false;
                }
            }
            _ => {
                let dv = get_nested_value(doc, k);
                if !match_value(dv.as_ref(), v) {
                    return false;
                }
            }
        }
    }
    true
}

fn sort_docs(docs: &mut Vec<Value>, sort: &Value) {
    let Some(obj) = sort.as_object() else {
        return;
    };
    docs.sort_by(|a, b| {
        for (field, dir) in obj {
            let va = get_nested_value(a, field);
            let vb = get_nested_value(b, field);
            let cmp = compare_values(va.as_ref(), vb.as_ref());
            if cmp != 0 {
                let d = dir.as_i64().unwrap_or(1);
                return if d >= 0 {
                    cmp.cmp(&0)
                } else {
                    cmp.cmp(&0).reverse()
                };
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn apply_projection(doc: &Value, projection: &Value) -> Value {
    let Some(obj) = projection.as_object() else {
        return doc.clone();
    };
    let keys: Vec<&String> = obj.keys().collect();

    let has_includes = keys.iter().any(|k| {
        obj.get(*k)
            .map(|v| v.as_bool().unwrap_or(false) || v.as_i64() == Some(1))
            .unwrap_or(false)
    });
    let has_excludes = keys.iter().any(|k| {
        obj.get(*k)
            .map(|v| v.as_bool() == Some(false) || v.as_i64() == Some(0))
            .unwrap_or(false)
    });

    if has_includes && !has_excludes {
        let mut out = Value::Object(Map::new());
        let include_id = obj
            .get("_id")
            .map(|v| v.as_bool().unwrap_or(true) || v.as_i64() == Some(1))
            .unwrap_or(true);
        if include_id {
            if let Some(id) = doc.get("_id") {
                set_nested_value(&mut out, "_id", id.clone());
            }
        }
        for k in &keys {
            if *k == "_id" {
                continue;
            }
            if obj
                .get(*k)
                .map(|v| v.as_bool().unwrap_or(false) || v.as_i64() == Some(1))
                .unwrap_or(false)
            {
                if let Some(v) = get_nested_value(doc, k) {
                    set_nested_value(&mut out, k, v);
                }
            }
        }
        return out;
    }

    if has_excludes && !has_includes {
        let mut out = doc.clone();
        for k in &keys {
            unset_nested_value(&mut out, k);
        }
        return out;
    }

    doc.clone()
}

fn apply_update_operators(doc: &mut Value, update: &Value) -> bool {
    let mut modified = false;
    let Some(obj) = update.as_object() else {
        return false;
    };

    for (op, val) in obj {
        match op.as_str() {
            "$set" => {
                let Some(set_obj) = val.as_object() else {
                    continue;
                };
                for (k, v) in set_obj {
                    set_nested_value(doc, k, v.clone());
                    modified = true;
                }
            }
            "$unset" => {
                let Some(unset_obj) = val.as_object() else {
                    continue;
                };
                for k in unset_obj.keys() {
                    unset_nested_value(doc, k);
                    modified = true;
                }
            }
            "$inc" => {
                let Some(inc_obj) = val.as_object() else {
                    continue;
                };
                for (k, v) in inc_obj {
                    let cur = get_nested_value(doc, k)
                        .and_then(|c| c.as_f64())
                        .unwrap_or(0.0);
                    let add = v.as_f64().unwrap_or(0.0);
                    set_nested_value(
                        doc,
                        k,
                        Value::Number(Number::from_f64(cur + add).unwrap_or(Number::from(0))),
                    );
                    modified = true;
                }
            }
            "$mul" => {
                let Some(mul_obj) = val.as_object() else {
                    continue;
                };
                for (k, v) in mul_obj {
                    let cur = get_nested_value(doc, k)
                        .and_then(|c| c.as_f64())
                        .unwrap_or(0.0);
                    let factor = v.as_f64().unwrap_or(0.0);
                    set_nested_value(
                        doc,
                        k,
                        Value::Number(Number::from_f64(cur * factor).unwrap_or(Number::from(0))),
                    );
                    modified = true;
                }
            }
            "$push" => {
                let Some(push_obj) = val.as_object() else {
                    continue;
                };
                for (k, v) in push_obj {
                    let current = get_nested_value(doc, k).unwrap_or(Value::Array(vec![]));
                    let mut arr = match current {
                        Value::Array(a) => a,
                        _ => vec![],
                    };
                    if let Some(each_obj) = v.as_object() {
                        if let Some(items) = each_obj.get("$each").and_then(|x| x.as_array()) {
                            for item in items {
                                arr.push(item.clone());
                            }
                        } else {
                            arr.push(v.clone());
                        }
                    } else {
                        arr.push(v.clone());
                    }
                    set_nested_value(doc, k, Value::Array(arr));
                    modified = true;
                }
            }
            "$pull" => {
                let Some(pull_obj) = val.as_object() else {
                    continue;
                };
                for (k, v) in pull_obj {
                    let current = get_nested_value(doc, k).unwrap_or(Value::Array(vec![]));
                    let mut arr = match current {
                        Value::Array(a) => a,
                        _ => vec![],
                    };
                    let before = arr.len();
                    if v.is_object() {
                        arr.retain(|item| !match_filter(item, v));
                    } else {
                        arr.retain(|item| item != v);
                    }
                    if arr.len() != before {
                        set_nested_value(doc, k, Value::Array(arr));
                        modified = true;
                    }
                }
            }
            "$addToSet" => {
                let Some(add_obj) = val.as_object() else {
                    continue;
                };
                for (k, v) in add_obj {
                    let current = get_nested_value(doc, k).unwrap_or(Value::Array(vec![]));
                    let mut arr = match current {
                        Value::Array(a) => a,
                        _ => vec![],
                    };
                    let items = if let Some(each_obj) = v.as_object() {
                        if let Some(items) = each_obj.get("$each").and_then(|x| x.as_array()) {
                            items.clone()
                        } else {
                            vec![v.clone()]
                        }
                    } else {
                        vec![v.clone()]
                    };
                    for it in items {
                        if !arr
                            .iter()
                            .any(|existing| value_eq(Some(existing), Some(&it)))
                        {
                            arr.push(it);
                            modified = true;
                        }
                    }
                    set_nested_value(doc, k, Value::Array(arr));
                }
            }
            "$rename" => {
                let Some(ren_obj) = val.as_object() else {
                    continue;
                };
                for (k, new_k) in ren_obj {
                    if let Some(cur) = get_nested_value(doc, k) {
                        let new_key = new_k.as_str().unwrap_or(k);
                        set_nested_value(doc, new_key, cur);
                        unset_nested_value(doc, k);
                        modified = true;
                    }
                }
            }
            _ => {}
        }
    }
    modified
}

// =============================================================================
// EXTRACCIÓN DE PARÁMETROS
// =============================================================================

fn require_db_name(payload: &Value) -> Result<&str, String> {
    payload
        .get("database_name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "database_name is required".to_string())
}

fn require_collection_name(payload: &Value) -> Result<&str, String> {
    payload
        .get("collectionName")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "collectionName is required".to_string())
}

fn opt_str<'v>(payload: &'v Value, key: &str) -> Option<&'v str> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

// =============================================================================
// EMISOR DE EVENTOS
// =============================================================================

struct Emitter<'a> {
    tx: &'a Broadcaster,
}

impl<'a> Emitter<'a> {
    fn emit(&self, category: &str, event: &str, mut payload: Value) {
        if let Some(o) = payload.as_object_mut() {
            o.insert("kind".into(), Value::String("change".into()));
            o.insert("category".into(), Value::String(category.to_string()));
            o.insert("event".into(), Value::String(event.to_string()));
            o.insert("ts".into(), Value::Number(Number::from(now_ms())));
        }
        log_line(
            LogLevel::Debug,
            "ws_broadcast_emit",
            Some(json!({
                "category": category,
                "event": event,
                "payload_preview": preview_json(&payload, 1200)
            })),
        );
        let _ = self.tx.send(payload.to_string());
    }
}

// =============================================================================
// HANDLERS DE ACCIONES CON REDIS PRIMARIO
// =============================================================================

async fn handle_database_action(
    mongo: &MongoConnectionInfo,
    func: &str,
    payload: &Value,
    emitter: &Emitter<'_>,
    doc_cache: &Option<RedisDocumentCache>,
    index_mgr: &Option<RedisIndexManager>,
) -> Result<Value, String> {
    log_line(
        LogLevel::Debug,
        "db_action_start",
        Some(json!({ "function": func, "payload": preview_json(payload, 500) })),
    );

    let result = match func {
        "createDatabase" => {
            let db_name = require_db_name(payload)?;
            if is_reserved_database(db_name) {
                return Err(format!(
                    "Database '{db_name}' is reserved and cannot be managed"
                ));
            }
            if database_exists(mongo, db_name).await? {
                return Err("Database already exists".into());
            }
            ensure_database_exists(mongo, db_name).await?;
            emitter.emit(
                "database",
                "createDatabase",
                json!({
                    "database_name": db_name, "before": Value::Null, "after": Value::Null,
                    "summary": {"database_name": db_name}
                }),
            );
            Ok(json!({"acknowledged": true, "database_name": db_name}))
        }

        "deleteDatabase" => {
            let db_name = require_db_name(payload)?;
            if is_reserved_database(db_name) {
                return Err(format!(
                    "Database '{db_name}' is reserved and cannot be managed"
                ));
            }
            if !database_exists(mongo, db_name).await? {
                return Err("Database not found".into());
            }
            mongo
                .client
                .database(db_name)
                .drop()
                .await
                .map_err(|e| format!("Could not delete '{db_name}' from MongoDB: {e}"))?;

            if let Some(cache) = doc_cache {
                cache.invalidate_database(db_name).await;
            }
            if let Some(idx) = index_mgr {
                let pattern = format!("{}:*:{}:*", INDEX_PREFIX, cache_key_segment(db_name));
                let mut conn = idx.connection.clone();
                let _ = clear_keys(&mut conn, &pattern).await;
            }

            emitter.emit(
                "database",
                "deleteDatabase",
                json!({
                    "database_name": db_name, "before": Value::Null, "after": Value::Null,
                    "summary": {"database_name": db_name}
                }),
            );
            Ok(json!({"acknowledged": true, "deleted": true}))
        }

        "renameDatabase" => {
            let from = require_db_name(payload)?;
            let to =
                opt_str(payload, "new_database_name").ok_or("new_database_name is required")?;
            if is_reserved_database(from) || is_reserved_database(to) {
                return Err("Reserved databases cannot be renamed".into());
            }
            if !database_exists(mongo, from).await? {
                return Err("Database not found".into());
            }
            if database_exists(mongo, to).await? {
                return Err("Target database already exists".into());
            }
            let database = load_database(mongo, from).await?;
            save_database(mongo, to, &database).await?;
            mongo
                .client
                .database(from)
                .drop()
                .await
                .map_err(|e| format!("Could not delete '{from}' after rename: {e}"))?;

            if let Some(cache) = doc_cache {
                cache.invalidate_database(from).await;
            }

            emitter.emit(
                "database",
                "renameDatabase",
                json!({
                    "database_name": to, "before": from, "after": to,
                    "summary": {"from": from, "to": to}
                }),
            );
            Ok(json!({"acknowledged": true, "from": from, "to": to}))
        }

        "createCollection" => {
            let db_name = require_db_name(payload)?;
            let col_name = require_collection_name(payload)?;
            if is_reserved_collection(col_name) {
                return Err(format!(
                    "Collection '{col_name}' is reserved and cannot be modified"
                ));
            }
            mongo
                .client
                .database(db_name)
                .create_collection(col_name)
                .await
                .map_err(|e| format!("Could not create collection '{db_name}.{col_name}': {e}"))?;
            emitter.emit(
                "database",
                "createCollection",
                json!({
                    "database_name": db_name, "collectionName": col_name,
                    "before": Value::Null, "after": Value::Null,
                    "summary": {"collectionName": col_name}
                }),
            );
            Ok(json!({"acknowledged": true, "database_name": db_name, "collectionName": col_name}))
        }

        "deleteCollection" => {
            let db_name = require_db_name(payload)?;
            let col_name = require_collection_name(payload)?;
            if is_reserved_collection(col_name) {
                return Err(format!(
                    "Collection '{col_name}' is reserved and cannot be modified"
                ));
            }
            let deleted = list_collection_names(mongo, db_name)
                .await?
                .into_iter()
                .any(|n| n == col_name);
            mongo
                .client
                .database(db_name)
                .collection::<Document>(col_name)
                .drop()
                .await
                .map_err(|e| format!("Could not delete collection '{db_name}.{col_name}': {e}"))?;

            if let Some(cache) = doc_cache {
                cache.invalidate_collection(db_name, col_name).await;
            }
            if let Some(idx) = index_mgr {
                idx.drop_collection_indexes(db_name, col_name).await;
            }

            emitter.emit(
                "database",
                "deleteCollection",
                json!({
                    "database_name": db_name, "collectionName": col_name,
                    "before": Value::Null, "after": Value::Null,
                    "summary": {"collectionName": col_name, "deleted": deleted}
                }),
            );
            Ok(json!({"acknowledged": true, "deleted": deleted}))
        }

        "renameCollection" => {
            let db_name = require_db_name(payload)?;
            let from = require_collection_name(payload)?;
            let to =
                opt_str(payload, "new_collection_name").ok_or("new_collection_name is required")?;
            if is_reserved_collection(from) || is_reserved_collection(to) {
                return Err("Reserved collections cannot be renamed".into());
            }
            mongo
                .client
                .database("admin")
                .run_command(doc! {
                    "renameCollection": format!("{db_name}.{from}"),
                    "to": format!("{db_name}.{to}")
                })
                .await
                .map_err(|e| format!("Could not rename collection '{db_name}.{from}': {e}"))?;

            if let Some(cache) = doc_cache {
                cache.invalidate_collection(db_name, from).await;
            }

            emitter.emit(
                "database",
                "renameCollection",
                json!({
                    "database_name": db_name, "collectionName": to,
                    "before": from, "after": to,
                    "summary": {"from": from, "to": to}
                }),
            );
            Ok(json!({"acknowledged": true, "from": from, "to": to}))
        }

        "exportDatabase" => {
            let db_name = require_db_name(payload)?;
            if is_reserved_database(db_name) {
                return Err(format!(
                    "Database '{db_name}' is reserved and cannot be exported"
                ));
            }
            let database = load_database(mongo, db_name).await?;
            let mut obj = Map::new();
            for (k, v) in &database.collections {
                if !is_reserved_collection(k) {
                    obj.insert(k.clone(), Value::Array(v.clone()));
                }
            }
            Ok(Value::Object(obj))
        }

        "importDatabase" => {
            let db_name = require_db_name(payload)?;
            if is_reserved_database(db_name) {
                return Err(format!(
                    "Database '{db_name}' is reserved and cannot be managed"
                ));
            }
            let data = payload.get("data").ok_or("data is required")?;
            let data_obj = data
                .as_object()
                .ok_or("data must be an object mapping collection names to arrays")?;
            let mode = opt_str(payload, "mode").unwrap_or("replace");

            let mut incoming: HashMap<String, Vec<Value>> = HashMap::new();
            for (k, v) in data_obj {
                if is_reserved_collection(k) {
                    return Err(format!(
                        "Collection '{k}' is reserved and cannot be imported"
                    ));
                }
                let arr = v
                    .as_array()
                    .ok_or_else(|| format!("Collection '{k}' must be a JSON array"))?;
                let mut docs = Vec::with_capacity(arr.len());
                for raw in arr {
                    if !raw.is_object() {
                        return Err(format!("Collection '{k}' contains a non-object document"));
                    }
                    docs.push(raw.clone());
                }
                incoming.insert(k.clone(), docs);
            }

            let existed = database_exists(mongo, db_name).await?;
            let mut database = if existed {
                load_database(mongo, db_name).await?
            } else {
                Database {
                    collections: HashMap::new(),
                }
            };

            let mut imported_collections = 0usize;
            let mut imported_docs = 0usize;

            match mode {
                "merge" => {
                    for (col, docs) in incoming {
                        imported_collections += 1;
                        let target = database.collections.entry(col).or_insert_with(Vec::new);
                        for doc in docs {
                            imported_docs += 1;
                            match doc.get("_id").cloned() {
                                Some(id) => {
                                    if let Some(idx) = target
                                        .iter()
                                        .position(|d| value_eq(d.get("_id"), Some(&id)))
                                    {
                                        target[idx] = doc;
                                    } else {
                                        target.push(doc);
                                    }
                                }
                                None => target.push(doc),
                            }
                        }
                    }
                }
                _ => {
                    database.collections.clear();
                    for (col, docs) in incoming {
                        imported_collections += 1;
                        imported_docs += docs.len();
                        database.collections.insert(col, docs);
                    }
                }
            }

            save_database(mongo, db_name, &database).await?;

            if let Some(cache) = doc_cache {
                cache.invalidate_database(db_name).await;
            }

            emitter.emit(
                "database",
                "importDatabase",
                json!({
                    "database_name": db_name, "before": Value::Null, "after": Value::Null,
                    "summary": {
                        "mode": mode, "created": !existed,
                        "importedCollections": imported_collections,
                        "importedDocuments": imported_docs
                    }
                }),
            );
            Ok(json!({
                "acknowledged": true,
                "database_name": db_name,
                "mode": mode,
                "created": !existed,
                "importedCollections": imported_collections,
                "importedDocuments": imported_docs
            }))
        }

        _ => Err(format!("Unknown database function: {func}")),
    };

    match &result {
        Ok(v) => log_line(
            LogLevel::Debug,
            "db_action_done",
            Some(json!({ "function": func, "success": true, "result_kind": v })),
        ),
        Err(e) => log_line(
            LogLevel::Warn,
            "db_action_done",
            Some(json!({ "function": func, "success": false, "error": e })),
        ),
    }
    result
}

/// Resuelve documentos usando Redis primero. Si hay índices, los usa. Si no, fallback a MongoDB.
async fn resolve_docs_redis_first(
    mongo: &MongoConnectionInfo,
    doc_cache: &Option<RedisDocumentCache>,
    index_mgr: &Option<RedisIndexManager>,
    auto_index: &Option<AutoIndexManager>,
    db_name: &str,
    col_name: &str,
    filter: &Value,
    sort: Option<&Value>,
    projection: Option<&Value>,
    limit: Option<usize>,
    skip: usize,
) -> Result<Vec<Value>, String> {
    let plan = QueryPlanAnalyzer::analyze(filter);

    if let Some(ai) = auto_index {
        let fields = QueryPlanAnalyzer::extract_indexable_fields(filter);
        for field in fields {
            ai.record_query_field(db_name, col_name, &field).await;
        }
    }

    if plan.can_use_redis {
        if let (Some(cache), Some(idx_mgr)) = (doc_cache, index_mgr) {
            let doc_ids = match plan.index_strategy {
                IndexStrategy::PrimaryKeyLookup { id } => vec![id],
                IndexStrategy::NumericRange { field, min, max } => {
                    idx_mgr.query_numeric_range(db_name, col_name, &field, min, max).await
                }
                IndexStrategy::TagExact { field, value } => {
                    idx_mgr.query_tag(db_name, col_name, &field, &value).await
                }
                IndexStrategy::StringExact { field, value } => {
                    idx_mgr.query_string(db_name, col_name, &field, &value)
                        .await
                        .map(|id| vec![id])
                        .unwrap_or_default()
                }
                IndexStrategy::TagsAnd { field, values } => {
                    idx_mgr.query_tags_and(db_name, col_name, &field, &values).await
                }
                IndexStrategy::Composite(strategies) => {
                    let mut all_ids: Vec<Vec<String>> = Vec::new();
                    for s in strategies {
                        let ids = match s {
                            IndexStrategy::NumericRange { field, min, max } => {
                                idx_mgr.query_numeric_range(db_name, col_name, &field, min, max).await
                            }
                            IndexStrategy::TagExact { field, value } => {
                                idx_mgr.query_tag(db_name, col_name, &field, &value).await
                            }
                            IndexStrategy::StringExact { field, value } => {
                                idx_mgr.query_string(db_name, col_name, &field, &value)
                                    .await
                                    .map(|id| vec![id])
                                    .unwrap_or_default()
                            }
                            _ => vec![],
                        };
                        if !ids.is_empty() {
                            all_ids.push(ids);
                        }
                    }
                    if all_ids.is_empty() {
                        vec![]
                    } else {
                        let mut result = all_ids[0].clone();
                        for ids in &all_ids[1..] {
                            let set: HashSet<String> = ids.iter().cloned().collect();
                            result.retain(|id| set.contains(id));
                        }
                        result
                    }
                }
                IndexStrategy::None => vec![],
            };

            if !doc_ids.is_empty() {
                let docs = cache.mget_docs(db_name, col_name, &doc_ids).await;
                let mut matched: Vec<Value> = docs.into_iter().flatten().collect();
                matched.retain(|d| match_filter(d, filter));

                if let Some(s) = sort {
                    sort_docs(&mut matched, s);
                }
                let mut result: Vec<Value> = if let Some(lim) = limit {
                    matched.into_iter().skip(skip).take(lim).collect()
                } else {
                    matched.into_iter().skip(skip).collect()
                };
                if let Some(p) = projection {
                    result = result.into_iter().map(|d| apply_projection(&d, p)).collect();
                }

                log_line(
                    LogLevel::Info,
                    "redis_index_query_hit",
                    Some(json!({
                        "database_name": db_name,
                        "collection": col_name,
                        "strategy": format!("{:?}", plan.index_strategy),
                        "matched_docs": result.len()
                    })),
                );
                return Ok(result);
            }
        }
    }

    let db = load_database(mongo, db_name).await?;
    let docs = db
        .collections
        .get(col_name)
        .map(|v| v.as_slice())
        .unwrap_or(&[]);
    let mut matched: Vec<Value> = docs
        .iter()
        .filter(|d| match_filter(d, filter))
        .cloned()
        .collect();

    if let Some(s) = sort {
        sort_docs(&mut matched, &s);
    }
    let mut result: Vec<Value> = if let Some(lim) = limit {
        matched.into_iter().skip(skip).take(lim).collect()
    } else {
        matched.into_iter().skip(skip).collect()
    };
    if let Some(p) = projection {
        result = result.into_iter().map(|d| apply_projection(&d, p)).collect();
    }

    if let Some(cache) = doc_cache {
        for doc in &result {
            if let Some(id) = doc.get("_id") {
                let id_str = json_to_string(id);
                cache.set_doc(db_name, col_name, &id_str, doc).await;
            }
        }
    }

    Ok(result)
}

async fn handle_collection_action(
    mongo: &MongoConnectionInfo,
    db_name: &str,
    func: &str,
    payload: &Value,
    emitter: &Emitter<'_>,
    doc_cache: &Option<RedisDocumentCache>,
    index_mgr: &Option<RedisIndexManager>,
    auto_index: &Option<AutoIndexManager>,
) -> Result<Value, String> {
    let col_name = require_collection_name(payload)?;
    if is_reserved_collection(col_name) {
        return Err(format!(
            "Collection '{col_name}' is reserved and cannot be modified"
        ));
    }

    log_line(
        LogLevel::Debug,
        "collection_action_start",
        Some(json!({ "function": func, "database_name": db_name, "collectionName": col_name })),
    );

    let result = match func {
        "find" => {
            let filter = payload
                .get("filter")
                .cloned()
                .unwrap_or(Value::Object(Map::new()));
            let sort = payload.get("sort").cloned();
            let projection = payload.get("projection").cloned();
            let limit = payload.get("limit").and_then(|v| v.as_u64()).map(|v| v as usize);
            let skip = payload.get("skip").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

            let result_docs = resolve_docs_redis_first(
                mongo,
                doc_cache,
                index_mgr,
                auto_index,
                db_name,
                col_name,
                &filter,
                sort.as_ref(),
                projection.as_ref(),
                limit,
                skip,
            ).await?;

            Ok(Value::Array(result_docs))
        }

        "findOne" => {
            let filter = payload
                .get("filter")
                .or(payload.get("query"))
                .cloned()
                .unwrap_or(Value::Object(Map::new()));
            let sort = payload.get("sort").cloned();
            let projection = payload.get("projection").cloned();

            let mut result_docs = resolve_docs_redis_first(
                mongo,
                doc_cache,
                index_mgr,
                auto_index,
                db_name,
                col_name,
                &filter,
                sort.as_ref(),
                projection.as_ref(),
                Some(1),
                0,
            ).await?;

            let doc = result_docs.into_iter().next();
            Ok(doc
                .map(|d| apply_projection(&d, &projection.unwrap_or(Value::Null)))
                .unwrap_or(Value::Null))
        }

        "insertOne" => {
            let mut db = load_database(mongo, db_name).await?;
            let doc = payload
                .get("document")
                .cloned()
                .ok_or("document is required")?;
            let id = doc.get("_id").cloned().unwrap_or(Value::Null);
            let after = doc.clone();
            let docs = db
                .collections
                .entry(col_name.to_string())
                .or_insert_with(Vec::new);
            docs.push(doc.clone());

            save_database(mongo, db_name, &db).await?;

            if let Some(idx) = index_mgr {
                let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                idx.index_document(db_name, col_name, &after, &idx_defs).await;
            }
            if let Some(cache) = doc_cache {
                if let Some(id_val) = after.get("_id") {
                    cache.set_doc(db_name, col_name, &json_to_string(id_val), &after).await;
                }
            }

            emitter.emit(
                "collection",
                "insertOne",
                json!({
                    "database_name": db_name, "collectionName": col_name,
                    "before": Value::Null, "after": after,
                    "summary": {"insertedId": id.clone()}
                }),
            );
            Ok(json!({"acknowledged": true, "insertedId": id}))
        }

        "updateOne" => {
            let mut db = load_database(mongo, db_name).await?;
            let filter = payload
                .get("filter")
                .cloned()
                .unwrap_or(Value::Object(Map::new()));
            let update = payload.get("update").cloned().ok_or("update is required")?;
            let options = payload.get("options").cloned();
            let upsert = options
                .as_ref()
                .and_then(|o| o.get("upsert"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let docs = db
                .collections
                .entry(col_name.to_string())
                .or_insert_with(Vec::new);
            let mut matched = 0;
            let mut modified_count = 0;
            let mut upserted = 0;
            let mut upserted_id = Value::Null;
            let mut ev_before = Value::Null;
            let mut ev_after = Value::Null;
            let mut had_change = false;

            if let Some(idx) = docs.iter().position(|d| match_filter(d, &filter)) {
                matched = 1;
                ev_before = docs[idx].clone();
                let original_id = docs[idx].get("_id").cloned();

                if let Some(idx_mgr) = index_mgr {
                    let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                    idx_mgr.unindex_document(db_name, col_name, &ev_before, &idx_defs).await;
                }

                if is_operator_object(&update) {
                    if apply_update_operators(&mut docs[idx], &update) {
                        modified_count = 1;
                    }
                } else {
                    let mut replacement = update.clone();
                    if let Some(id) = original_id {
                        if let Some(obj) = replacement.as_object_mut() {
                            obj.insert("_id".into(), id);
                        }
                    }
                    docs[idx] = replacement;
                    modified_count = 1;
                }
                ev_after = docs[idx].clone();
                had_change = true;

                if let Some(idx_mgr) = index_mgr {
                    let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                    idx_mgr.index_document(db_name, col_name, &ev_after, &idx_defs).await;
                }
                if let Some(cache) = doc_cache {
                    if let Some(id_val) = ev_after.get("_id") {
                        cache.set_doc(db_name, col_name, &json_to_string(id_val), &ev_after).await;
                    }
                }
            } else if upsert {
                let mut new_doc = if let Some(f) = filter.as_object() {
                    let mut base = Map::new();
                    for (k, v) in f {
                        if !k.starts_with('$') && !is_operator_object(v) {
                            base.insert(k.clone(), v.clone());
                        }
                    }
                    Value::Object(base)
                } else {
                    Value::Object(Map::new())
                };
                if is_operator_object(&update) {
                    if let Some(set_on_insert) =
                        update.get("$setOnInsert").and_then(|v| v.as_object())
                    {
                        if let Some(nd) = new_doc.as_object_mut() {
                            for (k, v) in set_on_insert {
                                nd.entry(k.clone()).or_insert_with(|| v.clone());
                            }
                        }
                    }
                    apply_update_operators(&mut new_doc, &update);
                } else if let Some(u) = update.as_object() {
                    if let Some(nd) = new_doc.as_object_mut() {
                        for (k, v) in u {
                            nd.insert(k.clone(), v.clone());
                        }
                    }
                }
                upserted_id = new_doc.get("_id").cloned().unwrap_or(Value::Null);
                ev_after = new_doc.clone();
                docs.push(new_doc.clone());
                upserted = 1;
                had_change = true;

                if let Some(idx_mgr) = index_mgr {
                    let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                    idx_mgr.index_document(db_name, col_name, &ev_after, &idx_defs).await;
                }
                if let Some(cache) = doc_cache {
                    if let Some(id_val) = ev_after.get("_id") {
                        cache.set_doc(db_name, col_name, &json_to_string(id_val), &ev_after).await;
                    }
                }
            }

            if had_change {
                save_database(mongo, db_name, &db).await?;
            }

            if had_change {
                emitter.emit(
                    "collection",
                    "updateOne",
                    json!({
                        "database_name": db_name, "collectionName": col_name,
                        "before": ev_before, "after": ev_after,
                        "summary": {
                            "matchedCount": matched, "modifiedCount": modified_count,
                            "upsertedCount": upserted, "upsertedId": upserted_id.clone()
                        }
                    }),
                );
            }
            Ok(json!({
                "acknowledged": true,
                "matchedCount": matched,
                "modifiedCount": modified_count,
                "upsertedCount": upserted,
                "upsertedId": upserted_id
            }))
        }

        "replaceOne" => {
            let mut db = load_database(mongo, db_name).await?;
            let filter = payload
                .get("filter")
                .cloned()
                .unwrap_or(Value::Object(Map::new()));
            let replacement = payload
                .get("replacement")
                .cloned()
                .ok_or("replacement is required")?;
            let options = payload.get("options").cloned();
            let upsert = options
                .as_ref()
                .and_then(|o| o.get("upsert"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let docs = db
                .collections
                .entry(col_name.to_string())
                .or_insert_with(Vec::new);
            let mut matched = 0;
            let mut modified_count = 0;
            let mut upserted = 0;
            let mut upserted_id = Value::Null;
            let mut ev_before = Value::Null;
            let mut ev_after = Value::Null;
            let mut had_change = false;

            if let Some(idx) = docs.iter().position(|d| match_filter(d, &filter)) {
                matched = 1;
                ev_before = docs[idx].clone();

                if let Some(idx_mgr) = index_mgr {
                    let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                    idx_mgr.unindex_document(db_name, col_name, &ev_before, &idx_defs).await;
                }

                let original_id = docs[idx].get("_id").cloned();
                let mut new_doc = replacement.clone();
                if let Some(id) = original_id {
                    if let Some(obj) = new_doc.as_object_mut() {
                        obj.insert("_id".into(), id);
                    }
                }
                docs[idx] = new_doc.clone();
                ev_after = docs[idx].clone();
                modified_count = 1;
                had_change = true;

                if let Some(idx_mgr) = index_mgr {
                    let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                    idx_mgr.index_document(db_name, col_name, &ev_after, &idx_defs).await;
                }
                if let Some(cache) = doc_cache {
                    if let Some(id_val) = ev_after.get("_id") {
                        cache.set_doc(db_name, col_name, &json_to_string(id_val), &ev_after).await;
                    }
                }
            } else if upsert {
                let new_doc = replacement.clone();
                upserted_id = new_doc.get("_id").cloned().unwrap_or(Value::Null);
                ev_after = new_doc.clone();
                docs.push(new_doc.clone());
                upserted = 1;
                had_change = true;

                if let Some(idx_mgr) = index_mgr {
                    let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                    idx_mgr.index_document(db_name, col_name, &ev_after, &idx_defs).await;
                }
                if let Some(cache) = doc_cache {
                    if let Some(id_val) = ev_after.get("_id") {
                        cache.set_doc(db_name, col_name, &json_to_string(id_val), &ev_after).await;
                    }
                }
            }

            if had_change {
                save_database(mongo, db_name, &db).await?;
            }

            if had_change {
                emitter.emit(
                    "collection",
                    "replaceOne",
                    json!({
                        "database_name": db_name, "collectionName": col_name,
                        "before": ev_before, "after": ev_after,
                        "summary": {
                            "matchedCount": matched, "modifiedCount": modified_count,
                            "upsertedCount": upserted, "upsertedId": upserted_id.clone()
                        }
                    }),
                );
            }
            Ok(json!({
                "acknowledged": true,
                "matchedCount": matched,
                "modifiedCount": modified_count,
                "upsertedCount": upserted,
                "upsertedId": upserted_id
            }))
        }

        "deleteOne" => {
            let mut db = load_database(mongo, db_name).await?;
            let filter = payload
                .get("filter")
                .cloned()
                .unwrap_or(Value::Object(Map::new()));
            let docs = db
                .collections
                .entry(col_name.to_string())
                .or_insert_with(Vec::new);

            log_line(
                LogLevel::Info,
                "delete_one_filter_received",
                Some(json!({
                    "database_name": db_name,
                    "collectionName": col_name,
                    "filter_preview": preview_json(&filter, 600)
                })),
            );

            if let Some(idx) = docs.iter().position(|d| match_filter(d, &filter)) {
                let before = docs[idx].clone();

                if let Some(idx_mgr) = index_mgr {
                    let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                    idx_mgr.unindex_document(db_name, col_name, &before, &idx_defs).await;
                }
                if let Some(cache) = doc_cache {
                    if let Some(id_val) = before.get("_id") {
                        cache.del_doc(db_name, col_name, &json_to_string(id_val)).await;
                    }
                }

                log_line(
                    LogLevel::Info,
                    "delete_one_document_matched",
                    Some(json!({
                        "database_name": db_name,
                        "collectionName": col_name,
                        "matched_id": preview_json(&before.get("_id").cloned().unwrap_or(Value::Null), 300)
                    })),
                );
                docs.remove(idx);
                save_database(mongo, db_name, &db).await?;

                emitter.emit(
                    "collection",
                    "deleteOne",
                    json!({
                        "database_name": db_name, "collectionName": col_name,
                        "before": before, "after": Value::Null,
                        "summary": {"deletedCount": 1}
                    }),
                );
                Ok(json!({"acknowledged": true, "deletedCount": 1}))
            } else {
                Ok(json!({"acknowledged": true, "deletedCount": 0}))
            }
        }

        "deleteMany" => {
            let mut db = load_database(mongo, db_name).await?;
            let filter = payload
                .get("filter")
                .cloned()
                .unwrap_or(Value::Object(Map::new()));
            let docs = db
                .collections
                .entry(col_name.to_string())
                .or_insert_with(Vec::new);
            let removed: Vec<Value> = docs
                .iter()
                .filter(|d| match_filter(d, &filter))
                .cloned()
                .collect();

            if let Some(idx_mgr) = index_mgr {
                let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                for doc in &removed {
                    idx_mgr.unindex_document(db_name, col_name, doc, &idx_defs).await;
                }
            }
            if let Some(cache) = doc_cache {
                for doc in &removed {
                    if let Some(id_val) = doc.get("_id") {
                        cache.del_doc(db_name, col_name, &json_to_string(id_val)).await;
                    }
                }
            }

            let before = docs.len();
            docs.retain(|d| !match_filter(d, &filter));
            let deleted = before - docs.len();

            if deleted > 0 {
                save_database(mongo, db_name, &db).await?;
            }

            if deleted > 0 {
                emitter.emit(
                    "collection",
                    "deleteMany",
                    json!({
                        "database_name": db_name, "collectionName": col_name,
                        "before": Value::Array(removed), "after": Value::Null,
                        "summary": {"deletedCount": deleted}
                    }),
                );
            }
            Ok(json!({"acknowledged": true, "deletedCount": deleted}))
        }

        "updateCollection" => {
            let mut db = load_database(mongo, db_name).await?;
            let docs = db
                .collections
                .entry(col_name.to_string())
                .or_insert_with(Vec::new);

            let result = if let Some(Value::Array(data)) = payload
                .get("data")
                .or(payload.get("documents"))
                .or(payload.get("docs"))
                .or(payload.get("items"))
            {
                if data.is_empty() {
                    Ok(json!({"acknowledged": true, "insertedCount": 0, "modifiedCount": 0}))
                } else {
                    let mut inserted = 0;
                    let mut modified_count = 0;
                    for raw in data {
                        if raw.is_object() {
                            if let Some(id) = raw.get("_id").cloned() {
                                if let Some(idx) =
                                    docs.iter().position(|d| value_eq(d.get("_id"), Some(&id)))
                                {
                                    if let Some(idx_mgr) = index_mgr {
                                        let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                                        idx_mgr.unindex_document(db_name, col_name, &docs[idx], &idx_defs).await;
                                    }
                                    docs[idx] = raw.clone();
                                    modified_count += 1;
                                    if let Some(idx_mgr) = index_mgr {
                                        let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                                        idx_mgr.index_document(db_name, col_name, &docs[idx], &idx_defs).await;
                                    }
                                    if let Some(cache) = doc_cache {
                                        if let Some(id_val) = docs[idx].get("_id") {
                                            cache.set_doc(db_name, col_name, &json_to_string(id_val), &docs[idx]).await;
                                        }
                                    }
                                } else {
                                    docs.push(raw.clone());
                                    inserted += 1;
                                    if let Some(idx_mgr) = index_mgr {
                                        let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                                        idx_mgr.index_document(db_name, col_name, raw, &idx_defs).await;
                                    }
                                    if let Some(cache) = doc_cache {
                                        if let Some(id_val) = raw.get("_id") {
                                            cache.set_doc(db_name, col_name, &json_to_string(id_val), raw).await;
                                        }
                                    }
                                }
                            } else {
                                docs.push(raw.clone());
                                inserted += 1;
                                if let Some(idx_mgr) = index_mgr {
                                    let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                                    idx_mgr.index_document(db_name, col_name, raw, &idx_defs).await;
                                }
                                if let Some(cache) = doc_cache {
                                    if let Some(id_val) = raw.get("_id") {
                                        cache.set_doc(db_name, col_name, &json_to_string(id_val), raw).await;
                                    }
                                }
                            }
                        }
                    }
                    Ok(json!({
                        "acknowledged": true,
                        "insertedCount": inserted,
                        "modifiedCount": modified_count
                    }))
                }
            } else {
                let filter = payload
                    .get("filter")
                    .cloned()
                    .unwrap_or(Value::Object(Map::new()));
                let update = payload.get("update").cloned().ok_or("update is required")?;
                let mut matched = 0;
                let mut modified_count = 0;
                for doc in docs.iter_mut() {
                    if match_filter(doc, &filter) {
                        matched += 1;

                        if let Some(idx_mgr) = index_mgr {
                            let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                            idx_mgr.unindex_document(db_name, col_name, doc, &idx_defs).await;
                        }

                        if is_operator_object(&update) {
                            if apply_update_operators(doc, &update) {
                                modified_count += 1;
                            }
                        } else {
                            let original_id = doc.get("_id").cloned();
                            *doc = update.clone();
                            if let Some(id) = original_id {
                                if let Some(obj) = doc.as_object_mut() {
                                    obj.insert("_id".into(), id);
                                }
                            }
                            modified_count += 1;
                        }

                        if let Some(idx_mgr) = index_mgr {
                            let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                            idx_mgr.index_document(db_name, col_name, doc, &idx_defs).await;
                        }
                        if let Some(cache) = doc_cache {
                            if let Some(id_val) = doc.get("_id") {
                                cache.set_doc(db_name, col_name, &json_to_string(id_val), doc).await;
                            }
                        }
                    }
                }
                Ok(json!({
                    "acknowledged": true,
                    "matchedCount": matched,
                    "modifiedCount": modified_count
                }))
            };

            if let Ok(ref val) = result {
                let had_change = val
                    .get("insertedCount")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0)
                    > 0
                    || val
                        .get("modifiedCount")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0)
                        > 0
                    || val
                        .get("matchedCount")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0)
                        > 0;
                if had_change {
                    save_database(mongo, db_name, &db).await?;
                }
            }

            if let Ok(ref val) = result {
                emitter.emit(
                    "collection",
                    "updateCollection",
                    json!({
                        "database_name": db_name, "collectionName": col_name,
                        "before": Value::Null, "after": Value::Null,
                        "summary": val
                    }),
                );
            }
            result
        }

        "exportCollection" => {
            let db = load_database(mongo, db_name).await?;
            let docs = db.collections.get(col_name).cloned().unwrap_or_default();
            Ok(Value::Array(docs))
        }

        "importCollection" => {
            let data = payload
                .get("data")
                .or(payload.get("documents"))
                .cloned()
                .ok_or("data is required (array of documents)")?;
            let arr = data.as_array().ok_or("data must be a JSON array")?;
            let mode = opt_str(payload, "mode").unwrap_or("merge");

            for raw in arr {
                if !raw.is_object() {
                    return Err("All documents must be JSON objects".into());
                }
            }

            let mut db = load_database(mongo, db_name).await?;
            let docs = db
                .collections
                .entry(col_name.to_string())
                .or_insert_with(Vec::new);
            let mut inserted = 0usize;
            let mut updated = 0usize;

            if let Some(idx_mgr) = index_mgr {
                idx_mgr.drop_collection_indexes(db_name, col_name).await;
            }
            if let Some(cache) = doc_cache {
                cache.invalidate_collection(db_name, col_name).await;
            }

            match mode {
                "replace" => {
                    let mut new_docs = Vec::with_capacity(arr.len());
                    for raw in arr {
                        new_docs.push(raw.clone());
                    }
                    inserted = new_docs.len();
                    *docs = new_docs;
                }
                "append" => {
                    for raw in arr {
                        docs.push(raw.clone());
                        inserted += 1;
                    }
                }
                _ => {
                    for raw in arr {
                        match raw.get("_id").cloned() {
                            Some(id) => {
                                if let Some(idx) =
                                    docs.iter().position(|x| value_eq(x.get("_id"), Some(&id)))
                                {
                                    docs[idx] = raw.clone();
                                    updated += 1;
                                } else {
                                    docs.push(raw.clone());
                                    inserted += 1;
                                }
                            }
                            None => {
                                docs.push(raw.clone());
                                inserted += 1;
                            }
                        }
                    }
                }
            }

            save_database(mongo, db_name, &db).await?;

            if let Some(idx_mgr) = index_mgr {
                let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
                for doc in docs.iter() {
                    idx_mgr.index_document(db_name, col_name, doc, &idx_defs).await;
                }
            }
            if let Some(cache) = doc_cache {
                for doc in docs.iter() {
                    if let Some(id_val) = doc.get("_id") {
                        cache.set_doc(db_name, col_name, &json_to_string(id_val), doc).await;
                    }
                }
            }

            emitter.emit(
                "collection",
                "importCollection",
                json!({
                    "database_name": db_name, "collectionName": col_name,
                    "before": Value::Null, "after": Value::Null,
                    "summary": {"mode": mode, "insertedCount": inserted, "modifiedCount": updated}
                }),
            );
            Ok(json!({
                "acknowledged": true,
                "mode": mode,
                "insertedCount": inserted,
                "modifiedCount": updated
            }))
        }

        _ => Err(format!("Unknown collection function: {func}")),
    };

    match &result {
        Ok(v) => log_line(
            LogLevel::Debug,
            "collection_action_done",
            Some(json!({
                "function": func, "database_name": db_name, "collectionName": col_name, "result": preview_json(v, 500)
            })),
        ),
        Err(e) => log_line(
            LogLevel::Warn,
            "collection_action_done",
            Some(json!({
                "function": func, "database_name": db_name, "collectionName": col_name, "error": e
            })),
        ),
    }
    result
}

// =============================================================================
// RESPUESTAS
// =============================================================================

fn success_response(data: Value) -> WsResponse {
    WsResponse {
        success: true,
        response_json: serde_json::to_string(&data).unwrap_or_else(|_| "null".to_string()),
        message: "OK".to_string(),
    }
}

fn error_response(message: &str) -> WsResponse {
    WsResponse {
        success: false,
        response_json: "null".to_string(),
        message: message.to_string(),
    }
}

// =============================================================================
// PROCESAMIENTO DE REQUESTS
// =============================================================================

async fn process_request(
    req: WsRequest,
    mongo: &MongoConnectionInfo,
    bcast: &Broadcaster,
    request_cache: Option<RequestCache>,
    doc_cache: Option<RedisDocumentCache>,
    index_mgr: Option<RedisIndexManager>,
    auto_index: Option<AutoIndexManager>,
) -> WsResponse {
    let req_snapshot = req.clone();
    let emitter = Emitter { tx: bcast };
    let payload_ref = req_snapshot.payload.as_ref().unwrap_or(&Value::Null);
    let request_database_name = opt_str(payload_ref, "database_name")
        .or(req_snapshot.database_name.as_deref())
        .unwrap_or_default()
        .to_string();
    let request_collection_name = opt_str(payload_ref, "collectionName")
        .unwrap_or_default()
        .to_string();

    log_line(
        LogLevel::Debug,
        "ws_request_received",
        Some(json!({
            "type": req_snapshot.msg_type,
            "category": req_snapshot.category.clone().unwrap_or_default(),
            "function": req_snapshot.function_name.clone().unwrap_or_default(),
            "database_name": request_database_name,
            "cache_enabled": request_cache.is_some(),
            "doc_cache_enabled": doc_cache.is_some(),
            "index_enabled": index_mgr.is_some(),
            "cacheable": is_cacheable_request(&req_snapshot),
            "mutating": is_mutating_request(&req_snapshot),
            "payload_preview": preview_json(payload_ref, 1500),
        })),
    );
    if req_snapshot.msg_type == "action" {
        log_line(
            LogLevel::Info,
            "ws_action_received",
            Some(json!({
                "source": "websocket",
                "category": req_snapshot.category.clone().unwrap_or_default(),
                "function": req_snapshot.function_name.clone().unwrap_or_default(),
                "database_name": request_database_name,
                "collectionName": request_collection_name,
                "mutating": is_mutating_request(&req_snapshot),
                "payload_preview": preview_json(payload_ref, 1500)
            })),
        );
    }

    let cache_key = if let Some(cache) = request_cache.as_ref() {
        match cache.build_key(&req_snapshot).await {
            Some(key) => {
                if let Some(cached) = cache.get_response(&key).await {
                    log_line(
                        LogLevel::Info,
                        "ws_response_served_from_cache",
                        Some(json!({ "cache_key": key })),
                    );
                    return WsResponse {
                        success: true,
                        response_json: cached,
                        message: "OK".to_string(),
                    };
                }
                log_line(
                    LogLevel::Debug,
                    "cache_miss",
                    Some(json!({ "cache_key": key })),
                );
                Some(key)
            }
            None => None,
        }
    } else {
        None
    };

    let response = match req.msg_type.as_str() {
        "precache" => {
            let db_name = req.database_name.unwrap_or_default();
            if db_name.is_empty() {
                return error_response("database_name is required");
            }
            if let Err(e) = load_database(mongo, &db_name).await {
                return error_response(&e);
            }
            let collections = req.collections.unwrap_or_default();
            if let Some(cache) = doc_cache.as_ref() {
                if !collections.is_empty() {
                    if let Ok(db) = load_database(mongo, &db_name).await {
                        for col in &collections {
                            if let Some(docs) = db.collections.get(col) {
                                for doc in docs {
                                    if let Some(id) = doc.get("_id") {
                                        cache.set_doc(&db_name, col, &json_to_string(id), doc).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            success_response(json!({"ok": true, "message": "Cache established"}))
        }

        "action" => {
            let payload = req.payload.unwrap_or(Value::Null);
            let cat = req.category.unwrap_or_default();
            let func = req.function_name.unwrap_or_default();

            match cat.as_str() {
                "collection" => {
                    let db_name = opt_str(&payload, "database_name").unwrap_or("");
                    if db_name.is_empty() {
                        return error_response("database_name missing in payload");
                    }
                    match handle_collection_action(
                        mongo, db_name, &func, &payload, &emitter,
                        &doc_cache, &index_mgr, &auto_index
                    ).await
                    {
                        Ok(val) => success_response(val),
                        Err(e) => error_response(&e),
                    }
                }
                "database" => {
                    match handle_database_action(
                        mongo, &func, &payload, &emitter,
                        &doc_cache, &index_mgr
                    ).await {
                        Ok(val) => success_response(val),
                        Err(e) => error_response(&e),
                    }
                }
                _ => error_response("Unknown category"),
            }
        }

        "metadata" => success_response(json!({
            "api_format": "websocket.microservicedb.DatabaseService",
            "categories": ["collection", "database"],
            "functions": {
                "collection": ["find", "findOne", "insertOne", "updateOne", "replaceOne", "deleteOne", "deleteMany", "updateCollection", "exportCollection", "importCollection"],
                "database": ["createDatabase", "deleteDatabase", "renameDatabase", "createCollection", "deleteCollection", "renameCollection", "exportDatabase", "importDatabase"]
            },
            "events": {
                "kind": "change",
                "categories": ["collection", "database"],
                "fields": ["event", "database_name", "collectionName", "before", "after", "summary", "ts"]
            },
            "redis_first": true,
            "auto_index": true,
            "doc_cache": true
        })),

        "list_databases" => match list_database_names(mongo).await {
            Ok(mut dbs) => {
                dbs.sort();
                success_response(Value::Array(dbs.into_iter().map(Value::String).collect()))
            }
            Err(e) => error_response(&e),
        },

        "list_collections" => {
            let db_name = req.database_name.unwrap_or_default();
            if db_name.is_empty() {
                return error_response("database_name is required");
            }
            match list_collection_names(mongo, &db_name).await {
                Ok(mut cols) => {
                    cols.sort();
                    success_response(Value::Array(cols.into_iter().map(Value::String).collect()))
                }
                Err(e) => error_response(&e),
            }
        }

        _ => error_response("Unknown type"),
    };

    if response.success {
        if let (Some(cache), Some(key)) = (request_cache.as_ref(), cache_key.as_ref()) {
            cache.set_response(key, &response.response_json).await;
            log_line(
                LogLevel::Debug,
                "cache_set",
                Some(json!({ "cache_key": key })),
            );
        }

        if is_mutating_request(&req_snapshot)
            && response_indicates_mutation(&req_snapshot, &response)
        {
            if let Some(cache) = request_cache.as_ref() {
                let payload = req_snapshot.payload.as_ref().unwrap_or(&Value::Null);
                let db_name = opt_str(payload, "database_name").unwrap_or_default();
                let new_db_name = opt_str(payload, "new_database_name").unwrap_or_default();
                let category = req_snapshot.category.as_deref().unwrap_or_default();
                let function_name = req_snapshot.function_name.as_deref().unwrap_or_default();

                if category == "database"
                    && matches!(
                        function_name,
                        "createDatabase" | "deleteDatabase" | "renameDatabase"
                    )
                {
                    cache.invalidate_list_databases().await;
                }

                if !db_name.is_empty() {
                    cache.invalidate_database(db_name).await;
                }
                if !new_db_name.is_empty() && new_db_name != db_name {
                    cache.invalidate_database(new_db_name).await;
                }

                log_line(
                    LogLevel::Info,
                    "cache_invalidate_after_mutation",
                    Some(json!({
                        "category": category,
                        "function": function_name,
                        "database_name": db_name,
                        "new_database_name": if new_db_name.is_empty() { Value::Null } else { Value::String(new_db_name.to_string()) }
                    })),
                );
            }
        }
    }

    log_line(
        LogLevel::Info,
        "ws_response_served",
        Some(json!({
            "source": if cache_key.is_some() { "mongodb_then_cached" } else { "mongodb" },
            "success": response.success
        })),
    );
    if req_snapshot.msg_type == "action" {
        log_line(
            LogLevel::Info,
            "ws_action_response",
            Some(json!({
                "category": req_snapshot.category.clone().unwrap_or_default(),
                "function": req_snapshot.function_name.clone().unwrap_or_default(),
                "database_name": request_database_name,
                "collectionName": request_collection_name,
                "success": response.success,
                "message": response.message,
                "response_preview": preview_text(&response.response_json, 1500)
            })),
        );
    }

    response
}

// =============================================================================
// WEBSOCKET HANDLER
// =============================================================================

async fn handle_socket(
    ws: WebSocket,
    mongo: MongoState,
    bcast: Broadcaster,
    request_cache: Option<RequestCache>,
    doc_cache: Option<RedisDocumentCache>,
    index_mgr: Option<RedisIndexManager>,
    auto_index: Option<AutoIndexManager>,
) {
    log_line(
        LogLevel::Info,
        "ws_connection_open",
        Some(json!({
            "cache_enabled": request_cache.is_some(),
            "doc_cache_enabled": doc_cache.is_some(),
            "index_enabled": index_mgr.is_some(),
            "auto_index_enabled": auto_index.is_some()
        })),
    );

    let (mut sink, mut rx) = ws.split();
    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();

    let writer = tokio::spawn(async move {
        while let Some(m) = out_rx.recv().await {
            let preview = m
                .to_str()
                .ok()
                .map(|t| preview_text(t, 1500))
                .unwrap_or(Value::String("<non-text-frame>".to_string()));
            log_line(
                LogLevel::Debug,
                "ws_message_out",
                Some(json!({ "preview": preview })),
            );
            if let Err(e) = sink.send(m).await {
                log_line(
                    LogLevel::Warn,
                    "ws_message_out_failed",
                    Some(json!({ "error": e.to_string() })),
                );
                break;
            }
        }
    });

    let mut bcast_rx = bcast.subscribe();
    let bcast_out = out_tx.clone();
    let bcast_task = tokio::spawn(async move {
        loop {
            match bcast_rx.recv().await {
                Ok(text) => {
                    log_line(
                        LogLevel::Debug,
                        "ws_broadcast_forward",
                        Some(json!({ "preview": preview_text(&text, 1500) })),
                    );
                    if bcast_out.send(Message::text(text)).is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    log_line(
                        LogLevel::Warn,
                        "ws_broadcast_lagged",
                        Some(json!({ "skipped": skipped })),
                    );
                }
                Err(broadcast::error::RecvError::Closed) => {
                    log_line(LogLevel::Warn, "ws_broadcast_closed", None);
                    break;
                }
            }
        }
    });

    while let Some(Ok(msg)) = rx.next().await {
        if let Ok(text) = msg.to_str() {
            log_line(
                LogLevel::Trace,
                "ws_message_in",
                Some(json!({
                    "bytes": text.len(),
                    "preview": preview_text(text, 1500)
                })),
            );

            let req: WsRequest = match serde_json::from_str(text) {
                Ok(r) => r,
                Err(e) => {
                    log_line(
                        LogLevel::Warn,
                        "ws_message_invalid_json",
                        Some(json!({ "error": e.to_string() })),
                    );
                    let resp = error_response(&format!("Invalid JSON: {e}"));
                    if out_tx
                        .send(Message::text(serde_json::to_string(&resp).unwrap()))
                        .is_err()
                    {
                        break;
                    }
                    continue;
                }
            };

            let resp = process_request(
                req, &mongo, &bcast,
                request_cache.clone(),
                doc_cache.clone(),
                index_mgr.clone(),
                auto_index.clone(),
            ).await;
            let text = serde_json::to_string(&resp).unwrap();
            log_line(
                LogLevel::Debug,
                "ws_response_ready",
                Some(json!({
                    "success": resp.success, "message": resp.message, "preview": preview_text(&text, 1500)
                })),
            );
            if out_tx.send(Message::text(text)).is_err() {
                break;
            }
        }
    }

    drop(out_tx);
    bcast_task.abort();
    let _ = writer.await;
    log_line(LogLevel::Info, "ws_connection_closed", None);
}

// =============================================================================
// CONEXIÓN REDIS AUXILIAR (doc cache, index manager, auto index)
// =============================================================================

async fn connect_redis_doc_cache(config: &AppConfig) -> Option<RedisDocumentCache> {
    let url = match config.dragonfly_url.as_deref() {
        Some(v) if !v.trim().is_empty() => v,
        _ => return None,
    };

    let client = match redis::Client::open(url) {
        Ok(c) => c,
        Err(_) => return None,
    };

    let connection = match client.get_multiplexed_tokio_connection().await {
        Ok(c) => c,
        Err(_) => return None,
    };

    if let Err(_) = redis::cmd("PING").query_async::<String>(&mut connection.clone()).await {
        return None;
    }

    log_line(
        LogLevel::Info,
        "doc_cache_connect_ok",
        Some(json!({ "ttl_seconds": config.doc_cache_ttl_secs })),
    );

    Some(RedisDocumentCache {
        connection,
        ttl_seconds: config.doc_cache_ttl_secs,
        prefix: DOC_CACHE_PREFIX.to_string(),
    })
}

async fn connect_redis_index_manager(config: &AppConfig) -> Option<RedisIndexManager> {
    let url = match config.dragonfly_url.as_deref() {
        Some(v) if !v.trim().is_empty() => v,
        _ => return None,
    };

    let client = match redis::Client::open(url) {
        Ok(c) => c,
        Err(_) => return None,
    };

    let connection = match client.get_multiplexed_tokio_connection().await {
        Ok(c) => c,
        Err(_) => return None,
    };

    if let Err(_) = redis::cmd("PING").query_async::<String>(&mut connection.clone()).await {
        return None;
    }

    log_line(LogLevel::Info, "index_manager_connect_ok", None);

    Some(RedisIndexManager {
        connection,
        prefix: INDEX_PREFIX.to_string(),
    })
}

async fn connect_auto_index_manager(config: &AppConfig) -> Option<AutoIndexManager> {
    let url = match config.dragonfly_url.as_deref() {
        Some(v) if !v.trim().is_empty() => v,
        _ => return None,
    };

    let client = match redis::Client::open(url) {
        Ok(c) => c,
        Err(_) => return None,
    };

    let connection = match client.get_multiplexed_tokio_connection().await {
        Ok(c) => c,
        Err(_) => return None,
    };

    if let Err(_) = redis::cmd("PING").query_async::<String>(&mut connection.clone()).await {
        return None;
    }

    log_line(
        LogLevel::Info,
        "auto_index_connect_ok",
        Some(json!({
            "threshold": config.autoindex_threshold,
            "window_secs": config.autoindex_window_secs,
            "drop_idle_secs": config.autoindex_drop_idle_secs
        })),
    );

    Some(AutoIndexManager {
        connection,
        threshold: config.autoindex_threshold,
        window_secs: config.autoindex_window_secs,
        drop_idle_secs: config.autoindex_drop_idle_secs,
        prefix: INDEX_STATS_PREFIX.to_string(),
    })
}

async fn auto_index_agent_loop(
    mongo: MongoState,
    index_mgr: RedisIndexManager,
    auto_index: AutoIndexManager,
) {
    let mut ticker = interval(Duration::from_secs(60));
    loop {
        ticker.tick().await;

        if let Ok(databases) = list_database_names(&mongo).await {
            for db_name in databases {
                if let Ok(collections) = list_collection_names(&mongo, &db_name).await {
                    for col_name in collections {
                        auto_index.check_and_create_indexes(&mongo, &index_mgr, &db_name, &col_name).await;
                        auto_index.drop_idle_indexes(&mongo, &index_mgr, &db_name, &col_name).await;
                    }
                }
            }
        }
    }
}

// =============================================================================
// MAIN
// =============================================================================

#[tokio::main]
async fn main() {
    let _ = dotenv();

    let config = match load_app_config() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    };

    let tls = match tls_config() {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    };

    let mongo: MongoState = match connect_mongodb(&config).await {
        Ok(m) => Arc::new(m),
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    };

    let request_cache = connect_request_cache(&config).await;
    let doc_cache = connect_redis_doc_cache(&config).await;
    let index_mgr = connect_redis_index_manager(&config).await;
    let auto_index = connect_auto_index_manager(&config).await;

    let (bcast_tx, _bcast_rx) = broadcast::channel::<String>(1024);

    if config.backup_enabled {
        let backup_config = config.clone();
        let mongo_for_backup = mongo.clone();

        tokio::spawn(async move {
            backup_agent_loop(mongo_for_backup, backup_config).await;
        });
    } else {
        log_line(
            LogLevel::Info,
            "backup_agent_disabled",
            Some(json!({ "backup_enabled": false })),
        );
    }

    // Auto-index agent
    if let (Some(idx), Some(ai)) = (index_mgr.clone(), auto_index.clone()) {
        let mongo_for_ai = mongo.clone();
        tokio::spawn(async move {
            auto_index_agent_loop(mongo_for_ai, idx, ai).await;
        });
    }

    let mongo_for_ws = mongo.clone();
    let request_cache_for_ws = request_cache.clone();
    let doc_cache_for_ws = doc_cache.clone();
    let index_mgr_for_ws = index_mgr.clone();
    let auto_index_for_ws = auto_index.clone();

    let ws = warp::path("ws")
        .and(warp::ws())
        .and(warp::any().map(move || mongo_for_ws.clone()))
        .and(warp::any().map(move || bcast_tx.clone()))
        .and(warp::any().map(move || request_cache_for_ws.clone()))
        .and(warp::any().map(move || doc_cache_for_ws.clone()))
        .and(warp::any().map(move || index_mgr_for_ws.clone()))
        .and(warp::any().map(move || auto_index_for_ws.clone()))
        .and_then(
            |ws: warp::ws::Ws,
             mongo: MongoState,
             bcast: Broadcaster,
             request_cache: Option<RequestCache>,
             doc_cache: Option<RedisDocumentCache>,
             index_mgr: Option<RedisIndexManager>,
             auto_index: Option<AutoIndexManager>| async move {
                Ok::<_, warp::Rejection>(
                    ws.on_upgrade(move |socket| handle_socket(
                        socket, mongo, bcast,
                        request_cache, doc_cache, index_mgr, auto_index
                    )),
                )
            },
        );

    let event = warp::path("event")
        .and(warp::post())
        .and(warp::body::json())
        .map(|payload: Value| {
            log_line(
                LogLevel::Debug,
                "frontend_debug_event",
                Some(json!({ "source": "index_html", "payload": payload })),
            );
            warp::reply::with_status(String::new(), StatusCode::NO_CONTENT)
        });

    let index = warp::path::end().map(|| {
        warp::reply::html(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/index.html"
        )))
    });

    let routes = ws.or(event).or(index);
    let host = [0, 0, 0, 0];

    println!("MongoDB server connected");
    if doc_cache.is_some() {
        println!("Redis document cache: ENABLED");
    }
    if index_mgr.is_some() {
        println!("Redis index manager: ENABLED");
    }
    if auto_index.is_some() {
        println!("Auto-index manager: ENABLED");
    }

    let local_ip = get_local_ip();

    match tls {
        Some((cert_path, key_path)) => {
            println!("JSON DB Server running on https://{}:{}", local_ip, config.port);
            println!("WebSocket endpoint: wss://{}:{}/ws", local_ip, config.port);
            warp::serve(routes)
                .tls()
                .cert_path(cert_path)
                .key_path(key_path)
                .run((host, config.port))
                .await;
        }
        None => {
            println!("JSON DB Server running on http://{}:{}", local_ip, config.port);
            println!("WebSocket endpoint: ws://{}:{}/ws", local_ip, config.port);
            println!(
                "HTTPS/WSS not enabled: set {} and {} to activate it",
                ENV_SSL_CERT_PATH, ENV_SSL_KEY_PATH
            );
            warp::serve(routes).run((host, config.port)).await;
        }
    }
}