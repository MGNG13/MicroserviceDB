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

mod auth;
use auth::{Auth, Principal};

use chrono::{DateTime, Local, Timelike};
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::{SinkExt, StreamExt};
use mongodb::{
    bson::{doc, Bson, Document},
    options::{Acknowledgment, ClientOptions, CollectionOptions, IndexOptions, WriteConcern},
    Client,
    IndexModel,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Number, Value};
use tokio::fs;
use tokio::process::Command;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration, interval};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tonic::transport::Server as TonicServer;
use tonic::{Request, Response, Status};
use warp::http::StatusCode;
use warp::ws::{Message, WebSocket};
use warp::Filter;

pub mod pb {
    tonic::include_proto!("microservicedb");
}

use pb::database_service_server::{DatabaseService, DatabaseServiceServer};
use pb::{
    ActionRequest, ActionResponse, ChangeEvent, DocumentBatch, ExportCollectionRequest,
    StreamChangesRequest,
};

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
const DEFAULT_AUTOINDEX_REVIEW_SECS: u64 = 600;
const DEFAULT_AUTOINDEX_MIN_DOCS: u64 = 1000;
const DEFAULT_AUTOINDEX_SAMPLE_SIZE: u64 = 1000;
const DEFAULT_AUTOINDEX_MAX_MATCH_RATIO: f64 = 0.10;
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
const ENV_PROFILE: &str = "MICROSERVICEDB_PROFILE";
const ENV_GRPC_PORT: &str = "MICROSERVICEDB_GRPC_PORT";
const ENV_MONGODB_CONNECT_TIMEOUT_SECS: &str = "MICROSERVICEDB_MONGODB_CONNECT_TIMEOUT_SECS";
const ENV_MONGODB_SERVER_SELECTION_TIMEOUT_SECS: &str =
    "MICROSERVICEDB_MONGODB_SERVER_SELECTION_TIMEOUT_SECS";
const ENV_MONGODB_MAX_POOL_SIZE: &str = "MICROSERVICEDB_MONGODB_MAX_POOL_SIZE";
const DEFAULT_BACKUP_INTERVAL_MINS: u64 = 30;
const EXPORT_STREAM_BATCH_SIZE: usize = 500;
const EXPORT_STREAM_BATCH_BYTES: usize = 1024 * 1024;

const BOOTSTRAP_COLLECTION: &str = "__mdb_placeholder_empty__";

// =============================================================================
// PERFILES DE EJECUCIÓN (MICROSERVICEDB_PROFILE)
// =============================================================================

/// Perfil que fija los valores por defecto de conexión a MongoDB, respaldos y logs.
/// Cualquier variable de entorno explícita tiene prioridad sobre el perfil.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Profile {
    #[default]
    Production,
    Local,
    Dev,
    Custom,
}

impl Profile {
    fn from_env() -> Self {
        match env_string(ENV_PROFILE) {
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "prod" | "production" => Profile::Production,
                "local" => Profile::Local,
                "dev" | "development" => Profile::Dev,
                _ => Profile::Custom,
            },
            None => Profile::default(),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Profile::Production => "production",
            Profile::Local => "local",
            Profile::Dev => "dev",
            Profile::Custom => "custom",
        }
    }
}

#[derive(Debug, Clone)]
struct TuningDefaults {
    mongodb_connect_timeout_secs: u64,
    mongodb_server_selection_timeout_secs: u64,
    mongodb_max_pool_size: u32,
    backup_interval_mins_default: u64,
    log_level_default: &'static str,
}

impl TuningDefaults {
    fn from_profile(p: Profile) -> Self {
        match p {
            Profile::Production | Profile::Custom => TuningDefaults {
                mongodb_connect_timeout_secs: 300,
                mongodb_server_selection_timeout_secs: 300,
                mongodb_max_pool_size: 20,
                backup_interval_mins_default: DEFAULT_BACKUP_INTERVAL_MINS,
                log_level_default: "info",
            },
            Profile::Local => TuningDefaults {
                mongodb_connect_timeout_secs: 30,
                mongodb_server_selection_timeout_secs: 15,
                mongodb_max_pool_size: 10,
                backup_interval_mins_default: DEFAULT_BACKUP_INTERVAL_MINS,
                log_level_default: "debug",
            },
            Profile::Dev => TuningDefaults {
                mongodb_connect_timeout_secs: 10,
                mongodb_server_selection_timeout_secs: 5,
                mongodb_max_pool_size: 5,
                backup_interval_mins_default: DEFAULT_BACKUP_INTERVAL_MINS,
                log_level_default: "trace",
            },
        }
    }
}

fn env_u64_or(name: &str, default: u64) -> Result<u64, String> {
    match env_string(name) {
        Some(raw) => parse_u64(&raw, name),
        None => Ok(default),
    }
}

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
        let td = TuningDefaults::from_profile(Profile::from_env());
        let level = env::var(ENV_LOG_LEVEL)
            .ok()
            .and_then(|s| parse_log_level(&s))
            .or_else(|| parse_log_level(td.log_level_default))
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
    profile: Profile,
    port: u16,
    grpc_port: u16,
    mongodb_connect_timeout_secs: u64,
    mongodb_server_selection_timeout_secs: u64,
    mongodb_max_pool_size: u32,
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
    autoindex_review_secs: u64,
    autoindex_min_docs: u64,
    autoindex_sample_size: u64,
    autoindex_max_match_ratio: f64,
}

fn load_app_config() -> Result<AppConfig, String> {
    let cli = parse_cli_args()?;
    let profile = Profile::from_env();
    let td = TuningDefaults::from_profile(profile);
    let port = match cli.port {
        Some(value) => value,
        None => match env_string(ENV_PORT) {
            Some(value) => parse_port(&value, ENV_PORT)?,
            None => DEFAULT_PORT,
        },
    };
    // gRPC escucha en su propio puerto: por defecto el siguiente al HTTP/WebSocket.
    let grpc_port = match env_string(ENV_GRPC_PORT) {
        Some(value) => parse_port(&value, ENV_GRPC_PORT)?,
        None => port
            .checked_add(1)
            .ok_or_else(|| format!("Cannot derive gRPC port from {port}; set {ENV_GRPC_PORT}"))?,
    };
    if grpc_port == port {
        return Err(format!("{ENV_GRPC_PORT} must differ from the HTTP/WebSocket port ({port})"));
    }

    let mongodb_connect_timeout_secs =
        env_u64_or(ENV_MONGODB_CONNECT_TIMEOUT_SECS, td.mongodb_connect_timeout_secs)?;
    let mongodb_server_selection_timeout_secs = env_u64_or(
        ENV_MONGODB_SERVER_SELECTION_TIMEOUT_SECS,
        td.mongodb_server_selection_timeout_secs,
    )?;
    let mongodb_max_pool_size = env_u64_or(ENV_MONGODB_MAX_POOL_SIZE, td.mongodb_max_pool_size as u64)?
        .clamp(1, u32::MAX as u64) as u32;

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
    let autoindex_review_secs =
        env_u64_or("MICROSERVICEDB_AUTOINDEX_REVIEW_SECS", DEFAULT_AUTOINDEX_REVIEW_SECS)?.max(60);
    let autoindex_min_docs = env_u64_or("MICROSERVICEDB_AUTOINDEX_MIN_DOCS", DEFAULT_AUTOINDEX_MIN_DOCS)?;
    let autoindex_sample_size =
        env_u64_or("MICROSERVICEDB_AUTOINDEX_SAMPLE_SIZE", DEFAULT_AUTOINDEX_SAMPLE_SIZE)?.max(1);
    let autoindex_max_match_ratio = match env_string("MICROSERVICEDB_AUTOINDEX_MAX_MATCH_RATIO") {
        Some(raw) => raw
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|v| *v > 0.0 && *v <= 1.0)
            .ok_or_else(|| "MICROSERVICEDB_AUTOINDEX_MAX_MATCH_RATIO must be a number in (0, 1]".to_string())?,
        None => DEFAULT_AUTOINDEX_MAX_MATCH_RATIO,
    };

    let backup_interval_mins = match env_string(ENV_BACKUP_INTERVAL_MINS) {
        Some(value) => {
            let parsed = parse_u64(&value, ENV_BACKUP_INTERVAL_MINS)?;
            if parsed == 0 {
                return Err(format!("{ENV_BACKUP_INTERVAL_MINS} must be greater than 0"));
            }
            Some(parsed)
        }
        None => Some(td.backup_interval_mins_default),
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

    log_line(
        LogLevel::Info,
        "app_profile_loaded",
        Some(json!({
            "profile": profile.name(),
            "port": port,
            "grpc_port": grpc_port,
            "mongodb_connect_timeout_secs": mongodb_connect_timeout_secs,
            "mongodb_server_selection_timeout_secs": mongodb_server_selection_timeout_secs,
            "mongodb_max_pool_size": mongodb_max_pool_size,
            "backup_enabled": cli.backup_enabled,
            "backup_interval_mins": backup_interval_mins,
        })),
    );

    Ok(AppConfig {
        profile,
        port,
        grpc_port,
        mongodb_connect_timeout_secs,
        mongodb_server_selection_timeout_secs,
        mongodb_max_pool_size,
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
        autoindex_review_secs,
        autoindex_min_docs,
        autoindex_sample_size,
        autoindex_max_match_ratio,
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

/// Identidad TLS de gRPC a partir del mismo par certificado/llave PEM de HTTPS.
fn grpc_tls_identity() -> Result<Option<tonic::transport::Identity>, String> {
    let Some((cert, key)) = tls_config()? else {
        return Ok(None);
    };
    let cert_pem = std::fs::read(&cert).map_err(|e| format!("Could not read TLS cert '{cert}': {e}"))?;
    let key_pem = std::fs::read(&key).map_err(|e| format!("Could not read TLS key '{key}': {e}"))?;
    Ok(Some(tonic::transport::Identity::from_pem(cert_pem, key_pem)))
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
        Some(json!({
            "uri": uri,
            "profile": config.profile.name(),
            "connect_timeout_secs": config.mongodb_connect_timeout_secs,
            "server_selection_timeout_secs": config.mongodb_server_selection_timeout_secs,
            "max_pool_size": config.mongodb_max_pool_size,
        })),
    );

    let mut client_options = ClientOptions::parse(uri)
        .await
        .map_err(|e| format!("Failed to parse MongoDB URI: {e}"))?;
    client_options.connect_timeout = Some(Duration::from_secs(config.mongodb_connect_timeout_secs));
    client_options.server_selection_timeout =
        Some(Duration::from_secs(config.mongodb_server_selection_timeout_secs));
    client_options.max_pool_size = Some(config.mongodb_max_pool_size);

    let client = Client::with_options(client_options)
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
        format!("{}:{}:{}:{}", self.prefix, cache_key_segment(db), cache_key_segment(collection), cache_key_segment(doc_id))
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

// Hoy las escrituras solo mantienen el índice de _id (Tag); el resto de tipos
// se conservan para el borrado de claves existentes.
#[allow(dead_code)]
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

    /// Elimina todos los índices de una colección.
    async fn drop_collection_indexes(&self, db: &str, collection: &str) {
        let mut conn = self.connection.clone();
        let pattern = format!("{}:{}:{}:*", self.prefix, cache_key_segment(db), cache_key_segment(collection));
        let _ = clear_keys(&mut conn, &pattern).await;
    }

    /// Elimina los índices de un solo campo (numeric, tag y string).
    async fn drop_field_indexes(&self, db: &str, collection: &str, field: &str) {
        let mut conn = self.connection.clone();
        let pattern = format!(
            "{}:{}:{}:{}:*",
            self.prefix,
            cache_key_segment(db),
            cache_key_segment(collection),
            cache_key_segment(field)
        );
        let _ = clear_keys(&mut conn, &pattern).await;
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

struct QueryPlanAnalyzer;

impl QueryPlanAnalyzer {

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

/// Prefijo de nombre de los índices que crea el autoindexado en MongoDB. Solo se
/// eliminan índices con este prefijo: los índices creados a mano nunca se tocan.
const AUTOINDEX_NAME_PREFIX: &str = "mdb_auto_";

/// Metadatos de un índice gestionado por el autoindexado (clave `...:exists` en Redis).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct AutoIndexMeta {
    /// Nombre del índice en MongoDB.
    name: String,
    /// Epoch (s) en que se creó o adoptó.
    created: u64,
    /// `true` si el índice ya existía en MongoDB con otro nombre (creado a mano):
    /// se usa para Redis, pero jamás se elimina de MongoDB.
    #[serde(default)]
    external: bool,
}

fn hex_decode(raw: &str) -> Option<String> {
    if raw.len() % 2 != 0 || !raw.is_ascii() {
        return None;
    }
    let bytes = (0..raw.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&raw[i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>()
        .ok()?;
    String::from_utf8(bytes).ok()
}

#[derive(Clone)]
struct AutoIndexManager {
    connection: redis::aio::MultiplexedConnection,
    /// Consultas sobre un campo necesarias para crear su índice.
    threshold: u64,
    /// Ventana (s) del contador de consultas.
    window_secs: u64,
    /// Un índice sin consultas durante este tiempo (s) se elimina.
    drop_idle_secs: u64,
    /// Cada cuánto (s) se revisan eficiencia, inactividad e índices huérfanos.
    review_secs: u64,
    /// Colecciones con menos documentos no justifican un índice.
    min_docs: u64,
    /// Documentos muestreados para medir la selectividad.
    sample_size: u64,
    /// Fracción esperada de la colección que devuelve una igualdad sobre el campo;
    /// por encima de este valor el índice no es eficiente.
    max_match_ratio: f64,
    prefix: String,
}

impl AutoIndexManager {
    fn key(&self, db: &str, collection: &str, field: &str, suffix: &str) -> String {
        format!(
            "{}:{}:{}:{}:{}",
            self.prefix,
            cache_key_segment(db),
            cache_key_segment(collection),
            cache_key_segment(field),
            suffix
        )
    }

    /// Descompone `prefix:db:col:field:suffix` en (db, col, field).
    fn parse_key(&self, key: &str) -> Option<(String, String, String)> {
        let rest = key.strip_prefix(&self.prefix)?.strip_prefix(':')?;
        let mut parts = rest.split(':');
        let db = hex_decode(parts.next()?)?;
        let col = hex_decode(parts.next()?)?;
        let field = hex_decode(parts.next()?)?;
        Some((db, col, field))
    }

    fn is_eligible_field(field: &str) -> bool {
        !field.is_empty() && field != "_id" && !field.starts_with('$') && !field.contains(".$")
    }

    fn index_name(field: &str) -> String {
        format!("{AUTOINDEX_NAME_PREFIX}{field}")
    }

    async fn scan_keys(&self, pattern: &str) -> Vec<String> {
        let mut conn = self.connection.clone();
        let mut cursor = "0".to_string();
        let mut out = Vec::new();
        loop {
            let (next, keys): (String, Vec<String>) = match redis::cmd("SCAN")
                .arg(&cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut conn)
                .await
            {
                Ok(r) => r,
                Err(_) => break,
            };
            out.extend(keys);
            if next == "0" {
                break;
            }
            cursor = next;
        }
        out
    }

    async fn get_meta(&self, db: &str, collection: &str, field: &str) -> Option<AutoIndexMeta> {
        let mut conn = self.connection.clone();
        let raw: Option<String> = conn.get(self.key(db, collection, field, "exists")).await.ok()?;
        let raw = raw?;
        if raw == "1" {
            // Formato anterior: el índice se creó con el nombre por defecto `<campo>_1`.
            // Se le da un periodo de gracia completo desde ahora.
            return Some(AutoIndexMeta { name: format!("{field}_1"), created: now_ms() / 1000, external: false });
        }
        serde_json::from_str(&raw).ok()
    }

    async fn set_meta(&self, db: &str, collection: &str, field: &str, meta: &AutoIndexMeta) {
        let mut conn = self.connection.clone();
        let raw = serde_json::to_string(meta).unwrap_or_default();
        let _: Result<(), _> = conn.set(self.key(db, collection, field, "exists"), raw).await;
    }

    /// Borra todos los metadatos del campo y su índice secundario en Redis.
    async fn forget_field(&self, index_mgr: &RedisIndexManager, db: &str, collection: &str, field: &str) {
        let mut conn = self.connection.clone();
        let _: Result<(), _> = redis::cmd("DEL")
            .arg(self.key(db, collection, field, "exists"))
            .arg(self.key(db, collection, field, "queries"))
            .arg(self.key(db, collection, field, "last_used"))
            .arg(self.key(db, collection, field, "rejected"))
            .query_async(&mut conn)
            .await;
        index_mgr.drop_field_indexes(db, collection, field).await;
    }

    /// Estado de un campo para el panel: metadatos, consultas en la ventana y último uso.
    async fn describe_field(&self, db: &str, collection: &str, field: &str) -> Value {
        let mut conn = self.connection.clone();
        let meta = self.get_meta(db, collection, field).await;
        let queries: u64 = conn
            .get::<_, Option<u64>>(self.key(db, collection, field, "queries"))
            .await
            .ok()
            .flatten()
            .unwrap_or(0);
        let last_used: Option<u64> = conn
            .get::<_, Option<String>>(self.key(db, collection, field, "last_used"))
            .await
            .ok()
            .flatten()
            .and_then(|v| v.parse().ok());
        json!({
            "managed": meta.is_some(),
            "created": meta.as_ref().map(|m| m.created),
            "external": meta.as_ref().map(|m| m.external).unwrap_or(false),
            "queries_in_window": queries,
            "window_secs": self.window_secs,
            "threshold": self.threshold,
            "last_used": last_used,
            "drop_idle_secs": self.drop_idle_secs,
            "review_secs": self.review_secs
        })
    }

    /// Registra que un campo fue usado en una consulta.
    async fn record_query_field(&self, db: &str, collection: &str, field: &str) {
        if !Self::is_eligible_field(field) {
            return;
        }
        let mut conn = self.connection.clone();
        let stats_key = self.key(db, collection, field, "queries");
        let last_key = self.key(db, collection, field, "last_used");
        let now = now_ms() / 1000;
        // `last_used` vive más que `drop_idle_secs` para que la revisión pueda
        // ver que el índice lleva inactivo más de ese tiempo.
        let last_ttl = self.drop_idle_secs.saturating_mul(2).max(self.window_secs).max(60);

        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.incr(&stats_key, 1);
        pipe.expire(&stats_key, self.window_secs as i64);
        pipe.set(&last_key, now.to_string());
        pipe.expire(&last_key, last_ttl as i64);
        let _: Result<(), _> = pipe.query_async(&mut conn).await;
    }

    /// Mide si un índice sobre `field` sería eficiente con los datos actuales.
    /// Devuelve `Ok(Some(motivo))` si no lo es; `Err` si MongoDB no pudo responder
    /// (en ese caso no se toma ninguna decisión).
    async fn evaluate_efficiency(
        &self,
        mongo: &MongoConnectionInfo,
        db: &str,
        collection: &str,
        field: &str,
    ) -> Result<Option<String>, String> {
        let col = mongo.client.database(db).collection::<Document>(collection);
        let total = col
            .estimated_document_count()
            .await
            .map_err(|e| format!("count failed: {e}"))?;
        if total < self.min_docs {
            return Ok(Some(format!("collection_too_small ({total} < {})", self.min_docs)));
        }

        let pipeline = vec![
            doc! { "$sample": { "size": self.sample_size.max(1) as i64 } },
            doc! { "$group": { "_id": format!("${field}"), "n": { "$sum": 1_i32 } } },
        ];
        let mut cursor = col
            .aggregate(pipeline)
            .await
            .map_err(|e| format!("sample failed: {e}"))?;
        let mut counts: Vec<f64> = Vec::new();
        let mut sampled = 0f64;
        while let Some(row) = cursor.next().await {
            let row = row.map_err(|e| format!("sample cursor failed: {e}"))?;
            let n = match row.get("n") {
                Some(mongodb::bson::Bson::Int32(v)) => *v as f64,
                Some(mongodb::bson::Bson::Int64(v)) => *v as f64,
                Some(mongodb::bson::Bson::Double(v)) => *v,
                _ => 0.0,
            };
            sampled += n;
            // Campo ausente o null: no aporta a la selectividad.
            if !matches!(row.get("_id"), None | Some(mongodb::bson::Bson::Null)) {
                counts.push(n);
            }
        }

        let present: f64 = counts.iter().sum();
        if sampled == 0.0 || present == 0.0 {
            return Ok(Some("field_absent".to_string()));
        }
        if counts.len() <= 1 {
            return Ok(Some("single_value".to_string()));
        }
        // Fracción esperada de documentos que devuelve una igualdad sobre el campo.
        let expected_match: f64 = counts.iter().map(|n| (n / present).powi(2)).sum();
        if expected_match > self.max_match_ratio {
            return Ok(Some(format!(
                "low_selectivity (match {:.3} > {:.3}, {} valores en {} docs)",
                expected_match,
                self.max_match_ratio,
                counts.len(),
                sampled as u64
            )));
        }
        Ok(None)
    }

    /// Busca en MongoDB un índice de un solo campo sobre `field`; devuelve su nombre.
    async fn find_mongo_index(
        mongo: &MongoConnectionInfo,
        db: &str,
        collection: &str,
        field: &str,
    ) -> Result<Option<String>, String> {
        let mut cursor = mongo
            .client
            .database(db)
            .collection::<Document>(collection)
            .list_indexes()
            .await
            .map_err(|e| format!("list_indexes failed: {e}"))?;
        while let Some(model) = cursor.next().await {
            let model = model.map_err(|e| format!("list_indexes cursor failed: {e}"))?;
            if model.keys.len() == 1 && model.keys.contains_key(field) {
                let name = model.options.and_then(|o| o.name).unwrap_or_default();
                return Ok(Some(name));
            }
        }
        Ok(None)
    }

    /// Crea índices para los campos que superaron el umbral de consultas.
    async fn check_and_create_indexes(
        &self,
        mongo: &MongoConnectionInfo,
        db: &str,
        collection: &str,
    ) {
        let pattern = format!(
            "{}:{}:{}:*:queries",
            self.prefix,
            cache_key_segment(db),
            cache_key_segment(collection)
        );
        let mut conn = self.connection.clone();
        for key in self.scan_keys(&pattern).await {
            let Some((_, _, field)) = self.parse_key(&key) else { continue };
            if !Self::is_eligible_field(&field) {
                continue;
            }
            let count: u64 = conn.get(&key).await.unwrap_or(0);
            if count < self.threshold {
                continue;
            }
            let exists: bool = conn.exists(self.key(db, collection, &field, "exists")).await.unwrap_or(true);
            let rejected: bool = conn.exists(self.key(db, collection, &field, "rejected")).await.unwrap_or(true);
            if exists || rejected {
                continue;
            }
            self.create_index(mongo, db, collection, &field).await;
        }
    }

    /// Crea el índice en MongoDB si es eficiente.
    async fn create_index(
        &self,
        mongo: &MongoConnectionInfo,
        db: &str,
        collection: &str,
        field: &str,
    ) {
        match self.evaluate_efficiency(mongo, db, collection, field).await {
            Ok(Some(reason)) => {
                log_line(
                    LogLevel::Info,
                    "autoindex_rejected",
                    Some(json!({ "database_name": db, "collection": collection, "field": field, "reason": reason })),
                );
                // No se vuelve a evaluar hasta la siguiente revisión.
                let mut conn = self.connection.clone();
                let _: Result<(), _> = conn
                    .set_ex(self.key(db, collection, field, "rejected"), reason, self.review_secs.max(60))
                    .await;
                return;
            }
            Ok(None) => {}
            Err(e) => {
                log_line(
                    LogLevel::Warn,
                    "autoindex_evaluate_failed",
                    Some(json!({ "database_name": db, "collection": collection, "field": field, "error": e })),
                );
                return;
            }
        }

        let now = now_ms() / 1000;
        let meta = match Self::find_mongo_index(mongo, db, collection, field).await {
            Ok(Some(existing)) => AutoIndexMeta {
                external: !existing.starts_with(AUTOINDEX_NAME_PREFIX),
                name: existing,
                created: now,
            },
            Ok(None) => {
                let name = Self::index_name(field);
                let model = IndexModel::builder()
                    .keys(doc! { field: 1_i32 })
                    .options(IndexOptions::builder().name(name.clone()).build())
                    .build();
                if let Err(e) = mongo
                    .client
                    .database(db)
                    .collection::<Document>(collection)
                    .create_index(model)
                    .await
                {
                    log_line(
                        LogLevel::Warn,
                        "autoindex_mongo_failed",
                        Some(json!({ "database_name": db, "collection": collection, "field": field, "error": e.to_string() })),
                    );
                    return;
                }
                AutoIndexMeta { name, created: now, external: false }
            }
            Err(e) => {
                log_line(
                    LogLevel::Warn,
                    "autoindex_mongo_failed",
                    Some(json!({ "database_name": db, "collection": collection, "field": field, "error": e })),
                );
                return;
            }
        };

        log_line(
            LogLevel::Info,
            "autoindex_create",
            Some(json!({
                "database_name": db,
                "collection": collection,
                "field": field,
                "index_name": meta.name,
                "external": meta.external
            })),
        );
        self.set_meta(db, collection, field, &meta).await;
    }

    /// Revisión periódica de una colección:
    /// 1. adopta índices `mdb_auto_*` de MongoDB sin metadatos (p. ej. tras reiniciar
    ///    Dragonfly, que no persiste, o tras renombrar la colección);
    /// 2. elimina índices sin uso durante `drop_idle_secs`;
    /// 3. elimina índices que ya no son eficientes con los datos actuales;
    /// 4. olvida metadatos de índices que ya no existen en MongoDB.
    async fn review_collection(
        &self,
        mongo: &MongoConnectionInfo,
        index_mgr: &RedisIndexManager,
        db: &str,
        collection: &str,
    ) {
        let now = now_ms() / 1000;

        // Índices actuales en MongoDB de un solo campo: nombre -> campo.
        let mut mongo_indexes: HashMap<String, String> = HashMap::new();
        match mongo.client.database(db).collection::<Document>(collection).list_indexes().await {
            Ok(mut cursor) => {
                while let Some(model) = cursor.next().await {
                    let Ok(model) = model else { return };
                    if model.keys.len() != 1 {
                        continue;
                    }
                    let Some(field) = model.keys.keys().next().cloned() else { continue };
                    let name = model.options.and_then(|o| o.name).unwrap_or_default();
                    mongo_indexes.insert(name, field);
                }
            }
            Err(e) => {
                log_line(
                    LogLevel::Warn,
                    "autoindex_review_failed",
                    Some(json!({ "database_name": db, "collection": collection, "error": e.to_string() })),
                );
                return;
            }
        }

        // 1. Adopción.
        for (name, field) in &mongo_indexes {
            if name.starts_with(AUTOINDEX_NAME_PREFIX) && self.get_meta(db, collection, field).await.is_none() {
                log_line(
                    LogLevel::Info,
                    "autoindex_adopt",
                    Some(json!({ "database_name": db, "collection": collection, "field": field, "index_name": name })),
                );
                self.set_meta(db, collection, field, &AutoIndexMeta { name: name.clone(), created: now, external: false })
                    .await;
            }
        }

        // 2-4. Revisión de cada índice gestionado.
        let pattern = format!(
            "{}:{}:{}:*:exists",
            self.prefix,
            cache_key_segment(db),
            cache_key_segment(collection)
        );
        let mut conn = self.connection.clone();
        for key in self.scan_keys(&pattern).await {
            let Some((_, _, field)) = self.parse_key(&key) else { continue };
            let Some(mut meta) = self.get_meta(db, collection, &field).await else { continue };
            let raw: Option<String> = conn.get(&key).await.unwrap_or(None);
            if raw.as_deref() == Some("1") {
                // Migra el formato anterior.
                self.set_meta(db, collection, &field, &meta).await;
            }

            if !mongo_indexes.contains_key(&meta.name) {
                if meta.external {
                    // El índice manual sobre el campo pudo renombrarse: buscarlo de nuevo.
                    if let Some(name) = mongo_indexes.iter().find(|(_, f)| **f == field).map(|(n, _)| n.clone()) {
                        meta.name = name;
                        self.set_meta(db, collection, &field, &meta).await;
                        continue;
                    }
                }
                log_line(
                    LogLevel::Info,
                    "autoindex_forget_missing",
                    Some(json!({ "database_name": db, "collection": collection, "field": field, "index_name": meta.name })),
                );
                self.forget_field(index_mgr, db, collection, &field).await;
                continue;
            }

            let last_used: u64 = conn
                .get::<_, Option<String>>(self.key(db, collection, &field, "last_used"))
                .await
                .ok()
                .flatten()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let last_activity = last_used.max(meta.created);
            if now.saturating_sub(last_activity) > self.drop_idle_secs {
                let idle = now.saturating_sub(last_activity);
                self.drop_index(mongo, index_mgr, db, collection, &field, &meta, &format!("idle {idle}s"))
                    .await;
                continue;
            }

            match self.evaluate_efficiency(mongo, db, collection, &field).await {
                Ok(Some(reason)) => {
                    self.drop_index(mongo, index_mgr, db, collection, &field, &meta, &reason).await;
                    // Evita recrearlo en el siguiente minuto.
                    let _: Result<(), _> = conn
                        .set_ex(self.key(db, collection, &field, "rejected"), reason, self.review_secs.max(60))
                        .await;
                }
                Ok(None) => {}
                Err(e) => log_line(
                    LogLevel::Warn,
                    "autoindex_evaluate_failed",
                    Some(json!({ "database_name": db, "collection": collection, "field": field, "error": e })),
                ),
            }
        }
    }

    /// Elimina un índice gestionado. Los índices externos (creados a mano) se
    /// quedan en MongoDB; solo se olvidan sus metadatos y su índice en Redis.
    #[allow(clippy::too_many_arguments)]
    async fn drop_index(
        &self,
        mongo: &MongoConnectionInfo,
        index_mgr: &RedisIndexManager,
        db: &str,
        collection: &str,
        field: &str,
        meta: &AutoIndexMeta,
        reason: &str,
    ) {
        log_line(
            LogLevel::Info,
            "autoindex_drop",
            Some(json!({
                "database_name": db,
                "collection": collection,
                "field": field,
                "index_name": meta.name,
                "external": meta.external,
                "reason": reason
            })),
        );
        if !meta.external {
            if let Err(e) = mongo
                .client
                .database(db)
                .collection::<Document>(collection)
                .drop_index(meta.name.clone())
                .await
            {
                log_line(
                    LogLevel::Warn,
                    "autoindex_drop_failed",
                    Some(json!({ "database_name": db, "collection": collection, "field": field, "error": e.to_string() })),
                );
                return;
            }
        }
        self.forget_field(index_mgr, db, collection, field).await;
    }

    /// Borra metadatos e índices Redis de bases de datos o colecciones que ya no existen.
    async fn sweep_orphans(&self, index_mgr: &RedisIndexManager, existing: &HashSet<(String, String)>) {
        let pattern = format!("{}:*:exists", self.prefix);
        let mut dropped_collections: HashSet<(String, String)> = HashSet::new();
        for key in self.scan_keys(&pattern).await {
            let Some((db, col, field)) = self.parse_key(&key) else { continue };
            let pair = (db, col);
            if existing.contains(&pair) {
                continue;
            }
            log_line(
                LogLevel::Info,
                "autoindex_forget_orphan",
                Some(json!({ "database_name": pair.0, "collection": pair.1, "field": field })),
            );
            self.forget_field(index_mgr, &pair.0, &pair.1, &field).await;
            if dropped_collections.insert(pair.clone()) {
                index_mgr.drop_collection_indexes(&pair.0, &pair.1).await;
            }
        }
    }
}

/// Reconstruye completamente los índices secundarios en Redis y repuebla el cache
/// de documentos para una colección. Pensado para operaciones de import donde el
/// estado de Redis puede quedar inconsistente respecto de MongoDB.
async fn rebuild_collection_redis_state(
    db_name: &str,
    col_name: &str,
    docs: &[Value],
    index_mgr: &Option<RedisIndexManager>,
    doc_cache: &Option<RedisDocumentCache>,
) {
    // 1. Invalidar + borrar todo lo existente
    if let Some(idx) = index_mgr {
        idx.drop_collection_indexes(db_name, col_name).await;
    }
    if let Some(cache) = doc_cache {
        cache.invalidate_collection(db_name, col_name).await;
    }

    if docs.is_empty() {
        return;
    }

    // 2. Repoblar cache de documentos (hash individual por doc)
    if let Some(cache) = doc_cache {
        for doc in docs {
            if let Some(id_val) = doc.get("_id") {
                let id = json_to_string(id_val);
                cache.set_doc(db_name, col_name, &id, doc).await;
            }
        }
    }

    log_line(
        LogLevel::Info,
        "rebuild_collection_indexes_done",
        Some(json!({
            "database_name": db_name,
            "collection": col_name,
            "documents": docs.len()
        })),
    );
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
    // `_id: 0` es compatible con una proyección de inclusión (como en MongoDB).
    let has_excludes = keys.iter().filter(|k| k.as_str() != "_id" || !has_includes).any(|k| {
        obj.get(*k)
            .map(|v| v.as_bool() == Some(false) || v.as_i64() == Some(0))
            .unwrap_or(false)
    });

    if has_includes && !has_excludes {
        let mut out = Value::Object(Map::new());
        let include_id = obj
            .get("_id")
            .map(|v| !(v.as_bool() == Some(false) || v.as_i64() == Some(0)))
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
                let pattern = format!("{}:{}:*", INDEX_PREFIX, cache_key_segment(db_name));
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
            if let Some(idx) = index_mgr {
                let pattern = format!("{}:{}:*", INDEX_PREFIX, cache_key_segment(from));
                let mut conn = idx.connection.clone();
                let _ = clear_keys(&mut conn, &pattern).await;
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
            if let Some(idx) = index_mgr {
                idx.drop_collection_indexes(db_name, from).await;
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
            let merge = mode == "merge";
            if existed && !merge {
                // replace: las colecciones que no vienen en el import se eliminan.
                for existing in list_collection_names(mongo, db_name).await? {
                    if !incoming.contains_key(&existing) {
                        write_collection(mongo, db_name, &existing)
                            .drop()
                            .await
                            .map_err(|e| format!("Could not drop collection '{db_name}.{existing}': {e}"))?;
                    }
                }
            }

            let mut imported_collections = 0usize;
            let mut imported_docs = 0usize;
            for (col, docs) in &incoming {
                imported_collections += 1;
                imported_docs += docs.len();
                let refs: Vec<&Value> = docs.iter().collect();
                let coll = write_collection(mongo, db_name, col);
                import_into_collection(&coll, db_name, col, if merge { "merge" } else { "replace" }, &refs)
                    .await?;
            }
            if incoming.is_empty() {
                ensure_database_exists(mongo, db_name).await?;
            } else {
                let _ = mongo
                    .client
                    .database(db_name)
                    .collection::<Document>(BOOTSTRAP_COLLECTION)
                    .drop()
                    .await;
            }

            // Invalidación y Write-Through de lo importado por colección
            if doc_cache.is_some() || index_mgr.is_some() {
                if !merge {
                    if let Some(cache) = doc_cache {
                        cache.invalidate_database(db_name).await;
                    }
                }
                for (col_name, docs) in &incoming {
                    rebuild_collection_redis_state(
                        db_name,
                        col_name,
                        docs,
                        index_mgr,
                        doc_cache,
                    ).await;
                }
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

// -----------------------------------------------------------------------------
// Traducción de filtros a MongoDB
//
// Las consultas se resuelven en MongoDB (que usa sus índices, incluidos los
// autoíndices). La traducción conserva la semántica que tenía `match_filter`:
// - un texto de 24 hex o {"$oid": ...} coincide tanto con ObjectId como con el texto;
// - `$exists: true` exige además que el valor no sea null;
// - `$nin` no coincide con documentos sin el campo;
// - `$not` en el nivel superior equivale a `$nor`.
// -----------------------------------------------------------------------------

/// Claves de JSON extendido que representan un valor (no un operador de consulta).
const EXTENDED_JSON_KEYS: &[&str] = &[
    "$oid",
    "$date",
    "$numberLong",
    "$numberInt",
    "$numberDouble",
    "$numberDecimal",
    "$binary",
    "$timestamp",
    "$regularExpression",
    "$symbol",
    "$code",
    "$minKey",
    "$maxKey",
    "$undefined",
];

fn is_extended_json_value(v: &Value) -> bool {
    v.as_object()
        .map(|o| !o.is_empty() && o.keys().any(|k| EXTENDED_JSON_KEYS.contains(&k.as_str())))
        .unwrap_or(false)
}

/// Variantes con las que un valor ObjectId puede estar guardado.
fn object_id_variants(v: &Value) -> Option<Vec<Value>> {
    let hex = value_as_object_id_text(v)?;
    let mut out = vec![json!({ "$oid": hex }), Value::String(hex.clone())];
    if let Value::String(original) = v {
        if *original != hex {
            out.push(v.clone());
        }
    }
    Some(out)
}

fn expand_values(values: &[Value]) -> Vec<Value> {
    let mut out = Vec::with_capacity(values.len());
    for v in values {
        match object_id_variants(v) {
            Some(variants) => out.extend(variants),
            None => out.push(v.clone()),
        }
    }
    out
}

/// Condición que nunca coincide (equivale al `false` de `match_filter`).
fn never_match() -> Value {
    json!({ "$expr": false })
}

fn single_condition(field: &str, cond: Value) -> Value {
    let mut m = Map::new();
    m.insert(field.to_string(), cond);
    Value::Object(m)
}

fn eq_condition(field: &str, v: &Value) -> Value {
    match object_id_variants(v) {
        Some(variants) => single_condition(field, json!({ "$in": variants })),
        None => single_condition(field, v.clone()),
    }
}

fn translate_field_condition(field: &str, qv: &Value, out: &mut Vec<Value>) {
    if !is_operator_object(qv) || is_extended_json_value(qv) {
        out.push(eq_condition(field, qv));
        return;
    }
    let ops = qv.as_object().expect("operator object");
    for (op, ov) in ops {
        match op.as_str() {
            "$eq" => out.push(eq_condition(field, ov)),
            "$ne" => match object_id_variants(ov) {
                Some(variants) => out.push(single_condition(field, json!({ "$nin": variants }))),
                None => out.push(single_condition(field, json!({ "$ne": ov }))),
            },
            "$in" => match ov.as_array() {
                Some(arr) => out.push(single_condition(field, json!({ "$in": expand_values(arr) }))),
                None => out.push(never_match()),
            },
            "$nin" => match ov.as_array() {
                Some(arr) => {
                    out.push(single_condition(field, json!({ "$exists": true })));
                    out.push(single_condition(field, json!({ "$nin": expand_values(arr) })));
                }
                None => out.push(never_match()),
            },
            "$exists" => {
                if ov.as_bool().unwrap_or(true) {
                    out.push(single_condition(field, json!({ "$exists": true, "$ne": Value::Null })));
                } else {
                    out.push(single_condition(field, Value::Null));
                }
            }
            "$regex" => {
                let mut cond = Map::new();
                cond.insert("$regex".into(), Value::String(json_to_string(ov)));
                if let Some(options) = ops.get("$options") {
                    cond.insert("$options".into(), options.clone());
                }
                out.push(single_condition(field, Value::Object(cond)));
            }
            "$options" => {}
            _ => out.push(single_condition(field, json!({ op.clone(): ov }))),
        }
    }
}

/// Traduce un filtro JSON del protocolo a un filtro equivalente de MongoDB.
fn translate_filter(filter: &Value) -> Value {
    let Some(obj) = filter.as_object() else {
        return Value::Object(Map::new());
    };
    let mut conditions: Vec<Value> = Vec::new();
    for (k, v) in obj {
        match k.as_str() {
            "$and" | "$or" | "$nor" => match v.as_array() {
                Some(arr) if !(k == "$or" && arr.is_empty()) => {
                    let items: Vec<Value> = arr.iter().map(translate_filter).collect();
                    if items.is_empty() {
                        // $and/$nor vacíos no restringen nada.
                        continue;
                    }
                    conditions.push(json!({ k.clone(): items }));
                }
                _ => conditions.push(never_match()),
            },
            "$not" => conditions.push(json!({ "$nor": [translate_filter(v)] })),
            _ if k.starts_with('$') => conditions.push(single_condition(k, v.clone())),
            _ => translate_field_condition(k, v, &mut conditions),
        }
    }
    match conditions.len() {
        0 => Value::Object(Map::new()),
        1 => conditions.pop().unwrap(),
        _ => json!({ "$and": conditions }),
    }
}

// =============================================================================
// ESCRITURAS NATIVAS EN MONGODB + WRITE-THROUGH
// =============================================================================

/// Write concern de todas las escrituras. Por defecto `w: majority` con journal (§8.3, §9.3).
/// `MICROSERVICEDB_MONGODB_WRITE_CONCERN`: `majority` (defecto) o un número de nodos.
/// `MICROSERVICEDB_MONGODB_JOURNAL`: `true` (defecto) o `false`.
fn write_concern() -> WriteConcern {
    static WC: OnceLock<WriteConcern> = OnceLock::new();
    WC.get_or_init(|| {
        let w = match env::var("MICROSERVICEDB_MONGODB_WRITE_CONCERN") {
            Ok(v) => match v.trim().parse::<u32>() {
                Ok(n) => Acknowledgment::Nodes(n),
                Err(_) if v.trim().eq_ignore_ascii_case("majority") || v.trim().is_empty() => {
                    Acknowledgment::Majority
                }
                Err(_) => Acknowledgment::Custom(v.trim().to_string()),
            },
            Err(_) => Acknowledgment::Majority,
        };
        let journal = env::var("MICROSERVICEDB_MONGODB_JOURNAL")
            .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
            .unwrap_or(true);
        WriteConcern::builder().w(w).journal(journal).build()
    })
    .clone()
}

fn write_collection(
    mongo: &MongoConnectionInfo,
    db_name: &str,
    col_name: &str,
) -> mongodb::Collection<Document> {
    mongo.client.database(db_name).collection_with_options::<Document>(
        col_name,
        CollectionOptions::builder().write_concern(write_concern()).build(),
    )
}

async fn require_database(mongo: &MongoConnectionInfo, db_name: &str) -> Result<(), String> {
    if database_exists(mongo, db_name).await? {
        Ok(())
    } else {
        Err(format!("Database '{}' not found", db_name))
    }
}

fn value_to_bson(value: &Value) -> Result<Bson, String> {
    let mut d = value_to_document(&json!({ "v": value }))?;
    d.remove("v").ok_or_else(|| "Invalid BSON value".to_string())
}

fn bson_to_value(value: &Bson) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

/// Filtro del protocolo traducido a MongoDB (mismas reglas que `find`).
fn bson_filter(filter: &Value) -> Result<Document, String> {
    value_to_document(&translate_filter(filter))
}

fn id_filter_of(doc: &Value) -> Result<Document, String> {
    let id = doc.get("_id").ok_or("document without _id")?;
    Ok(doc! { "_id": value_to_bson(id)? })
}

async fn find_one_raw(
    coll: &mongodb::Collection<Document>,
    filter: Document,
) -> Result<Option<Value>, String> {
    coll.find_one(filter)
        .await
        .map_err(|e| format!("MongoDB findOne failed: {e}"))?
        .map(document_to_value)
        .transpose()
}

async fn find_many_raw(
    coll: &mongodb::Collection<Document>,
    filter: Document,
) -> Result<Vec<Value>, String> {
    let mut cursor = coll
        .find(filter)
        .await
        .map_err(|e| format!("MongoDB find failed: {e}"))?;
    let mut out = Vec::new();
    while let Some(res) = cursor.next().await {
        out.push(document_to_value(res.map_err(|e| format!("MongoDB cursor error: {e}"))?)?);
    }
    Ok(out)
}

/// Write-Through: tras confirmar la escritura en MongoDB, el documento resultante se guarda en
/// Dragonfly (TTL de la caché de documentos) y se actualiza el índice por `_id`.
async fn write_through_set(
    index_mgr: &Option<RedisIndexManager>,
    doc_cache: &Option<RedisDocumentCache>,
    db_name: &str,
    col_name: &str,
    before: Option<&Value>,
    after: &Value,
) {
    let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
    if let (Some(idx), Some(b)) = (index_mgr, before) {
        idx.unindex_document(db_name, col_name, b, &idx_defs).await;
    }
    if after.is_null() {
        return;
    }
    if let Some(idx) = index_mgr {
        idx.index_document(db_name, col_name, after, &idx_defs).await;
    }
    if let (Some(cache), Some(id)) = (doc_cache, after.get("_id")) {
        cache.set_doc(db_name, col_name, &json_to_string(id), after).await;
    }
}

async fn write_through_delete(
    index_mgr: &Option<RedisIndexManager>,
    doc_cache: &Option<RedisDocumentCache>,
    db_name: &str,
    col_name: &str,
    before: &Value,
) {
    if let Some(idx) = index_mgr {
        let idx_defs = vec![(String::from("_id"), IndexType::Tag)];
        idx.unindex_document(db_name, col_name, before, &idx_defs).await;
    }
    if let (Some(cache), Some(id)) = (doc_cache, before.get("_id")) {
        cache.del_doc(db_name, col_name, &json_to_string(id)).await;
    }
}

/// Inserta o reemplaza por `_id` (upsert). Devuelve (insertados, modificados, documentos escritos).
async fn upsert_by_id(
    coll: &mongodb::Collection<Document>,
    db_name: &str,
    col_name: &str,
    docs: &[&Value],
) -> Result<(u64, u64, Vec<Value>), String> {
    let mut inserted = 0u64;
    let mut modified = 0u64;
    let mut written = Vec::with_capacity(docs.len());
    let mut pending_inserts: Vec<Document> = Vec::new();
    for raw in docs {
        let doc = value_to_document(raw)?;
        match raw.get("_id") {
            Some(id) => {
                let r = coll
                    .replace_one(doc! { "_id": value_to_bson(id)? }, doc)
                    .upsert(true)
                    .await
                    .map_err(|e| format!("MongoDB upsert failed on '{db_name}.{col_name}': {e}"))?;
                if r.matched_count > 0 {
                    modified += 1;
                } else {
                    inserted += 1;
                }
                written.push((*raw).clone());
            }
            None => pending_inserts.push(doc),
        }
    }
    if !pending_inserts.is_empty() {
        let originals: Vec<&Value> = docs.iter().copied().filter(|d| d.get("_id").is_none()).collect();
        let r = coll
            .insert_many(pending_inserts)
            .await
            .map_err(|e| format!("MongoDB insertMany failed on '{db_name}.{col_name}': {e}"))?;
        inserted += r.inserted_ids.len() as u64;
        for (i, raw) in originals.into_iter().enumerate() {
            let mut d = raw.clone();
            if let (Some(obj), Some(id)) = (d.as_object_mut(), r.inserted_ids.get(&i)) {
                obj.insert("_id".into(), bson_to_value(id));
            }
            written.push(d);
        }
    }
    Ok((inserted, modified, written))
}

async fn upsert_documents_by_id(
    coll: &mongodb::Collection<Document>,
    db_name: &str,
    col_name: &str,
    docs: &[&Value],
    index_mgr: &Option<RedisIndexManager>,
    doc_cache: &Option<RedisDocumentCache>,
) -> Result<(u64, u64), String> {
    let (inserted, modified, written) = upsert_by_id(coll, db_name, col_name, docs).await?;
    for after in &written {
        write_through_set(index_mgr, doc_cache, db_name, col_name, None, after).await;
    }
    Ok((inserted, modified))
}

/// Importa documentos a una colección. Modos: `replace` (vacía la colección conservando sus
/// índices), `append` (inserta todo) y `merge` (upsert por `_id`). Devuelve (insertados, modificados).
async fn import_into_collection(
    coll: &mongodb::Collection<Document>,
    db_name: &str,
    col_name: &str,
    mode: &str,
    docs: &[&Value],
) -> Result<(u64, u64), String> {
    match mode {
        "replace" | "append" => {
            if mode == "replace" {
                coll.delete_many(doc! {})
                    .await
                    .map_err(|e| format!("MongoDB could not clear '{db_name}.{col_name}': {e}"))?;
            }
            let mut inserted = 0u64;
            for chunk in docs.chunks(1000) {
                let batch = chunk.iter().map(|d| value_to_document(d)).collect::<Result<Vec<_>, _>>()?;
                let r = coll
                    .insert_many(batch)
                    .await
                    .map_err(|e| format!("MongoDB insertMany failed on '{db_name}.{col_name}': {e}"))?;
                inserted += r.inserted_ids.len() as u64;
            }
            if docs.is_empty() && mode == "replace" {
                // Asegura que la colección exista aunque quede vacía.
                let _ = coll.client().database(db_name).create_collection(col_name).await;
            }
            Ok((inserted, 0))
        }
        _ => {
            let (i, m, _) = upsert_by_id(coll, db_name, col_name, docs).await?;
            Ok((i, m))
        }
    }
}

/// Índices de una colección; una colección que aún no existe no tiene índices.
async fn list_index_models(
    mongo: &MongoConnectionInfo,
    db_name: &str,
    col_name: &str,
) -> Result<Vec<IndexModel>, String> {
    let mut cursor = match mongo
        .client
        .database(db_name)
        .collection::<Document>(col_name)
        .list_indexes()
        .await
    {
        Ok(c) => c,
        Err(e) if e.to_string().contains("NamespaceNotFound") || e.to_string().contains("ns does not exist") => {
            return Ok(Vec::new());
        }
        Err(e) => return Err(format!("MongoDB listIndexes failed on '{db_name}.{col_name}': {e}")),
    };
    let mut out = Vec::new();
    while let Some(model) = cursor.next().await {
        out.push(model.map_err(|e| format!("MongoDB listIndexes cursor error: {e}"))?);
    }
    Ok(out)
}

/// Claves de un índice como lista ordenada `[{field, type}]` (un objeto JSON perdería el orden).
fn index_keys_to_json(keys: &Document) -> Value {
    Value::Array(
        keys.iter()
            .map(|(field, kind)| {
                let kind = match kind {
                    Bson::Int32(n) => json!(n),
                    Bson::Int64(n) => json!(n),
                    Bson::Double(n) => json!(n),
                    Bson::String(s) => json!(s),
                    other => json!(other.to_string()),
                };
                json!({ "field": field, "type": kind })
            })
            .collect(),
    )
}

/// Valida las claves de `createIndex`: lista ordenada `[{field, type}]`, donde `type` es
/// 1, -1, "text", "hashed", "2dsphere" o "2d". Se acepta un objeto si tiene una sola clave.
fn parse_index_keys(raw: &Value) -> Result<Vec<(String, Bson)>, String> {
    let pairs: Vec<(String, Value)> = match raw {
        Value::Array(items) => items
            .iter()
            .map(|it| {
                let field = it.get("field").and_then(|v| v.as_str()).unwrap_or("").trim().to_string();
                let kind = it.get("type").cloned().unwrap_or(json!(1));
                (field, kind)
            })
            .collect(),
        Value::Object(map) if map.len() == 1 => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        Value::Object(_) => {
            return Err("keys with several fields must be an ordered array: [{\"field\":\"a\",\"type\":1}, ...]".into())
        }
        _ => return Err("keys must be an array of {field, type}".into()),
    };
    if pairs.is_empty() {
        return Err("keys must contain at least one field".into());
    }
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(pairs.len());
    for (field, kind) in pairs {
        if field.is_empty() || field.starts_with('$') || field.contains(".$") {
            return Err(format!("Invalid index field '{field}'"));
        }
        if !seen.insert(field.clone()) {
            return Err(format!("Field '{field}' is repeated in the index"));
        }
        let bson = match &kind {
            Value::Number(n) if n.as_i64() == Some(1) => Bson::Int32(1),
            Value::Number(n) if n.as_i64() == Some(-1) => Bson::Int32(-1),
            Value::String(s) if matches!(s.as_str(), "text" | "hashed" | "2dsphere" | "2d") => Bson::String(s.clone()),
            Value::String(s) if s == "1" => Bson::Int32(1),
            Value::String(s) if s == "-1" => Bson::Int32(-1),
            other => {
                return Err(format!(
                    "Invalid index type {other} for '{field}' (1, -1, \"text\", \"hashed\", \"2dsphere\", \"2d\")"
                ))
            }
        };
        out.push((field, bson));
    }
    Ok(out)
}

/// Ejecuta la consulta en MongoDB con filtro, orden, skip y límite del lado del servidor.
async fn find_docs_mongo(
    mongo: &MongoConnectionInfo,
    db_name: &str,
    col_name: &str,
    filter: &Value,
    sort: Option<&Value>,
    limit: Option<usize>,
    skip: usize,
) -> Result<Vec<Value>, String> {
    if limit == Some(0) {
        return Ok(Vec::new());
    }
    if !database_exists(mongo, db_name).await? {
        return Err(format!("Database '{}' not found", db_name));
    }
    let translated = translate_filter(filter);
    let bson_filter = value_to_document(&translated)?;
    let collection = mongo.client.database(db_name).collection::<Document>(col_name);
    let mut find = collection.find(bson_filter);
    if let Some(s) = sort.filter(|s| s.as_object().map(|o| !o.is_empty()).unwrap_or(false)) {
        find = find.sort(value_to_document(s)?);
    }
    if skip > 0 {
        find = find.skip(skip as u64);
    }
    if let Some(lim) = limit {
        find = find.limit(lim.min(i64::MAX as usize) as i64);
    }
    let mut cursor = find
        .await
        .map_err(|e| format!("MongoDB find failed on '{db_name}.{col_name}': {e}"))?;
    let mut out = Vec::new();
    while let Some(res) = cursor.next().await {
        let doc = res.map_err(|e| format!("MongoDB cursor error on '{db_name}.{col_name}': {e}"))?;
        out.push(document_to_value(doc)?);
    }
    log_line(
        LogLevel::Debug,
        "mongo_find",
        Some(json!({
            "database_name": db_name,
            "collection": col_name,
            "filter": preview_json(&translated, 500),
            "returned": out.len()
        })),
    );
    Ok(out)
}

/// Resuelve `find`/`findOne`. Las búsquedas por `_id` exacto se sirven desde la
/// caché de documentos de Redis cuando el documento está ahí; todo lo demás (y
/// cualquier fallo de caché) se resuelve en MongoDB con sus índices.
#[allow(clippy::too_many_arguments)]
async fn resolve_docs_redis_first(
    mongo: &MongoConnectionInfo,
    doc_cache: &Option<RedisDocumentCache>,
    _index_mgr: &Option<RedisIndexManager>,
    auto_index: &Option<AutoIndexManager>,
    db_name: &str,
    col_name: &str,
    filter: &Value,
    sort: Option<&Value>,
    projection: Option<&Value>,
    limit: Option<usize>,
    skip: usize,
) -> Result<Vec<Value>, String> {
    if let Some(ai) = auto_index {
        for field in QueryPlanAnalyzer::extract_indexable_fields(filter) {
            ai.record_query_field(db_name, col_name, &field).await;
        }
    }

    let finish = |docs: Vec<Value>| -> Vec<Value> {
        match projection {
            Some(p) => docs.into_iter().map(|d| apply_projection(&d, p)).collect(),
            None => docs,
        }
    };

    // Búsqueda por _id exacto: caché de documentos (Redis).
    if let (Some(cache), Some(id_val)) = (doc_cache, filter.get("_id")) {
        if skip == 0 && (!is_operator_object(id_val) || is_extended_json_value(id_val)) {
            let mut keys = vec![json_to_string(id_val)];
            if let Some(variants) = object_id_variants(id_val) {
                for v in variants {
                    let k = json_to_string(&v);
                    if !keys.contains(&k) {
                        keys.push(k);
                    }
                }
            }
            for key in keys {
                if let Some(Some(doc)) = cache.mget_docs(db_name, col_name, &[key]).await.into_iter().next() {
                    let mut matched: Vec<Value> = if match_filter(&doc, filter) { vec![doc] } else { vec![] };
                    if limit == Some(0) {
                        matched.clear();
                    }
                    log_line(
                        LogLevel::Debug,
                        "redis_doc_cache_hit",
                        Some(json!({ "database_name": db_name, "collection": col_name })),
                    );
                    return Ok(finish(matched));
                }
            }
        }
    }

    let result = find_docs_mongo(mongo, db_name, col_name, filter, sort, limit, skip).await?;

    if let Some(cache) = doc_cache {
        for doc in &result {
            if let Some(id) = doc.get("_id") {
                cache.set_doc(db_name, col_name, &json_to_string(id), doc).await;
            }
        }
    }

    Ok(finish(result))
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
                Some(1),
                0,
            ).await?;

            let doc = result_docs.into_iter().next();
            Ok(doc
                .map(|d| apply_projection(&d, &projection.unwrap_or(Value::Null)))
                .unwrap_or(Value::Null))
        }

        "insertOne" => {
            require_database(mongo, db_name).await?;
            let doc = payload
                .get("document")
                .cloned()
                .ok_or("document is required")?;
            if !doc.is_object() {
                return Err("document must be a JSON object".into());
            }
            let coll = write_collection(mongo, db_name, col_name);
            let res = coll
                .insert_one(value_to_document(&doc)?)
                .await
                .map_err(|e| format!("MongoDB insertOne failed on '{db_name}.{col_name}': {e}"))?;
            let id = bson_to_value(&res.inserted_id);
            let mut after = doc.clone();
            if let Some(obj) = after.as_object_mut() {
                obj.entry("_id").or_insert_with(|| id.clone());
            }

            write_through_set(index_mgr, doc_cache, db_name, col_name, None, &after).await;
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

        "updateOne" | "replaceOne" => {
            require_database(mongo, db_name).await?;
            let filter = payload
                .get("filter")
                .cloned()
                .unwrap_or(Value::Object(Map::new()));
            let body = if func == "updateOne" {
                payload.get("update").cloned().ok_or("update is required")?
            } else {
                payload.get("replacement").cloned().ok_or("replacement is required")?
            };
            let upsert = payload
                .get("options")
                .and_then(|o| o.get("upsert"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let use_operators = func == "updateOne" && is_operator_object(&body);
            if !use_operators && body.as_object().map(|o| o.keys().any(|k| k.starts_with('$'))).unwrap_or(true) {
                return Err("replacement document must be an object without update operators".into());
            }
            let mut body_doc = value_to_document(&body)?;
            if !use_operators {
                // El _id del documento original se conserva siempre.
                body_doc.remove("_id");
            }

            let coll = write_collection(mongo, db_name, col_name);
            let before = find_one_raw(&coll, bson_filter(&filter)?).await?;

            let (target, upsert_now) = match before.as_ref() {
                Some(b) => (id_filter_of(b)?, false),
                None if upsert => (bson_filter(&filter)?, true),
                None => {
                    return Ok(json!({
                        "acknowledged": true, "matchedCount": 0, "modifiedCount": 0,
                        "upsertedCount": 0, "upsertedId": Value::Null
                    }));
                }
            };
            let (modified, upserted_bson) = if use_operators {
                let r = coll
                    .update_one(target.clone(), body_doc)
                    .upsert(upsert_now)
                    .await
                    .map_err(|e| format!("MongoDB {func} failed on '{db_name}.{col_name}': {e}"))?;
                (r.modified_count, r.upserted_id)
            } else {
                if upsert_now {
                    // Mismo comportamiento que antes: en upsert por reemplazo, el _id viene del reemplazo.
                    if let Some(id) = body.get("_id") {
                        body_doc.insert("_id", value_to_bson(id)?);
                    }
                }
                let r = coll
                    .replace_one(target.clone(), body_doc)
                    .upsert(upsert_now)
                    .await
                    .map_err(|e| format!("MongoDB {func} failed on '{db_name}.{col_name}': {e}"))?;
                (r.modified_count, r.upserted_id)
            };

            let upserted_id = upserted_bson.as_ref().map(bson_to_value).unwrap_or(Value::Null);
            let after_filter = match (&before, &upserted_bson) {
                (Some(b), _) => id_filter_of(b)?,
                (None, Some(id)) => doc! { "_id": id.clone() },
                (None, None) => target,
            };
            let after = find_one_raw(&coll, after_filter).await?.unwrap_or(Value::Null);
            let matched = u64::from(before.is_some());
            let upserted = u64::from(upserted_bson.is_some());
            // Un reemplazo cuenta como modificado aunque el contenido sea igual (compatibilidad).
            let modified = if before.is_some() && !use_operators { 1 } else { modified };

            write_through_set(index_mgr, doc_cache, db_name, col_name, before.as_ref(), &after).await;
            emitter.emit(
                "collection",
                func,
                json!({
                    "database_name": db_name, "collectionName": col_name,
                    "before": before.clone().unwrap_or(Value::Null), "after": after,
                    "summary": {
                        "matchedCount": matched, "modifiedCount": modified,
                        "upsertedCount": upserted, "upsertedId": upserted_id.clone()
                    }
                }),
            );
            Ok(json!({
                "acknowledged": true,
                "matchedCount": matched,
                "modifiedCount": modified,
                "upsertedCount": upserted,
                "upsertedId": upserted_id
            }))
        }

        "deleteOne" => {
            require_database(mongo, db_name).await?;
            let filter = payload
                .get("filter")
                .cloned()
                .unwrap_or(Value::Object(Map::new()));
            log_line(
                LogLevel::Info,
                "delete_one_filter_received",
                Some(json!({
                    "database_name": db_name,
                    "collectionName": col_name,
                    "filter_preview": preview_json(&filter, 600)
                })),
            );
            let coll = write_collection(mongo, db_name, col_name);
            let removed = coll
                .find_one_and_delete(bson_filter(&filter)?)
                .await
                .map_err(|e| format!("MongoDB deleteOne failed on '{db_name}.{col_name}': {e}"))?;
            let Some(removed) = removed else {
                return Ok(json!({"acknowledged": true, "deletedCount": 0}));
            };
            let before = document_to_value(removed)?;
            write_through_delete(index_mgr, doc_cache, db_name, col_name, &before).await;
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
        }

        "deleteMany" => {
            require_database(mongo, db_name).await?;
            let filter = payload
                .get("filter")
                .cloned()
                .unwrap_or(Value::Object(Map::new()));
            let coll = write_collection(mongo, db_name, col_name);
            // Se leen los documentos que coinciden para el evento y la caché; luego se borran por _id
            // para que el evento refleje exactamente lo eliminado.
            let removed = find_many_raw(&coll, bson_filter(&filter)?).await?;
            let mut deleted = 0u64;
            for chunk in removed.chunks(1000) {
                let ids = chunk
                    .iter()
                    .filter_map(|d| d.get("_id"))
                    .map(value_to_bson)
                    .collect::<Result<Vec<_>, _>>()?;
                let r = coll
                    .delete_many(doc! { "_id": { "$in": ids } })
                    .await
                    .map_err(|e| format!("MongoDB deleteMany failed on '{db_name}.{col_name}': {e}"))?;
                deleted += r.deleted_count;
            }
            for doc in &removed {
                write_through_delete(index_mgr, doc_cache, db_name, col_name, doc).await;
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
            require_database(mongo, db_name).await?;
            let coll = write_collection(mongo, db_name, col_name);

            let result = if let Some(Value::Array(data)) = payload
                .get("data")
                .or(payload.get("documents"))
                .or(payload.get("docs"))
                .or(payload.get("items"))
            {
                let objects: Vec<&Value> = data.iter().filter(|d| d.is_object()).collect();
                let (inserted, modified) =
                    upsert_documents_by_id(&coll, db_name, col_name, &objects, index_mgr, doc_cache).await?;
                json!({"acknowledged": true, "insertedCount": inserted, "modifiedCount": modified})
            } else {
                let filter = payload
                    .get("filter")
                    .cloned()
                    .unwrap_or(Value::Object(Map::new()));
                let update = payload.get("update").cloned().ok_or("update is required")?;
                let ids: Vec<Value> = find_many_raw(&coll, bson_filter(&filter)?)
                    .await?
                    .into_iter()
                    .filter_map(|d| d.get("_id").cloned())
                    .collect();
                let matched = ids.len() as u64;
                let mut modified = 0u64;
                if is_operator_object(&update) {
                    let update_doc = value_to_document(&update)?;
                    for chunk in ids.chunks(1000) {
                        let bson_ids = chunk.iter().map(value_to_bson).collect::<Result<Vec<_>, _>>()?;
                        let r = coll
                            .update_many(doc! { "_id": { "$in": bson_ids } }, update_doc.clone())
                            .await
                            .map_err(|e| format!("MongoDB updateMany failed on '{db_name}.{col_name}': {e}"))?;
                        modified += r.modified_count;
                    }
                } else {
                    let mut replacement = value_to_document(&update)?;
                    replacement.remove("_id");
                    for id in &ids {
                        coll.replace_one(doc! { "_id": value_to_bson(id)? }, replacement.clone())
                            .await
                            .map_err(|e| format!("MongoDB replace failed on '{db_name}.{col_name}': {e}"))?;
                        modified += 1;
                    }
                }
                // Write-Through de los documentos ya confirmados.
                for chunk in ids.chunks(1000) {
                    let bson_ids = chunk.iter().map(value_to_bson).collect::<Result<Vec<_>, _>>()?;
                    for after in find_many_raw(&coll, doc! { "_id": { "$in": bson_ids } }).await? {
                        write_through_set(index_mgr, doc_cache, db_name, col_name, None, &after).await;
                    }
                }
                json!({"acknowledged": true, "matchedCount": matched, "modifiedCount": modified})
            };

            emitter.emit(
                "collection",
                "updateCollection",
                json!({
                    "database_name": db_name, "collectionName": col_name,
                    "before": Value::Null, "after": Value::Null,
                    "summary": result
                }),
            );
            Ok(result)
        }

        "exportCollection" => {
            require_database(mongo, db_name).await?;
            let coll = mongo.client.database(db_name).collection::<Document>(col_name);
            Ok(Value::Array(find_many_raw(&coll, doc! {}).await?))
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
            require_database(mongo, db_name).await?;
            let coll = write_collection(mongo, db_name, col_name);
            let docs: Vec<&Value> = arr.iter().collect();
            let (inserted, updated) =
                import_into_collection(&coll, db_name, col_name, mode, &docs).await?;

            // La caché de la colección se reinicia y se repuebla con lo importado (Write-Through);
            // el resto se carga bajo demanda (Cache-Aside).
            rebuild_collection_redis_state(db_name, col_name, arr, index_mgr, doc_cache).await;

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

        "listIndexes" => {
            require_database(mongo, db_name).await?;
            let models = list_index_models(mongo, db_name, col_name).await?;
            let mut out = Vec::with_capacity(models.len());
            for model in models {
                let opts = model.options.clone().unwrap_or_default();
                let name = opts.name.clone().unwrap_or_default();
                let keys = index_keys_to_json(&model.keys);
                let kind = if name == "_id_" {
                    "system"
                } else if name.starts_with(AUTOINDEX_NAME_PREFIX) {
                    "auto"
                } else {
                    "manual"
                };
                // Campo único del índice: con él se consultan los metadatos del autoindexado.
                let single_field = (model.keys.len() == 1)
                    .then(|| model.keys.keys().next().cloned())
                    .flatten();
                let auto_info = match (auto_index, &single_field) {
                    (Some(ai), Some(field)) if kind != "system" => {
                        Some(ai.describe_field(db_name, col_name, field).await)
                    }
                    _ => None,
                };
                out.push(json!({
                    "name": name,
                    "keys": keys,
                    "kind": kind,
                    // Desde el panel ningún índice se borra: los automáticos los retira el autoindexado
                    // y los manuales y de sistema están protegidos.
                    "deletable": false,
                    "unique": opts.unique.unwrap_or(false),
                    "sparse": opts.sparse.unwrap_or(false),
                    "expireAfterSeconds": opts.expire_after.map(|d| d.as_secs()),
                    "partialFilterExpression": opts
                        .partial_filter_expression
                        .clone()
                        .and_then(|d| document_to_value(d).ok()),
                    "autoIndex": auto_info
                }));
            }
            Ok(json!({
                "indexes": out,
                "autoIndexEnabled": auto_index.is_some()
            }))
        }

        "createIndex" => {
            require_database(mongo, db_name).await?;
            let keys = parse_index_keys(payload.get("keys").ok_or("keys is required")?)?;
            let options = payload.get("options").cloned().unwrap_or(Value::Object(Map::new()));
            let options = options.as_object().ok_or("options must be an object")?;
            let name = options
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            if let Some(n) = &name {
                if n.starts_with(AUTOINDEX_NAME_PREFIX) || n == "_id_" {
                    return Err(format!(
                        "Index names starting with '{AUTOINDEX_NAME_PREFIX}' or '_id_' are reserved"
                    ));
                }
            }
            let mut index_options = IndexOptions::builder().build();
            index_options.name = name.clone();
            if options.get("unique").and_then(|v| v.as_bool()).unwrap_or(false) {
                index_options.unique = Some(true);
            }
            if options.get("sparse").and_then(|v| v.as_bool()).unwrap_or(false) {
                index_options.sparse = Some(true);
            }
            if let Some(ttl) = options.get("expireAfterSeconds").filter(|v| !v.is_null()) {
                let secs = ttl.as_u64().ok_or("expireAfterSeconds must be a non-negative integer")?;
                if keys.len() != 1 {
                    return Err("TTL indexes must have exactly one field".into());
                }
                index_options.expire_after = Some(Duration::from_secs(secs));
            }
            if let Some(pfe) = options.get("partialFilterExpression").filter(|v| !v.is_null()) {
                if !pfe.is_object() {
                    return Err("partialFilterExpression must be an object".into());
                }
                index_options.partial_filter_expression = Some(value_to_document(pfe)?);
            }

            let mut key_doc = Document::new();
            for (field, kind) in &keys {
                key_doc.insert(field.clone(), kind.clone());
            }

            // Si ya existe un autoíndice con las mismas claves, el índice manual lo sustituye:
            // MongoDB no admite dos índices con la misma especificación de claves.
            let mut replaced_auto: Option<String> = None;
            for model in list_index_models(mongo, db_name, col_name).await? {
                let existing = model.options.as_ref().and_then(|o| o.name.clone()).unwrap_or_default();
                if existing.starts_with(AUTOINDEX_NAME_PREFIX) && model.keys == key_doc {
                    mongo
                        .client
                        .database(db_name)
                        .collection::<Document>(col_name)
                        .drop_index(existing.clone())
                        .await
                        .map_err(|e| format!("Could not replace auto index '{existing}': {e}"))?;
                    if let (Some(ai), Some(idx), Some(field)) =
                        (auto_index, index_mgr, model.keys.keys().next())
                    {
                        ai.forget_field(idx, db_name, col_name, field).await;
                    }
                    replaced_auto = Some(existing);
                }
            }

            let model = IndexModel::builder().keys(key_doc).options(index_options).build();
            let created = mongo
                .client
                .database(db_name)
                .collection::<Document>(col_name)
                .create_index(model)
                .await
                .map_err(|e| format!("MongoDB createIndex failed on '{db_name}.{col_name}': {e}"))?;
            let index_name = created.index_name;
            log_line(
                LogLevel::Info,
                "manual_index_created",
                Some(json!({
                    "database_name": db_name, "collectionName": col_name,
                    "index_name": index_name, "replaced_auto_index": replaced_auto
                })),
            );
            emitter.emit(
                "collection",
                "createIndex",
                json!({
                    "database_name": db_name, "collectionName": col_name,
                    "before": Value::Null, "after": Value::Null,
                    "summary": {"name": index_name, "replacedAutoIndex": replaced_auto}
                }),
            );
            Ok(json!({"acknowledged": true, "name": index_name, "replacedAutoIndex": replaced_auto}))
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

/// Comprueba el permiso RBAC del request. Las denegaciones quedan en el log (auditoría).
fn authorize_request(principal: &Principal, req: &WsRequest) -> Result<(), String> {
    let payload = req.payload.as_ref().unwrap_or(&Value::Null);
    let db = opt_str(payload, "database_name")
        .or(req.database_name.as_deref())
        .unwrap_or("");
    let col = opt_str(payload, "collectionName").unwrap_or("");
    let category = req.category.as_deref().unwrap_or("");
    let function = req.function_name.as_deref().unwrap_or("");
    let Some(need) = auth::required_permission(&req.msg_type, category, function, db, col) else {
        return Ok(());
    };
    if principal.allows(&need) {
        return Ok(());
    }
    log_line(
        LogLevel::Warn,
        "auth_denied",
        Some(json!({
            "principal": principal.name, "type": req.msg_type, "category": category,
            "function": function, "database_name": db, "collectionName": col,
            "required": need.level.name()
        })),
    );
    let target = match (&need.database, &need.collection) {
        _ if need.level == auth::Level::Admin => "the server".to_string(),
        (Some(d), Some(c)) => format!("'{d}.{c}'"),
        (Some(d), None) => format!("database '{d}'"),
        _ => "all databases".to_string(),
    };
    Err(format!(
        "Permission denied: '{}' requires role {} on {}",
        function_or_type(req),
        need.level.name(),
        target
    ))
}

fn function_or_type(req: &WsRequest) -> &str {
    req.function_name.as_deref().filter(|f| !f.is_empty()).unwrap_or(&req.msg_type)
}

/// process_request con autorización previa y filtrado de listados por permisos.
#[allow(clippy::too_many_arguments)]
async fn process_authorized(
    principal: &Principal,
    req: WsRequest,
    mongo: &MongoConnectionInfo,
    bcast: &Broadcaster,
    request_cache: Option<RequestCache>,
    doc_cache: Option<RedisDocumentCache>,
    index_mgr: Option<RedisIndexManager>,
    auto_index: Option<AutoIndexManager>,
) -> WsResponse {
    if let Err(e) = authorize_request(principal, &req) {
        return error_response(&e);
    }
    let is_list = req.msg_type == "list_databases";
    let mut resp = process_request(req, mongo, bcast, request_cache, doc_cache, index_mgr, auto_index).await;
    if is_list && resp.success && !principal.is_admin() {
        if let Ok(Value::Array(dbs)) = serde_json::from_str::<Value>(&resp.response_json) {
            let visible: Vec<Value> = dbs
                .into_iter()
                .filter(|d| d.as_str().map_or(false, |n| principal.can_read_database(n)))
                .collect();
            resp.response_json = Value::Array(visible).to_string();
        }
    }
    resp
}

/// ¿Puede el principal ver este evento de cambio?
fn event_visible(principal: &Principal, text: &str) -> bool {
    if principal.is_admin() {
        return true;
    }
    let Ok(ev) = serde_json::from_str::<Value>(text) else { return false };
    let db = event_field(&ev, &["database_name", "databaseName"]);
    let col = event_field(&ev, &["collectionName", "collection_name"]);
    !db.is_empty() && principal.can_read_collection(db, col)
}

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
                "collection": ["find", "findOne", "insertOne", "updateOne", "replaceOne", "deleteOne", "deleteMany", "updateCollection", "exportCollection", "importCollection", "listIndexes", "createIndex"],
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
    principal: Arc<Principal>,
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
            "principal": principal.name,
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
    let bcast_principal = principal.clone();
    let bcast_task = tokio::spawn(async move {
        loop {
            match bcast_rx.recv().await {
                Ok(text) => {
                    if !event_visible(&bcast_principal, &text) {
                        continue;
                    }
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

            let resp = process_authorized(
                &principal, req, &mongo, &bcast,
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
            "drop_idle_secs": config.autoindex_drop_idle_secs,
            "review_secs": config.autoindex_review_secs,
            "min_docs": config.autoindex_min_docs,
            "sample_size": config.autoindex_sample_size,
            "max_match_ratio": config.autoindex_max_match_ratio
        })),
    );

    Some(AutoIndexManager {
        connection,
        threshold: config.autoindex_threshold,
        window_secs: config.autoindex_window_secs,
        drop_idle_secs: config.autoindex_drop_idle_secs,
        review_secs: config.autoindex_review_secs,
        min_docs: config.autoindex_min_docs,
        sample_size: config.autoindex_sample_size,
        max_match_ratio: config.autoindex_max_match_ratio,
        prefix: INDEX_STATS_PREFIX.to_string(),
    })
}

async fn auto_index_agent_loop(
    mongo: MongoState,
    index_mgr: RedisIndexManager,
    auto_index: AutoIndexManager,
) {
    let mut ticker = interval(Duration::from_secs(60));
    let mut last_review: u64 = 0;
    loop {
        ticker.tick().await;
        let now = now_ms() / 1000;
        let review = now.saturating_sub(last_review) >= auto_index.review_secs;

        let databases = match list_database_names(&mongo).await {
            Ok(d) => d,
            Err(e) => {
                log_line(LogLevel::Warn, "autoindex_list_databases_failed", Some(json!({ "error": e })));
                continue;
            }
        };
        let mut existing: HashSet<(String, String)> = HashSet::new();
        let mut listing_complete = true;
        for db_name in databases {
            match list_collection_names(&mongo, &db_name).await {
                Ok(collections) => {
                    for col_name in collections {
                        existing.insert((db_name.clone(), col_name.clone()));
                        auto_index.check_and_create_indexes(&mongo, &db_name, &col_name).await;
                        if review {
                            auto_index.review_collection(&mongo, &index_mgr, &db_name, &col_name).await;
                        }
                    }
                }
                Err(e) => {
                    listing_complete = false;
                    log_line(LogLevel::Warn, "autoindex_list_collections_failed", Some(json!({ "error": e })));
                }
            }
        }
        if review {
            // Solo con un listado completo: si no, se borrarían metadatos de colecciones vivas.
            if listing_complete {
                auto_index.sweep_orphans(&index_mgr, &existing).await;
            }
            last_review = now;
        }
    }
}

// =============================================================================
// SERVIDOR gRPC (proto/microservicedb.proto)
// =============================================================================

/// Expone por gRPC el mismo `process_request` que usa el WebSocket, de modo que
/// las cachés de Redis, el índice y el autoindexado aplican igual en ambos transportes.
#[derive(Clone)]
struct DatabaseServiceImpl {
    mongo: MongoState,
    bcast: Broadcaster,
    request_cache: Option<RequestCache>,
    doc_cache: Option<RedisDocumentCache>,
    index_mgr: Option<RedisIndexManager>,
    auto_index: Option<AutoIndexManager>,
    auth: Auth,
}

impl DatabaseServiceImpl {
    /// Autentica con los metadatos "authorization: Bearer <key>" o "x-api-key: <key>".
    #[allow(clippy::result_large_err)]
    fn principal<T>(&self, request: &Request<T>) -> Result<Arc<Principal>, Status> {
        let md = request.metadata();
        let get = |k: &str| md.get(k).and_then(|v| v.to_str().ok());
        let key = auth::key_from_headers(get("authorization"), get("x-api-key"));
        self.auth.authenticate(key.as_deref()).map_err(|e| {
            log_line(LogLevel::Warn, "auth_failed", Some(json!({ "transport": "grpc", "error": e })));
            Status::unauthenticated(e)
        })
    }
}

fn non_empty(s: String) -> Option<String> {
    if s.trim().is_empty() { None } else { Some(s) }
}

/// Valor de campo de un evento de cambio; acepta snake_case y camelCase.
fn event_field<'v>(ev: &'v Value, keys: &[&str]) -> &'v str {
    keys.iter()
        .find_map(|k| ev.get(*k).and_then(|v| v.as_str()))
        .unwrap_or("")
}

type GrpcStream<T> =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl DatabaseService for DatabaseServiceImpl {
    type StreamChangesStream = GrpcStream<ChangeEvent>;
    type ExportCollectionStreamStream = GrpcStream<DocumentBatch>;

    async fn action(&self, request: Request<ActionRequest>) -> Result<Response<ActionResponse>, Status> {
        let principal = self.principal(&request)?;
        let req = request.into_inner();
        let payload: Value = if req.payload_json.trim().is_empty() {
            Value::Object(Map::new())
        } else {
            serde_json::from_str(&req.payload_json)
                .map_err(|e| Status::invalid_argument(format!("Invalid payload_json: {e}")))?
        };
        let database_name = opt_str(&payload, "database_name").map(|s| s.to_string());
        let collections = payload.get("collections").and_then(|v| v.as_array()).map(|arr| {
            arr.iter().filter_map(|e| e.as_str().map(|s| s.to_string())).collect::<Vec<_>>()
        });
        let msg_type = non_empty(req.msg_type).unwrap_or_else(|| "action".to_string());
        let ws_req = WsRequest {
            msg_type,
            category: non_empty(req.category),
            function_name: non_empty(req.function_name),
            payload: Some(payload),
            database_name,
            collections,
        };
        log_line(
            LogLevel::Debug,
            "grpc_action_received",
            Some(json!({
                "type": ws_req.msg_type,
                "category": ws_req.category,
                "function": ws_req.function_name,
                "request_id": req.request_id,
            })),
        );
        let resp = process_authorized(
            &principal,
            ws_req,
            &self.mongo,
            &self.bcast,
            self.request_cache.clone(),
            self.doc_cache.clone(),
            self.index_mgr.clone(),
            self.auto_index.clone(),
        )
        .await;
        Ok(Response::new(ActionResponse {
            success: resp.success,
            response_json: resp.response_json,
            message: resp.message,
            request_id: req.request_id,
        }))
    }

    async fn stream_changes(
        &self,
        request: Request<StreamChangesRequest>,
    ) -> Result<Response<Self::StreamChangesStream>, Status> {
        let principal = self.principal(&request)?;
        let filter = Arc::new(request.into_inner());
        if !filter.database_name.is_empty()
            && !principal.can_read_collection(&filter.database_name, &filter.collection_name)
        {
            return Err(Status::permission_denied(format!(
                "Permission denied: read access to '{}' required",
                filter.database_name
            )));
        }
        let stream = BroadcastStream::new(self.bcast.subscribe()).filter_map(move |r| {
            let filter = filter.clone();
            let principal = principal.clone();
            async move {
                let msg = r.ok()?;
                if !event_visible(&principal, &msg) {
                    return None;
                }
                let ev: Value = serde_json::from_str(&msg).ok()?;
                let database_name = event_field(&ev, &["database_name", "databaseName"]).to_string();
                let collection_name = event_field(&ev, &["collectionName", "collection_name"]).to_string();
                if !filter.database_name.is_empty() && filter.database_name != database_name {
                    return None;
                }
                if !filter.collection_name.is_empty() && filter.collection_name != collection_name {
                    return None;
                }
                if !filter.include_system
                    && (is_reserved_database(&database_name)
                        || (!collection_name.is_empty() && is_reserved_collection(&collection_name)))
                {
                    return None;
                }
                Some(Ok(ChangeEvent {
                    event: event_field(&ev, &["event"]).to_string(),
                    category: event_field(&ev, &["category"]).to_string(),
                    database_name,
                    collection_name,
                    ts_ms: ev.get("ts").and_then(|v| v.as_i64()).unwrap_or_else(|| now_ms() as i64),
                    event_json: msg,
                }))
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn export_collection_stream(
        &self,
        request: Request<ExportCollectionRequest>,
    ) -> Result<Response<Self::ExportCollectionStreamStream>, Status> {
        let principal = self.principal(&request)?;
        let req = request.into_inner();
        let need = auth::Need {
            level: auth::Level::Read,
            database: Some(req.database_name.clone()),
            collection: Some(req.collection_name.clone()),
        };
        if !principal.allows(&need) {
            log_line(
                LogLevel::Warn,
                "auth_denied",
                Some(json!({ "principal": principal.name, "function": "ExportCollectionStream",
                    "database_name": req.database_name, "collectionName": req.collection_name })),
            );
            return Err(Status::permission_denied(format!(
                "Permission denied: role read required on '{}.{}'",
                req.database_name, req.collection_name
            )));
        }
        if req.database_name.is_empty() || req.collection_name.is_empty() {
            return Err(Status::invalid_argument("database_name and collection_name are required"));
        }
        if is_reserved_collection(&req.collection_name) {
            return Err(Status::invalid_argument(format!(
                "Collection '{}' is reserved",
                req.collection_name
            )));
        }
        let filter: Value = if req.filter_json.trim().is_empty() {
            Value::Object(Map::new())
        } else {
            serde_json::from_str(&req.filter_json)
                .map_err(|e| Status::invalid_argument(format!("Invalid filter_json: {e}")))?
        };
        let bson_filter = value_to_document(&filter).map_err(Status::invalid_argument)?;
        let batch_size = if req.batch_size == 0 {
            EXPORT_STREAM_BATCH_SIZE
        } else {
            req.batch_size as usize
        };

        let collection = self
            .mongo
            .client
            .database(&req.database_name)
            .collection::<Document>(&req.collection_name);
        let total = collection
            .count_documents(bson_filter.clone())
            .await
            .map_err(|e| Status::internal(format!("MongoDB count failed: {e}")))?;
        let mut cursor = collection
            .find(bson_filter)
            .batch_size(batch_size.min(u32::MAX as usize) as u32)
            .await
            .map_err(|e| Status::internal(format!("MongoDB find failed: {e}")))?;

        log_line(
            LogLevel::Info,
            "export_stream_start",
            Some(json!({
                "database_name": req.database_name,
                "collectionName": req.collection_name,
                "total": total,
                "batch_size": batch_size
            })),
        );

        // Canal acotado: si el cliente va lento, se deja de leer de MongoDB hasta que consuma.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<DocumentBatch, Status>>(4);
        tokio::spawn(async move {
            let mut pending: Vec<String> = Vec::new();
            let mut pending_bytes = 0usize;
            let mut sent = 0u64;
            loop {
                let next = cursor.next().await;
                let done = next.is_none();
                if let Some(res) = next {
                    let text = res
                        .map_err(|e| format!("MongoDB cursor error: {e}"))
                        .and_then(document_to_value)
                        .and_then(|v| serde_json::to_string(&v).map_err(|e| e.to_string()));
                    match text {
                        Ok(text) => {
                            pending_bytes += text.len();
                            pending.push(text);
                        }
                        Err(e) => {
                            let _ = tx.send(Err(Status::internal(e))).await;
                            return;
                        }
                    }
                }
                let full = pending.len() >= batch_size || pending_bytes >= EXPORT_STREAM_BATCH_BYTES;
                if full || (done && (!pending.is_empty() || sent == 0)) {
                    sent += pending.len() as u64;
                    let batch = DocumentBatch {
                        documents_json: format!("[{}]", pending.join(",")),
                        total,
                        sent,
                    };
                    pending.clear();
                    pending_bytes = 0;
                    if tx.send(Ok(batch)).await.is_err() {
                        return;
                    }
                }
                if done {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

// =============================================================================
// MAIN
// =============================================================================

#[tokio::main]
async fn main() {
    let _ = dotenv();

    if env::args().nth(1).as_deref() == Some("generate-api-key") {
        let key = auth::generate_api_key();
        println!("API key (entregar al cliente, no se vuelve a mostrar): {key}");
        println!("sha256 (para {}): {}", auth::ENV_API_KEYS_FILE, auth::sha256_hex(&key));
        return;
    }

    let auth = match Auth::from_env() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    };

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

    let grpc_service = DatabaseServiceImpl {
        mongo: mongo.clone(),
        bcast: bcast_tx.clone(),
        request_cache: request_cache.clone(),
        doc_cache: doc_cache.clone(),
        index_mgr: index_mgr.clone(),
        auto_index: auto_index.clone(),
        auth: auth.clone(),
    };

    let mongo_for_ws = mongo.clone();
    let request_cache_for_ws = request_cache.clone();
    let doc_cache_for_ws = doc_cache.clone();
    let index_mgr_for_ws = index_mgr.clone();
    let auto_index_for_ws = auto_index.clone();

    let auth_for_ws = auth.clone();
    let ws = warp::path("ws")
        .and(warp::ws())
        .and(warp::header::optional::<String>("authorization"))
        .and(warp::header::optional::<String>("x-api-key"))
        .and(warp::query::raw().or(warp::any().map(String::new)).unify())
        .and(warp::any().map(move || auth_for_ws.clone()))
        .and(warp::any().map(move || mongo_for_ws.clone()))
        .and(warp::any().map(move || bcast_tx.clone()))
        .and(warp::any().map(move || request_cache_for_ws.clone()))
        .and(warp::any().map(move || doc_cache_for_ws.clone()))
        .and(warp::any().map(move || index_mgr_for_ws.clone()))
        .and(warp::any().map(move || auto_index_for_ws.clone()))
        .and_then(
            |ws: warp::ws::Ws,
             authorization: Option<String>,
             x_api_key: Option<String>,
             query: String,
             auth: Auth,
             mongo: MongoState,
             bcast: Broadcaster,
             request_cache: Option<RequestCache>,
             doc_cache: Option<RedisDocumentCache>,
             index_mgr: Option<RedisIndexManager>,
             auto_index: Option<AutoIndexManager>| async move {
                // Los navegadores no pueden enviar cabeceras en WebSocket: se acepta también ?api_key=.
                let from_query = query
                    .split('&')
                    .find_map(|kv| kv.strip_prefix("api_key="))
                    .map(|v| v.to_string());
                let key = auth::key_from_headers(authorization.as_deref(), x_api_key.as_deref())
                    .or(from_query);
                let reply: Box<dyn warp::Reply> = match auth.authenticate(key.as_deref()) {
                    Ok(principal) => Box::new(ws.on_upgrade(move |socket| handle_socket(
                        socket, principal, mongo, bcast,
                        request_cache, doc_cache, index_mgr, auto_index
                    ))),
                    Err(e) => {
                        log_line(LogLevel::Warn, "auth_failed", Some(json!({ "transport": "websocket", "error": e })));
                        Box::new(warp::reply::with_status(e, StatusCode::UNAUTHORIZED))
                    }
                };
                Ok::<_, warp::Rejection>(reply)
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
            "/MicroserviceDB.html"
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
    println!("Profile: {}", config.profile.name());
    if auth.enabled() {
        println!("Auth: REQUIRED ({} API keys)", auth.key_count());
    } else {
        println!("Auth: DISABLED ({}=disabled) - do not use in production", auth::ENV_AUTH);
    }

    let http_port = config.port;
    let http_future = async move {
        match tls {
            Some((cert_path, key_path)) => {
                println!("JSON DB Server running on https://{}:{}", local_ip, http_port);
                println!("WebSocket endpoint: wss://{}:{}/ws", local_ip, http_port);
                warp::serve(routes)
                    .tls()
                    .cert_path(cert_path)
                    .key_path(key_path)
                    .run((host, http_port))
                    .await;
            }
            None => {
                println!("JSON DB Server running on http://{}:{}", local_ip, http_port);
                println!("WebSocket endpoint: ws://{}:{}/ws", local_ip, http_port);
                println!(
                    "HTTPS/WSS not enabled: set {} and {} to activate it",
                    ENV_SSL_CERT_PATH, ENV_SSL_KEY_PATH
                );
                warp::serve(routes).run((host, http_port)).await;
            }
        }
    };

    let grpc_addr = std::net::SocketAddr::from((host, config.grpc_port));
    // gRPC usa el mismo certificado que HTTPS/WSS (MICROSERVICEDB_SSL_CERT / MICROSERVICEDB_SSL_KEY).
    let mut grpc_builder = TonicServer::builder();
    match grpc_tls_identity() {
        Ok(Some(identity)) => {
            grpc_builder = match grpc_builder.tls_config(tonic::transport::ServerTlsConfig::new().identity(identity)) {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("Invalid gRPC TLS configuration: {e}");
                    process::exit(1);
                }
            };
            println!("gRPC Server running on {} (TLS)", grpc_addr);
        }
        Ok(None) => println!("gRPC Server running on {} (plaintext, no TLS)", grpc_addr),
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    }
    let grpc_future = grpc_builder
        .add_service(
            DatabaseServiceServer::new(grpc_service)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
        )
        .serve(grpc_addr);

    // Si cualquiera de los dos servidores termina, el proceso sale con error.
    tokio::select! {
        _ = http_future => {
            log_line(LogLevel::Error, "http_server_stopped", None);
            process::exit(1);
        }
        res = grpc_future => {
            log_line(
                LogLevel::Error,
                "grpc_server_stopped",
                Some(json!({ "error": res.err().map(|e| e.to_string()) })),
            );
            process::exit(1);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn bson(v: &Value) -> Document {
        value_to_document(&translate_filter(v)).expect("filtro traducido debe ser BSON válido")
    }

    #[test]
    fn plain_equality_passes_through() {
        assert_eq!(translate_filter(&json!({"name": "ana"})), json!({"name": "ana"}));
        assert_eq!(translate_filter(&json!({})), json!({}));
    }

    #[test]
    fn object_id_text_matches_both_forms() {
        let id = "65a1b2c3d4e5f60718293a4b";
        let t = translate_filter(&json!({ "_id": id }));
        assert_eq!(t, json!({ "_id": { "$in": [ { "$oid": id }, id ] } }));
        let d = bson(&json!({ "_id": id }));
        let arr = d.get_document("_id").unwrap().get_array("$in").unwrap();
        assert!(matches!(arr[0], mongodb::bson::Bson::ObjectId(_)));
        assert!(matches!(arr[1], mongodb::bson::Bson::String(_)));
        // {"$oid": ...} también se trata como valor, no como operador.
        let t2 = translate_filter(&json!({ "_id": { "$oid": id } }));
        assert_eq!(t2, t);
    }

    #[test]
    fn operators_keep_previous_semantics() {
        assert_eq!(
            translate_filter(&json!({ "a": { "$exists": true } })),
            json!({ "a": { "$exists": true, "$ne": null } })
        );
        assert_eq!(translate_filter(&json!({ "a": { "$exists": false } })), json!({ "a": null }));
        assert_eq!(
            translate_filter(&json!({ "a": { "$nin": [1, 2] } })),
            json!({ "$and": [ { "a": { "$exists": true } }, { "a": { "$nin": [1, 2] } } ] })
        );
        assert_eq!(
            translate_filter(&json!({ "a": { "$gte": 1, "$lt": 5 } })),
            json!({ "$and": [ { "a": { "$gte": 1 } }, { "a": { "$lt": 5 } } ] })
        );
        assert_eq!(
            translate_filter(&json!({ "a": { "$regex": "^x", "$options": "i" } })),
            json!({ "a": { "$regex": "^x", "$options": "i" } })
        );
        assert_eq!(translate_filter(&json!({ "a": { "$in": 3 } })), json!({ "$expr": false }));
        assert_eq!(translate_filter(&json!({ "$or": [] })), json!({ "$expr": false }));
    }

    #[test]
    fn logical_operators_recurse() {
        let id = "65a1b2c3d4e5f60718293a4b";
        let t = translate_filter(&json!({ "$or": [ { "_id": id }, { "n": 1 } ], "x": 2 }));
        assert_eq!(
            t,
            json!({ "$and": [
                { "$or": [ { "_id": { "$in": [ { "$oid": id }, id ] } }, { "n": 1 } ] },
                { "x": 2 }
            ] })
        );
        assert_eq!(translate_filter(&json!({ "$not": { "a": 1 } })), json!({ "$nor": [ { "a": 1 } ] }));
        bson(&json!({ "$not": { "a": { "$in": [1] } }, "b": { "$ne": id } }));
    }

    #[test]
    fn extended_json_dates_are_values() {
        let f = json!({ "at": { "$date": { "$numberLong": "1700000000000" } } });
        let d = bson(&f);
        assert!(matches!(d.get("at"), Some(mongodb::bson::Bson::DateTime(_))));
    }
}

#[cfg(test)]
mod projection_tests {
    use super::*;

    #[test]
    fn include_with_id_excluded() {
        let doc = json!({ "_id": 1, "sku": "a", "price": 3 });
        assert_eq!(apply_projection(&doc, &json!({ "sku": 1, "_id": 0 })), json!({ "sku": "a" }));
        assert_eq!(apply_projection(&doc, &json!({ "sku": 1 })), json!({ "_id": 1, "sku": "a" }));
        assert_eq!(apply_projection(&doc, &json!({ "price": 0 })), json!({ "_id": 1, "sku": "a" }));
        assert_eq!(apply_projection(&doc, &json!({ "_id": 0 })), json!({ "sku": "a", "price": 3 }));
    }
}
