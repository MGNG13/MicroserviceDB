//! Cliente Rust de MicroserviceDB.
//!
//! Transportes:
//! - **WebSocket** (por defecto): `MICROSERVICEDB_DATABASE_WS_URL` (o `..._URL`, `..._BASE_URL`),
//!   default `ws://127.0.0.1:3329/ws`.
//! - **gRPC**: `MICROSERVICEDB_TRANSPORT=grpc` o `ClientOptions { transport: Transport::Grpc, .. }`.
//!   Dirección en `MICROSERVICEDB_DATABASE_GRPC_ADDRESS` (default `http://127.0.0.1:3330`).
//!   Los cambios llegan por `StreamChanges` filtrados a esta base de datos.
//!
//! TLS: gRPC usa TLS si la dirección empieza por `https://` o `MICROSERVICEDB_GRPC_TLS=true`; para
//! certificados propios, `MICROSERVICEDB_TLS_CA_FILE` (PEM). WebSocket seguro con `wss://` (requiere
//! una feature TLS de `tokio-tungstenite`, p. ej. `rustls-tls-native-roots`).
//!
//! Dependencias del crate consumidor: `tokio` (full), `futures-util`, `serde` (derive),
//! `serde_json`, `tokio-tungstenite`, `tonic = { version = "0.12", features = ["tls", "tls-native-roots"] }`,
//! `prost = "0.13"`.

use std::collections::{HashMap, VecDeque};
use std::env;
use std::sync::{Arc, Once};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};

// =============================================================================
// Tipos públicos
// =============================================================================

pub type DocumentLike = Value;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FindOptions {
    pub sort: Option<HashMap<String, i32>>,
    pub projection: Option<HashMap<String, i32>>,
    pub limit: Option<i64>,
    pub skip: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InsertResult {
    #[serde(default)]
    pub acknowledged: bool,
    pub inserted_id: Option<Value>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateResult {
    #[serde(default)]
    pub acknowledged: bool,
    #[serde(default)]
    pub modified_count: i64,
    #[serde(default)]
    pub upserted_count: i64,
    pub upserted_id: Option<Value>,
    #[serde(default)]
    pub matched_count: i64,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteResult {
    #[serde(default)]
    pub acknowledged: bool,
    #[serde(default)]
    pub deleted_count: i64,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeEvent {
    pub category: String,
    pub collection_name: String,
    pub event: String,
    pub before: Option<Value>,
    pub after: Option<Value>,
}

pub type ChangeListener = Arc<dyn Fn(ChangeEvent) + Send + Sync + 'static>;

// =============================================================================
// Constantes
// =============================================================================

const DEFAULT_WS_URL: &str = "ws://127.0.0.1:3329/ws";
const DEFAULT_GRPC_ADDRESS: &str = "http://127.0.0.1:3330";
const GRPC_ACTION: &str = "/microservicedb.DatabaseService/Action";
const GRPC_STREAM_CHANGES: &str = "/microservicedb.DatabaseService/StreamChanges";
const GRPC_EXPORT_STREAM: &str = "/microservicedb.DatabaseService/ExportCollectionStream";

// =============================================================================
// gRPC (proto/microservicedb.proto) — mensajes definidos a mano con prost
// =============================================================================

#[allow(dead_code)]
mod pb {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ActionRequest {
        #[prost(string, tag = "1")]
        pub msg_type: String,
        #[prost(string, tag = "2")]
        pub category: String,
        #[prost(string, tag = "3")]
        pub function_name: String,
        #[prost(string, tag = "4")]
        pub payload_json: String,
        #[prost(string, tag = "5")]
        pub request_id: String,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ActionResponse {
        #[prost(bool, tag = "1")]
        pub success: bool,
        #[prost(string, tag = "2")]
        pub response_json: String,
        #[prost(string, tag = "3")]
        pub message: String,
        #[prost(string, tag = "4")]
        pub request_id: String,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StreamChangesRequest {
        #[prost(string, tag = "1")]
        pub database_name: String,
        #[prost(string, tag = "2")]
        pub collection_name: String,
        #[prost(bool, tag = "3")]
        pub include_system: bool,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ChangeEvent {
        #[prost(string, tag = "1")]
        pub event: String,
        #[prost(string, tag = "2")]
        pub category: String,
        #[prost(string, tag = "3")]
        pub database_name: String,
        #[prost(string, tag = "4")]
        pub collection_name: String,
        #[prost(string, tag = "5")]
        pub event_json: String,
        #[prost(int64, tag = "6")]
        pub ts_ms: i64,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ExportCollectionRequest {
        #[prost(string, tag = "1")]
        pub database_name: String,
        #[prost(string, tag = "2")]
        pub collection_name: String,
        #[prost(string, tag = "3")]
        pub filter_json: String,
        #[prost(uint32, tag = "4")]
        pub batch_size: u32,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DocumentBatch {
        #[prost(string, tag = "1")]
        pub documents_json: String,
        #[prost(uint64, tag = "2")]
        pub total: u64,
        #[prost(uint64, tag = "3")]
        pub sent: u64,
    }
}

/// Request gRPC con la API key en `authorization: Bearer <key>`.
fn grpc_request<Req>(req: Req, api_key: Option<&str>) -> tonic::Request<Req> {
    let mut request = tonic::Request::new(req);
    if let Some(key) = api_key.filter(|k| !k.is_empty()) {
        if let Ok(v) = format!("Bearer {key}").parse() {
            request.metadata_mut().insert("authorization", v);
        }
    }
    request
}

async fn grpc_unary<Req, Resp>(
    channel: Channel,
    path: &'static str,
    req: Req,
    api_key: Option<&str>,
) -> Result<Resp, tonic::Status>
where
    Req: ::prost::Message + Send + Sync + 'static,
    Resp: ::prost::Message + Default + Send + Sync + 'static,
{
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let codec = tonic::codec::ProstCodec::<Req, Resp>::default();
    client
        .unary(grpc_request(req, api_key), PathAndQuery::from_static(path), codec)
        .await
        .map(|r| r.into_inner())
}

async fn grpc_stream<Req, Resp>(
    channel: Channel,
    path: &'static str,
    req: Req,
    api_key: Option<&str>,
) -> Result<tonic::Streaming<Resp>, tonic::Status>
where
    Req: ::prost::Message + Send + Sync + 'static,
    Resp: ::prost::Message + Default + Send + Sync + 'static,
{
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let codec = tonic::codec::ProstCodec::<Req, Resp>::default();
    client
        .server_streaming(grpc_request(req, api_key), PathAndQuery::from_static(path), codec)
        .await
        .map(|r| r.into_inner())
}

/// Transporte usado para hablar con el servidor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    WebSocket,
    Grpc,
}

/// Opciones del cliente. `Default` lee las variables de entorno.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub transport: Transport,
    /// Dirección gRPC (`http://host:3330`). `None` = entorno o default.
    pub grpc_address: Option<String>,
    /// API key del servidor. `None` = variable de entorno `MICROSERVICEDB_API_KEY`.
    pub api_key: Option<String>,
    /// CA en PEM (ruta) para TLS de gRPC. `None` = `MICROSERVICEDB_TLS_CA_FILE` o raíces del sistema.
    pub tls_ca_file: Option<String>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        let transport = match env::var("MICROSERVICEDB_TRANSPORT") {
            Ok(v) if v.trim().eq_ignore_ascii_case("grpc") => Transport::Grpc,
            _ => Transport::WebSocket,
        };
        Self { transport, grpc_address: None, api_key: None, tls_ca_file: None }
    }
}

fn resolve_grpc_address(explicit: Option<String>) -> String {
    let raw = explicit
        .or_else(|| env::var("MICROSERVICEDB_DATABASE_GRPC_ADDRESS").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_GRPC_ADDRESS.to_string());
    let tls_env = env::var("MICROSERVICEDB_GRPC_TLS").map(|v| v.trim().eq_ignore_ascii_case("true")).unwrap_or(false);
    match raw.split_once("://") {
        Some(_) => raw,
        None if tls_env => format!("https://{raw}"),
        None => format!("http://{raw}"),
    }
}

fn grpc_tls_config(ca_file: Option<String>) -> Result<tonic::transport::ClientTlsConfig, String> {
    let tls = tonic::transport::ClientTlsConfig::new();
    let ca_file = ca_file
        .or_else(|| env::var("MICROSERVICEDB_TLS_CA_FILE").ok())
        .filter(|f| !f.trim().is_empty());
    match ca_file {
        Some(path) => {
            let pem = std::fs::read(path.trim()).map_err(|e| format!("Could not read CA '{path}': {e}"))?;
            Ok(tls.ca_certificate(tonic::transport::Certificate::from_pem(pem)))
        }
        None => Ok(tls.with_native_roots()),
    }
}
const DEFAULT_REQ_TIMEOUT: Duration = Duration::from_secs(15);
const RECONNECT_DELAY: Duration = Duration::from_secs(3);

// =============================================================================
// Singleton global
// =============================================================================

type InstanceMap = HashMap<String, Arc<MicroserviceDBAPI>>;

static mut INSTANCES: Option<Mutex<InstanceMap>> = None;
static INSTANCES_INIT: Once = Once::new();

#[allow(static_mut_refs)]
fn instances() -> &'static Mutex<InstanceMap> {
    INSTANCES_INIT.call_once(|| {
        // Safety: call_once garantiza exactamente una escritura.
        unsafe {
            INSTANCES = Some(Mutex::new(HashMap::new()));
        }
    });
    // Safety: ya inicializado por call_once.
    unsafe { INSTANCES.as_ref().unwrap() }
}

// =============================================================================
// Tipos internos
// =============================================================================

#[derive(Debug, Clone, Deserialize)]
struct WsResponse {
    pub success: bool,
    #[serde(default)]
    pub response_json: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsChange {
    #[allow(dead_code)]
    pub kind: String,
    pub category: String,
    pub event: String,
    pub collection_name: String,
    #[serde(default)]
    pub before: Option<Value>,
    #[serde(default)]
    pub after: Option<Value>,
}

struct Pending {
    tx: oneshot::Sender<Result<Value, String>>,
}

struct Inner {
    database_name: String,
    precache_collections: Mutex<Vec<String>>,
    ready: Mutex<bool>,
    close_flag: Mutex<bool>,
    writer_tx: mpsc::UnboundedSender<Value>,
    pending: Mutex<VecDeque<Pending>>,
    offline_queue: Mutex<Vec<Value>>,
    listeners: Mutex<Vec<ChangeListener>>,
    transport: Transport,
    grpc_channel: Option<Channel>,
    api_key: Option<String>,
}

// =============================================================================
// MicroserviceDBAPI
// =============================================================================

#[derive(Clone)]
pub struct MicroserviceDBAPI {
    inner: Arc<Inner>,
}

impl MicroserviceDBAPI {
    /// Crea un cliente con las opciones del entorno (`ClientOptions::default()`).
    pub fn new(database_name: impl Into<String>, collections: &[&str]) -> Self {
        Self::with_options(database_name, collections, ClientOptions::default())
    }

    /// Crea un cliente con transporte explícito.
    pub fn with_options(
        database_name: impl Into<String>,
        collections: &[&str],
        options: ClientOptions,
    ) -> Self {
        let database_name: String = database_name.into();
        let grpc_channel = if options.transport == Transport::Grpc {
            let address = resolve_grpc_address(options.grpc_address.clone());
            let endpoint = Endpoint::from_shared(address.clone())
                .map_err(|e| e.to_string())
                .and_then(|ep| {
                    if address.starts_with("https://") {
                        let tls = grpc_tls_config(options.tls_ca_file.clone())?;
                        ep.tls_config(tls).map_err(|e| e.to_string())
                    } else {
                        Ok(ep)
                    }
                });
            match endpoint {
                Ok(ep) => Some(
                    ep.connect_timeout(Duration::from_secs(10))
                        .http2_keep_alive_interval(Duration::from_secs(30))
                        .keep_alive_while_idle(true)
                        .connect_lazy(),
                ),
                Err(e) => {
                    eprintln!("[MicroserviceDBAPI] invalid gRPC address '{address}': {e}");
                    None
                }
            }
        } else {
            None
        };
        let precache = collections
            .iter()
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        let (writer_tx, writer_rx) = mpsc::unbounded_channel::<Value>();

        let inner = Arc::new(Inner {
            database_name,
            precache_collections: Mutex::new(precache),
            ready: Mutex::new(false),
            close_flag: Mutex::new(false),
            writer_tx,
            pending: Mutex::new(VecDeque::new()),
            offline_queue: Mutex::new(Vec::new()),
            listeners: Mutex::new(Vec::new()),
            transport: options.transport,
            grpc_channel,
            api_key: options
                .api_key
                .clone()
                .or_else(|| env::var("MICROSERVICEDB_API_KEY").ok())
                .map(|k| k.trim().to_string())
                .filter(|k| !k.is_empty()),
        });

        let api = Self {
            inner: inner.clone(),
        };

        if inner.transport == Transport::Grpc {
            drop(writer_rx);
            tokio::spawn(async move {
                Self::grpc_supervisor(inner).await;
            });
        } else {
            tokio::spawn(async move {
                Self::supervisor_loop(inner, writer_rx).await;
            });
        }

        api
    }

    pub async fn get_instance(
        database_name: impl Into<String>,
        collections: &[&str],
    ) -> Arc<MicroserviceDBAPI> {
        let key: String = database_name.into();
        if key.is_empty() {
            panic!("MicroserviceDBAPI::get_instance: databaseName is required");
        }

        let mut guard = instances().lock().await;
        if let Some(existing) = guard.get(&key) {
            if !collections.is_empty() {
                let mut pc = existing.inner.precache_collections.lock().await;
                let mut set: std::collections::HashSet<String> = pc.drain(..).collect();
                for c in collections {
                    if !c.is_empty() {
                        set.insert(c.to_string());
                    }
                }
                *pc = set.into_iter().collect();
            }
            return existing.clone();
        }

        let inst = Arc::new(Self::new(key.clone(), collections));
        guard.insert(key, inst.clone());
        inst
    }

    // -------------------------------------------------------------------------
    // Configuración de URL
    // -------------------------------------------------------------------------

    fn resolve_ws_url() -> String {
        for var in [
            "MICROSERVICEDB_DATABASE_WS_URL",
            "MICROSERVICEDB_DATABASE_URL",
            "MICROSERVICEDB_DATABASE_BASE_URL",
        ] {
            if let Ok(raw) = env::var(var) {
                let normalized = Self::normalize_ws_url(&raw);
                if !normalized.is_empty() {
                    return normalized;
                }
            }
        }
        DEFAULT_WS_URL.to_string()
    }

    fn normalize_ws_url(raw: &str) -> String {
        let input = raw.trim();
        if input.is_empty() {
            return String::new();
        }
        let mut candidate = input.to_string();

        if !candidate.contains("://") {
            candidate = if candidate.starts_with("//") {
                format!("ws:{candidate}")
            } else {
                format!("ws://{candidate}")
            };
        }

        if let Some(rest) = candidate.strip_prefix("http://") {
            candidate = format!("ws://{rest}");
        } else if let Some(rest) = candidate.strip_prefix("https://") {
            candidate = format!("wss://{rest}");
        }

        let (scheme, rest) = match candidate.split_once("://") {
            Some(p) => p,
            None => return String::new(),
        };
        if scheme != "ws" && scheme != "wss" {
            return String::new();
        }

        let (authority, path_and_more) = match rest.split_once('/') {
            Some((a, p)) => (a, format!("/{p}")),
            None => (rest, String::new()),
        };

        let path_no_query = path_and_more
            .split_once('?')
            .map(|(p, _)| p.to_string())
            .unwrap_or(path_and_more);
        let path_no_frag = path_no_query
            .split_once('#')
            .map(|(p, _)| p.to_string())
            .unwrap_or(path_no_query);
        let path_clean = path_no_frag.trim_end_matches('/').to_string();

        let final_path = if path_clean.is_empty() {
            "/ws".to_string()
        } else {
            path_clean
        };

        format!("{scheme}://{authority}{final_path}")
    }

    // -------------------------------------------------------------------------
    // Bucle supervisor / conexión
    // -------------------------------------------------------------------------

    async fn supervisor_loop(
        inner: Arc<Inner>,
        mut writer_rx: mpsc::UnboundedReceiver<Value>,
    ) {
        loop {
            if *inner.close_flag.lock().await {
                return;
            }

            let base_url = Self::resolve_ws_url();
            // La key viaja como ?api_key= (igual que el cliente TS y la UI).
            let url = match inner.api_key.as_deref() {
                Some(k) => format!("{base_url}?api_key={k}"),
                None => base_url,
            };
            match tokio_tungstenite::connect_async(&url).await {
                Ok((ws_stream, _resp)) => {
                    let (mut sink, mut stream) = ws_stream.split();

                    if let Err(e) = Self::on_ws_open(&inner, &mut sink, &mut writer_rx).await {
                        eprintln!(
                            "[MicroserviceDBAPI:{0}] on_ws_open error: {e}",
                            inner.database_name
                        );
                    }
                    *inner.ready.lock().await = true;

                    let mut writer_rx_closed = false;
                    loop {
                        tokio::select! {
                            biased;

                            msg = writer_rx.recv(), if !writer_rx_closed => match msg {
                                Some(val) => {
                                    let text = serde_json::to_string(&val).unwrap_or_default();
                                    if sink.send(WsMessage::Text(text)).await.is_err() {
                                        break;
                                    }
                                }
                                None => {
                                    writer_rx_closed = true;
                                }
                            },

                            frame = stream.next() => {
                                match frame {
                                    Some(Ok(WsMessage::Text(text))) => {
                                        Self::dispatch_incoming(&inner, text).await;
                                    }
                                    Some(Ok(WsMessage::Binary(_))) => {}
                                    Some(Ok(WsMessage::Close(_))) | None => break,
                                    Some(Err(_)) => break,
                                    _ => {}
                                }
                            }

                            _ = tokio::time::sleep(Duration::from_millis(500)) => {
                                if *inner.close_flag.lock().await {
                                    let _ = sink.close().await;
                                    Self::reject_all_pending(&inner, "MicroserviceDBAPI closed").await;
                                    return;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[MicroserviceDBAPI:{0}] connect failed ({e:?}), retrying in 3s...",
                        inner.database_name
                    );
                }
            }

            *inner.ready.lock().await = false;
            Self::reject_all_pending(&inner, "WebSocket disconnected").await;

            if *inner.close_flag.lock().await {
                return;
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }

    /// gRPC: marca listo, envía precache y mantiene `StreamChanges` con reconexión.
    async fn grpc_supervisor(inner: Arc<Inner>) {
        let Some(channel) = inner.grpc_channel.clone() else { return };
        *inner.ready.lock().await = true;

        let precache = inner.precache_collections.lock().await.clone();
        if !precache.is_empty() {
            let req = Self::to_action_request(&json!({
                "type": "precache",
                "database_name": inner.database_name,
                "collections": precache,
            }));
            if let Err(e) = grpc_unary::<_, pb::ActionResponse>(channel.clone(), GRPC_ACTION, req, inner.api_key.as_deref()).await {
                eprintln!("[MicroserviceDBAPI:{}] gRPC precache failed: {e}", inner.database_name);
            }
        }

        loop {
            if *inner.close_flag.lock().await {
                return;
            }
            let req = pb::StreamChangesRequest {
                database_name: inner.database_name.clone(),
                collection_name: String::new(),
                include_system: false,
            };
            match grpc_stream::<_, pb::ChangeEvent>(channel.clone(), GRPC_STREAM_CHANGES, req, inner.api_key.as_deref()).await {
                Ok(mut stream) => loop {
                    tokio::select! {
                        msg = stream.message() => match msg {
                            Ok(Some(ev)) => Self::dispatch_incoming(&inner, ev.event_json).await,
                            _ => break,
                        },
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {
                            if *inner.close_flag.lock().await {
                                return;
                            }
                        }
                    }
                },
                Err(e) => eprintln!(
                    "[MicroserviceDBAPI:{}] gRPC StreamChanges failed ({e}), retrying in 3s...",
                    inner.database_name
                ),
            }
            if *inner.close_flag.lock().await {
                return;
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }

    /// Convierte un mensaje del protocolo JSON (`type`, `category`, `function_name`,
    /// `payload`) en un `ActionRequest` de gRPC.
    fn to_action_request(msg: &Value) -> pb::ActionRequest {
        let get = |k: &str| msg.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
        let payload = match msg.get("payload") {
            Some(p) if !p.is_null() => p.clone(),
            _ => {
                // precache / metadata llevan los datos en el nivel superior.
                let mut m = msg.as_object().cloned().unwrap_or_default();
                for k in ["type", "category", "function_name"] {
                    m.remove(k);
                }
                Value::Object(m)
            }
        };
        pb::ActionRequest {
            msg_type: get("type"),
            category: get("category"),
            function_name: get("function_name"),
            payload_json: payload.to_string(),
            request_id: String::new(),
        }
    }

    async fn on_ws_open<
        S: SinkExt<WsMessage, Error = E> + std::marker::Unpin,
        E: std::fmt::Display,
    >(
        inner: &Arc<Inner>,
        sink: &mut S,
        writer_rx: &mut mpsc::UnboundedReceiver<Value>,
    ) -> Result<(), String> {
        let precache = inner.precache_collections.lock().await.clone();
        if !precache.is_empty() {
            let req = json!({
                "type": "precache",
                "database_name": inner.database_name,
                "collections": precache,
            });
            let text = serde_json::to_string(&req).map_err(|e| e.to_string())?;
            sink.send(WsMessage::Text(text))
                .await
                .map_err(|e| e.to_string())?;
        }
        drop(precache);

        let offline: Vec<Value> = inner.offline_queue.lock().await.drain(..).collect();
        for msg in offline {
            let text = serde_json::to_string(&msg).map_err(|e| e.to_string())?;
            sink.send(WsMessage::Text(text))
                .await
                .map_err(|e| e.to_string())?;
        }
        while let Ok(msg) = writer_rx.try_recv() {
            let text = serde_json::to_string(&msg).map_err(|e| e.to_string())?;
            sink.send(WsMessage::Text(text))
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn dispatch_incoming(inner: &Arc<Inner>, text: String) {
        let Ok(msg) = serde_json::from_str::<Value>(&text) else {
            return;
        };

        if msg.get("kind").and_then(|v| v.as_str()) == Some("change") {
            if let Ok(evt) = serde_json::from_value::<WsChange>(msg.clone()) {
                let change_event = ChangeEvent {
                    category: evt.category,
                    collection_name: evt.collection_name,
                    event: evt.event,
                    before: evt.before,
                    after: evt.after,
                };
                for listener in inner.listeners.lock().await.iter() {
                    listener(change_event.clone());
                }
            }
            return;
        }

        if msg.get("success").and_then(|v| v.as_bool()).is_some() {
            if let Ok(resp) = serde_json::from_value::<WsResponse>(msg) {
                let payload = if resp.success {
                    let parsed = resp
                        .response_json
                        .as_deref()
                        .and_then(|s| serde_json::from_str::<Value>(s).ok())
                        .unwrap_or(Value::Null);
                    Ok(parsed)
                } else {
                    Err(resp.message.unwrap_or_else(|| "Action failed".into()))
                };

                let mut queue = inner.pending.lock().await;
                while let Some(pending) = queue.pop_front() {
                    if pending.tx.send(payload.clone()).is_ok() {
                        break;
                    }
                }
            }
        }
    }

    async fn reject_all_pending(inner: &Arc<Inner>, reason: &str) {
        let mut pending = inner.pending.lock().await;
        for p in pending.drain(..) {
            let _ = p.tx.send(Err(reason.to_string()));
        }
    }

    // -------------------------------------------------------------------------
    // Envío de requests
    // -------------------------------------------------------------------------

    #[allow(dead_code)]
    async fn send(&self, payload: Value) {
        if *self.inner.ready.lock().await {
            let _ = self.inner.writer_tx.send(payload);
        } else {
            self.inner.offline_queue.lock().await.push(payload);
        }
    }

    async fn send_and_wait<T: for<'de> Deserialize<'de>>(
        &self,
        payload: Value,
    ) -> Result<T, String> {
        self.ensure_ready().await?;

        if let Some(channel) = self.inner.grpc_channel.clone() {
            let req = Self::to_action_request(&payload);
            let resp = match tokio::time::timeout(
                DEFAULT_REQ_TIMEOUT,
                grpc_unary::<_, pb::ActionResponse>(channel, GRPC_ACTION, req, self.inner.api_key.as_deref()),
            )
            .await
            {
                Ok(Ok(r)) => r,
                Ok(Err(status)) => return Err(format!("gRPC error: {}", status.message())),
                Err(_) => return Err("Request timeout".to_string()),
            };
            if !resp.success {
                return Err(if resp.message.is_empty() { "Action failed".into() } else { resp.message });
            }
            let value = serde_json::from_str::<Value>(&resp.response_json).unwrap_or(Value::Null);
            return serde_json::from_value::<T>(value).map_err(|e| format!("Invalid response shape: {e}"));
        }

        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().await.push_back(Pending { tx });

        if *self.inner.ready.lock().await {
            let _ = self.inner.writer_tx.send(payload);
        } else {
            self.inner.offline_queue.lock().await.push(payload);
        }

        let value: Value = match tokio::time::timeout(DEFAULT_REQ_TIMEOUT, rx).await {
            Ok(Ok(Ok(v))) => v,
            Ok(Ok(Err(e))) => return Err(e),
            Ok(Err(_)) => return Err("Request cancelled".to_string()),
            Err(_) => return Err("Request timeout".to_string()),
        };

        serde_json::from_value::<T>(value)
            .map_err(|e| format!("Invalid response shape: {e}"))
    }

    async fn ensure_ready(&self) -> Result<(), String> {
        for _ in 0..50 {
            if *self.inner.ready.lock().await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(match self.inner.transport {
            Transport::Grpc => "gRPC not available".to_string(),
            Transport::WebSocket => "WebSocket not available".to_string(),
        })
    }

    // -------------------------------------------------------------------------
    // API pública
    // -------------------------------------------------------------------------

    pub async fn insert_one(
        &self,
        collection_name: impl Into<String>,
        document: Value,
    ) -> Result<InsertResult, String> {
        let collection_name = collection_name.into();
        let payload = json!({
            "type": "action",
            "category": "collection",
            "function_name": "insertOne",
            "payload": {
                "database_name": self.inner.database_name,
                "collectionName": collection_name,
                "document": document,
            }
        });

        self.send_and_wait::<InsertResult>(payload).await
    }

    pub async fn find_one(
        &self,
        collection_name: impl Into<String>,
        query: Value,
    ) -> Result<Option<Value>, String> {
        let payload = json!({
            "type": "action",
            "category": "collection",
            "function_name": "findOne",
            "payload": {
                "database_name": self.inner.database_name,
                "collectionName": collection_name.into(),
                "filter": query,
            }
        });
        let val: Value = self.send_and_wait(payload).await?;
        Ok(if val.is_null() { None } else { Some(val) })
    }

    pub async fn find(
        &self,
        collection_name: impl Into<String>,
        query: Option<Value>,
        options: Option<FindOptions>,
    ) -> Result<Option<Vec<Value>>, String> {
        let collection_name = collection_name.into();
        let query = query.unwrap_or_else(|| Value::Object(Map::new()));
        let options = options.unwrap_or_default();

        let mut payload_obj = Map::new();
        payload_obj.insert(
            "database_name".into(),
            Value::String(self.inner.database_name.clone()),
        );
        payload_obj.insert("collectionName".into(), Value::String(collection_name.clone()));
        payload_obj.insert("filter".into(), query.clone());
        if let Some(s) = options.sort.as_ref() {
            payload_obj.insert(
                "sort".into(),
                serde_json::to_value(s).unwrap_or(Value::Null),
            );
        }
        if let Some(p) = options.projection.as_ref() {
            payload_obj.insert(
                "projection".into(),
                serde_json::to_value(p).unwrap_or(Value::Null),
            );
        }
        if let Some(l) = options.limit {
            payload_obj.insert("limit".into(), Value::from(l));
        }
        if let Some(s) = options.skip {
            payload_obj.insert("skip".into(), Value::from(s));
        }

        let payload = json!({
            "type": "action",
            "category": "collection",
            "function_name": "find",
            "payload": Value::Object(payload_obj),
        });

        let arr: Value = self.send_and_wait(payload).await?;
        if arr.is_null() {
            return Ok(None);
        }
        let rows = match arr {
            Value::Array(v) => v,
            other => vec![other],
        };
        Ok(Some(rows))
    }

    pub async fn update_collection(
        &self,
        collection_name: impl Into<String>,
        data: Value,
    ) -> Result<Value, String> {
        let payload = json!({
            "type": "action",
            "category": "collection",
            "function_name": "updateCollection",
            "payload": {
                "database_name": self.inner.database_name,
                "collectionName": collection_name.into(),
                "data": data,
            }
        });
        self.send_and_wait(payload).await
    }

    pub async fn update_one(
        &self,
        collection_name: impl Into<String>,
        filter: Value,
        update: Value,
        options: Option<Value>,
    ) -> Result<UpdateResult, String> {
        let cn = collection_name.into();
        let payload = json!({
            "type": "action",
            "category": "collection",
            "function_name": "updateOne",
            "payload": {
                "database_name": self.inner.database_name,
                "collectionName": cn.clone(),
                "filter": filter,
                "update": update,
                "options": options.unwrap_or(Value::Object(Map::new())),
            }
        });
        self.send_and_wait(payload).await
    }

    pub async fn replace_one(
        &self,
        collection_name: impl Into<String>,
        filter: Value,
        replacement: Value,
        options: Option<Value>,
    ) -> Result<UpdateResult, String> {
        let cn = collection_name.into();
        let payload = json!({
            "type": "action",
            "category": "collection",
            "function_name": "replaceOne",
            "payload": {
                "database_name": self.inner.database_name,
                "collectionName": cn.clone(),
                "filter": filter,
                "replacement": replacement,
                "options": options.unwrap_or(Value::Object(Map::new())),
            }
        });
        let result: UpdateResult = self.send_and_wait(payload).await?;

        Ok(result)
    }

    pub async fn delete_one(
        &self,
        collection_name: impl Into<String>,
        filter: Value,
    ) -> Result<DeleteResult, String> {
        let cn = collection_name.into();
        let payload = json!({
            "type": "action",
            "category": "collection",
            "function_name": "deleteOne",
            "payload": {
                "database_name": self.inner.database_name,
                "collectionName": cn.clone(),
                "filter": filter,
            }
        });
        let result: DeleteResult = self.send_and_wait(payload).await?;
        Ok(result)
    }

    pub async fn get_metadata(&self) -> Value {
        let payload = json!({
            "type": "metadata",
            "database_name": self.inner.database_name,
        });
        self.send_and_wait::<Value>(payload)
            .await
            .unwrap_or(Value::Null)
    }

    pub async fn subscribe(&self, listener: ChangeListener) -> impl Fn() -> bool {
        self.inner.listeners.lock().await.push(listener);
        let inner = self.inner.clone();
        move || {
            // Best-effort unsubscribe: remove last-inserted matching Arc ref.
            // Safe: uses try_lock so it never deadlocks if called during emit.
            if let Ok(mut g) = inner.listeners.try_lock() {
                if !g.is_empty() {
                    g.pop();
                    return true;
                }
            }
            false
        }
    }

    /// Transporte activo.
    pub fn transport(&self) -> Transport {
        self.inner.transport
    }

    /// Exporta una colección por lotes (solo gRPC). `on_batch` recibe cada lote,
    /// el total de documentos y cuántos se han enviado. Devuelve el total enviado.
    pub async fn export_collection_stream<F>(
        &self,
        collection_name: impl Into<String>,
        filter: Option<Value>,
        batch_size: u32,
        mut on_batch: F,
    ) -> Result<u64, String>
    where
        F: FnMut(Vec<Value>, u64, u64),
    {
        let channel = self
            .inner
            .grpc_channel
            .clone()
            .ok_or_else(|| "export_collection_stream requires the gRPC transport".to_string())?;
        let req = pb::ExportCollectionRequest {
            database_name: self.inner.database_name.clone(),
            collection_name: collection_name.into(),
            filter_json: filter.map(|f| f.to_string()).unwrap_or_default(),
            batch_size,
        };
        let mut stream = grpc_stream::<_, pb::DocumentBatch>(channel, GRPC_EXPORT_STREAM, req, self.inner.api_key.as_deref())
            .await
            .map_err(|e| format!("gRPC error: {}", e.message()))?;
        let mut sent = 0;
        while let Some(batch) = stream.message().await.map_err(|e| format!("gRPC error: {}", e.message()))? {
            let docs: Vec<Value> = serde_json::from_str(&batch.documents_json).map_err(|e| e.to_string())?;
            sent = batch.sent;
            on_batch(docs, batch.total, batch.sent);
        }
        Ok(sent)
    }

    pub async fn close(&self) {
        *self.inner.close_flag.lock().await = true;
        *self.inner.ready.lock().await = false;
        Self::reject_all_pending(&self.inner, "MicroserviceDBAPI closed").await;
    }
}
