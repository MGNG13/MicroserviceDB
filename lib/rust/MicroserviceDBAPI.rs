use std::collections::{HashMap, VecDeque};
use std::env;
use std::sync::{Arc, Once};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::tungstenite::Message as WsMessage;

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

#[derive(Default, Clone)]
struct FindCacheEntry {
    collection_name: String,
    query: Value,
    options: FindOptions,
    result: Vec<Value>,
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
    find_cache: Mutex<HashMap<String, FindCacheEntry>>,
}

// =============================================================================
// MicroserviceDBAPI
// =============================================================================

#[derive(Clone)]
pub struct MicroserviceDBAPI {
    inner: Arc<Inner>,
}

impl MicroserviceDBAPI {
    pub fn new(database_name: impl Into<String>, collections: &[&str]) -> Self {
        let database_name: String = database_name.into();
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
            find_cache: Mutex::new(HashMap::new()),
        });

        let api = Self {
            inner: inner.clone(),
        };

        tokio::spawn(async move {
            Self::supervisor_loop(inner, writer_rx).await;
        });

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

            let url = Self::resolve_ws_url();
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
                Self::update_find_caches_from_change(inner, &evt).await;
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
    // Helpers internos de caché
    // -------------------------------------------------------------------------

    fn build_find_cache_key(
        collection_name: &str,
        query: &Value,
        options: &FindOptions,
    ) -> String {
        json!({
            "collectionName": collection_name,
            "query": query,
            "options": options,
        })
        .to_string()
    }

    fn is_default_snapshot(entry: &FindCacheEntry) -> bool {
        let q = entry.query.as_object().cloned().unwrap_or_default();
        if !q.is_empty() {
            return false;
        }
        entry.options.sort.is_none()
            && entry.options.projection.is_none()
            && entry.options.skip.unwrap_or(0) == 0
    }

    fn doc_key(doc: &Value) -> Option<String> {
        Some(doc.as_object()?.get("_id")?.to_string())
    }

    async fn clear_find_caches_for_collection(inner: &Arc<Inner>, collection: &str) {
        let mut cache = inner.find_cache.lock().await;
        cache.retain(|_, v| v.collection_name != collection);
    }

    async fn update_find_caches_from_change(inner: &Arc<Inner>, evt: &WsChange) {
        if evt.category != "collection" {
            return;
        }
        let mut cache = inner.find_cache.lock().await;
        let keys = cache.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let should_remove = {
                let entry = match cache.get(&key) {
                    Some(e) => e,
                    None => continue,
                };
                if entry.collection_name != evt.collection_name {
                    continue;
                }
                !Self::is_default_snapshot(entry)
            };
            if should_remove {
                cache.remove(&key);
                continue;
            }
            let entry = cache.get_mut(&key).unwrap();
            let before_key = evt.before.as_ref().and_then(Self::doc_key);
            let after_key = evt.after.as_ref().and_then(Self::doc_key);

            match evt.event.as_str() {
                "insertOne" => {
                    if let Some(after) = &evt.after {
                        let ak = Self::doc_key(after);
                        entry.result.retain(|r| Self::doc_key(r).as_ref() != ak.as_ref());
                        entry.result.insert(0, after.clone());
                        if let Some(lim) = entry.options.limit {
                            entry.result.truncate(lim.max(0) as usize);
                        }
                    }
                }
                "updateOne" | "replaceOne" => {
                    if let Some(after) = &evt.after {
                        entry.result.retain(|r| {
                            let k = Self::doc_key(r);
                            k.as_ref() != before_key.as_ref()
                                && k.as_ref() != after_key.as_ref()
                        });
                        entry.result.insert(0, after.clone());
                        if let Some(lim) = entry.options.limit {
                            entry.result.truncate(lim.max(0) as usize);
                        }
                    }
                }
                "deleteOne" => {
                    if let Some(bk) = before_key {
                        entry
                            .result
                            .retain(|r| Self::doc_key(r).as_deref() != Some(bk.as_str()));
                    }
                }
                _ => {
                    cache.remove(&key);
                }
            }
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
        Err("WebSocket not available".to_string())
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

        let result: InsertResult = self.send_and_wait::<InsertResult>(payload).await?;

        let mut next_doc = document.clone();
        if let Some(obj) = next_doc.as_object_mut() {
            if !obj.contains_key("_id") {
                if let Some(id) = result.inserted_id.clone() {
                    obj.insert("_id".into(), id);
                }
            }
        }
        self.apply_local_mutation(&collection_name, "insertOne", None, Some(next_doc))
            .await;

        Ok(result)
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

        let key = Self::build_find_cache_key(&collection_name, &query, &options);
        let mut cache = self.inner.find_cache.lock().await;
        cache.insert(
            key,
            FindCacheEntry {
                collection_name: collection_name.clone(),
                query,
                options,
                result: rows.clone(),
            },
        );
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
        let result: UpdateResult = self.send_and_wait(payload).await?;
        if result.modified_count > 0 || result.upserted_count > 0 {
            Self::clear_find_caches_for_collection(&self.inner, &cn).await;
        }
        Ok(result)
    }

    pub async fn replace_one(
        &self,
        collection_name: impl Into<String>,
        filter: Value,
        replacement: Value,
        options: Option<Value>,
    ) -> Result<UpdateResult, String> {
        let cn = collection_name.into();

        let before = filter
            .as_object()
            .filter(|o| o.len() == 1 && o.contains_key("_id"))
            .and_then(|o| Some(json!({ "_id": o.get("_id")?.clone() })));

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

        if result.modified_count > 0 || result.upserted_count > 0 {
            let mut after = replacement.clone();
            if let Some(obj) = after.as_object_mut() {
                if !obj.contains_key("_id") {
                    if let Some(b) = before.as_ref().and_then(|b| b.get("_id").cloned()) {
                        obj.insert("_id".into(), b);
                    } else if let Some(id) = result.upserted_id.clone() {
                        obj.insert("_id".into(), id);
                    }
                }
            }
            self.apply_local_mutation(&cn, "replaceOne", before, Some(after))
                .await;
        }
        Ok(result)
    }

    pub async fn delete_one(
        &self,
        collection_name: impl Into<String>,
        filter: Value,
    ) -> Result<DeleteResult, String> {
        let cn = collection_name.into();
        let before = filter
            .as_object()
            .filter(|o| o.len() == 1 && o.contains_key("_id"))
            .and_then(|o| Some(json!({ "_id": o.get("_id")?.clone() })));

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
        if result.deleted_count > 0 {
            self.apply_local_mutation(&cn, "deleteOne", before, None)
                .await;
        }
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

    pub fn get_cached_find(
        &self,
        collection_name: impl Into<String>,
        query: Option<Value>,
        options: Option<FindOptions>,
    ) -> Option<Vec<Value>> {
        let cn = collection_name.into();
        let q = query.unwrap_or_else(|| Value::Object(Map::new()));
        let o = options.unwrap_or_default();
        let key = Self::build_find_cache_key(&cn, &q, &o);
        self.inner
            .find_cache
            .try_lock()
            .ok()
            .and_then(|g| g.get(&key).map(|e| e.result.clone()))
    }

    pub async fn close(&self) {
        *self.inner.close_flag.lock().await = true;
        *self.inner.ready.lock().await = false;
        Self::reject_all_pending(&self.inner, "MicroserviceDBAPI closed").await;
        self.inner.find_cache.lock().await.clear();
    }

    async fn apply_local_mutation(
        &self,
        collection_name: &str,
        event: &str,
        before: Option<Value>,
        after: Option<Value>,
    ) {
        let evt = WsChange {
            kind: "change".into(),
            category: "collection".into(),
            event: event.into(),
            collection_name: collection_name.into(),
            before: before.clone(),
            after: after.clone(),
        };
        Self::update_find_caches_from_change(&self.inner, &evt).await;

        let change_event = ChangeEvent {
            category: evt.category,
            collection_name: evt.collection_name,
            event: evt.event,
            before,
            after,
        };
        for listener in self.inner.listeners.lock().await.iter() {
            listener(change_event.clone());
        }
    }
}
