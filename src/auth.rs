//! Autenticación por API key y autorización por roles (RBAC).
//!
//! - Las API keys son tokens aleatorios de alta entropía; en el servidor solo se guarda su SHA-256
//!   (un hash lento como Argon2id no aporta nada con 192 bits de entropía y costaría ms por llamada).
//! - Roles, de menor a mayor: `read` < `readWrite` < `dbAdmin` < `admin`.
//!   `read`/`readWrite` se pueden limitar a una base y, opcionalmente, a una colección.
//!   `dbAdmin` se limita a una base. `admin` es global.
//!
//! Configuración:
//! - `MICROSERVICEDB_AUTH`: `required` (defecto) o `disabled`.
//! - `MICROSERVICEDB_ADMIN_API_KEY`: key de administrador en claro (mínimo 32 caracteres).
//! - `MICROSERVICEDB_API_KEYS_FILE`: JSON con las demás keys:
//!   `{"keys":[{"name":"svc-a","sha256":"<hex>","roles":[{"role":"readWrite","database":"shop","collection":"*"}]}]}`

use std::collections::HashMap;
use std::sync::Arc;

use rand::RngCore;
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub const ENV_AUTH: &str = "MICROSERVICEDB_AUTH";
pub const ENV_ADMIN_API_KEY: &str = "MICROSERVICEDB_ADMIN_API_KEY";
pub const ENV_API_KEYS_FILE: &str = "MICROSERVICEDB_API_KEYS_FILE";
const MIN_ADMIN_KEY_LEN: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Read = 1,
    ReadWrite = 2,
    DbAdmin = 3,
    Admin = 4,
}

impl Level {
    fn parse(s: &str) -> Result<Level, String> {
        match s {
            "read" => Ok(Level::Read),
            "readWrite" | "readwrite" | "read_write" => Ok(Level::ReadWrite),
            "dbAdmin" | "dbadmin" | "db_admin" => Ok(Level::DbAdmin),
            "admin" => Ok(Level::Admin),
            other => Err(format!("Unknown role '{other}' (read, readWrite, dbAdmin, admin)")),
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Level::Read => "read",
            Level::ReadWrite => "readWrite",
            Level::DbAdmin => "dbAdmin",
            Level::Admin => "admin",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Grant {
    pub level: Level,
    /// `None` = todas las bases.
    pub database: Option<String>,
    /// `None` = todas las colecciones de la base.
    pub collection: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Principal {
    pub name: String,
    pub grants: Vec<Grant>,
}

/// Permiso que exige una operación.
#[derive(Debug, Clone, PartialEq)]
pub struct Need {
    pub level: Level,
    pub database: Option<String>,
    pub collection: Option<String>,
}

impl Principal {
    pub fn anonymous_admin() -> Principal {
        Principal {
            name: "anonymous(auth disabled)".into(),
            grants: vec![Grant { level: Level::Admin, database: None, collection: None }],
        }
    }

    pub fn is_admin(&self) -> bool {
        self.grants.iter().any(|g| g.level == Level::Admin)
    }

    pub fn allows(&self, need: &Need) -> bool {
        self.grants.iter().any(|g| grant_covers(g, need))
    }

    /// ¿Puede leer algo de esta base? (para filtrar listados y eventos)
    pub fn can_read_database(&self, db: &str) -> bool {
        self.grants.iter().any(|g| {
            g.level == Level::Admin || g.database.as_deref().map_or(true, |d| d == db)
        })
    }

    pub fn can_read_collection(&self, db: &str, col: &str) -> bool {
        let need = Need {
            level: Level::Read,
            database: Some(db.to_string()),
            collection: if col.is_empty() { None } else { Some(col.to_string()) },
        };
        if col.is_empty() {
            // Eventos de nivel base: basta con acceso de lectura a algo de la base.
            return self.can_read_database(db);
        }
        self.allows(&need)
    }
}

fn grant_covers(g: &Grant, need: &Need) -> bool {
    if g.level == Level::Admin {
        return true;
    }
    if g.level < need.level || need.level == Level::Admin {
        return false;
    }
    let db_ok = match (&g.database, &need.database) {
        (None, _) => true,
        (Some(gd), Some(nd)) => gd == nd,
        (Some(_), None) => false,
    };
    if !db_ok {
        return false;
    }
    match (&g.collection, &need.collection) {
        (None, _) => true,
        (Some(gc), Some(nc)) => gc == nc,
        // La operación es de toda la base y el grant solo cubre una colección.
        (Some(_), None) => false,
    }
}

/// Permiso requerido por un request del protocolo. `None` = basta con estar autenticado.
pub fn required_permission(
    msg_type: &str,
    category: &str,
    function: &str,
    database: &str,
    collection: &str,
) -> Option<Need> {
    let db = (!database.is_empty()).then(|| database.to_string());
    let col = (!collection.is_empty()).then(|| collection.to_string());
    let need = |level, collection| Some(Need { level, database: db.clone(), collection });
    match msg_type {
        "metadata" | "list_databases" => None,
        "list_collections" | "precache" => need(Level::Read, None),
        "action" => match category {
            "collection" => match function {
                "find" | "findOne" | "exportCollection" | "listIndexes" => need(Level::Read, col),
                // Crear índices cambia el esquema de la colección: requiere dbAdmin en la base.
                "createIndex" => need(Level::DbAdmin, None),
                "insertOne" | "updateOne" | "replaceOne" | "deleteOne" | "deleteMany"
                | "updateCollection" | "importCollection" => need(Level::ReadWrite, col),
                _ => need(Level::ReadWrite, col),
            },
            "database" => match function {
                "exportDatabase" => need(Level::Read, None),
                "createCollection" | "deleteCollection" | "renameCollection" | "importDatabase" => {
                    need(Level::DbAdmin, None)
                }
                // createDatabase, deleteDatabase, renameDatabase y cualquier otra.
                _ => Some(Need { level: Level::Admin, database: None, collection: None }),
            },
            _ => Some(Need { level: Level::Admin, database: None, collection: None }),
        },
        _ => Some(Need { level: Level::Admin, database: None, collection: None }),
    }
}

pub fn sha256_hex(key: &str) -> String {
    let digest = Sha256::digest(key.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn generate_api_key() -> String {
    let mut bytes = [0u8; 24];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    format!("mdb_{}", bytes.iter().map(|b| format!("{b:02x}")).collect::<String>())
}

#[derive(Deserialize)]
struct KeysFile {
    keys: Vec<KeyEntry>,
}

#[derive(Deserialize)]
struct KeyEntry {
    name: String,
    sha256: String,
    roles: Vec<RoleEntry>,
}

#[derive(Deserialize)]
struct RoleEntry {
    role: String,
    #[serde(default)]
    database: Option<String>,
    #[serde(default)]
    collection: Option<String>,
}

fn scope(v: Option<String>) -> Option<String> {
    v.filter(|s| !s.is_empty() && s != "*")
}

#[derive(Clone)]
pub struct Auth {
    /// `None` = autenticación desactivada.
    keys: Option<Arc<HashMap<String, Arc<Principal>>>>,
}

impl Auth {
    pub fn disabled() -> Auth {
        Auth { keys: None }
    }

    pub fn from_keys(entries: Vec<(String, Principal)>) -> Auth {
        let map = entries.into_iter().map(|(h, p)| (h, Arc::new(p))).collect();
        Auth { keys: Some(Arc::new(map)) }
    }

    pub fn enabled(&self) -> bool {
        self.keys.is_some()
    }

    pub fn key_count(&self) -> usize {
        self.keys.as_ref().map_or(0, |k| k.len())
    }

    /// Carga la configuración desde el entorno.
    pub fn from_env() -> Result<Auth, String> {
        let mode = std::env::var(ENV_AUTH).unwrap_or_else(|_| "required".into());
        match mode.trim().to_ascii_lowercase().as_str() {
            "disabled" | "off" | "false" | "none" => return Ok(Auth::disabled()),
            "required" | "" | "on" | "true" => {}
            other => return Err(format!("{ENV_AUTH} must be 'required' or 'disabled', got '{other}'")),
        }

        let mut entries: Vec<(String, Principal)> = Vec::new();
        if let Ok(admin) = std::env::var(ENV_ADMIN_API_KEY) {
            let admin = admin.trim();
            if !admin.is_empty() {
                if admin.len() < MIN_ADMIN_KEY_LEN {
                    return Err(format!(
                        "{ENV_ADMIN_API_KEY} must have at least {MIN_ADMIN_KEY_LEN} characters"
                    ));
                }
                entries.push((
                    sha256_hex(admin),
                    Principal {
                        name: "admin".into(),
                        grants: vec![Grant { level: Level::Admin, database: None, collection: None }],
                    },
                ));
            }
        }
        if let Ok(path) = std::env::var(ENV_API_KEYS_FILE) {
            if !path.trim().is_empty() {
                let text = std::fs::read_to_string(path.trim())
                    .map_err(|e| format!("Could not read {ENV_API_KEYS_FILE} '{path}': {e}"))?;
                entries.extend(parse_keys_file(&text)?);
            }
        }
        if entries.is_empty() {
            return Err(format!(
                "Authentication is required but no API keys are configured. Set {ENV_ADMIN_API_KEY} \
                 and/or {ENV_API_KEYS_FILE} (generate one with `microservice-db generate-api-key`), \
                 or set {ENV_AUTH}=disabled for local development only."
            ));
        }
        Ok(Auth::from_keys(entries))
    }

    /// Devuelve el principal para una key presentada por el cliente.
    pub fn authenticate(&self, presented: Option<&str>) -> Result<Arc<Principal>, String> {
        let Some(keys) = &self.keys else {
            return Ok(Arc::new(Principal::anonymous_admin()));
        };
        let key = presented.map(str::trim).filter(|k| !k.is_empty()).ok_or("API key required")?;
        keys.get(&sha256_hex(key)).cloned().ok_or_else(|| "Invalid API key".to_string())
    }
}

pub fn parse_keys_file(text: &str) -> Result<Vec<(String, Principal)>, String> {
    let file: KeysFile =
        serde_json::from_str(text).map_err(|e| format!("Invalid API keys file: {e}"))?;
    let mut out = Vec::new();
    for k in file.keys {
        let hash = k.sha256.trim().to_ascii_lowercase();
        if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(format!("Key '{}': sha256 must be 64 hex characters", k.name));
        }
        if k.roles.is_empty() {
            return Err(format!("Key '{}' has no roles", k.name));
        }
        let mut grants = Vec::new();
        for r in k.roles {
            let level = Level::parse(&r.role).map_err(|e| format!("Key '{}': {e}", k.name))?;
            let database = scope(r.database);
            let collection = scope(r.collection);
            if level == Level::DbAdmin && collection.is_some() {
                return Err(format!("Key '{}': dbAdmin cannot be limited to a collection", k.name));
            }
            if collection.is_some() && database.is_none() {
                return Err(format!("Key '{}': a collection scope needs a database", k.name));
            }
            grants.push(Grant { level, database, collection });
        }
        out.push((hash, Principal { name: k.name, grants }));
    }
    Ok(out)
}

/// Extrae la key de `Authorization: Bearer <key>` o `x-api-key: <key>`.
pub fn key_from_headers(authorization: Option<&str>, x_api_key: Option<&str>) -> Option<String> {
    if let Some(v) = x_api_key.filter(|v| !v.trim().is_empty()) {
        return Some(v.trim().to_string());
    }
    let auth = authorization?.trim();
    let (scheme, rest) = auth.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| rest.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(grants: Vec<(Level, Option<&str>, Option<&str>)>) -> Principal {
        Principal {
            name: "t".into(),
            grants: grants
                .into_iter()
                .map(|(level, d, c)| Grant { level, database: d.map(Into::into), collection: c.map(Into::into) })
                .collect(),
        }
    }
    fn need(t: &str, cat: &str, f: &str, d: &str, c: &str) -> Need {
        required_permission(t, cat, f, d, c).unwrap()
    }

    #[test]
    fn rbac_rules() {
        let reader = p(vec![(Level::Read, Some("shop"), None)]);
        assert!(reader.allows(&need("action", "collection", "find", "shop", "items")));
        assert!(!reader.allows(&need("action", "collection", "insertOne", "shop", "items")));
        assert!(!reader.allows(&need("action", "collection", "find", "other", "items")));
        assert!(reader.allows(&need("action", "database", "exportDatabase", "shop", "")));

        let col_writer = p(vec![(Level::ReadWrite, Some("shop"), Some("items"))]);
        assert!(col_writer.allows(&need("action", "collection", "updateOne", "shop", "items")));
        assert!(!col_writer.allows(&need("action", "collection", "updateOne", "shop", "users")));
        assert!(!col_writer.allows(&need("action", "database", "exportDatabase", "shop", "")));

        let dba = p(vec![(Level::DbAdmin, Some("shop"), None)]);
        assert!(dba.allows(&need("action", "database", "createCollection", "shop", "")));
        assert!(dba.allows(&need("action", "collection", "deleteMany", "shop", "x")));
        assert!(dba.allows(&need("action", "collection", "createIndex", "shop", "x")));
        assert!(!col_writer.allows(&need("action", "collection", "createIndex", "shop", "items")));
        assert!(reader.allows(&need("action", "collection", "listIndexes", "shop", "items")));
        assert!(!dba.allows(&need("action", "database", "deleteDatabase", "shop", "")));
        assert!(!dba.allows(&need("action", "database", "createCollection", "other", "")));

        let admin = p(vec![(Level::Admin, None, None)]);
        assert!(admin.allows(&need("action", "database", "deleteDatabase", "shop", "")));
        assert!(required_permission("metadata", "", "", "", "").is_none());
    }

    #[test]
    fn keys_and_headers() {
        let key = generate_api_key();
        assert!(key.starts_with("mdb_") && key.len() == 52);
        let file = format!(
            r#"{{"keys":[{{"name":"svc","sha256":"{}","roles":[{{"role":"read","database":"shop","collection":"*"}}]}}]}}"#,
            sha256_hex(&key)
        );
        let auth = Auth::from_keys(parse_keys_file(&file).unwrap());
        let pr = auth.authenticate(Some(&key)).unwrap();
        assert_eq!(pr.name, "svc");
        assert!(pr.grants[0].collection.is_none());
        assert!(auth.authenticate(Some("mdb_wrong")).is_err());
        assert!(auth.authenticate(None).is_err());
        assert!(Auth::disabled().authenticate(None).unwrap().is_admin());
        assert_eq!(key_from_headers(Some("Bearer abc"), None).as_deref(), Some("abc"));
        assert_eq!(key_from_headers(Some("Basic abc"), Some("k")).as_deref(), Some("k"));
        assert_eq!(key_from_headers(Some("Basic abc"), None), None);
    }
}
