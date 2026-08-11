<div align="center">

<!--
  Banner opcional: coloca una imagen en docs/banner.png (o promotional_repository.png,
  como en tu repo CacheProxyComputer) y descomenta la línea de abajo.
  <img src="docs/banner.png" alt="MicroserviceDB Banner" width="100%" />
-->

<h1>MicroserviceDB</h1>

<p><strong>Motor de base de datos JSON en tiempo real sobre WebSocket, con panel administrativo embebido, caché Redis/Dragonfly, auto-indexado y backups automáticos.</strong></p>

<p>
  <img src="https://img.shields.io/badge/Rust-1.78+-000000?style=for-the-badge&logo=rust&logoColor=white" alt="Rust" />
  <img src="https://img.shields.io/badge/Tokio-async-0B7261?style=for-the-badge" alt="Tokio" />
  <img src="https://img.shields.io/badge/MongoDB-7.x-47A248?style=for-the-badge&logo=mongodb&logoColor=white" alt="MongoDB" />
  <img src="https://img.shields.io/badge/Cache-Redis%20%2F%20Dragonfly-DC382D?style=for-the-badge&logo=redis&logoColor=white" alt="Redis / Dragonfly" />
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
</p>
<p>
  <img src="https://img.shields.io/badge/WebSocket-API%20principal-4353FF?style=for-the-badge" alt="WebSocket" />
  <img src="https://img.shields.io/badge/Plataforma-Windows%20%7C%20Linux%20%7C%20macOS-555555?style=for-the-badge" alt="Plataforma" />
  <img src="https://img.shields.io/badge/Versi%C3%B3n-2.0.0-success?style=for-the-badge" alt="Versión" />
  <img src="https://img.shields.io/badge/Estado-Activo-success?style=for-the-badge" alt="Estado" />
</p>

<p>
Servidor de base de datos JSON escrito íntegramente en <strong>Rust asíncrono</strong> (Tokio · Warp · MongoDB) que expone toda su operativa a través de un <strong>WebSocket bidireccional</strong>. Un binario único y autocontenido: el panel administrativo se sirve desde el propio ejecutable.
</p>

<p><em>Autor: <a href="https://github.com/MGNG13">Magnus Norgaard (MGNG13)</a></em></p>

</div>

## 📑 Índice

1. [📌 Descripción General](#-descripción-general)
2. [✨ Características Principales](#-características-principales)
3. [🧠 Stack Tecnológico](#-stack-tecnológico)
4. [🏗️ Arquitectura](#️-arquitectura)
5. [📋 Requisitos Previos](#-requisitos-previos)
6. [🚀 Instalación Rápida](#-instalación-rápida)
   - [Con Docker Compose (recomendado)](#con-docker-compose-recomendado)
   - [Desde fuente](#desde-fuente)
7. [⚙️ Configuración](#️-configuración)
   - [Variables de Entorno](#variables-de-entorno)
   - [CLI](#cli)
8. [🌐 Endpoints HTTP](#-endpoints-http)
9. [🔌 API WebSocket](#-api-websocket)
   - [Formato de Petición](#formato-de-petición)
   - [Formato de Respuesta](#formato-de-respuesta)
   - [Comandos de `type`](#comandos-de-type)
   - [Categoría `collection`](#categoría-collection)
   - [Categoría `database`](#categoría-database)
   - [Eventos (change-stream)](#eventos-change-stream)
10. [📦 Librerías Cliente](#-librerías-cliente)
    - [TypeScript / Browser / Node.js](#typescript--browser--nodejs)
    - [Rust](#rust)
11. [🖥️ Panel Administrativo Web](#️-panel-administrativo-web)
12. [🧩 Caché y Auto-Indexado](#-caché-y-auto-indexado)
13. [💾 Backups Automáticos](#-backups-automáticos)
14. [🛠️ Scripts Utilitarios](#️-scripts-utilitarios)
15. [🧰 Builds Multiplataforma](#-builds-multiplataforma)
16. [📄 Licencia](#-licencia)

## 📌 Descripción General

**MicroserviceDB** es un servidor de base de datos JSON escrito íntegramente en **Rust asíncrono** (Tokio + Warp + MongoDB) que expone toda su operativa a través de **WebSocket bidireccional**. Se sitúa como capa intermedia entre tu aplicación y MongoDB, añadiendo:

- Panel administrativo SPA embebido en el binario (sin build step de frontend).
- Caché multi-nivel: caché de requests en Redis/Dragonfly + caché de documentos + gestor de índices.
- **Auto-indexado** dinámico basado en patrones de consulta.
- Backups incrementales cada 30 min comprimidos con 7-Zip.
- Change-streams broadcast vía WebSocket (`insertOne`, `updateOne`, `replaceOne`, `deleteOne`, …).
- API compatible con el estilo de MongoDB (`find`, `findOne`, `insertOne`, `updateOne`, `deleteMany`, …).
- TLS/SSL nativo configurable con dos variables de entorno.

El binario resultante es único y autocontenido; el panel HTML se sirve desde el propio ejecutable.

## ✨ Características Principales

| Capa                      | Detalle                                                             |
| ------------------------- | ------------------------------------------------------------------- |
| ⚙️ **Runtime**            | Rust 1.80+, Tokio async runtime completo                            |
| 🌐 **Servidor**           | Warp 0.3 con TLS nativo (rustls)                                    |
| 🗄️ **Persistencia**       | MongoDB 7.x (driver oficial 3.x), compatible con sharded clusters   |
| ⚡ **Caché**              | Redis / Dragonfly (opcional) — requests, documentos e índices       |
| 🧠 **Auto-indexado**      | Monitoriza campos frecuentemente consultados y propone/crea índices |
| 🔌 **Protocolo**          | WebSocket + JSON (principal) · HTTP POST `/event` (debug UI)        |
| 🖥️ **Panel Web**          | SPA en `MicroserviceDB.html` embebida con `include_str!`            |
| 💾 **Backups**            | JSON completo + 7z (LZMA2, nivel 9), con rotación de 2 slots/hora   |
| 📦 **Clientes oficiales** | TypeScript (browser + Node) y Rust                                  |
| 🚀 **Despliegue**         | Binario único · Docker Compose para Mongo sharded + Dragonfly       |

## 🧠 Stack Tecnológico

| Tecnología               | Rol en el proyecto                            |
| ------------------------ | --------------------------------------------- |
| **Rust (edición 2021)**  | Lenguaje central del servidor                 |
| **Tokio**                | Runtime asíncrono y concurrencia              |
| **Warp**                 | Servidor HTTP/WebSocket con TLS (rustls)      |
| **MongoDB (driver 3.x)** | Persistencia, compatible con clusters sharded |
| **Redis / Dragonfly**    | Caché de requests, documentos e índices       |
| **7-Zip (LZMA2)**        | Compresión de backups                         |
| **TypeScript & Rust**    | Librerías cliente oficiales                   |
| **Docker Compose**       | Orquestación del cluster de desarrollo        |

## 🏗️ Arquitectura

```
┌──────────────────────────────────────────────────────────────┐
│                   Aplicación / Panel Web                     │
│         (TypeScript / Rust client vía WebSocket)             │
└──────────────────────┬───────────────────────────────────────┘
                       │  ws(s)://host:3329/ws
                       ▼
┌──────────────────────────────────────────────────────────────┐
│                 MicroserviceDB (Rust / Tokio)                │
│  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐ │
│  │ Warp HTTP  │  │ Ws Router  │  │   Broadcast (tokio-    │ │
│  │  + TLS     │  │            │  │      broadcast)        │ │
│  └─────┬──────┘  └─────┬──────┘  └───────────┬────────────┘ │
│        │               │                     │                │
│  ┌─────┴───────────────┴─────────────────────┴────────────┐ │
│  │                    Motor MongoDB                        │ │
│  │  list · find · insert/update/delete · agg · admin ops  │ │
│  └─────┬──────────────────────┬───────────────────────────┘ │
│        │                      │                               │
│  ┌─────┴──────┐         ┌─────┴──────────────────────────┐  │
│  │ Caché L1   │         │  Auto-index manager            │  │
│  │ Dragonfly  │         │  + Redis index manager         │  │
│  └────────────┘         └────────────────────────────────┘  │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
       ┌───────────────────────────────┐
       │ MongoDB Standalone / Sharded  │
       └───────────────────────────────┘
```

## 📋 Requisitos Previos

| Componente            | Versión mínima                                           |
| --------------------- | -------------------------------------------------------- |
| **Rust**              | 1.78 (edición 2021)                                      |
| **MongoDB**           | 6.0+ (recomendado 7.x)                                   |
| **Redis / Dragonfly** | Cualquier versión compatible RESP2 (opcional)            |
| **7-Zip**             | Si se usa `--backup` (ejecutable `7z` o `7za` en `PATH`) |
| **Node.js**           | Solo si usas la librería TypeScript cliente (opcional)   |

## 🚀 Instalación Rápida

### Con Docker Compose (recomendado)

Levanta un **cluster sharded de MongoDB (3 shards × 3 nodos)** + **mongos router** + **Dragonfly** alojado en el mismo `docker-compose.yml`:

```bash
# Windows (PowerShell)
scripts\install\docker-compose_windows.ps1 up -d

# Linux / macOS
bash scripts/install/docker-compose_linux_mac.sh up -d

# O directamente
docker compose -f scripts/install/docker-compose.yml up -d
```

Lanza MicroserviceDB apuntando al cluster por defecto (las credenciales ya vienen listas en el `.env` de ejemplo):

```bash
cargo run --release
```

### Desde fuente

```bash
git clone https://github.com/MGNG13/MicroserviceDB.git
cd MicroserviceDB
cargo build --release
# El binario queda en target/release/json-db-server[.exe]
./target/release/json-db-server --backup
```

## ⚙️ Configuración

### Variables de Entorno

Edita el archivo `.env` en la raíz del proyecto (se carga con `dotenvy`). Todas las variables están prefijadas con `MICROSERVICEDB_` excepto los TTL de caché.

| Variable | Obligatoria | Default | Descripción |
| | :: |--|--|
| `MICROSERVICEDB_MONGODB_URI` | ✅ | `mongodb://127.0.0.1:27017/…` | URI de MongoDB (standalone, replSet o sharded `mongodb://mongos`) |
| `MICROSERVICEDB_DRAGONFLY_URL` | ❌ | — | URL de Redis/Dragonfly. Sin ella, la caché se desactiva pero el servidor funciona. |
| `MICROSERVICEDB_PORT` | ❌ | `3329` | Puerto HTTP / WebSocket |
| `MICROSERVICEDB_LOG_LEVEL` | ❌ | `info` | `error \| warn \| info \| debug \| trace` |
| `REQUEST_CACHE_TTL_SECS` | ❌ | `300` (5 min) | TTL en segundos de entradas en caché de requests |
| `MICROSERVICEDB_BACKUP_DIR` | ❌ si `--backup` | `./backups` | Directorio donde almacenar los backups |
| `MICROSERVICEDB_BACKUP_INTERVAL_MINS` | ❌ | `30` | Intervalo entre backups (minutos) |
| `MICROSERVICEDB_SSL_CERT` | ❌ | — | Ruta a certificado PEM (activa HTTPS/WSS) |
| `MICROSERVICEDB_SSL_KEY` | ❌ | — | Ruta a clave privada PEM (requerido si se usa cert) |

### CLI

```
json-db-server [--backup]

  --backup   Habilita el ciclo de backups automáticos cada N minutos.
             Requiere MICROSERVICEDB_BACKUP_DIR y 7z disponible.
```

## 🌐 Endpoints HTTP

MicroserviceDB solo expone 3 rutas HTTP; toda la operativa real se realiza vía WebSocket:

| Ruta | Método | Descripción |
|--|--| |
| `/` | `GET` | Devuelve el **Panel Administrativo Web** embebido (`MicroserviceDB.html`) |
| `/ws` | `GET` (Upgrade) | **Endpoint WebSocket principal** por donde viajan todas las queries |
| `/event` | `POST` | Endpoint de depuración del panel UI. Acepta JSON y lo loguea a nivel `debug`, devuelve `204 No Content` |

## 🔌 API WebSocket

Conecta a `ws://<host>:3329/ws` (o `wss://` si TLS está activado). El protocolo es JSON sobre mensajes de texto.

### Formato de Petición

```jsonc
{
  "type": "request", // "request" | "metadata" | "list_databases" | "list_collections" | "subscribe" | …
  "category": "collection", // "collection" | "database"   (solo para type=request)
  "function_name": "find", // nombre de la operación      (solo para type=request)
  "database_name": "ventas", // contexto de base de datos
  "collections": ["clientes"], // contexto múltiple (opcional)
  "payload": {
    "collection_name": "clientes",
    "filter": { "pais": "MX" },
    "options": { "sort": { "creado": -1 }, "limit": 50 },
  },
}
```

### Formato de Respuesta

```jsonc
{
  "success": true,
  "message": "OK",
  "response_json": "{ ...respuesta serializada... }",
}
```

Deserializa `response_json` para obtener el cuerpo de la operación (array de documentos, `InsertOneResult`, conteo, etc.).

### Comandos de `type`

| `type`             | Descripción                                                                |
| ------------------ | -------------------------------------------------------------------------- |
| `metadata`         | Devuelve el catálogo de funciones, eventos y capacidades del servidor.     |
| `list_databases`   | Array con los nombres de todas las bases de datos.                         |
| `list_collections` | Requiere `database_name`. Array con nombres de colecciones.                |
| `request`          | Ejecuta una operación CRUD/administrativa en `category` + `function_name`. |

### Categoría `collection`

Requieren `payload.collection_name` y `database_name` (por mensaje o global).

| `function_name` | Payload (mínimo) | Salida (en `response_json`) |
|--|--| |
| `find` | `{ filter, options? }` — options: `sort, projection, limit, skip` | `Document[]` |
| `findOne` | `{ filter?, options? }` | `Document \| null` |
| `insertOne` | `{ document }` | `{ acknowledged, insertedId }` |
| `updateOne` | `{ filter, update, upsert? }` | `{ acknowledged, matchedCount, modifiedCount, upsertedId? }` |
| `replaceOne` | `{ filter, replacement, upsert? }` | idem `updateOne` |
| `deleteOne` | `{ filter }` | `{ acknowledged, deletedCount }` |
| `deleteMany` | `{ filter }` | `{ acknowledged, deletedCount }` |
| `updateCollection` | `{ updates: [{ filter, update }…] }` | Lote de `updateOne` |
| `exportCollection` | `{ format?: "json" }` | JSON serializado de todos los documentos |
| `importCollection` | `{ data \| documents \| docs \| items: [...] }` | `{ insertedCount, errors? }` |

### Categoría `database`

Requieren `database_name`.

| `function_name` | Payload | Descripción |
|--|--| |
| `createDatabase` | `{ new_database_name, collection_name? }` | Crea la BD (y una colección inicial opcional) insertando un documento semilla. |
| `deleteDatabase` | — | Elimina la base de datos y todas sus colecciones. |
| `renameDatabase` | `{ new_database_name }` | Copia todas las colecciones al nuevo nombre y elimina la vieja. |
| `createCollection` | `{ collection_name, validator? }` | Crea una colección nueva. |
| `deleteCollection` | `{ collection_name }` | Elimina la colección y sus índices. |
| `renameCollection` | `{ from, to }` | Renombra una colección dentro de la misma BD. |
| `exportDatabase` | `{ format?: "json" }` | JSON `{ "<colección>": Document[] }` completo. |
| `importDatabase` | `{ data: { "<colección>": Document[] } }` | Importa en masa todas las colecciones del payload. |

### Eventos (change-stream)

Cuando una mutación (`insertOne`, `updateOne`, `replaceOne`, `deleteOne`, `deleteMany`, creación/renombre/eliminación de BD o colección) se confirma, el **servidor broadcast** a todos los clientes conectados un mensaje con:

```jsonc
{
  "success": true,
  "message": "change_event",
  "response_json": {
    "event": "insertOne", // "updateOne" | "replaceOne" | "deleteOne" | "deleteMany" | "createDatabase" | …
    "category": "collection", // "collection" | "database"
    "database_name": "ventas",
    "collectionName": "clientes", // cuando aplica
    "ts": 1720000000000,
    "summary": { "insertedId": "68a…" },
    "before": {
      /* documento antes, cuando aplica */
    },
    "after": {
      /* documento después, cuando aplica */
    },
  },
}
```

En TypeScript puedes suscribirte con `MicroserviceDBAPI#addChangeListener(listener)`.

## 📦 Librerías Cliente

### TypeScript / Browser / Node.js

> Archivo: [`lib/typescript/MicroserviceDBAPI.ts`](lib/typescript/MicroserviceDBAPI.ts)

Características:

- Singleton por BD (`getInstance`).
- Reconexión automática con **reintento exponencial** y re-autenticación de suscripciones.
- Cacheo local de `find()` y pre-carga en caliente de colecciones indicadas en el constructor.
- Change-listeners tipados.

```typescript
import MicroserviceDBAPI from "./lib/typescript/MicroserviceDBAPI";

const db = MicroserviceDBAPI.getInstance("ventas", ["clientes", "pedidos"]);
await db.ready;

const clientes = await db.find("clientes", { pais: "MX" }, { limit: 50 });
const insertado = await db.insertOne("clientes", {
  nombre: "ACME",
  pais: "MX",
});

db.addChangeListener((ev) => {
  console.log(`[${ev.collectionName}] ${ev.event}`, ev.after);
});
```

Métodos disponibles: `find`, `findOne`, `insertOne`, `updateOne`, `replaceOne`, `deleteOne`, `deleteMany`, `updateCollection`, `exportCollection`, `importCollection`, `listCollections`, `createDatabase`, `deleteDatabase`, `renameDatabase`, `createCollection`, `deleteCollection`, `renameCollection`, `exportDatabase`, `importDatabase`.

### Rust

> Archivo: [`lib/rust/MicroserviceDBAPI.rs`](lib/rust/MicroserviceDBAPI.rs)

Cliente Rust asíncrono con `tokio-tungstenite` / `tokio-native-tls` (ajusta el import TLS en entornos sin OpenSSL). Inspirado en la API TypeScript pero tipado con `serde_json::Value`.

## 🖥️ Panel Administrativo Web

Abre el navegador en `http(s)://<host>:3329/`. Incluye:

- **Árbol lateral** de bases de datos y colecciones con búsqueda y contadores de documentos.
- **Vista de colección** con tabla/paginación, previsualización JSON syntax-highlight y edición inline con formularios estructurados.
- **Formularios** de inserción/actualización por campos (no edición JSON cruda).
- **Panel de eventos** en tiempo real con el change-stream.
- **Export/Import** JSON por colección o base de datos completa.
- **Operaciones de administración**: crear/renombrar/eliminar bases y colecciones.
- **Tema claro** de alta densidad, tipografías Plus Jakarta Sans + JetBrains Mono.

El HTML completo se encuentra en la raíz: [`MicroserviceDB.html`](MicroserviceDB.html) y se embebe en el binario en tiempo de compilación.

## 🧩 Caché y Auto-Indexado

Si `MICROSERVICEDB_DRAGONFLY_URL` está presente, el servidor activa varias capas:

1. **Caché de requests (L1):** cada `find`/`findOne`/`list_*` se indexa por (operación + db + collection + filtro + opciones). TTL configurable; en escrituras se invalida el árbol completo de la BD involucrada.
2. **Caché de documentos (L2):** almacena documentos por `_id` para acelerar operaciones de `findOne` y actualizaciones intermedias.
3. **Gestor de índices Redis:** registra qué índices MongoDB existen y reutiliza su metadata.
4. **Auto-index manager:** observa patrones de uso frecuentes en `find`/`updateOne`/`deleteOne` y registra sugerencias de índices en Redis; el panel los muestra como recomendaciones.

El mensaje `metadata` reporta `redis_first: true`, `auto_index: true` y `doc_cache: true` cuando todo está activo.

## 💾 Backups Automáticos

Lanza el proceso con `--backup` para habilitar el ciclo. Cada 30 minutos (configurable con `MICROSERVICEDB_BACKUP_INTERVAL_MINS`) el servidor:

1. Crea una estructura `BACKUP_DIR/<YYYY-MM-DD>/<HH>/backup_[00|30]/`.
2. Exporta todas las bases/colecciones a un único JSON.
3. Lo comprime con 7-Zip (`LZMA2, mx=9, mfb=273, md=64m, ms=on`).
4. Elimina el JSON temporal para ahorrar espacio.

Requiere el ejecutable `7z` o `7za` accesible en el `PATH`.

## 🛠️ Scripts Utilitarios

| Ruta                                          | Descripción                                    |
| --------------------------------------------- | ---------------------------------------------- |
| `scripts/install/docker-compose.yml`          | Cluster Mongo sharded 3×3 + mongos + Dragonfly |
| `scripts/install/docker-compose_windows.ps1`  | Envoltura Windows                              |
| `scripts/install/docker-compose_linux_mac.sh` | Envoltura Unix                                 |
| `scripts/build/build_windows.ps1`             | Build release + empaquetado (Windows)          |
| `scripts/build/build_linux_mac.sh`            | Build release + empaquetado (Unix)             |
| `scripts/dev/MicroserviceDB_MongoManager.py`  | Herramienta de manejo/seed de Mongo en Python  |

## 🧰 Builds Multiplataforma

```bash
# Linux / macOS
bash scripts/build/build_linux_mac.sh

# Windows PowerShell
scripts\build\build_windows.ps1
```

El binario resultante lleva embebido el panel administrativo y no requiere archivos adicionales, salvo el `.env` (opcional; las variables se leen también del entorno del SO).

## 📄 Licencia

© Magnus Norgaard. Todos los derechos reservados. Consulta el documento PDF en [`docs/MicroserviceDB_v2.0.0.pdf`](docs/MicroserviceDB_v2.0.0.pdf) para términos de uso y distribución específicos.

<div align="center">

Hecho por **[Magnus Norgaard (MGNG13)](https://github.com/MGNG13)** · Full-Stack Developer

<a href="https://github.com/MGNG13/MicroserviceDB">
  <img src="https://github-readme-stats-fast.vercel.app/api/pin/?username=MGNG13&repo=MicroserviceDB" alt="MicroserviceDB repo card" />
</a>

<sub>Si este proyecto te resulta útil, considera dejar una ⭐ en el repositorio.</sub>

</div>
