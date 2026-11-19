#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MicroserviceDBMongoManager — Web Server Edition
Flask + SocketIO + HTML/CSS/JS

Requisitos:
    pip install flask flask-socketio pymongo bson

Ejecutar:
    python app.py
    Abrir http://localhost:5000
"""
import argparse
import json
import logging
import os
import sys
import tempfile
import threading
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Sequence
from urllib.parse import urlparse
from queue import Queue, Empty

from flask import Flask, render_template, request, send_file, jsonify
from flask_socketio import SocketIO, emit
from bson import ObjectId, json_util
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError

DEFAULT_TIMEOUT_MS = 60 * 60 * 1000
DEFAULT_RANGE_SIZE = 5000
MAX_RETRIES = 5


def _json_safe(obj, *, _seen=None):
    """Convierte cualquier dato a un equivalente 100% JSON-serializable.

    - datetime / date → isoformat str
    - set / frozenset → list
    - tuple → list
    - Path → str
    - ObjectId / BSON → str
    - dict → copia recursiva
    - list → copia recursiva
    - Non-serializable (funciones, bytes, objetos sin JSON) → str() o drop.
    - Protege contra referencias circulares.
    """
    if _seen is None:
        _seen = set()
    try:
        _id = id(obj)
        if _id in _seen:
            return None
        _seen.add(_id)
    except Exception:
        _id = None

    if obj is None:
        return None
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, (int, float)):
        # Evitar NaN/Infinity no JSON
        try:
            import math
            if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
                return None
        except Exception:
            pass
        return obj
    if isinstance(obj, (datetime,)):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    if hasattr(obj, "isoformat") and callable(getattr(obj, "isoformat")) and not isinstance(obj, str):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    if isinstance(obj, (bytes, bytearray)):
        try:
            return obj.decode("utf-8", errors="replace")
        except Exception:
            return str(obj)
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, (set, frozenset)):
        return [_json_safe(x, _seen=_seen) for x in obj]
    if isinstance(obj, tuple):
        return [_json_safe(x, _seen=_seen) for x in obj]
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            key = k if isinstance(k, (str, int, float, bool)) or k is None else str(k)
            try:
                out[key] = _json_safe(v, _seen=_seen)
            except Exception as e:
                out[key] = f"<inaccesible: {e}>"
        return out
    if isinstance(obj, list):
        return [_json_safe(x, _seen=_seen) for x in obj]
    # Strings & otros primitivos
    if isinstance(obj, str):
        return obj
    # Último fallback: intentar json.dumps via default (bson compat)
    try:
        import json as _json
        _json.dumps({"x": obj}, default=json_util.default)
        return json.loads(json.dumps({"x": obj}, default=json_util.default))["x"]
    except Exception:
        pass
    try:
        return str(obj)
    except Exception:
        return None


def _emit_complete_safe(sio, event, payload, *, room=None, namespace=None):
    """Envía un socketio.emit de complete sanitizando el payload a JSON-safe.

    Si ocurre cualquier error al serializar, devuelve un payload fallback
    garantizado serializable con un error descriptivo.
    """
    try:
        safe = _json_safe(payload)
    except Exception as e:
        safe = {
            "success": False,
            "error": f"Error serializando respuesta del servidor: {e}",
            "stats": {"databases": 0, "collections": 0, "documents": 0, "errors": [], "warnings": []},
            "duration": "-",
            "errors": [],
            "warnings": [],
            "selected": [],
            "imported": [],
            "output_dir": payload.get("output_dir") if isinstance(payload, dict) else None,
        }
    kwargs = {}
    if room:
        kwargs["room"] = room
    if namespace:
        kwargs["namespace"] = namespace
    try:
        sio.emit(event, safe, **kwargs)
    except Exception as e:
        fallback = {
            "success": False,
            "error": f"Fallo interno emit socketio: {e}",
            "stats": {"databases": 0, "collections": 0, "documents": 0, "errors": [], "warnings": []},
            "duration": "-",
            "errors": [],
            "warnings": [],
            "output_dir": payload.get("output_dir") if isinstance(payload, dict) else None,
        }
        sio.emit(event, fallback, **kwargs)


def normalize_mongo_uri(raw_uri: str, *, empty_to_localhost: bool = True) -> str:
    """Normaliza y valida una URI MongoDB previo a entregarla al cliente.

    Aplica las siguientes correcciones defensivas (sin cambiar la semántica):
    - Strip de espacios/CR/LF
    - Quita trailing commas en la lista de hosts (error muy común)
    - Elimina hosts vacíos consecutivos en listas separadas por coma
    - Construye la URI canónica preservando credenciales, opciones y authSource
    - Fallback seguro: si el scheme es ``mongodb://`` sin hosts válidos,
      reemplaza por el default ``mongodb://localhost:27017``
    - Si ``empty_to_localhost`` es True, la cadena vacía / None / solo espacios
      devuelve el default local, nunca ValueError. Útil para endpoints tipo
      test-connection donde el navegador envía una URI sin rellenar.
    - Lanza ValueError detallado si la URI es irrecuperable
    """
    DEFAULT_LOCAL = "mongodb://localhost:27017/"

    if raw_uri is None:
        if empty_to_localhost:
            return DEFAULT_LOCAL
        raise ValueError("URI MongoDB no puede ser None")

    uri = str(raw_uri).strip().strip("\n").strip("\r")
    if not uri:
        if empty_to_localhost:
            return DEFAULT_LOCAL
        raise ValueError("URI MongoDB está vacía")

    original = uri

    # Detectar scheme (mongodb:// o mongodb+srv://)
    if uri.lower().startswith("mongodb+srv://"):
        scheme = "mongodb+srv://"
        rest = uri[len("mongodb+srv://"):]
        srv_mode = True
    elif uri.lower().startswith("mongodb://"):
        scheme = "mongodb://"
        rest = uri[len("mongodb://"):]
        srv_mode = False
    else:
        raise ValueError(
            "URI MongoDB debe comenzar con 'mongodb://' o 'mongodb+srv://'. "
            f"Recibido: {original[:100]}"
        )

    # Separar credenciales (user:pass@) del netloc+path
    userinfo = None
    if "@" in rest:
        userinfo, rest = rest.rsplit("@", 1)

    # Separar hosts/puerto del path+query
    if "/" in rest:
        netloc, path_and_options = rest.split("/", 1)
    elif "?" in rest:
        # Caso borde: URI sin path /db_name pero con ?options=
        idx = rest.find("?")
        netloc = rest[:idx]
        path_and_options = rest[idx:]
    else:
        netloc = rest
        path_and_options = ""

    # Limpiar hosts: quitar trailing commas, filtrar vacíos, eliminar espacios inter-host
    raw_hosts = [h.strip() for h in netloc.split(",")]
    hosts = [h for h in raw_hosts if h]

    if not hosts:
        if srv_mode:
            raise ValueError(
                "URI MongoDB SRV no contiene host válido. "
                f"Recibido: {original[:100]}"
            )
        # Fallback amigable para modo clásico (muy habitual en localhost)
        hosts = ["localhost:27017"]

    # +SRV solo puede tener UN host. Si hay más, forzamos scheme estándar.
    if srv_mode and len(hosts) > 1:
        srv_mode = False
        scheme = "mongodb://"

    # Reconstruir la URI canónica
    rebuilt_netloc = ",".join(hosts)
    rebuilt = scheme
    if userinfo:
        rebuilt += f"{userinfo}@"
    rebuilt += rebuilt_netloc
    if path_and_options:
        rebuilt += f"/{path_and_options}" if not path_and_options.startswith("?") else f"/{path_and_options}"

    return rebuilt


def _raise_if_invalid_hosts_in_uri(uri: str) -> None:
    """Validación final del host list antes de crear el cliente.

    PyMongo a veces no falla en el constructor sino al hacer el primer comando.
    Validamos aquí para dar un mensaje claro y estable.
    """
    cleaned = normalize_mongo_uri(uri)
    stripped = cleaned.split("//", 1)[1] if "//" in cleaned else cleaned
    if "@" in stripped:
        stripped = stripped.rsplit("@", 1)[1]
    stripped = stripped.split("/", 1)[0] if "/" in stripped else stripped
    stripped = stripped.split("?", 1)[0] if "?" in stripped else stripped
    hosts = [h for h in [x.strip() for x in stripped.split(",")] if h]
    if not hosts:
        raise ValueError(
            f"URI MongoDB no contiene ningún host válido después de normalizar: {uri[:100]!r}"
        )
    for host in hosts:
        if not host or "," in host:
            raise ValueError(
                f"Host inválido en la lista de la URI MongoDB: {host!r} (original: {uri[:100]!r})"
            )


def _safe_uri_for_log(uri: str) -> str:
    """Quita credenciales user:pass de una URI para dejarla loggeable sin leaks."""
    try:
        if "@" not in uri:
            return uri
        prefix, suffix = uri.split("://", 1)
        if "@" in suffix:
            credentials, hosts = suffix.rsplit("@", 1)
            if credentials:
                return f"{prefix}://***:***@{hosts}"
        return uri
    except Exception:
        return uri[:80] + ("..." if len(uri) > 80 else "")

app = Flask(__name__)
app.config["SECRET_KEY"] = "mongo-manager-secret-2026"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# =============================================================================
# MODELO DE NEGOCIO (reutilizado del script original)
# =============================================================================

@dataclass
class MongoConnOptions:
    uri: str
    timeout_ms: int = DEFAULT_TIMEOUT_MS
    x509_file: Optional[str] = None

    def normalized_uri(self) -> str:
        return normalize_mongo_uri(self.uri)


class ProgressLogger(logging.Handler):
    def __init__(self, queue: Queue):
        super().__init__()
        self.queue = queue
        self.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    def emit(self, record):
        msg = self.format(record)
        self.queue.put(("log", msg))


class MicroserviceDBMongoManager:
    SYSTEM_DATABASES = {"admin", "local", "config", "test"}

    def __init__(
        self,
        conn: MongoConnOptions,
        output_dir: str = "./mongo_export",
        include_system: bool = False,
        batch_size: int = 500,
        socket_timeout_ms: Optional[int] = None,
        range_size: int = DEFAULT_RANGE_SIZE,
        max_retries: int = MAX_RETRIES,
        progress_queue: Optional[Queue] = None,
    ):
        self.conn = conn
        self.output_dir = Path(output_dir)
        self.include_system = include_system
        self.batch_size = batch_size
        self.socket_timeout_ms = socket_timeout_ms if socket_timeout_ms is not None else conn.timeout_ms
        self.range_size = range_size
        self.max_retries = max_retries
        self.client: Optional[MongoClient] = None
        self.progress_queue = progress_queue or Queue()
        self.last_error: Optional[str] = None

        self.stats = {
            "databases": 0,
            "collections": 0,
            "documents": 0,
            "errors": [],
            "selected": [],
            "start_time": None,
            "end_time": None,
        }

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._setup_logging()

    def _setup_logging(self):
        log_format = "%(asctime)s - %(levelname)s - %(message)s"
        self.logger = logging.getLogger(f"{__name__}_{id(self)}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers = []

        fh = logging.FileHandler(self.output_dir / "microservice_db_mongo_manager.log")
        fh.setFormatter(logging.Formatter(log_format))
        self.logger.addHandler(fh)

        ph = ProgressLogger(self.progress_queue)
        self.logger.addHandler(ph)

        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter(log_format))
        self.logger.addHandler(sh)

    def _send_progress(self, current: int, total: int, message: str = ""):
        if self.progress_queue:
            self.progress_queue.put(("progress", current, total, message))

    def connect(self) -> bool:
        self.last_error = None
        try:
            self.logger.info("Conectando a MongoDB...")
            self.client = self._create_client(self.conn)
            self.client.admin.command("ping")
            server_info = self.client.server_info()
            self.logger.info(f"✓ Conectado exitosamente a MongoDB {server_info.get('version', 'desconocida')}")
            return True
        except ServerSelectionTimeoutError as e:
            self.last_error = f"No se pudo conectar al servidor (timeout). Comprueba host/puerto y reachabilidad."
            self.logger.error(f"✗ No se pudo conectar al servidor: {e}")
            return False
        except ConnectionFailure as e:
            self.last_error = f"Fallo de conexión a MongoDB: {e}"
            self.logger.error(f"✗ Fallo de conexión: {e}")
            return False
        except ValueError as e:
            self.last_error = f"URI MongoDB inválida: {e}"
            self.logger.error(f"✗ URI MongoDB inválida: {e}")
            return False
        except Exception as e:
            self.last_error = f"Error inesperado al conectar: {e}"
            self.logger.error(f"✗ Error inesperado al conectar: {e}")
            return False

    def close(self):
        try:
            if self.client:
                self.client.close()
        finally:
            self.client = None

    def _build_client_kwargs(self, conn: MongoConnOptions) -> dict:
        kwargs = {
            "serverSelectionTimeoutMS": conn.timeout_ms,
            "connectTimeoutMS": conn.timeout_ms,
            "socketTimeoutMS": self.socket_timeout_ms,
        }
        if conn.x509_file:
            cert_file = Path(conn.x509_file).expanduser()
            if not cert_file.is_file():
                raise FileNotFoundError(f"No se encontró el archivo X.509: {cert_file}")
            kwargs["tls"] = True
            kwargs["tlsCertificateKeyFile"] = str(cert_file)
            self.logger.info(f"Usando certificado X.509: {cert_file}")
        return kwargs

    def _create_client(self, conn: MongoConnOptions) -> MongoClient:
        cleaned_uri = conn.normalized_uri()
        _raise_if_invalid_hosts_in_uri(cleaned_uri)
        if cleaned_uri != conn.uri:
            # Si el normalizador aplicó correcciones, logearlas (sin credenciales)
            safe_orig = _safe_uri_for_log(conn.uri)
            safe_clean = _safe_uri_for_log(cleaned_uri)
            log_safe = None
            try:
                import logging as _lgtmp
                log_safe = getattr(self, "logger", None)
                if log_safe is None:
                    log_safe = _lgtmp.getLogger(__name__)
            except Exception:
                pass
            if log_safe:
                log_safe.info(f"URI MongoDB normalizada: {safe_orig} → {safe_clean}")
        return MongoClient(cleaned_uri, **self._build_client_kwargs(conn))


    def get_databases(self) -> List[str]:
        try:
            databases = self.client.list_database_names() if self.client else []
            if not self.include_system:
                databases = [db for db in databases if db not in self.SYSTEM_DATABASES]
            databases.sort()
            return databases
        except OperationFailure as e:
            self.logger.error(f"No se pudieron listar las bases de datos: {e}")
            parsed = urlparse(self.conn.uri)
            if parsed.path and parsed.path.strip("/"):
                return [parsed.path.strip("/")]
            return []

    def get_collections(self, db_name: str) -> List[str]:
        db = self.client[db_name]
        cols = db.list_collection_names()
        cols.sort()
        return cols

    def _upsert_selected(self, mode: str, db: str, **fields):
        selected = self.stats.get("selected")
        if not isinstance(selected, list):
            selected = []
            self.stats["selected"] = selected
        for item in selected:
            if item.get("mode") == mode and item.get("db") == db:
                item.update(fields)
                return
        record = {"mode": mode, "db": db}
        record.update(fields)
        selected.append(record)

    def _sleep_backoff(self, attempt: int):
        delay = min(2 ** attempt, 30)
        self.logger.warning(f"  Reintentando en {delay}s (intento {attempt + 1}/{self.max_retries})...")
        time.sleep(delay)

    def _is_timeout_error(self, exc: Exception) -> bool:
        import pymongo.errors as pmerr
        msg = str(exc).lower()
        if isinstance(exc, (pmerr.ExecutionTimeout, pmerr.WTimeoutError)):
            return True
        if "maxtimemsexpired" in msg or "operation exceeded time limit" in msg:
            return True
        if "timeouterror" in msg or "timeout" in msg:
            return True
        if isinstance(exc, (pmerr.NetworkTimeout,)):
            return True
        return False

    def _iterate_with_range_pagination(self, collection, projection=None):
        range_size = self.range_size
        last_id = None
        total_fetched = 0

        while True:
            query = {}
            if last_id is not None:
                query["_id"] = {"$gt": last_id}

            current_range = None
            batch_attempt = 0
            fetched = 0
            while batch_attempt < self.max_retries:
                try:
                    cursor = (
                        collection.find(query, projection=projection)
                        .sort("_id", 1)
                        .limit(range_size)
                        .batch_size(self.batch_size)
                        .hint([("_id", 1)])
                    )
                    current_range = list(cursor)
                    try:
                        cursor.close()
                    except Exception:
                        pass
                    fetched = len(current_range)
                    break
                except Exception as e:
                    if self._is_timeout_error(e) and batch_attempt < self.max_retries - 1:
                        self.logger.warning(f"  ⚠ Timeout leyendo rango (último _id={last_id}): {e}")
                        batch_attempt += 1
                        self._sleep_backoff(batch_attempt)
                        range_size = max(100, range_size // 2)
                        self.logger.warning(f"  Reduciendo range_size a {range_size} para reintento")
                    else:
                        raise

            if fetched == 0:
                return

            for doc in current_range:
                yield doc
                last_id = doc["_id"]
                total_fetched += 1
                if total_fetched % 1000 == 0:
                    self._send_progress(total_fetched, total_fetched + range_size, f"Procesando... {total_fetched} docs")

            if fetched < range_size:
                return

    def _write_collection_json_array(self, cursor: Iterable[dict], output_file: Path, total_docs: Optional[int]) -> int:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("[\n")
            first = True
            count = 0
            for doc in cursor:
                if not first:
                    f.write(",\n")
                f.write(json_util.dumps(doc, ensure_ascii=False))
                first = False
                count += 1
                if count % 1000 == 0 and total_docs:
                    self._send_progress(count, total_docs, f"Exportando {output_file.stem}: {count}/{total_docs}")
            f.write("\n]\n")
        return count

    def export_collection(self, db_name: str, collection_name: str) -> dict:
        db_dir = self.output_dir / db_name
        db_dir.mkdir(parents=True, exist_ok=True)
        output_file = db_dir / f"{collection_name}.json"

        try:
            db = self.client[db_name]
            collection = db[collection_name]
            try:
                total_docs = collection.estimated_document_count()
            except Exception:
                total_docs = None

            self.logger.info(f"  → Exportando {collection_name} (estimado: {total_docs if total_docs is not None else '?' } documentos)")
            iterator = self._iterate_with_range_pagination(collection)
            doc_count = self._write_collection_json_array(iterator, output_file, total_docs)

            file_size = output_file.stat().st_size
            self.logger.info(f"  ✓ {collection_name}: {doc_count:,} documentos ({self._human_readable_size(file_size)})")
            self.stats["documents"] += doc_count
            return {"success": True, "documents": doc_count, "file_size": file_size, "file_path": str(output_file)}
        except Exception as e:
            error_msg = f"Error exportando {db_name}.{collection_name}: {str(e)}"
            self.logger.error(f"  ✗ {error_msg}")
            self.stats["errors"].append(error_msg)
            return {"success": False, "error": str(e)}

    def export_database(self, db_name: str, collections: Optional[Sequence[str]] = None) -> dict:
        self.logger.info(f"\n📦 Exportando base de datos: '{db_name}'")
        try:
            all_collections = self.get_collections(db_name)
            if not all_collections:
                self.logger.info("  (Sin colecciones)")
                return {"success": True, "collections": 0, "documents_estimated": 0}

            if collections:
                selected = [c for c in all_collections if c in set(collections)]
            else:
                selected = all_collections

            if not selected:
                self.logger.info("  (Sin colecciones seleccionadas)")
                return {"success": True, "collections": 0, "documents_estimated": 0}

            results = []
            doc_total = 0
            for idx, coll_name in enumerate(selected):
                self._send_progress(idx, len(selected), f"Exportando {db_name}.{coll_name}")
                r = self.export_collection(db_name, coll_name)
                results.append(r)
                if r.get("success"):
                    doc_total += int(r.get("documents", 0))

            successful = sum(1 for r in results if r.get("success"))
            self.logger.info(f"✓ Base de datos '{db_name}' completada: {successful}/{len(selected)} colecciones")
            return {"success": successful == len(selected), "collections": len(selected), "documents": doc_total, "results": results}
        except Exception as e:
            error_msg = f"Error exportando base de datos '{db_name}': {str(e)}"
            self.logger.error(f"✗ {error_msg}")
            self.stats["errors"].append(error_msg)
            return {"success": False, "error": str(e)}

    def _normalize_stats_response(self, *, success: bool) -> dict:
        stats = self.stats or {}
        stats.setdefault("errors", [])
        stats.setdefault("warnings", [])
        selected = stats.get("selected") or []

        dbs_count = 0
        cols_count = 0
        docs_count = 0
        derived_errors = []
        derived_warnings = []
        seen_dbs = set()
        for entry in selected or []:
            if not isinstance(entry, dict):
                continue
            db_name = entry.get("db")
            if db_name and db_name not in seen_dbs:
                dbs_count += 1
                seen_dbs.add(db_name)
            cols_in_entry = entry.get("collections") or []
            if isinstance(cols_in_entry, (list, tuple, set)):
                cols_count += len([c for c in cols_in_entry if c])
            imported = entry.get("imported") or []
            if isinstance(imported, (list, tuple)):
                for imp in imported:
                    if not isinstance(imp, dict):
                        continue
                    ins = imp.get("inserted")
                    try:
                        docs_count += int(ins) if ins is not None else 0
                    except Exception:
                        pass
                    col_name = imp.get("collection") or "?"
                    zero_warn = imp.get("inserted") == 0 and not imp.get("errors")
                    if zero_warn and db_name:
                        derived_warnings.append(
                            f"Colección '{db_name}.{col_name}' copiada con 0 documentos (vacía en origen)."
                        )
            errs_per_db = entry.get("errors") or []
            if isinstance(errs_per_db, (list, tuple)):
                for e in errs_per_db:
                    if not e:
                        continue
                    if isinstance(e, dict):
                        col_name = e.get("collection") or "?"
                        msg = e.get("error") or str(e)
                        derived_errors.append(
                            f"{db_name + '.' if db_name else ''}{col_name}: {msg}"
                        )
                    else:
                        derived_errors.append(
                            f"{db_name + ': ' if db_name else ''}{str(e)}"
                        )
            missing = entry.get("missing_collections") or []
            if isinstance(missing, (list, tuple)) and missing:
                derived_warnings.append(
                    f"DB '{db_name}': colecciones no encontradas y omitidas: {', '.join(str(m) for m in missing)}"
                )

        stats["databases"] = int(max(dbs_count, int(stats.get("databases") or 0)))
        stats["collections"] = int(max(cols_count, int(stats.get("collections") or 0)))
        stats["documents"] = int(max(docs_count, int(stats.get("documents") or 0)))
        stats["selected"] = selected

        def _is_real_error(entry):
            if not entry:
                return False
            if isinstance(entry, str):
                return bool(entry.strip())
            if isinstance(entry, dict):
                return any(v for v in entry.values() if (isinstance(v, str) and v.strip()) or (not isinstance(v, str) and v))
            return True

        combined_errors = list(stats.get("errors") or []) + list(derived_errors or [])
        cleaned_errors = []
        seen_errors = set()
        for e in combined_errors:
            if not _is_real_error(e):
                continue
            try:
                key = repr(e)
            except Exception:
                key = str(e)
            if key in seen_errors:
                continue
            seen_errors.add(key)
            cleaned_errors.append(e)
        stats["errors"] = cleaned_errors

        combined_warnings = list(stats.get("warnings") or []) + list(derived_warnings or [])
        cleaned_warnings = []
        seen_warnings = set()
        for w in combined_warnings:
            if isinstance(w, str) and not w.strip():
                continue
            if not w:
                continue
            try:
                key = repr(w)
            except Exception:
                key = str(w)
            if key in seen_warnings:
                continue
            seen_warnings.add(key)
            cleaned_warnings.append(w)
        stats["warnings"] = cleaned_warnings

        end = stats.get("end_time") or datetime.now()
        start = stats.get("start_time") or end
        try:
            duration = str(end - start)
        except Exception:
            duration = "-"

        has_real_errors = bool([e for e in stats.get("errors") or [] if _is_real_error(e)])
        final_success = bool(success) and not has_real_errors

        def _clean_errors_list(errs):
            seen = set()
            out = []
            for e in (errs or []):
                if isinstance(e, dict):
                    has_content = False
                    clean_inner = []
                    for inner_e in (e.get("errors") or []):
                        if _is_real_error(inner_e):
                            has_content = True
                            clean_inner.append(inner_e)
                    if not has_content:
                        continue
                    e2 = {**e, "errors": clean_inner}
                    try:
                        key = repr(e2)
                    except Exception:
                        key = str(e2)
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append(e2)
                else:
                    if _is_real_error(e):
                        try:
                            key = repr(e)
                        except Exception:
                            key = str(e)
                        if key in seen:
                            continue
                        seen.add(key)
                        out.append(e)
            return out

        return {
            "success": bool(final_success),
            "stats": stats,
            "duration": duration,
            "errors": _clean_errors_list(stats["errors"]),
            "warnings": cleaned_warnings,
            "selected": selected,
        }

    def run_export(
        self,
        db_name: Optional[str],
        collections: Optional[Sequence[str]],
        selected_dbs: Optional[List[str]] = None,
        selected_collections: Optional[Dict[str, List[str]]] = None,
    ) -> dict:
        self.stats["start_time"] = datetime.now()
        self.logger.info("=" * 60)
        self.logger.info("INICIANDO MODO EXPORT (JSON)")
        self.logger.info(f"Directorio de salida: {self.output_dir.absolute()}")
        self.logger.info(f"Incluir DBs de sistema: {self.include_system}")
        self.logger.info("=" * 60)

        if not self.connect():
            return {**self._normalize_stats_response(success=False), "error": "No se pudo conectar"}

        sel_cols_lookup: Dict[str, List[str]] = {
            k: list(v) for k, v in (selected_collections or {}).items() if isinstance(v, (list, tuple, set))
        }

        try:
            databases = self.get_databases()
            if not databases:
                self.logger.warning("No se encontraron bases de datos.")
                return {**self._normalize_stats_response(success=True), "databases": 0}

            if db_name:
                selected_dbs = [db_name]
            elif selected_dbs is None:
                selected_dbs = databases

            self.logger.info(f"DBs seleccionadas ({len(selected_dbs)}): {', '.join(selected_dbs)}")

            export_plan = []
            for db in selected_dbs:
                all_cols = self.get_collections(db)
                if not all_cols:
                    self._upsert_selected("EXPORT", db, collections=[], missing_collections=[])
                    export_plan.append({"db": db, "collections": []})
                    continue

                db_requested = sel_cols_lookup.get(db)
                if db_requested:
                    requested = list(db_requested)
                    all_set = set(all_cols)
                    selected_cols = [c for c in requested if c in all_set]
                    missing = [c for c in requested if c not in all_set]
                elif collections is not None:
                    requested = list(collections)
                    all_set = set(all_cols)
                    selected_cols = [c for c in requested if c in all_set]
                    missing = [c for c in requested if c not in all_set]
                else:
                    selected_cols = list(all_cols)
                    missing = []

                if not selected_cols:
                    selected_cols = list(all_cols)
                    missing = []

                self._upsert_selected("EXPORT", db, collections=selected_cols, missing_collections=missing)
                export_plan.append({"db": db, "collections": selected_cols})

            for item in export_plan:
                db = item["db"]
                cols = item["collections"]
                if not cols:
                    self.logger.info(f"\n📦 Exportando base de datos: '{db}'")
                    self.logger.info("  (Sin colecciones seleccionadas)")
                    continue
                result = self.export_database(db, cols)
                if result.get("success"):
                    self.stats["databases"] += 1
                    self.stats["collections"] += result.get("collections", 0)
                    self.stats["documents"] += int(result.get("documents", 0))

            self.stats["end_time"] = datetime.now()
            duration = self.stats["end_time"] - self.stats["start_time"]
            self._generate_summary(duration, mode="EXPORT")
            has_real_errors = bool([e for e in self.stats.get("errors") or [] if e])
            return self._normalize_stats_response(success=not has_real_errors)
        finally:
            self.close()

    def _insert_batch_with_retry(self, dst_col, batch: list) -> int:
        if not batch:
            return 0
        attempt = 0
        while attempt < self.max_retries:
            try:
                dst_col.insert_many(batch, ordered=False)
                return len(batch)
            except Exception as e:
                if self._is_timeout_error(e) and attempt < self.max_retries - 1:
                    self.logger.warning(f"  ⚠ Timeout insertando lote ({len(batch)} docs): {e}")
                    attempt += 1
                    self._sleep_backoff(attempt)
                else:
                    raise
        return 0

    def copy_collections(self, source_db: str, dest_db: str, collections: Sequence[str], dest_client: MongoClient) -> dict:
        src_db = self.client[source_db]
        dst_db = dest_client[dest_db]
        existing = set(dst_db.list_collection_names())
        imported = []
        errors = []

        for idx, col in enumerate(collections):
            try:
                self._send_progress(idx, len(collections), f"Copiando {source_db}.{col}")
                src_col = src_db[col]
                dst_col = dst_db[col]
                refresh_object_ids = col in existing
                deleted_existing_docs = 0

                if refresh_object_ids:
                    delete_result = dst_col.delete_many({})
                    deleted_existing_docs = int(delete_result.deleted_count)

                try:
                    total_docs = src_col.estimated_document_count()
                except Exception:
                    total_docs = None

                self.logger.info(f"  → Copiando {col} (estimado: {total_docs if total_docs is not None else '?'} documentos)")

                inserted = 0
                object_ids_updated = 0
                batch = []
                iterator = self._iterate_with_range_pagination(src_col)

                for doc in iterator:
                    if refresh_object_ids:
                        doc["_id"] = ObjectId()
                        object_ids_updated += 1
                    batch.append(doc)
                    if len(batch) >= self.batch_size:
                        inserted += self._insert_batch_with_retry(dst_col, batch)
                        batch = []
                if batch:
                    inserted += self._insert_batch_with_retry(dst_col, batch)

                imported.append({
                    "collection": col,
                    "inserted": inserted,
                    "object_ids_updated": object_ids_updated,
                    "existing_dest_collection": refresh_object_ids,
                    "deleted_existing_docs": deleted_existing_docs,
                })
            except Exception as e:
                errors.append({"collection": col, "error": str(e)})

        return {"success": len(errors) == 0, "imported": imported, "errors": errors}

    def run_copy(
        self,
        source_db: Optional[str],
        collections: Optional[Sequence[str]],
        dest_conn: MongoConnOptions,
        selected_dbs: Optional[List[str]] = None,
        selected_collections: Optional[Dict[str, List[str]]] = None,
    ) -> dict:
        self.stats["start_time"] = datetime.now()
        self.logger.info("=" * 60)
        self.logger.info("INICIANDO MODO COPY (MongoDB -> MongoDB)")
        self.logger.info(f"Origen: {self.conn.uri}")
        self.logger.info(f"Destino: {dest_conn.uri}")
        self.logger.info("Regla: si la colección existe en destino, se vacía y se recopia con _id nuevos")
        self.logger.info("Regla: la DB destino usa el MISMO nombre que la DB origen")
        self.logger.info("=" * 60)

        if not self.connect():
            return {"success": False, "error": "No se pudo conectar al origen"}

        sel_cols_lookup: Dict[str, List[str]] = {
            k: list(v) for k, v in (selected_collections or {}).items() if isinstance(v, (list, tuple, set))
        }

        dest_client = None
        try:
            dest_client = self._create_client(dest_conn)
            dest_client.admin.command("ping")

            src_dbs = self.get_databases()
            if not src_dbs:
                return {"success": False, "error": "No se encontraron DBs en el origen"}

            if source_db:
                selected_dbs = [source_db]
            elif selected_dbs is None:
                selected_dbs = src_dbs

            if not selected_dbs:
                return {"success": False, "error": "No se seleccionaron DBs"}

            self.logger.info(f"DBs seleccionadas ({len(selected_dbs)}): {', '.join(selected_dbs)}")

            copy_plan = []
            for db in selected_dbs:
                col_names = self.get_collections(db)
                if not col_names:
                    self._upsert_selected("COPY", db, dest_db=db, collections=[], missing_collections=[])
                    copy_plan.append({"db": db, "collections": [], "missing": []})
                    continue

                db_requested = sel_cols_lookup.get(db)
                if db_requested:
                    requested = list(db_requested)
                    all_set = set(col_names)
                    cols = [c for c in requested if c in all_set]
                    missing = [c for c in requested if c not in all_set]
                elif collections:
                    requested = list(collections)
                    all_set = set(col_names)
                    cols = [c for c in requested if c in all_set]
                    missing = [c for c in requested if c not in all_set]
                else:
                    cols = list(col_names)
                    missing = []

                if not cols:
                    cols = list(col_names)
                    missing = []

                self._upsert_selected("COPY", db, dest_db=db, collections=cols, missing_collections=missing)
                copy_plan.append({"db": db, "collections": cols, "missing": missing})

            overall_success = True
            overall_imported = []
            overall_warnings = []
            overall_errors = []

            for item in copy_plan:
                db = item["db"]
                cols = item["collections"]
                missing = item["missing"]
                if not cols:
                    overall_warnings.append(
                        f"DB '{db}': sin colecciones, se omite."
                    )
                    self.logger.info(f"\n📦 {db}: (Sin colecciones seleccionadas, se omite)")
                    continue

                dest_db_name = db
                self.logger.info(f"\n📦 Copiando DB '{db}' -> '{dest_db_name}' ({len(cols)} colecciones)")
                self.logger.info(f"  Colecciones seleccionadas ({len(cols)}): {', '.join(cols)}")
                if missing:
                    overall_warnings.append(
                        f"DB '{db}': colecciones no encontradas y omitidas: {', '.join(missing)}"
                    )
                    self.logger.info(f"  Colecciones no encontradas y omitidas ({len(missing)}): {', '.join(missing)}")

                result = self.copy_collections(db, dest_db_name, cols, dest_client)
                self.stats["databases"] += 1
                self.stats["collections"] += len(cols)
                inserted_total = sum(int(x.get("inserted", 0)) for x in result.get("imported", []))
                zero_docs = [x for x in result.get("imported", []) if int(x.get("inserted", 0)) == 0]
                for zd in zero_docs:
                    overall_warnings.append(
                        f"Colección '{db}.{zd.get('collection')}' copiada con 0 documentos (vacía en origen)."
                    )
                self.stats["documents"] += inserted_total

                real_errors_in_db = [e for e in (result.get("errors") or []) if e]
                self._upsert_selected(
                    "COPY",
                    db,
                    dest_db=dest_db_name,
                    imported=result.get("imported", []),
                    errors=real_errors_in_db,
                )

                if result.get("imported"):
                    overall_imported.append({"db": db, "collections": result["imported"]})
                if real_errors_in_db:
                    overall_errors.append({"db": db, "errors": real_errors_in_db})
                    overall_success = False

                if result.get("imported"):
                    imported_txt = ", ".join(
                        (f"{x['collection']} ({x['inserted']}, eliminados: {x.get('deleted_existing_docs', 0)}, _id nuevos: {x.get('object_ids_updated', 0)})"
                         if x.get("existing_dest_collection") else f"{x['collection']} ({x['inserted']})")
                        for x in result["imported"]
                    )
                    self.logger.info(f"  Importadas: {imported_txt}")
                if real_errors_in_db:
                    for e in real_errors_in_db:
                        try:
                            self.logger.error(f"  Error en {e['collection']}: {e['error']}")
                        except Exception:
                            self.logger.error(f"  Error: {e}")

            overall_errors = [
                g for g in overall_errors
                if isinstance(g, dict) and [e for e in (g.get("errors") or []) if e]
            ]

            self.stats["end_time"] = datetime.now()
            duration = self.stats["end_time"] - self.stats["start_time"]
            self._generate_summary(duration, mode="COPY")
            resp = self._normalize_stats_response(success=overall_success)
            resp["imported"] = overall_imported
            existing_groups = {
                g.get("db") for g in (resp.get("errors") or []) if isinstance(g, dict) and g.get("db")
            }
            merged_groups = list(resp.get("errors") or [])
            for g in overall_errors:
                if not isinstance(g, dict) or not g.get("db"):
                    continue
                db = g["db"]
                inner = [e for e in (g.get("errors") or []) if e]
                if not inner:
                    continue
                if db in existing_groups:
                    continue
                merged_groups.append({"db": db, "errors": inner})
            resp["errors"] = merged_groups
            base_warn = list(resp.get("warnings") or [])
            seen_w = set()
            for w in base_warn:
                try:
                    seen_w.add(repr(w))
                except Exception:
                    seen_w.add(str(w))
            for w in overall_warnings:
                if not w or (isinstance(w, str) and not w.strip()):
                    continue
                try:
                    k = repr(w)
                except Exception:
                    k = str(w)
                if k in seen_w:
                    continue
                seen_w.add(k)
                base_warn.append(w)
            resp["warnings"] = base_warn
            return resp
        except Exception as e:
            return {**self._normalize_stats_response(success=False), "error": str(e)}
        finally:
            try:
                if dest_client:
                    dest_client.close()
            finally:
                self.close()

    def _generate_summary(self, duration, mode: str):
        summary_file = self.output_dir / "RESUMEN_microservice_db_mongo_manager.txt"
        summary_lines = [
            "=" * 60,
            f"RESUMEN MONGO MANAGER ({mode})",
            "=" * 60,
            f"Fecha inicio: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S') if self.stats['start_time'] else 'N/A'}",
            f"Fecha fin: {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S') if self.stats['end_time'] else 'N/A'}",
            f"Duración: {duration}",
            "-" * 60,
            f"Bases de datos procesadas: {self.stats.get('databases', 0)}",
            f"Colecciones procesadas: {self.stats.get('collections', 0)}",
            f"Documentos procesados: {self.stats.get('documents', 0):,}",
            f"Errores: {len(self.stats.get('errors', []))}",
            "=" * 60,
        ]

        if self.stats.get("errors"):
            summary_lines.append("\nERRORES ENCONTRADOS:")
            for i, error in enumerate(self.stats["errors"], 1):
                summary_lines.append(f"{i}. {error}")
        else:
            summary_lines.append("\n✓ Operación completada sin errores.")

        selected = self.stats.get("selected") or []
        if selected:
            summary_lines.append("\nSELECCIÓN:")
            for item in selected:
                m = item.get("mode", mode)
                db = item.get("db", "N/A")
                dest_db = item.get("dest_db")
                cols = item.get("collections") or []
                missing = item.get("missing_collections") or []

                if m == "COPY" and dest_db:
                    summary_lines.append(f"\n- {m} DB: {db} -> {dest_db}")
                else:
                    summary_lines.append(f"\n- {m} DB: {db}")

                summary_lines.append(f"  Colecciones seleccionadas: {len(cols)}")
                for c in cols:
                    summary_lines.append(f"    - {c}")

                if missing:
                    summary_lines.append(f"  Colecciones no encontradas (omitidas): {len(missing)}")
                    for c in missing:
                        summary_lines.append(f"    - {c}")

                imported = item.get("imported") or []
                if imported:
                    summary_lines.append(f"  Importadas: {len(imported)}")
                    for x in imported:
                        if x.get("existing_dest_collection"):
                            summary_lines.append(f"    - {x.get('collection')} ({x.get('inserted', 0)}, eliminados: {x.get('deleted_existing_docs', 0)}, _id nuevos: {x.get('object_ids_updated', 0)})")
                        else:
                            summary_lines.append(f"    - {x.get('collection')} ({x.get('inserted', 0)})")

                errs = item.get("errors") or []
                if errs:
                    summary_lines.append(f"  Errores en colecciones: {len(errs)}")
                    for e in errs:
                        summary_lines.append(f"    - {e.get('collection')}: {e.get('error')}")

        summary_lines.append(f"\nArchivos/logs en: {self.output_dir.absolute()}")
        summary_lines.append("=" * 60)

        summary_text = "\n".join(summary_lines)
        with open(summary_file, "w", encoding="utf-8") as f:
            f.write(summary_text)
        self.logger.info("\n" + summary_text)

    @staticmethod
    def _human_readable_size(size_bytes: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"


# =============================================================================
# RUTAS FLASK / SOCKETIO
# =============================================================================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/test-connection", methods=["POST"])
def test_connection():
    data = request.json or {}
    uri = data.get("uri", "")
    timeout = int(data.get("timeout_ms", DEFAULT_TIMEOUT_MS))
    x509_path = data.get("x509_path")

    conn = MongoConnOptions(uri=uri, timeout_ms=timeout, x509_file=x509_path)
    mgr = MicroserviceDBMongoManager(conn=conn, output_dir="./tmp_test")
    ok = mgr.connect()
    dbs = []
    err = None
    if ok:
        try:
            dbs = mgr.get_databases()
        except Exception as e:
            err = f"No se pudieron listar bases de datos: {e}"
    else:
        err = mgr.last_error or "No se pudo conectar"
    mgr.close()
    resp = {"success": ok, "databases": dbs}
    if err and not ok:
        resp["error"] = err
    return jsonify(resp)


@app.route("/api/collections", methods=["POST"])
def get_collections_api():
    data = request.json or {}
    uri = data.get("uri", "")
    db_name = data.get("db", "")
    timeout = int(data.get("timeout_ms", DEFAULT_TIMEOUT_MS))
    x509_path = data.get("x509_path")
    include_system = data.get("include_system", False)

    conn = MongoConnOptions(uri=uri, timeout_ms=timeout, x509_file=x509_path)
    mgr = MicroserviceDBMongoManager(conn=conn, include_system=include_system)
    if not mgr.connect():
        err = mgr.last_error or "No se pudo conectar"
        mgr.close()
        return jsonify({"success": False, "error": err})
    try:
        cols = mgr.get_collections(db_name)
        return jsonify({"success": True, "collections": cols})
    except Exception as e:
        return jsonify({"success": False, "error": f"No se pudieron listar colecciones: {e}"})
    finally:
        mgr.close()


@app.route("/api/upload-x509", methods=["POST"])
def upload_x509():
    if "file" not in request.files:
        return jsonify({"success": False, "error": "No file provided"})
    f = request.files["file"]
    if f.filename == "":
        return jsonify({"success": False, "error": "Empty filename"})
    suffix = Path(f.filename).suffix
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, prefix="x509_")
    f.save(tmp.name)
    tmp.close()
    return jsonify({"success": True, "path": tmp.name})


@app.route("/api/download-zip")
def download_zip():
    out_dir = request.args.get("out_dir", "./mongo_export")
    out_path = Path(out_dir)
    if not out_path.exists():
        return jsonify({"success": False, "error": "Directorio no encontrado"})
    zip_path = out_path.with_suffix(".zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in out_path.rglob("*"):
            if f.is_file():
                zf.write(f, f.relative_to(out_path))
    return send_file(zip_path, as_attachment=True, download_name=f"{out_path.name}.zip")


# =============================================================================
# SOCKETIO — OPERACIONES EN TIEMPO REAL
# =============================================================================

@socketio.on("run_export")
def handle_run_export(data):
    sid = request.sid
    uri = data.get("uri", "")
    timeout = int(data.get("timeout_ms", DEFAULT_TIMEOUT_MS))
    x509_path = data.get("x509_path")
    include_system = data.get("include_system", False)
    selected_dbs = data.get("selected_dbs", [])
    selected_collections = data.get("selected_collections", {})
    output_dir = data.get("output_dir", f"./mongo_export_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}")

    q = Queue()
    conn = MongoConnOptions(uri=uri, timeout_ms=timeout, x509_file=x509_path)
    mgr = MicroserviceDBMongoManager(
        conn=conn,
        output_dir=output_dir,
        include_system=include_system,
        progress_queue=q,
    )

    def emit_loop():
        while True:
            try:
                item = q.get(timeout=0.5)
                if item[0] == "log":
                    socketio.emit("log", {"message": item[1]}, room=sid)
                elif item[0] == "progress":
                    socketio.emit("progress", {
                        "current": item[1],
                        "total": item[2],
                        "message": item[3]
                    }, room=sid)
            except Empty:
                pass
            except Exception:
                break

    def worker():
        try:
            result = mgr.run_export(
                db_name=None,
                collections=None,
                selected_dbs=selected_dbs,
                selected_collections=selected_collections,
            )
            payload = {
                **result,
                "results": result.get("results", []),
                "output_dir": str(mgr.output_dir),
            }
            _emit_complete_safe(socketio, "complete", payload, room=sid)
        except Exception as e:
            _emit_complete_safe(
                socketio,
                "complete",
                {"success": False, "error": str(e), "output_dir": str(mgr.output_dir)},
                room=sid,
            )
        finally:
            mgr.close()

    threading.Thread(target=emit_loop, daemon=True).start()
    threading.Thread(target=worker, daemon=True).start()


@socketio.on("run_copy")
def handle_run_copy(data):
    sid = request.sid
    uri = data.get("uri", "") or data.get("source_uri", "")
    dest_uri = data.get("dest_uri", "")
    timeout = int(data.get("timeout_ms", DEFAULT_TIMEOUT_MS))
    source_x509 = data.get("x509_path") or data.get("source_x509")
    dest_x509 = data.get("dest_x509")
    include_system = data.get("include_system", False)
    selected_dbs = data.get("selected_dbs", [])
    selected_collections = data.get("selected_collections", {})
    output_dir = data.get("output_dir", f"./mongo_copy_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}")

    q = Queue()
    source_conn = MongoConnOptions(uri=uri, timeout_ms=timeout, x509_file=source_x509)
    dest_conn = MongoConnOptions(uri=dest_uri, timeout_ms=timeout, x509_file=dest_x509)
    mgr = MicroserviceDBMongoManager(
        conn=source_conn,
        output_dir=output_dir,
        include_system=include_system,
        progress_queue=q,
    )

    def emit_loop():
        while True:
            try:
                item = q.get(timeout=0.5)
                if item[0] == "log":
                    socketio.emit("log", {"message": item[1]}, room=sid)
                elif item[0] == "progress":
                    socketio.emit("progress", {
                        "current": item[1],
                        "total": item[2],
                        "message": item[3]
                    }, room=sid)
            except Empty:
                pass
            except Exception:
                break

    def worker():
        try:
            result = mgr.run_copy(
                source_db=None,
                collections=None,
                dest_conn=dest_conn,
                selected_dbs=selected_dbs,
                selected_collections=selected_collections,
            )
            payload = {**result, "output_dir": str(mgr.output_dir)}
            _emit_complete_safe(socketio, "complete", payload, room=sid)
        except Exception as e:
            _emit_complete_safe(
                socketio,
                "complete",
                {"success": False, "error": str(e), "output_dir": str(mgr.output_dir)},
                room=sid,
            )
        finally:
            mgr.close()

    threading.Thread(target=emit_loop, daemon=True).start()
    threading.Thread(target=worker, daemon=True).start()


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MongoDB Manager Web Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host (default 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5000, help="Puerto (default 5000)")
    parser.add_argument("--debug", action="store_true", help="Modo debug")
    args = parser.parse_args()

    print(f"🚀 MongoDB Manager Web iniciado en http://{args.host}:{args.port}")
    socketio.run(app, host=args.host, port=args.port, debug=args.debug, allow_unsafe_werkzeug=True)
