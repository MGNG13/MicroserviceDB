/**
 * Cliente TypeScript de MicroserviceDB.
 *
 * Transportes:
 * - WebSocket (por defecto): MICROSERVICEDB_DATABASE_WS_URL (o _URL / _BASE_URL), default ws://127.0.0.1:3329/ws.
 * - gRPC (solo Node.js): MICROSERVICEDB_TRANSPORT=grpc o `{ transport: 'grpc' }`.
 *   Dirección: MICROSERVICEDB_DATABASE_GRPC_ADDRESS (default 127.0.0.1:3330).
 *   Requiere las dependencias `@grpc/grpc-js` y `@grpc/proto-loader`.
 */
declare const process: {
    env?: Record<string, string | undefined>;
};
// eslint-disable-next-line @typescript-eslint/no-explicit-any
declare const require: (id: string) => any;

import type WebSocketCtor from 'ws';
declare const NodeWebSocket: typeof WebSocketCtor;

const WebSocketImpl: typeof WebSocket = typeof globalThis !== 'undefined' && typeof globalThis.WebSocket === 'function' ? (globalThis.WebSocket as typeof WebSocket) : (NodeWebSocket as unknown as typeof WebSocket);

interface DocumentLike {
    _id?: unknown;
    [key: string]: unknown;
}

interface FindOptions {
    sort?: Record<string, 1 | -1> | undefined;
    projection?: Record<string, 1 | 0> | undefined;
    limit?: number | undefined;
    skip?: number | undefined;
}

interface InsertResult {
    acknowledged?: boolean;
    insertedId?: unknown;
    [key: string]: unknown;
}

interface UpdateResult {
    acknowledged?: boolean;
    modifiedCount?: number;
    upsertedCount?: number;
    upsertedId?: unknown;
    matchedCount?: number;
    [key: string]: unknown;
}

interface DeleteResult {
    acknowledged?: boolean;
    deletedCount?: number;
    [key: string]: unknown;
}

interface ChangeEvent {
    category: 'collection';
    collectionName: string;
    event: 'insertOne' | 'updateOne' | 'replaceOne' | 'deleteOne' | string;
    before: DocumentLike | null;
    after: DocumentLike | null;
}

type ChangeListener = (event: ChangeEvent) => void;

export type Transport = 'websocket' | 'grpc';

export interface ClientOptions {
    transport?: Transport;
    /** host:puerto del servidor gRPC (default 127.0.0.1:3330). */
    grpcAddress?: string;
    /** API key del servidor (default: variable de entorno MICROSERVICEDB_API_KEY). */
    apiKey?: string;
    /** gRPC con TLS. Default: true si grpcAddress empieza por https:// o MICROSERVICEDB_GRPC_TLS=true. */
    grpcTls?: boolean;
    /** CA en PEM para certificados propios (default: archivo en MICROSERVICEDB_TLS_CA_FILE). */
    tlsCa?: string | Buffer;
}

export interface ExportBatch {
    documents: DocumentLike[];
    total: number;
    sent: number;
}

/** Descriptor de proto/microservicedb.proto (formato JSON de protobufjs). */
const PROTO_DESCRIPTOR = {
    nested: {
        microservicedb: {
            nested: {
                DatabaseService: {
                    methods: {
                        Action: { requestType: 'ActionRequest', responseType: 'ActionResponse' },
                        StreamChanges: {
                            requestType: 'StreamChangesRequest',
                            responseType: 'ChangeEvent',
                            responseStream: true
                        },
                        ExportCollectionStream: {
                            requestType: 'ExportCollectionRequest',
                            responseType: 'DocumentBatch',
                            responseStream: true
                        }
                    }
                },
                ActionRequest: {
                    fields: {
                        msg_type: { type: 'string', id: 1 },
                        category: { type: 'string', id: 2 },
                        function_name: { type: 'string', id: 3 },
                        payload_json: { type: 'string', id: 4 },
                        request_id: { type: 'string', id: 5 }
                    }
                },
                ActionResponse: {
                    fields: {
                        success: { type: 'bool', id: 1 },
                        response_json: { type: 'string', id: 2 },
                        message: { type: 'string', id: 3 },
                        request_id: { type: 'string', id: 4 }
                    }
                },
                StreamChangesRequest: {
                    fields: {
                        database_name: { type: 'string', id: 1 },
                        collection_name: { type: 'string', id: 2 },
                        include_system: { type: 'bool', id: 3 }
                    }
                },
                ChangeEvent: {
                    fields: {
                        event: { type: 'string', id: 1 },
                        category: { type: 'string', id: 2 },
                        database_name: { type: 'string', id: 3 },
                        collection_name: { type: 'string', id: 4 },
                        event_json: { type: 'string', id: 5 },
                        ts_ms: { type: 'int64', id: 6 }
                    }
                },
                ExportCollectionRequest: {
                    fields: {
                        database_name: { type: 'string', id: 1 },
                        collection_name: { type: 'string', id: 2 },
                        filter_json: { type: 'string', id: 3 },
                        batch_size: { type: 'uint32', id: 4 }
                    }
                },
                DocumentBatch: {
                    fields: {
                        documents_json: { type: 'string', id: 1 },
                        total: { type: 'uint64', id: 2 },
                        sent: { type: 'uint64', id: 3 }
                    }
                }
            }
        }
    }
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type GrpcClient = any;

interface PendingRequest {
    resolve: (value: unknown) => void;
    reject: (reason: Error) => void;
    timer: ReturnType<typeof setTimeout> | null;
}

class MicroserviceDBAPI {
    private static _instances: Map<string, MicroserviceDBAPI> | undefined;
    public readonly databaseName: string;
    public precacheCollections: string[];
    public ready: boolean;
    private ws: InstanceType<typeof WebSocketImpl> | null;
    private pendingQueue: PendingRequest[];
    private queue: Record<string, unknown>[];
    private reconnectTimer: ReturnType<typeof setTimeout> | null;
    private changeListeners: Set<ChangeListener>;
    public readonly transport: Transport;
    private readonly grpcAddress: string;
    readonly #apiKey: string;
    readonly #grpcTls: boolean;
    readonly #tlsCa: string | Buffer;
    #grpcJs: GrpcClient | null = null;
    private grpc: GrpcClient | null;
    private grpcStream: { cancel: () => void } | null;
    private closed: boolean;
    constructor(databaseName: unknown, collections: unknown[] = [], options: ClientOptions = {}) {
        this.databaseName = String(databaseName || '').trim();
        this.precacheCollections = Array.isArray(collections)
            ? collections.map((c) => String(c || '').trim()).filter(Boolean)
            : [];
        this.ws = null;
        this.ready = false;
        this.pendingQueue = [];
        this.queue = [];
        this.reconnectTimer = null;
        this.changeListeners = new Set();
        const envTransport = typeof process !== 'undefined' ? process.env?.MICROSERVICEDB_TRANSPORT : undefined;
        this.transport =
            options.transport ?? (String(envTransport || '').trim().toLowerCase() === 'grpc' ? 'grpc' : 'websocket');
        const env = typeof process !== 'undefined' ? process.env : undefined;
        const rawGrpc = String(options.grpcAddress ?? env?.MICROSERVICEDB_DATABASE_GRPC_ADDRESS ?? '').trim();
        this.grpcAddress = rawGrpc.replace(/^[a-z]+:\/\//i, '') || '127.0.0.1:3330';
        this.#grpcTls =
            options.grpcTls ??
            (/^https:\/\//i.test(rawGrpc) || String(env?.MICROSERVICEDB_GRPC_TLS || '').toLowerCase() === 'true');
        this.#tlsCa = options.tlsCa ?? env?.MICROSERVICEDB_TLS_CA_FILE ?? '';
        this.#apiKey = String(
            options.apiKey ?? (typeof process !== 'undefined' ? process.env?.MICROSERVICEDB_API_KEY : '') ?? ''
        ).trim();
        this.grpc = null;
        this.grpcStream = null;
        this.closed = false;
        this.connect();
    }
    static getInstance(databaseName: unknown, collections: unknown[] = [], options: ClientOptions = {}): MicroserviceDBAPI {
        const key = String(databaseName || '').trim();
        if (!key) throw new Error('MicroserviceDBAPI.getInstance: databaseName is required');
        if (!this._instances) this._instances = new Map();
        if (this._instances.has(key)) {
            const existing = this._instances.get(key) as MicroserviceDBAPI;
            if (Array.isArray(collections) && collections.length > 0) {
                const set = new Set(existing.precacheCollections);
                for (const c of collections) {
                    const n = String(c || '').trim();
                    if (n) set.add(n);
                }
                existing.precacheCollections = Array.from(set);
            }
            return existing;
        }
        const inst = new MicroserviceDBAPI(key, collections, options);
        this._instances.set(key, inst);
        return inst;
    }
    #debugEnabled(): boolean {
        const raw = typeof process !== 'undefined' ? (process.env?.MICROSERVICEDB_DATABASE_DEBUG ?? process.env?.MICROSERVICEDB_DEBUG) : undefined;
        if (raw === undefined) return true;
        const v = String(raw).trim().toLowerCase();
        return v === '' || v === '1' || v === 'true' || v === 'yes' || v === 'on' || v === 'debug';
    }
    #log(kind: 'error' | 'warn' | 'info', message: string, details?: unknown): void {
        if (!this.#debugEnabled()) return;
        const prefix = `[MicroserviceDBAPI:${this.databaseName || 'unknown'}]`;
        let payload = '';
        if (details !== undefined) {
            try {
                payload = ` ${JSON.stringify(details)}`;
            } catch {
                payload = ' [unserializable details]';
            }
        }
        const line = `${prefix} ${message}${payload}`;
        if (kind === 'error') console.error(line);
        else if (kind === 'warn') console.warn(line);
        else console.log(line);
    }
    #clone<T>(value: T): T {
        if (value === undefined) return value as T;
        try {
            return JSON.parse(JSON.stringify(value)) as T;
        } catch {
            return value as T;
        }
    }
    #notifyChange(event: ChangeEvent): void {
        for (const listener of this.changeListeners) {
            try {
                listener(this.#clone(event));
            } catch {
                /* ignore listener errors */
            }
        }
    }
    #normalizeWebSocketUrl(rawUrl: unknown): string {
        const input = String(rawUrl || '').trim();
        if (!input) return '';
        let candidate = input;
        if (!/^[a-zA-Z][a-zA-Z\d+\-.]*:\/\//.test(candidate)) {
            candidate = candidate.startsWith('//') ? `ws:${candidate}` : `ws://${candidate}`;
        }
        try {
            const url = new URL(candidate);
            if (url.protocol === 'http:') url.protocol = 'ws:';
            else if (url.protocol === 'https:') url.protocol = 'wss:';
            else if (url.protocol !== 'ws:' && url.protocol !== 'wss:') return '';
            const pathname = (url.pathname || '').replace(/\/+$/g, '');
            if (!pathname || pathname === '/') url.pathname = '/ws';
            url.search = '';
            url.hash = '';
            return url.toString();
        } catch {
            return '';
        }
    }
    #resolveProcessPortWebSocketUrl(): string {
        return 'ws://127.0.0.1:3329/ws';
    }
    #resolveWebSocketUrl(): string {
        if (typeof process !== 'undefined') {
            const envUrl =
                process.env?.MICROSERVICEDB_DATABASE_WS_URL ??
                process.env?.MICROSERVICEDB_DATABASE_URL ??
                process.env?.MICROSERVICEDB_DATABASE_BASE_URL;
            return this.#normalizeWebSocketUrl(envUrl) || this.#resolveProcessPortWebSocketUrl();
        }
        return this.#resolveProcessPortWebSocketUrl();
    }
    #toActionRequest(msg: Record<string, unknown>): Record<string, string> {
        const { type, category, function_name, payload, ...rest } = msg as {
            type?: string;
            category?: string;
            function_name?: string;
            payload?: unknown;
            [k: string]: unknown;
        };
        const body = payload !== undefined && payload !== null ? payload : rest;
        return {
            msg_type: String(type ?? 'action'),
            category: String(category ?? ''),
            function_name: String(function_name ?? ''),
            payload_json: JSON.stringify(body ?? {}),
            request_id: ''
        };
    }
    #connectGrpc(): void {
        if (this.grpc) return;
        let grpcJs: GrpcClient;
        let protoLoader: GrpcClient;
        try {
            grpcJs = require('@grpc/grpc-js');
            protoLoader = require('@grpc/proto-loader');
        } catch (error) {
            this.#log('error', 'gRPC transport requires @grpc/grpc-js and @grpc/proto-loader', {
                message: (error as Error)?.message
            });
            return;
        }
        this.#grpcJs = grpcJs;
        const definition = protoLoader.fromJSON(PROTO_DESCRIPTOR, {
            keepCase: true,
            longs: Number,
            defaults: true
        });
        const pkg = grpcJs.loadPackageDefinition(definition).microservicedb;
        let credentials;
        if (this.#grpcTls) {
            let ca: Buffer | null = null;
            if (Buffer.isBuffer(this.#tlsCa)) ca = this.#tlsCa;
            else if (this.#tlsCa && this.#tlsCa.includes('-----BEGIN')) ca = Buffer.from(this.#tlsCa);
            else if (this.#tlsCa) ca = require('fs').readFileSync(this.#tlsCa);
            credentials = grpcJs.credentials.createSsl(ca);
        } else {
            credentials = grpcJs.credentials.createInsecure();
        }
        this.grpc = new pkg.DatabaseService(this.grpcAddress, credentials, {
            'grpc.max_receive_message_length': -1,
            'grpc.max_send_message_length': -1,
            'grpc.keepalive_time_ms': 30000
        });
        this.ready = true;
        this.#log('info', 'gRPC client ready', { address: this.grpcAddress, tls: this.#grpcTls });
        if (this.precacheCollections.length > 0) {
            this.grpc.Action(
                this.#toActionRequest({
                    type: 'precache',
                    database_name: this.databaseName,
                    collections: this.precacheCollections
                }),
                this.#grpcMetadata(),
                (err: Error | null) => {
                    if (err) this.#log('warn', 'gRPC precache failed', { message: err.message });
                }
            );
        }
        this.#openChangeStream();
    }
    #grpcMetadata(): unknown {
        const md = new this.#grpcJs.Metadata();
        if (this.#apiKey) md.set('authorization', `Bearer ${this.#apiKey}`);
        return md;
    }
    #openChangeStream(): void {
        if (!this.grpc || this.closed) return;
        const call = this.grpc.StreamChanges(
            {
                database_name: this.databaseName,
                collection_name: '',
                include_system: false
            },
            this.#grpcMetadata()
        );
        this.grpcStream = call;
        call.on('data', (ev: { event_json?: string }) => {
            try {
                const msg = JSON.parse(ev.event_json || '{}');
                if (msg && msg.kind === 'change') this.#notifyChange(msg as ChangeEvent);
            } catch {
                /* evento inválido */
            }
        });
        const retry = (): void => {
            if (this.grpcStream !== call) return;
            this.grpcStream = null;
            if (this.closed) return;
            if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
            this.reconnectTimer = setTimeout(() => this.#openChangeStream(), 3000);
        };
        call.on('error', (err: { code?: number; message?: string }) => {
            if (err?.code !== 1) this.#log('warn', 'gRPC change stream error, retrying in 3s', { message: err?.message });
            retry();
        });
        call.on('end', retry);
    }
    #grpcAction<T>(payload: Record<string, unknown>): Promise<T> {
        return new Promise<T>((resolve, reject) => {
            if (!this.grpc) return reject(new Error('gRPC not available'));
            const deadline = new Date(Date.now() + 15000);
            this.grpc.Action(
                this.#toActionRequest(payload),
                this.#grpcMetadata(),
                { deadline },
                (err: Error | null, resp: { success: boolean; response_json?: string; message?: string }) => {
                    if (err) return reject(new Error(`gRPC error: ${err.message}`));
                    if (!resp?.success) return reject(new Error(resp?.message || 'Action failed'));
                    try {
                        resolve((resp.response_json ? JSON.parse(resp.response_json) : null) as T);
                    } catch {
                        resolve((resp.response_json ?? null) as T);
                    }
                }
            );
        });
    }
    /** Exporta una colección por lotes (solo transporte gRPC). Devuelve el total enviado. */
    async exportCollectionStream(
        collectionName: string,
        onBatch: (batch: ExportBatch) => void,
        options: { filter?: Record<string, unknown>; batchSize?: number } = {}
    ): Promise<number> {
        if (this.transport !== 'grpc' || !this.grpc) throw new Error('exportCollectionStream requires the gRPC transport');
        return new Promise<number>((resolve, reject) => {
            let sent = 0;
            const call = this.grpc.ExportCollectionStream(
                {
                    database_name: this.databaseName,
                    collection_name: collectionName,
                    filter_json: options.filter ? JSON.stringify(options.filter) : '',
                    batch_size: options.batchSize ?? 0
                },
                this.#grpcMetadata()
            );
            call.on('data', (b: { documents_json: string; total: number; sent: number }) => {
                sent = Number(b.sent);
                onBatch({ documents: JSON.parse(b.documents_json || '[]'), total: Number(b.total), sent });
            });
            call.on('error', (err: Error) => reject(new Error(`gRPC error: ${err.message}`)));
            call.on('end', () => resolve(sent));
        });
    }
    connect(): void {
        if (this.transport === 'grpc') {
            this.#connectGrpc();
            return;
        }
        if (this.ws && (this.ws.readyState === WebSocketImpl.OPEN || this.ws.readyState === WebSocketImpl.CONNECTING))
            return;
        const url = this.#resolveWebSocketUrl();
        if (!url) {
            this.#log('error', 'WebSocket URL is not configured', {
                expectedEnv: [
                    'MICROSERVICEDB_DATABASE_WS_URL',
                    'MICROSERVICEDB_DATABASE_URL',
                    'MICROSERVICEDB_DATABASE_BASE_URL'
                ]
            });
            return;
        }
        this.#log('info', 'Connecting WebSocket...', { url });
        // WebSocket no admite cabeceras en todos los entornos: la key viaja como ?api_key=.
        this.ws = new WebSocketImpl(this.#apiKey ? `${url}?api_key=${encodeURIComponent(this.#apiKey)}` : url);
        this.ws.onopen = (): void => {
            this.#log('info', 'WebSocket connected');
            this.ready = true;
            if (this.precacheCollections.length > 0) {
                this.send({
                    type: 'precache',
                    database_name: this.databaseName,
                    collections: this.precacheCollections
                });
            }
            while (this.queue.length) {
                const msg = this.queue.shift() as Record<string, unknown>;
                this.ws?.send(JSON.stringify(msg));
            }
        };
        this.ws.onmessage = (event: { data: unknown }): void => {
            let msg:
                | { kind: 'change'; event: ChangeEvent['event']; collectionName: string; category: string }
                | { success: boolean; response_json?: unknown; message?: string }
                | undefined;
            const rawData =
                typeof event?.data === 'string' ? event.data : (event?.data as { toString?: () => string })?.toString?.();
            try {
                msg = JSON.parse(rawData as string) as typeof msg;
            } catch {
                return;
            }
            if (msg && 'kind' in msg && msg.kind === 'change') {
                this.#log('info', 'WebSocket change event', {
                    event: msg.event,
                    collectionName: msg.collectionName,
                    category: msg.category
                });
                this.#notifyChange(msg as unknown as ChangeEvent);
                return;
            }
            if (msg && typeof (msg as { success?: boolean }).success === 'boolean') {
                const pending = this.pendingQueue.shift();
                if (!pending) return;
                if (pending.timer) clearTimeout(pending.timer);
                if ((msg as { success: boolean }).success) {
                    try {
                        pending.resolve(
                            (msg as { response_json?: string }).response_json
                                ? JSON.parse((msg as { response_json: string }).response_json)
                                : null
                        );
                    } catch {
                        pending.resolve((msg as { response_json?: unknown }).response_json ?? null);
                    }
                } else {
                    pending.reject(new Error((msg as { message?: string }).message || 'Action failed'));
                }
            }
        };
        this.ws.onclose = (): void => {
            this.ready = false;
            if (this.closed) return;
            this.#log('warn', 'WebSocket closed, reconnecting in 3s...');
            if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
            this.reconnectTimer = setTimeout(() => this.connect(), 3000);
        };
        this.ws.onerror = (err: unknown): void => {
            this.#log('error', 'WebSocket error', err);
        };
    }
    send(payload: Record<string, unknown>): void {
        if (this.transport === 'grpc') {
            this.#grpcAction(payload).catch((e) => this.#log('warn', 'gRPC send failed', { message: e?.message }));
            return;
        }
        const msg = { ...payload };
        if (this.ready && this.ws && this.ws.readyState === WebSocketImpl.OPEN) {
            this.ws.send(JSON.stringify(msg));
        } else {
            this.queue.push(msg);
        }
    }
    async sendAndWait<T = unknown>(payload: Record<string, unknown>): Promise<T> {
        if (this.transport === 'grpc') {
            if (!this.grpc) throw new Error('gRPC not available');
            return this.#grpcAction<T>(payload);
        }
        await this.#ensureReady();
        return new Promise<T>((resolve, reject) => {
            const pending: PendingRequest = {
                resolve: resolve as (value: unknown) => void,
                reject,
                timer: null
            };
            this.pendingQueue.push(pending);
            const sendInner = (): void => {
                if (this.ws && this.ws.readyState === WebSocketImpl.OPEN) {
                    this.ws.send(JSON.stringify(payload));
                } else {
                    this.queue.push(payload);
                }
            };
            sendInner();
            pending.timer = setTimeout(() => {
                const idx = this.pendingQueue.indexOf(pending);
                if (idx >= 0) this.pendingQueue.splice(idx, 1);
                reject(new Error('Request timeout'));
            }, 15000);
        });
    }
    async #ensureReady(): Promise<void> {
        if (this.ready) return;
        return new Promise<void>((resolve, reject) => {
            const check = (): void => {
                if (this.ready) return resolve();
                if (!this.ws || this.ws.readyState === WebSocketImpl.CLOSED) {
                    return reject(new Error('WebSocket not available'));
                }
                setTimeout(check, 100);
            };
            check();
        });
    }
    async insertOne(collectionName: string, document: DocumentLike): Promise<InsertResult> {
        const result = await this.sendAndWait<InsertResult>({
            type: 'action',
            category: 'collection',
            function_name: 'insertOne',
            payload: { database_name: this.databaseName, collectionName, document }
        });
        return result;
    }
    async findOne(collectionName: string, query: unknown): Promise<DocumentLike | null> {
        try {
            return await this.sendAndWait<DocumentLike | null>({
                type: 'action',
                category: 'collection',
                function_name: 'findOne',
                payload: { database_name: this.databaseName, collectionName, filter: query }
            });
        } catch (error) {
            this.#log('error', 'findOne failed', { message: (error as Error)?.message });
            return null;
        }
    }
    async find(
        collectionName: string,
        query: Record<string, unknown> = {},
        options: FindOptions = {}
    ): Promise<DocumentLike[] | null> {
        try {
            const opt: FindOptions = options && typeof options === 'object' ? options : {};
            const data: {
                database_name: string;
                collectionName: string;
                filter: Record<string, unknown>;
                sort?: FindOptions['sort'];
                projection?: FindOptions['projection'];
                limit?: number;
                skip?: number;
            } = {
                database_name: this.databaseName,
                collectionName,
                filter: query ?? {},
                sort: opt.sort ?? undefined,
                projection: opt.projection ?? undefined,
                limit: opt.limit ?? undefined,
                skip: opt.skip ?? undefined
            };
            const result = await this.sendAndWait<DocumentLike[] | null>({
                type: 'action',
                category: 'collection',
                function_name: 'find',
                payload: data
            });
            if (result === null) return null;
            return Array.isArray(result) ? result : [];
        } catch (error) {
            this.#log('error', 'find failed', { message: (error as Error)?.message });
            return null;
        }
    }
    async updateCollection(collectionName: string, data: unknown): Promise<unknown> {
        return this.sendAndWait({
            type: 'action',
            category: 'collection',
            function_name: 'updateCollection',
            payload: { database_name: this.databaseName, collectionName, data }
        });
    }
    async updateOne(
        collectionName: string,
        filter: unknown,
        update: unknown,
        options: Record<string, unknown> = {}
    ): Promise<UpdateResult> {
        const result = await this.sendAndWait<UpdateResult>({
            type: 'action',
            category: 'collection',
            function_name: 'updateOne',
            payload: { database_name: this.databaseName, collectionName, filter, update, options }
        });
        return result;
    }
    async replaceOne(
        collectionName: string,
        filter: unknown,
        replacement: DocumentLike,
        options: Record<string, unknown> = {}
    ): Promise<UpdateResult> {
        const result = await this.sendAndWait<UpdateResult>({
            type: 'action',
            category: 'collection',
            function_name: 'replaceOne',
            payload: { database_name: this.databaseName, collectionName, filter, replacement, options }
        });
        return result;
    }
    async deleteOne(collectionName: string, filter: unknown): Promise<DeleteResult> {
        const result = await this.sendAndWait<DeleteResult>({
            type: 'action',
            category: 'collection',
            function_name: 'deleteOne',
            payload: { database_name: this.databaseName, collectionName, filter }
        });
        return result;
    }
    async getMetadata(): Promise<unknown> {
        try {
            const result = await this.sendAndWait({ type: 'metadata', database_name: this.databaseName });
            return result;
        } catch {
            return null;
        }
    }
    subscribe(listener: ChangeListener): () => boolean {
        if (typeof listener !== 'function')
            throw new Error('MicroserviceDBAPI.subscribe: listener must be a function');
        this.changeListeners.add(listener);
        return (): boolean => this.changeListeners.delete(listener);
    }
    close(): void {
        this.closed = true;
        if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
        if (this.grpcStream) {
            const stream = this.grpcStream;
            this.grpcStream = null;
            stream.cancel();
        }
        if (this.grpc) {
            this.grpc.close();
            this.grpc = null;
        }
        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
        this.ready = false;
        while (this.pendingQueue.length) {
            const p = this.pendingQueue.shift() as PendingRequest;
            if (p?.timer) clearTimeout(p.timer);
            try {
                p.reject(new Error('MicroserviceDBAPI closed'));
            } catch {
                /* ignore */
            }
        }
    }
}

export default MicroserviceDBAPI;