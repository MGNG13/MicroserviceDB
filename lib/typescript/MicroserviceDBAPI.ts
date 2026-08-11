declare const process: {
    env?: Record<string, string | undefined>;
};

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

interface FindCacheEntry {
    collectionName: string;
    query: Record<string, unknown>;
    options: FindOptions;
    result: DocumentLike[];
}

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
    private findCache: Map<string, FindCacheEntry>;
    constructor(databaseName: unknown, collections: unknown[] = []) {
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
        this.findCache = new Map();
        this.connect();
    }
    static getInstance(databaseName: unknown, collections: unknown[] = []): MicroserviceDBAPI {
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
        const inst = new MicroserviceDBAPI(key, collections);
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
    #docKey(doc: unknown): string | null {
        if (!doc || typeof doc !== 'object' || !('_id' in (doc as object))) return null;
        try {
            return JSON.stringify((doc as DocumentLike)._id);
        } catch {
            return String((doc as DocumentLike)._id);
        }
    }
    #buildFindCacheKey(
        collectionName: string,
        query: unknown,
        options: FindOptions | undefined
    ): string {
        return JSON.stringify({
            collectionName,
            query: query ?? {},
            options: options && typeof options === 'object' ? options : {}
        });
    }
    #isDefaultSnapshot(entry: FindCacheEntry | undefined): boolean {
        if (!entry) return false;
        const query = entry.query && typeof entry.query === 'object' ? entry.query : {};
        const options = entry.options && typeof entry.options === 'object' ? entry.options : {};
        return (
            Object.keys(query).length === 0 &&
            !options.sort &&
            !options.projection &&
            (!options.skip || options.skip === 0)
        );
    }
    #getSimpleFilterDoc(filter: unknown): { _id: unknown } | null {
        if (!filter || typeof filter !== 'object' || Array.isArray(filter)) return null;
        const keys = Object.keys(filter as Record<string, unknown>);
        if (keys.length !== 1 || keys[0] !== '_id') return null;
        return { _id: this.#clone((filter as DocumentLike)._id) };
    }
    #clearFindCachesForCollection(collectionName: string): void {
        for (const [key, entry] of this.findCache.entries()) {
            if (entry?.collectionName === collectionName) this.findCache.delete(key);
        }
    }
    #applyLocalCollectionMutation(
        collectionName: string,
        eventName: ChangeEvent['event'],
        before: DocumentLike | null,
        after: DocumentLike | null
    ): void {
        if (!collectionName) return;
        if ((before && this.#docKey(before) != null) || (after && this.#docKey(after) != null)) {
            this.#updateFindCachesFromChange({
                category: 'collection',
                collectionName,
                event: eventName,
                before: before ? this.#clone(before) : null,
                after: after ? this.#clone(after) : null
            });
            return;
        }
        this.#clearFindCachesForCollection(collectionName);
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
    #updateFindCachesFromChange(event: ChangeEvent): void {
        if (!event || event.category !== 'collection' || !event.collectionName) return;
        for (const [key, entry] of this.findCache.entries()) {
            if (!entry || entry.collectionName !== event.collectionName) continue;
            if (!this.#isDefaultSnapshot(entry)) {
                this.findCache.delete(key);
                continue;
            }
            const rows = Array.isArray(entry.result) ? entry.result.slice() : [];
            const beforeKey = this.#docKey(event.before);
            const afterKey = this.#docKey(event.after);
            if (event.event === 'insertOne' && event.after) {
                const next = rows.filter((row) => this.#docKey(row) !== afterKey);
                next.unshift(this.#clone(event.after));
                entry.result = typeof entry.options?.limit === 'number' ? next.slice(0, entry.options.limit) : next;
                continue;
            }
            if ((event.event === 'updateOne' || event.event === 'replaceOne') && event.after) {
                const next = rows.filter(
                    (row) => this.#docKey(row) !== beforeKey && this.#docKey(row) !== afterKey
                );
                next.unshift(this.#clone(event.after));
                entry.result = typeof entry.options?.limit === 'number' ? next.slice(0, entry.options.limit) : next;
                continue;
            }
            if (event.event === 'deleteOne' && beforeKey != null) {
                entry.result = rows.filter((row) => this.#docKey(row) !== beforeKey);
                continue;
            }
            this.findCache.delete(key);
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
            if (url.protocol === 'http:' || url.protocol === 'https:' || url.protocol === 'wss:')
                url.protocol = 'ws:';
            else if (url.protocol !== 'ws:') return '';
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
    connect(): void {
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
        this.ws = new WebSocketImpl(url);
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
                this.#updateFindCachesFromChange(msg as unknown as ChangeEvent);
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
            this.#log('warn', 'WebSocket closed, reconnecting in 3s...');
            if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
            this.reconnectTimer = setTimeout(() => this.connect(), 3000);
        };
        this.ws.onerror = (err: unknown): void => {
            this.#log('error', 'WebSocket error', err);
        };
    }
    send(payload: Record<string, unknown>): void {
        const msg = { ...payload };
        if (this.ready && this.ws && this.ws.readyState === WebSocketImpl.OPEN) {
            this.ws.send(JSON.stringify(msg));
        } else {
            this.queue.push(msg);
        }
    }
    async sendAndWait<T = unknown>(payload: Record<string, unknown>): Promise<T> {
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
        const nextDoc = this.#clone(document);
        if (
            nextDoc &&
            typeof nextDoc === 'object' &&
            !Array.isArray(nextDoc) &&
            nextDoc._id === undefined &&
            result?.insertedId !== undefined
        ) {
            nextDoc._id = this.#clone(result.insertedId);
        }
        this.#applyLocalCollectionMutation(collectionName, 'insertOne', null, nextDoc);
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
            const rows = Array.isArray(result) ? result : [];
            this.findCache.set(this.#buildFindCacheKey(collectionName, query, opt), {
                collectionName,
                query: this.#clone(query ?? {}),
                options: this.#clone(opt),
                result: this.#clone(rows)
            });
            return rows;
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
        if ((result?.modifiedCount || 0) > 0 || (result?.upsertedCount || 0) > 0) {
            this.#clearFindCachesForCollection(collectionName);
        }
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
        if ((result?.modifiedCount || 0) > 0 || (result?.upsertedCount || 0) > 0) {
            const before = this.#getSimpleFilterDoc(filter);
            const after = this.#clone(replacement);
            if (after && typeof after === 'object' && !Array.isArray(after) && after._id === undefined) {
                if (before?._id !== undefined) after._id = this.#clone(before._id);
                else if (result?.upsertedId !== undefined) after._id = this.#clone(result.upsertedId);
            }
            this.#applyLocalCollectionMutation(collectionName, 'replaceOne', before, after);
        }
        return result;
    }
    async deleteOne(collectionName: string, filter: unknown): Promise<DeleteResult> {
        const result = await this.sendAndWait<DeleteResult>({
            type: 'action',
            category: 'collection',
            function_name: 'deleteOne',
            payload: { database_name: this.databaseName, collectionName, filter }
        });
        if ((result?.deletedCount || 0) > 0) {
            this.#applyLocalCollectionMutation(collectionName, 'deleteOne', this.#getSimpleFilterDoc(filter), null);
        }
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
    getCachedFind(
        collectionName: string,
        query: Record<string, unknown> = {},
        options: FindOptions = {}
    ): DocumentLike[] | null {
        const entry = this.findCache.get(this.#buildFindCacheKey(collectionName, query, options));
        return entry ? this.#clone(entry.result) : null;
    }
    close(): void {
        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
        this.ready = false;
        this.findCache.clear();
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