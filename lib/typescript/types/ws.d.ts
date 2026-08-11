declare module 'ws' {
    const WebSocketCtor: {
        prototype: {
            readyState: number;
            close(): void;
            send(data: string): void;
        };
        new (url: string): {
            readyState: number;
            close(): void;
            send(data: string): void;
            onopen: (() => void) | null;
            onmessage: ((event: { data: unknown }) => void) | null;
            onclose: (() => void) | null;
            onerror: ((err: unknown) => void) | null;
        };
        readonly CLOSED: number;
        readonly CLOSING: number;
        readonly CONNECTING: number;
        readonly OPEN: number;
    };
    export default WebSocketCtor;
}
