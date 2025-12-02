/**
 * SessionManager - IndexedDB-backed metadata store for WASM sessions.
 *
 * This implements the metadata layer described in ADR 0017. It stores
 * lightweight information about sessions and workbooks (which SQLite DB
 * file path they correspond to), but does NOT store large blobs – those
 * are handled via IDBFS at /db.
 */

const DB_NAME = 'excel_loader_sessions';
const DB_VERSION = 1;
const STORE_SESSIONS = 'sessions';
const STORE_WORKBOOKS = 'workbooks';

function openDb() {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(DB_NAME, DB_VERSION);
        req.onupgradeneeded = event => {
            const db = event.target.result;
            if (!db.objectStoreNames.contains(STORE_SESSIONS)) {
                db.createObjectStore(STORE_SESSIONS, { keyPath: 'sessionId' });
            }
            if (!db.objectStoreNames.contains(STORE_WORKBOOKS)) {
                db.createObjectStore(STORE_WORKBOOKS, { keyPath: 'workbookId' });
            }
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}

function tx(db, storeName, mode = 'readonly') {
    return db.transaction(storeName, mode).objectStore(storeName);
}

class SessionManager {
    constructor() {
        this._dbPromise = null;
    }

    _db() {
        if (!this._dbPromise) {
            this._dbPromise = openDb();
        }
        return this._dbPromise;
    }

    async ensureSession(sessionId) {
        const db = await this._db();
        const store = tx(db, STORE_SESSIONS, 'readwrite');
        const existing = await new Promise(resolve => {
            const req = store.get(sessionId);
            req.onsuccess = () => resolve(req.result || null);
            req.onerror = () => resolve(null);
        });
        const now = new Date().toISOString();
        if (!existing) {
            const rec = { sessionId, createdAt: now, lastUsedAt: now };
            await new Promise((resolve, reject) => {
                const req = store.put(rec);
                req.onsuccess = () => resolve();
                req.onerror = () => reject(req.error);
            });
            return rec;
        } else {
            existing.lastUsedAt = now;
            await new Promise((resolve, reject) => {
                const req = store.put(existing);
                req.onsuccess = () => resolve();
                req.onerror = () => reject(req.error);
            });
            return existing;
        }
    }

    async saveWorkbookMeta(workbookId, sessionId, dbPath, options, datasetsMeta) {
        const db = await this._db();
        const store = tx(db, STORE_WORKBOOKS, 'readwrite');
        const rec = {
            workbookId,
            sessionId,
            dbPath,
            options: options || {},
            datasetsMeta: datasetsMeta || {},
            updatedAt: new Date().toISOString(),
        };
        await new Promise((resolve, reject) => {
            const req = store.put(rec);
            req.onsuccess = () => resolve();
            req.onerror = () => reject(req.error);
        });
        await this.ensureSession(sessionId);
    }

    async loadWorkbookMeta(workbookId) {
        const db = await this._db();
        const store = tx(db, STORE_WORKBOOKS, 'readonly');
        return await new Promise((resolve, reject) => {
            const req = store.get(workbookId);
            req.onsuccess = () => resolve(req.result || null);
            req.onerror = () => reject(req.error);
        });
    }

    async deleteSession(sessionId) {
        const db = await this._db();
        const sessStore = tx(db, STORE_SESSIONS, 'readwrite');
        await new Promise((resolve, reject) => {
            const req = sessStore.delete(sessionId);
            req.onsuccess = () => resolve();
            req.onerror = () => reject(req.error);
        });

        const wbStore = tx(db, STORE_WORKBOOKS, 'readwrite');
        await new Promise((resolve, reject) => {
            const req = wbStore.openCursor();
            req.onsuccess = event => {
                const cursor = event.target.result;
                if (!cursor) {
                    resolve();
                    return;
                }
                const val = cursor.value;
                if (val.sessionId === sessionId) {
                    cursor.delete();
                }
                cursor.continue();
            };
            req.onerror = () => reject(req.error);
        });
    }
}

if (typeof module !== 'undefined') {
    module.exports = { SessionManager };
}

