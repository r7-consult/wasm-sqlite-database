/**
 * VecService - lightweight wrapper around the vector service C API
 * described in ADR 0016. This assumes the underlying sqlite3 build has
 * the sqlite-vec `vec0` module available so that CREATE VIRTUAL TABLE
 * ... USING vec0(...) works.
 */

class VecService {
    constructor(wasmModule) {
        this._module = wasmModule;
        this._initialized = false;
        this._available = null; // null = unknown, false = not available, true = ready
    }

    init() {
        if (!this._initialized) {
            if (typeof this._module._vec_init !== 'function') {
                // VecService is not wired into this build.
                this._available = false;
                this._initialized = true;
                return this;
            }
            try {
                this._module._vec_init();
                this._available = true;
            } catch (e) {
                const msg = e && e.message ? e.message : String(e);
                // Some Emscripten builds report wasm table issues as a generic
                // "null function or function signature mismatch" error when an
                // exported symbol is present but not callable. Treat this as an
                // indication that sqlite-vec is not usable in this build.
                if (msg.includes('null function or function signature mismatch')) {
                    this._available = false;
                } else {
                    throw e;
                }
            }
            this._initialized = true;
        }
        return this;
    }

    isAvailable() {
        // Ensure init() has run at least once so _available is populated.
        if (!this._initialized) {
            this.init();
        }
        return !!this._available;
    }

    /**
     * Create a vector table.
     * @param {string} tableName
     * @param {number} dims
     * @param {'float'|'int8'|'binary'} storage
     */
    createTable(tableName, dims, storage = 'float') {
        this.init();
        if (!this._available) {
            throw new Error('VecService is not available in this WASM build');
        }
        const namePtr = this._allocateString(tableName);
        const storagePtr = this._allocateString(storage);
        const rc = this._module._vec_create_table(namePtr, dims, storagePtr);
        this._module._free(namePtr);
        this._module._free(storagePtr);
        if (rc !== 0) {
            const err = this.getLastError();
            throw new Error(`vec_create_table failed: ${err}`);
        }
    }

    /**
     * Insert or upsert a vector.
     * @param {string} tableName
     * @param {number} id
     * @param {number[]|string} embedding - JSON array or array of numbers
     * @param {object|string|null} meta - optional metadata
     */
    insert(tableName, id, embedding, meta = null) {
        this.init();
        if (!this._available) {
            throw new Error('VecService is not available in this WASM build');
        }
        const namePtr = this._allocateString(tableName);
        const embeddingJson =
            Array.isArray(embedding) ? JSON.stringify(embedding) : String(embedding);
        const embeddingPtr = this._allocateString(embeddingJson);
        const metaJson =
            meta == null ? null : (typeof meta === 'string' ? meta : JSON.stringify(meta));
        const metaPtr = metaJson ? this._allocateString(metaJson) : 0;

        const rc = this._module._vec_insert(
            namePtr,
            BigInt(id), // long long
            embeddingPtr,
            metaPtr
        );

        this._module._free(namePtr);
        this._module._free(embeddingPtr);
        if (metaPtr) this._module._free(metaPtr);

        if (rc !== 0) {
            const err = this.getLastError();
            throw new Error(`vec_insert failed: ${err}`);
        }
    }

    /**
     * Search nearest neighbours.
     * @param {string} tableName
     * @param {number[]|string} embedding
     * @param {number} k
     * @param {object} [options]
     * @returns {{rows: {id:number, distance:number}[]}}
     */
    search(tableName, embedding, k, options = {}) {
        this.init();
        if (!this._available) {
            throw new Error('VecService is not available in this WASM build');
        }
        const namePtr = this._allocateString(tableName);
        const embeddingJson =
            Array.isArray(embedding) ? JSON.stringify(embedding) : String(embedding);
        const embeddingPtr = this._allocateString(embeddingJson);
        const optionsJson = JSON.stringify(options || {});
        const optionsPtr = this._allocateString(optionsJson);

        const resultPtr = this._module._vec_search(
            namePtr,
            embeddingPtr,
            k,
            optionsPtr
        );

        this._module._free(namePtr);
        this._module._free(embeddingPtr);
        this._module._free(optionsPtr);

        const jsonStr = this._module.UTF8ToString(resultPtr);
        if (!jsonStr) {
            return { rows: [] };
        }
        try {
            const obj = JSON.parse(jsonStr);
            if (!obj || typeof obj !== 'object') {
                return { rows: [] };
            }
            return obj;
        } catch {
            return { rows: [] };
        }
    }

    getLastError() {
        if (typeof this._module._vec_getLastError !== 'function') {
            return '';
        }
        const ptr = this._module._vec_getLastError();
        return this._module.UTF8ToString(ptr);
    }

    _allocateString(str) {
        const encoded = new TextEncoder().encode(str + '\0');
        const ptr = this._module._malloc(encoded.length);
        const heap = new Uint8Array(this._module.HEAPU8.buffer, ptr, encoded.length);
        heap.set(encoded);
        return ptr;
    }
}

if (typeof module !== 'undefined') {
    module.exports = { VecService };
}
