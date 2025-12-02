/**
 * Excel Loader WASM JavaScript Wrapper
 *
 * This module provides a high-level JavaScript API for the excel_loader
 * WebAssembly module. It wraps the low-level C functions exported by the
 * WASM build and provides a more ergonomic interface for JavaScript users.
 *
 * Heavy formats (Parquet, DuckDB) are handled via JavaScript libraries:
 * - Parquet: parquet-wasm or @duckdb/duckdb-wasm
 * - DuckDB: @duckdb/duckdb-wasm
 */

// Load optional heavy format handlers
let ParquetHandler = null;
let DuckDBHandler = null;

try {
    if (typeof require !== 'undefined') {
        try { ParquetHandler = require('./parquet_handler.js'); } catch (e) { /* optional */ }
        try { DuckDBHandler = require('./duckdb_handler.js'); } catch (e) { /* optional */ }
    }
} catch (e) { /* handlers not available */ }

// File format enum matching SeFileFormat in C++
const FileFormat = Object.freeze({
    Auto: 0,
    Csv: 1,
    Tsv: 2,
    Xlsx: 3,
    Xlsm: 4,
    Xltx: 5,
    Xls: 6,
    Xlsb: 7,
    Ods: 8,
    Sqlite: 9,
    Dbf: 10,
    Mdb: 11,
    Accdb: 12,
    Parquet: 13,
    DuckDb: 14,
    Jsonl: 15,
    Json: 16,
    Xml: 17,
    Html: 18,
    Txt: 19
});

let SessionManagerImpl = null;
try {
    if (typeof require !== 'undefined') {
        ({ SessionManager: SessionManagerImpl } = require('./session_manager.js'));
    }
} catch (e) {
    // Browser path will load via script tag
}

/**
 * ExcelLoader class - main interface for the WASM module
 */
class ExcelLoader {
    constructor(wasmModule) {
        this._module = wasmModule;
        this._initialized = false;
        this._handles = new Map(); // Track open file handles
        this._sessionManager = SessionManagerImpl ? new SessionManagerImpl() : null;
        this._defaultSessionId = 'default';
        this._activeWorkbooks = []; // { handleId, workbook, approxSizeBytes, lastUsedAt }
    }

    /**
     * Initialize the loader. Must be called before any other operations.
     */
    init() {
        if (!this._initialized) {
            this._module._ff_init();
            this._initialized = true;
        }
        return this;
    }

    /**
     * Open a file from a Uint8Array buffer
     * @param {Uint8Array} data - File contents as binary data
     * @param {string} fileName - Name of the file (used for format detection)
     * @param {Object} options - Optional settings
     * @param {number} options.format - File format (from FileFormat enum)
     * @param {string} options.delimiter - CSV/TSV delimiter character
     * @param {boolean} options.hasHeaderRow - Whether first row is header
     * @returns {Workbook|Promise<Workbook>} - Workbook object for querying
     */
    openFile(data, fileName, options = {}) {
        if (!this._initialized) {
            throw new Error('ExcelLoader not initialized. Call init() first.');
        }

        const format = options.format ?? FileFormat.Auto;

        // Check if this is a heavy format that should be handled in JavaScript
        const ext = fileName.toLowerCase().split('.').pop();
        const isParquet = (format === FileFormat.Parquet) || ext === 'parquet' || ext === 'pq';
        const isDuckDB = (format === FileFormat.DuckDb) || ext === 'duckdb';

        // Handle Parquet files
        if (isParquet) {
            return this._openParquetFile(data, fileName);
        }

        // Handle DuckDB files
        if (isDuckDB) {
            return this._openDuckDBFile(data, fileName);
        }

        // Standard path: use WASM module
        const delimiter = options.delimiter ?? ',';
        const hasHeaderRow = options.hasHeaderRow ?? true;

        // Allocate memory in WASM heap for the data
        const dataPtr = this._module._malloc(data.length);
        const dataHeap = new Uint8Array(this._module.HEAPU8.buffer, dataPtr, data.length);
        dataHeap.set(data);

        // Allocate memory for filename string
        const fileNamePtr = this._allocateString(fileName);

        // Call WASM function
        const handleId = this._module._ff_openFile(
            dataPtr,
            data.length,
            fileNamePtr,
            format,
            delimiter.charCodeAt(0),
            hasHeaderRow ? 1 : 0
        );

        // Free allocated memory
        this._module._free(dataPtr);
        this._module._free(fileNamePtr);

        if (handleId === 0) {
            const error = this.getLastError();
            throw new Error(`Failed to open file: ${error}`);
        }

        const workbook = new Workbook(this, handleId, fileName);
        this._handles.set(handleId, workbook);

        // Track workbook for LRU-style memory control (ADR 0017). We use
        // data.length as an approximate size in bytes.
        const approxSizeBytes = data.length ?? 0;
        this._registerActiveWorkbook(handleId, workbook, approxSizeBytes);
        this._enforceWorkbookLimits();

        return workbook;
    }

    /**
     * Open a Parquet file - uses native WASM support (miniparquet)
     * Falls back to JavaScript handlers if native support unavailable
     * @private
     */
    async _openParquetFile(data, fileName, options = {}) {
        // First try native WASM support (miniparquet built into the module)
        try {
            const delimiter = options.delimiter ?? ',';
            const hasHeaderRow = options.hasHeaderRow ?? true;

            // Allocate memory in WASM heap for the data
            const dataPtr = this._module._malloc(data.length);
            const dataHeap = new Uint8Array(this._module.HEAPU8.buffer, dataPtr, data.length);
            dataHeap.set(data);

            // Allocate memory for filename string
            const fileNamePtr = this._allocateString(fileName);

            // Call WASM function
            const handleId = this._module._ff_openFile(
                dataPtr,
                data.length,
                fileNamePtr,
                FileFormat.Parquet,
                delimiter.charCodeAt(0),
                hasHeaderRow ? 1 : 0
            );

            // Free allocated memory
            this._module._free(dataPtr);
            this._module._free(fileNamePtr);

            if (handleId !== 0) {
                const workbook = new Workbook(this, handleId, fileName);
                this._handles.set(handleId, workbook);
                return workbook;
            }
            // If handleId is 0, native support failed - fall through to JS handlers
        } catch (e) {
            // Native support not available, fall through to JS handlers
        }

        // Fall back to parquet-wasm
        if (ParquetHandler && ParquetHandler.isAvailable()) {
            const workbook = await ParquetHandler.openFile(data, fileName);
            return this._wrapJsWorkbook(workbook, fileName);
        }

        // Fall back to DuckDB-WASM
        if (DuckDBHandler && DuckDBHandler.isAvailable()) {
            const workbook = await DuckDBHandler.openParquetFile(data, fileName);
            return this._wrapJsWorkbook(workbook, fileName);
        }

        throw new Error(
            'Parquet support requires parquet-wasm or @duckdb/duckdb-wasm. ' +
            'Install with: npm install parquet-wasm apache-arrow'
        );
    }

    /**
     * Open a DuckDB file - uses native WASM support (DuckDB amalgamation)
     * Falls back to JavaScript handlers if native support unavailable
     * @private
     */
    async _openDuckDBFile(data, fileName, options = {}) {
        // First try native WASM support (DuckDB amalgamation built into the module)
        try {
            const delimiter = options.delimiter ?? ',';
            const hasHeaderRow = options.hasHeaderRow ?? true;

            // Allocate memory in WASM heap for the data
            const dataPtr = this._module._malloc(data.length);
            const dataHeap = new Uint8Array(this._module.HEAPU8.buffer, dataPtr, data.length);
            dataHeap.set(data);

            // Allocate memory for filename string
            const fileNamePtr = this._allocateString(fileName);

            // Call WASM function
            const handleId = this._module._ff_openFile(
                dataPtr,
                data.length,
                fileNamePtr,
                FileFormat.DuckDb,
                delimiter.charCodeAt(0),
                hasHeaderRow ? 1 : 0
            );

            // Free allocated memory
            this._module._free(dataPtr);
            this._module._free(fileNamePtr);

            if (handleId !== 0) {
                const workbook = new Workbook(this, handleId, fileName);
                this._handles.set(handleId, workbook);
                return workbook;
            }
            // If handleId is 0, native support failed - fall through to JS handlers
        } catch (e) {
            // Native support not available, fall through to JS handlers
        }

        // Fall back to DuckDB-WASM JavaScript handler
        if (DuckDBHandler && DuckDBHandler.isAvailable()) {
            const workbook = await DuckDBHandler.openDuckDBFile(data, fileName);
            return this._wrapJsWorkbook(workbook, fileName);
        }

        throw new Error(
            'DuckDB support requires @duckdb/duckdb-wasm. ' +
            'Install with: npm install @duckdb/duckdb-wasm'
        );
    }

    /**
     * Wrap a JavaScript workbook in a compatible interface
     * @private
     */
    _wrapJsWorkbook(jsWorkbook, fileName) {
        // Generate a unique handle ID (negative to distinguish from WASM handles)
        const handleId = -(this._nextJsHandle || 1);
        this._nextJsHandle = (this._nextJsHandle || 1) + 1;

        // Create a wrapper that provides the same interface as Workbook
        const wrapper = new JsWorkbookWrapper(this, jsWorkbook, handleId, fileName);
        this._handles.set(handleId, wrapper);
        return wrapper;
    }

    /**
     * Open a file from a File object (browser) or Buffer (Node.js)
     * @param {File|Buffer} file - File to open
     * @param {Object} options - Optional settings
     * @returns {Promise<Workbook>} - Promise resolving to Workbook object
     */
    async openFileAsync(file, options = {}) {
        let data;
        let fileName;

        if (typeof File !== 'undefined' && file instanceof File) {
            // Browser File API
            fileName = file.name;
            const arrayBuffer = await file.arrayBuffer();
            data = new Uint8Array(arrayBuffer);
        } else if (typeof Buffer !== 'undefined' && Buffer.isBuffer(file)) {
            // Node.js Buffer
            fileName = options.fileName || 'unknown';
            data = new Uint8Array(file);
        } else if (file instanceof Uint8Array) {
            fileName = options.fileName || 'unknown';
            data = file;
        } else {
            throw new Error('Unsupported file type. Expected File, Buffer, or Uint8Array.');
        }

        const workbook = this.openFile(data, fileName, options);

        // Save minimal workbook metadata for the default session when
        // SessionManager is available (browser path).
        if (this._sessionManager && typeof indexedDB !== 'undefined') {
            try {
                const sessionId = options.sessionId || this._defaultSessionId;
                const workbookId = `${sessionId}:${fileName}`;
                const dbPath = `/db/session_${sessionId}.db`;
                const meta = {
                    fileName,
                    format: options.format ?? FileFormat.Auto,
                };
                await this._sessionManager.saveWorkbookMeta(
                    workbookId,
                    sessionId,
                    dbPath,
                    options,
                    meta
                );
            } catch (e) {
                // Metadata persistence is best-effort; ignore failures.
                if (console && console.warn) {
                    console.warn('Failed to persist workbook metadata', e);
                }
            }
        }

        return workbook;
    }

    /**
     * Close a workbook and release resources
     * @param {number} handleId - Handle ID of the workbook
     */
    closeFile(handleId) {
        if (this._handles.has(handleId)) {
            this._module._ff_closeFile(handleId);
            this._handles.delete(handleId);
            this._activeWorkbooks = this._activeWorkbooks.filter(
                entry => entry.handleId !== handleId
            );
        }
    }

    /**
     * Get the last error message
     * @returns {string} - Error message
     */
    getLastError() {
        const ptr = this._module._ff_getLastError();
        return this._module.UTF8ToString(ptr);
    }

    /**
     * Get the last JSON result
     * @returns {string} - JSON string
     */
    getLastJson() {
        const ptr = this._module._ff_getLastJson();
        return this._module.UTF8ToString(ptr);
    }

    _registerActiveWorkbook(handleId, workbook, approxSizeBytes) {
        const now = Date.now();
        this._activeWorkbooks.push({
            handleId,
            workbook,
            approxSizeBytes: approxSizeBytes || 0,
            lastUsedAt: now,
        });
    }

    _touchHandle(handleId) {
        const now = Date.now();
        const entry = this._activeWorkbooks.find(e => e.handleId === handleId);
        if (entry) {
            entry.lastUsedAt = now;
        }
    }

    _enforceWorkbookLimits() {
        const MAX_ACTIVE_WORKBOOKS = 4;
        const MAX_ACTIVE_BYTES = 256 * 1024 * 1024; // 256MB

        const totalBytes = this._activeWorkbooks.reduce(
            (sum, e) => sum + (e.approxSizeBytes || 0),
            0
        );

        if (
            this._activeWorkbooks.length <= MAX_ACTIVE_WORKBOOKS &&
            totalBytes <= MAX_ACTIVE_BYTES
        ) {
            return;
        }

        // Sort by lastUsedAt ascending (oldest first)
        this._activeWorkbooks.sort((a, b) => a.lastUsedAt - b.lastUsedAt);

        let bytes = totalBytes;
        while (
            (this._activeWorkbooks.length > MAX_ACTIVE_WORKBOOKS ||
                bytes > MAX_ACTIVE_BYTES) &&
            this._activeWorkbooks.length > 0
        ) {
            const victim = this._activeWorkbooks.shift();
            if (!victim) break;
            // Close workbook via existing API
            try {
                victim.workbook.close();
            } catch (e) {
                // ignore errors closing; best-effort
            }
            bytes -= victim.approxSizeBytes || 0;
        }
    }

    /**
     * Allocate a UTF-8 string in WASM memory
     * @private
     */
    _allocateString(str) {
        const encoded = new TextEncoder().encode(str + '\0');
        const ptr = this._module._malloc(encoded.length);
        const heap = new Uint8Array(this._module.HEAPU8.buffer, ptr, encoded.length);
        heap.set(encoded);
        return ptr;
    }

    /**
     * Clean up all resources
     */
    destroy() {
        for (const handleId of this._handles.keys()) {
            this.closeFile(handleId);
        }
        this._initialized = false;
    }
}

/**
 * Workbook class - represents an opened file
 */
class Workbook {
    constructor(loader, handleId, fileName) {
        this._loader = loader;
        this._handleId = handleId;
        this._fileName = fileName;
        this._closed = false;
    }

    /**
     * Get the file name
     */
    get fileName() {
        return this._fileName;
    }

    /**
     * Get the handle ID
     */
    get handleId() {
        return this._handleId;
    }

    /**
     * Check if workbook is closed
     */
    get isClosed() {
        return this._closed;
    }

    /**
     * List all datasets (sheets/tables) in the workbook
     * @returns {Object} - JSON object with datasets info
     */
    listDatasets() {
        this._checkClosed();
        this._loader._touchHandle(this._handleId);
        const ptr = this._loader._module._ff_listDatasets(this._handleId);
        const json = this._loader._module.UTF8ToString(ptr);
        const obj = JSON.parse(json || '{}');
        if (!obj || Object.keys(obj).length === 0) {
            const err = this._loader.getLastError();
            if (err) {
                throw new Error(`listDatasets failed: ${err}`);
            }
        }
        return obj;
    }

    /**
     * Describe a specific dataset
     * @param {string} name - Dataset name
     * @returns {Object} - JSON object with dataset description
     */
    describeDataset(name) {
        this._checkClosed();
        this._loader._touchHandle(this._handleId);
        const namePtr = this._loader._allocateString(name);
        const ptr = this._loader._module._ff_describeDataset(this._handleId, namePtr);
        this._loader._module._free(namePtr);
        const json = this._loader._module.UTF8ToString(ptr);
        const obj = JSON.parse(json || '{}');
        if (!obj || Object.keys(obj).length === 0) {
            const err = this._loader.getLastError();
            if (err) {
                throw new Error(`describeDataset failed: ${err}`);
            }
        }
        return obj;
    }

    /**
     * Execute a SQL query on the workbook
     * @param {string} sql - SQL query string
     * @returns {QueryResult} - Query result object
     */
    query(sql) {
        this._checkClosed();
        this._loader._touchHandle(this._handleId);
        const sqlPtr = this._loader._allocateString(sql);
        const ptr = this._loader._module._ff_query(this._handleId, sqlPtr);
        this._loader._module._free(sqlPtr);
        const json = this._loader._module.UTF8ToString(ptr);
        const obj = JSON.parse(json || '{}');
        if (!obj || Object.keys(obj).length === 0) {
            const err = this._loader.getLastError();
            if (err) {
                throw new Error(`query failed: ${err}`);
            }
        }
        return new QueryResult(obj);
    }

    /**
     * Profile a dataset for data quality analysis
     * @param {string} datasetName - Name of dataset to profile
     * @returns {Object} - Profile results
     */
    profileDataset(datasetName) {
        this._checkClosed();
        this._loader._touchHandle(this._handleId);
        const namePtr = this._loader._allocateString(datasetName);
        const ptr = this._loader._module._ff_profileDataset(this._handleId, namePtr);
        this._loader._module._free(namePtr);
        const json = this._loader._module.UTF8ToString(ptr);
        const obj = JSON.parse(json || '{}');
        if (!obj || Object.keys(obj).length === 0) {
            const err = this._loader.getLastError();
            if (err) {
                throw new Error(`profileDataset failed: ${err}`);
            }
        }
        return obj;
    }

    /**
     * Evaluate data quality rules against a dataset
     * @param {string} datasetName - Name of dataset
     * @param {Array} rules - Array of quality rule objects
     * @returns {Object} - Quality evaluation results
     */
    evaluateQualityRules(datasetName, rules) {
        this._checkClosed();
        this._loader._touchHandle(this._handleId);
        const namePtr = this._loader._allocateString(datasetName);
        const rulesJson = JSON.stringify(rules);
        const rulesPtr = this._loader._allocateString(rulesJson);

        const ptr = this._loader._module._ff_evaluateQualityRules(
            this._handleId, namePtr, rulesPtr
        );

        this._loader._module._free(namePtr);
        this._loader._module._free(rulesPtr);

        const json = this._loader._module.UTF8ToString(ptr);
        const obj = JSON.parse(json || '{}');
        if (!obj || Object.keys(obj).length === 0) {
            const err = this._loader.getLastError();
            if (err) {
                throw new Error(`evaluateQualityRules failed: ${err}`);
            }
        }
        return obj;
    }

    /**
     * Close the workbook and release resources
     */
    close() {
        if (!this._closed) {
            this._loader.closeFile(this._handleId);
            this._closed = true;
        }
    }

    /**
     * @private
     */
    _checkClosed() {
        if (this._closed) {
            throw new Error('Workbook is closed');
        }
    }
}

/**
 * JsWorkbookWrapper - Wrapper for JavaScript-based workbooks (Parquet, DuckDB)
 * Provides the same interface as Workbook for consistency.
 */
class JsWorkbookWrapper {
    constructor(loader, jsWorkbook, handleId, fileName) {
        this._loader = loader;
        this._jsWorkbook = jsWorkbook;
        this._handleId = handleId;
        this._fileName = fileName;
        this._closed = false;
    }

    get fileName() {
        return this._fileName;
    }

    get handleId() {
        return this._handleId;
    }

    get isClosed() {
        return this._closed;
    }

    /**
     * List all datasets
     */
    listDatasets() {
        this._checkClosed();
        return this._jsWorkbook.listDatasets();
    }

    /**
     * Describe a dataset
     */
    describeDataset(name) {
        this._checkClosed();
        const result = this._jsWorkbook.describeDataset(name);
        // Handle async result
        if (result && typeof result.then === 'function') {
            return result;
        }
        return result;
    }

    /**
     * Execute a SQL query
     */
    query(sql) {
        this._checkClosed();
        const result = this._jsWorkbook.query(sql);
        // Handle async result
        if (result && typeof result.then === 'function') {
            return result.then(data => new QueryResult(data));
        }
        return new QueryResult(result);
    }

    /**
     * Profile a dataset
     */
    profileDataset(datasetName) {
        this._checkClosed();
        const result = this._jsWorkbook.profileDataset(datasetName);
        if (result && typeof result.then === 'function') {
            return result;
        }
        return result;
    }

    /**
     * Evaluate quality rules (not supported for JS workbooks)
     */
    evaluateQualityRules(datasetName, rules) {
        throw new Error('evaluateQualityRules not supported for this file format');
    }

    /**
     * Close the workbook
     */
    close() {
        if (!this._closed) {
            const closeResult = this._jsWorkbook.close();
            // Handle async close
            if (closeResult && typeof closeResult.then === 'function') {
                closeResult.catch(() => {}); // Ignore close errors
            }
            this._loader._handles.delete(this._handleId);
            this._closed = true;
        }
    }

    _checkClosed() {
        if (this._closed) {
            throw new Error('Workbook is closed');
        }
    }
}

/**
 * QueryResult class - represents SQL query results
 */
class QueryResult {
    constructor(data) {
        this._data = data || {};

        // Normalise column representation: the core JSON uses an array of
        // objects with a "name" property; accept either that or a string array.
        const rawCols = this._data.columns || [];
        this._columns = rawCols.map(c => {
            if (typeof c === 'string') {
                return c;
            }
            if (c && typeof c.name === 'string') {
                return c.name;
            }
            return String(c ?? '');
        });

        this._rows = this._data.rows || [];

        // Runtime view name is exposed via meta.runtimeViewName (ADR 0005).
        if (this._data.meta && typeof this._data.meta.runtimeViewName === 'string') {
            this._viewName = this._data.meta.runtimeViewName;
        } else {
            this._viewName = null;
        }
    }

    /**
     * Get column names
     */
    get columns() {
        return this._columns;
    }

    /**
     * Get row data as array of arrays
     */
    get rows() {
        return this._rows;
    }

    /**
     * Get the runtime view name (for chained queries)
     */
    get viewName() {
        return this._viewName;
    }

    /**
     * Get row count
     */
    get rowCount() {
        return this._rows.length;
    }

    /**
     * Get column count
     */
    get columnCount() {
        return this._columns.length;
    }

    /**
     * Convert to array of objects
     * @returns {Array<Object>} - Array of row objects
     */
    toObjects() {
        return this._rows.map(row => {
            const obj = {};
            this._columns.forEach((col, i) => {
                obj[col] = row[i];
            });
            return obj;
        });
    }

    /**
     * Get a single column as array
     * @param {string|number} column - Column name or index
     * @returns {Array} - Column values
     */
    getColumn(column) {
        let idx;
        if (typeof column === 'string') {
            idx = this._columns.indexOf(column);
            if (idx === -1) {
                throw new Error(`Column not found: ${column}`);
            }
        } else {
            idx = column;
        }
        return this._rows.map(row => row[idx]);
    }

    /**
     * Convert to CSV string
     * @returns {string} - CSV formatted string
     */
    toCsv() {
        const escape = (val) => {
            if (val === null || val === undefined) return '';
            const str = String(val);
            if (str.includes(',') || str.includes('"') || str.includes('\n')) {
                return '"' + str.replace(/"/g, '""') + '"';
            }
            return str;
        };

        const header = this._columns.map(escape).join(',');
        const rows = this._rows.map(row => row.map(escape).join(','));
        return [header, ...rows].join('\n');
    }

    /**
     * Convert to JSON string
     * @returns {string} - JSON formatted string
     */
    toJson() {
        return JSON.stringify({
            columns: this._columns,
            rows: this._rows,
            viewName: this._viewName
        }, null, 2);
    }
}

/**
 * DataFrame class - pandas-like interface for query results
 */
class DataFrame {
    constructor(data, columns = null) {
        if (data instanceof QueryResult) {
            this._columns = [...data.columns];
            this._data = data.rows.map(row => [...row]);
        } else if (Array.isArray(data)) {
            if (columns) {
                this._columns = columns;
                this._data = data;
            } else if (data.length > 0 && typeof data[0] === 'object' && !Array.isArray(data[0])) {
                // Array of objects
                this._columns = Object.keys(data[0]);
                this._data = data.map(obj => this._columns.map(col => obj[col]));
            } else {
                this._columns = [];
                this._data = data;
            }
        } else {
            this._columns = [];
            this._data = [];
        }
    }

    /**
     * Get column names
     */
    get columns() {
        return [...this._columns];
    }

    /**
     * Get number of rows
     */
    get length() {
        return this._data.length;
    }

    /**
     * Get shape [rows, columns]
     */
    get shape() {
        return [this._data.length, this._columns.length];
    }

    /**
     * Get first n rows
     * @param {number} n - Number of rows
     * @returns {DataFrame}
     */
    head(n = 5) {
        return new DataFrame(this._data.slice(0, n), this._columns);
    }

    /**
     * Get last n rows
     * @param {number} n - Number of rows
     * @returns {DataFrame}
     */
    tail(n = 5) {
        return new DataFrame(this._data.slice(-n), this._columns);
    }

    /**
     * Select specific columns
     * @param {Array<string>} columns - Column names
     * @returns {DataFrame}
     */
    select(columns) {
        const indices = columns.map(col => {
            const idx = this._columns.indexOf(col);
            if (idx === -1) throw new Error(`Column not found: ${col}`);
            return idx;
        });
        const data = this._data.map(row => indices.map(i => row[i]));
        return new DataFrame(data, columns);
    }

    /**
     * Filter rows based on predicate
     * @param {Function} predicate - Function that receives row object, returns boolean
     * @returns {DataFrame}
     */
    filter(predicate) {
        const filtered = [];
        for (const row of this._data) {
            const obj = {};
            this._columns.forEach((col, i) => obj[col] = row[i]);
            if (predicate(obj)) {
                filtered.push(row);
            }
        }
        return new DataFrame(filtered, this._columns);
    }

    /**
     * Sort by column
     * @param {string} column - Column name
     * @param {boolean} ascending - Sort order
     * @returns {DataFrame}
     */
    sortBy(column, ascending = true) {
        const idx = this._columns.indexOf(column);
        if (idx === -1) throw new Error(`Column not found: ${column}`);

        const sorted = [...this._data].sort((a, b) => {
            const va = a[idx], vb = b[idx];
            if (va === vb) return 0;
            if (va === null) return 1;
            if (vb === null) return -1;
            const cmp = va < vb ? -1 : 1;
            return ascending ? cmp : -cmp;
        });
        return new DataFrame(sorted, this._columns);
    }

    /**
     * Group by column and aggregate
     * @param {string} column - Column to group by
     * @param {Object} aggs - Aggregation specs { column: 'sum'|'count'|'avg'|'min'|'max' }
     * @returns {DataFrame}
     */
    groupBy(column, aggs) {
        const idx = this._columns.indexOf(column);
        if (idx === -1) throw new Error(`Column not found: ${column}`);

        const groups = new Map();
        for (const row of this._data) {
            const key = row[idx];
            if (!groups.has(key)) {
                groups.set(key, []);
            }
            groups.get(key).push(row);
        }

        const newColumns = [column];
        const aggSpecs = [];
        for (const [aggCol, aggFunc] of Object.entries(aggs)) {
            const aggIdx = this._columns.indexOf(aggCol);
            if (aggIdx === -1) throw new Error(`Column not found: ${aggCol}`);
            newColumns.push(`${aggCol}_${aggFunc}`);
            aggSpecs.push({ idx: aggIdx, func: aggFunc });
        }

        const result = [];
        for (const [key, rows] of groups) {
            const newRow = [key];
            for (const { idx: aggIdx, func } of aggSpecs) {
                const values = rows.map(r => r[aggIdx]).filter(v => v !== null);
                let aggValue;
                switch (func) {
                    case 'count': aggValue = values.length; break;
                    case 'sum': aggValue = values.reduce((a, b) => a + b, 0); break;
                    case 'avg': aggValue = values.length ? values.reduce((a, b) => a + b, 0) / values.length : null; break;
                    case 'min': aggValue = values.length ? Math.min(...values) : null; break;
                    case 'max': aggValue = values.length ? Math.max(...values) : null; break;
                    default: throw new Error(`Unknown aggregation: ${func}`);
                }
                newRow.push(aggValue);
            }
            result.push(newRow);
        }

        return new DataFrame(result, newColumns);
    }

    /**
     * Convert to array of objects
     * @returns {Array<Object>}
     */
    toObjects() {
        return this._data.map(row => {
            const obj = {};
            this._columns.forEach((col, i) => obj[col] = row[i]);
            return obj;
        });
    }

    /**
     * Convert to array of arrays (including header)
     * @returns {Array<Array>}
     */
    toArray() {
        return [this._columns, ...this._data];
    }

    /**
     * Pretty print the DataFrame
     * @returns {string}
     */
    toString() {
        const widths = this._columns.map((col, i) => {
            const values = [col, ...this._data.map(row => String(row[i] ?? ''))];
            return Math.max(...values.map(v => v.length));
        });

        const sep = '+' + widths.map(w => '-'.repeat(w + 2)).join('+') + '+';
        const formatRow = row => '| ' + row.map((v, i) => String(v ?? '').padEnd(widths[i])).join(' | ') + ' |';

        const lines = [
            sep,
            formatRow(this._columns),
            sep,
            ...this._data.map(row => formatRow(row)),
            sep
        ];
        return lines.join('\n');
    }
}

// Module exports for different environments
const ExcelLoaderModule = {
    FileFormat,
    ExcelLoader,
    Workbook,
    JsWorkbookWrapper,
    QueryResult,
    DataFrame,

    // Heavy format handlers (may be null if not available)
    ParquetHandler,
    DuckDBHandler,

    /**
     * Check if Parquet support is available
     * Native support via miniparquet is always available in heavy-format builds
     * @returns {boolean}
     */
    isParquetAvailable() {
        // Native miniparquet support is built into WASM module
        // JS handlers are optional fallbacks
        return true;
    },

    /**
     * Check if DuckDB support is available
     * Native support via DuckDB amalgamation is always available in heavy-format builds
     * @returns {boolean}
     */
    isDuckDBAvailable() {
        // Native DuckDB amalgamation support is built into WASM module
        // JS handlers are optional fallbacks
        return true;
    },

    /**
     * Create and initialize an ExcelLoader instance
     * @param {Object} wasmModule - Emscripten module instance
     * @returns {ExcelLoader}
     */
    create(wasmModule) {
        const loader = new ExcelLoader(wasmModule);
        return loader.init();
    },

    /**
     * Open a workbook from a project manifest and a map of path -> File/Blob.
     * This helper expects a manifest object matching ADR 0019 and a fileMap
     * built from a folder selection (keys are relative paths such as
     * "subdir/file.csv" or plain file names).
     *
     * @param {ExcelLoader} loader
     * @param {Object} manifest
     * @param {Map|string[]|FileList|Object} fileMap
     * @returns {Promise<Workbook>}
     */
    async openProjectFromManifest(loader, manifest, fileMap) {
        if (!loader || typeof loader.openFileAsync !== 'function') {
            throw new Error('openProjectFromManifest: invalid loader');
        }

        const normalizePath = (p) => {
            if (!p) return '';
            let s = String(p).replace(/\\/g, '/');
            if (s.startsWith('./')) {
                s = s.slice(2);
            }
            while (s.startsWith('/')) {
                s = s.slice(1);
            }
            return s;
        };

        const buildFileMap = (input) => {
            if (input instanceof Map) {
                return input;
            }
            const map = new Map();

            if (typeof FileList !== 'undefined' && input instanceof FileList) {
                for (const f of input) {
                    const rel = f.webkitRelativePath || f.name;
                    map.set(normalizePath(rel), f);
                }
                return map;
            }

            if (Array.isArray(input)) {
                for (const entry of input) {
                    if (!entry) continue;
                    if (typeof entry === 'object' && entry.path && entry.file) {
                        map.set(normalizePath(entry.path), entry.file);
                    }
                }
                return map;
            }

            if (input && typeof input === 'object') {
                for (const [k, v] of Object.entries(input)) {
                    map.set(normalizePath(k), v);
                }
            }

            return map;
        };

        const mapFormatStringToEnum = (fmt) => {
            if (!fmt) return FileFormat.Auto;
            const lower = String(fmt).toLowerCase();
            if (lower === 'csv') return FileFormat.Csv;
            if (lower === 'tsv') return FileFormat.Tsv;
            if (lower === 'xlsx') return FileFormat.Xlsx;
            if (lower === 'xlsm') return FileFormat.Xlsm;
            if (lower === 'xltx') return FileFormat.Xltx;
            if (lower === 'xls') return FileFormat.Xls;
            if (lower === 'xlsb') return FileFormat.Xlsb;
            if (lower === 'ods') return FileFormat.Ods;
            if (lower === 'sqlite' || lower === 'db') return FileFormat.Sqlite;
            if (lower === 'dbf') return FileFormat.Dbf;
            if (lower === 'mdb') return FileFormat.Mdb;
            if (lower === 'accdb') return FileFormat.Accdb;
            if (lower === 'parquet' || lower === 'pq') return FileFormat.Parquet;
            if (lower === 'duckdb') return FileFormat.DuckDb;
            if (lower === 'jsonl' || lower === 'ndjson') return FileFormat.Jsonl;
            if (lower === 'json') return FileFormat.Json;
            if (lower === 'xml') return FileFormat.Xml;
            if (lower === 'html' || lower === 'htm') return FileFormat.Html;
            if (lower === 'txt' || lower === 'log') return FileFormat.Txt;
            return FileFormat.Auto;
        };

        const attachSource = async (workbook, src, file) => {
            let data;
            if (typeof File !== 'undefined' && file instanceof File) {
                const buf = await file.arrayBuffer();
                data = new Uint8Array(buf);
            } else if (typeof Blob !== 'undefined' && file instanceof Blob) {
                const buf = await file.arrayBuffer();
                data = new Uint8Array(buf);
            } else if (file instanceof Uint8Array) {
                data = file;
            } else {
                throw new Error('openProjectFromManifest: unsupported file type for ' + src.path);
            }

            const module = loader._module;
            const dataPtr = module._malloc(data.length);
            const heap = new Uint8Array(module.HEAPU8.buffer, dataPtr, data.length);
            heap.set(data);

            const name = src.path || (file && file.name) || 'attached';
            const namePtr = loader._allocateString(name);

            const fmtEnum = mapFormatStringToEnum(src.format);
            const delimChar = (src.delimiter && String(src.delimiter)[0]) || ',';
            const hasHeaderRow = src.hasHeaderRow !== false ? 1 : 0;

            const rc = module._ff_attachFile(
                workbook.handleId,
                dataPtr,
                data.length,
                namePtr,
                fmtEnum,
                delimChar.charCodeAt(0),
                hasHeaderRow
            );

            module._free(dataPtr);
            module._free(namePtr);

            if (rc !== 0) {
                const err = loader.getLastError();
                throw new Error(
                    `openProjectFromManifest: attach failed for "${name}": ` +
                    (err || 'unknown error')
                );
            }
        };

        const manifestObj = manifest || {};
        const sources = Array.isArray(manifestObj.sources) ? manifestObj.sources : [];
        const baseFile = manifestObj.baseFile || '';
        const basePathNorm = normalizePath(baseFile);
        if (!basePathNorm || !sources.length) {
            throw new Error('openProjectFromManifest: manifest missing baseFile or sources');
        }

        const srcByNormPath = new Map();
        for (const src of sources) {
            if (!src || !src.path) continue;
            srcByNormPath.set(normalizePath(src.path), src);
        }

        const baseSrc = srcByNormPath.get(basePathNorm);
        if (!baseSrc) {
            throw new Error(
                `openProjectFromManifest: baseFile "${baseFile}" not found in sources[]`
            );
        }

        const files = buildFileMap(fileMap);
        const baseFileObj = files.get(basePathNorm);
        if (!baseFileObj) {
            throw new Error(
                `openProjectFromManifest: file "${baseFile}" not found in provided fileMap`
            );
        }

        const errors = [];

        // Open base workbook.
        const baseFormatEnum = mapFormatStringToEnum(baseSrc.format);
        const baseDelimiter = (baseSrc.delimiter && String(baseSrc.delimiter)[0]) || ',';
        const baseHasHeader = baseSrc.hasHeaderRow !== false;

        const workbook = await loader.openFileAsync(baseFileObj, {
            fileName: baseSrc.path || baseFileObj.name,
            format: baseFormatEnum,
            delimiter: baseDelimiter,
            hasHeaderRow: baseHasHeader
        });

        try {
            // Attach remaining sources.
            for (const [normPath, src] of srcByNormPath.entries()) {
                if (normPath === basePathNorm)
                    continue;

                const f = files.get(normPath);
                if (!f) {
                    errors.push(
                        `openProjectFromManifest: file "${src.path}" not found in provided fileMap`
                    );
                    continue;
                }

                try {
                    await attachSource(workbook, src, f);
                } catch (e) {
                    errors.push(String(e.message || e));
                }
            }

            // Apply renames if present.
            const renames = Array.isArray(manifestObj.renames)
                ? manifestObj.renames
                : [];
            if (renames.length > 0) {
                const module = loader._module;
                for (const r of renames) {
                    if (!r || !r.from || !r.to)
                        continue;
                    const oldPtr = loader._allocateString(r.from);
                    const newPtr = loader._allocateString(r.to);
                    const rc = module._ff_renameDataset(workbook.handleId, oldPtr, newPtr);
                    module._free(oldPtr);
                    module._free(newPtr);
                    if (rc !== 0) {
                        const err = loader.getLastError();
                        errors.push(
                            `openProjectFromManifest: rename failed ` +
                            `for "${r.from}" -> "${r.to}": ${err || 'unknown error'}`
                        );
                    }
                }
            }
        } catch (e) {
            // Unexpected error; ensure workbook is closed.
            try {
                workbook.close();
            } catch (_) {
                // ignore close errors
            }
            throw e;
        }

        if (errors.length > 0) {
            try {
                workbook.close();
            } catch (_) {
                // ignore
            }
            throw new Error(errors.join('\n'));
        }

        return workbook;
    }
};

// Export for different module systems
if (typeof module !== 'undefined' && module.exports) {
    // Node.js / CommonJS
    module.exports = ExcelLoaderModule;
} else if (typeof define === 'function' && define.amd) {
    // AMD
    define([], function() { return ExcelLoaderModule; });
} else if (typeof window !== 'undefined') {
    // Browser global
    window.ExcelLoaderModule = ExcelLoaderModule;
}
