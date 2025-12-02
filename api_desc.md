# wasm-sqlite-database
 

Эта сборка WASM для `excel_loader` добавляет новые API и флаги CLI, связанные с:

- Работой с **несколькими файлами внутри одного workbook** (multi‑file `EngineContext`).
- Просмотром **источников датасетов** (из какого файла/объекта пришёл каждый датасет).
- **Переименованием** датасетов и **отключением** (detach) источников во время работы.
- Получением приблизительных **статистик по памяти** для всего workbook и для отдельных датасетов.

Ниже приведён краткий обзор каждой возможности с примерами вызова из:

- Низкоуровневого C/WASM ABI (`ff_*`‑функции).
- Высокоуровневой JS‑обёртки (`ExcelLoaderEngine`).
- Нативного CLI (`excel_loader_cli`).

---

## 1. Многофайловые workbook (Attach)

Вы можете прикреплять дополнительные файлы к уже открытому workbook (одна общая встроенная база SQLite).

### 1.1 C ABI (WASM)

```c
// Прикрепить дополнительный файл к существующему workbook.
// Возвращает 0 при успехе, ненулевое значение при ошибке.
FF_EXPORT int ff_attachFile(FfHandleId handle,
                            const uint8_t* data,
                            uint32_t size,
                            const char* fileName,
                            int format,      // enum SeFileFormat; 0 = Auto
                            char delimiter,  // например ',' или '\t'
                            int hasHeaderRow);
```

**Пример (низкоуровневый JS):**

```js
// Предполагается, что Module — это модуль Emscripten, а handleId получен из ff_openFile
async function attachFile(handleId, file) {
    const fileName = file.name;
    const buf = new Uint8Array(await file.arrayBuffer());

    const dataPtr = Module._malloc(buf.length);
    new Uint8Array(Module.HEAPU8.buffer, dataPtr, buf.length).set(buf);

    const namePtr = loader._allocateString(fileName); // из ExcelLoader

    const rc = Module._ff_attachFile(
        handleId,
        dataPtr,
        buf.length,
        namePtr,
        /*format*/ 0,               // Auto
        ','.charCodeAt(0),
        1                            // hasHeaderRow = true
    );

    Module._free(dataPtr);
    Module._free(namePtr);

    if (rc !== 0) {
        const err = loader.getLastError();
        throw new Error(`Attach failed: ${err}`);
    }
}
```

### 1.2 Высокоуровневый JS (`ExcelLoaderEngine`)

```js
import { ExcelLoaderEngine } from '../js/excel_loader_engine.js';

const engine = new ExcelLoaderEngine(Module);

// Основной файл
const handleId = await engine.openFileFromBuffer(primaryBytes, {
    fileName: 'orders_2024.csv'
});

// Прикрепить другой файл
const ok = await engine.attachFileToHandle(handleId, customersBytes, {
    fileName: 'customers.csv'
});
if (!ok) {
    const err = await engine.getLastError();
    console.error('attachFileToHandle failed:', err);
}
```

### 1.3 CLI

```bash
excel_loader_cli \
  --file data/orders_2024.csv \
  --attach-file data/customers.csv \
  --list-datasets
```

---

## 2. Метаданные источников датасетов

Каждый датасет теперь имеет метаданные:

- `technicalName` – техническое SQL‑имя, используемое в запросах.
- `sourceFilePath` – путь к файлу / имя загруженного файла.
- `sourceObjectName` – имя листа/диапазона/таблицы внутри файла.

### 2.1 C ABI / WASM

```c
// JSON: {"datasets":[{technicalName,sourceFilePath,sourceObjectName},...]}
FF_EXPORT const char* ff_listDatasetSources(FfHandleId handle);

// JSON: {"paths":["/path/to/file1","/path/to/file2",...]}
FF_EXPORT const char* ff_getWorkbookSourcePaths(FfHandleId handle);
```

**Пример (низкоуровневый JS):**

```js
const ptrSrc = Module._ff_listDatasetSources(handleId);
const srcJson = Module.UTF8ToString(ptrSrc);
const srcInfo = JSON.parse(srcJson || '{}');

const ptrPaths = Module._ff_getWorkbookSourcePaths(handleId);
const pathsJson = Module.UTF8ToString(ptrPaths);
const pathsInfo = JSON.parse(pathsJson || '{}');
```

### 2.2 Высокоуровневый JS

```js
const { datasets } = await engine.listDatasetSources(handleId);
// datasets: [{ technicalName, sourceFilePath, sourceObjectName }, ...]

const { paths } = await engine.getWorkbookSourcePaths(handleId);
// paths: [ "orders_2024.csv", "customers.csv", ... ]
```

---

## 3. Переименование датасетов и отключение источников

### 3.1 Переименование датасета

```c
// Возвращает 0 при успехе, ненулевое значение при ошибке.
FF_EXPORT int ff_renameDataset(FfHandleId handle,
                               const char* oldName,
                               const char* newName);
```

**Пример (низкоуровневый JS):**

```js
const oldPtr = loader._allocateString('orders_2024.csv');
const newPtr = loader._allocateString('orders');
const rc = Module._ff_renameDataset(handleId, oldPtr, newPtr);
Module._free(oldPtr);
Module._free(newPtr);
if (rc !== 0) throw new Error(await engine.getLastError());
```

**Высокоуровневый пример:**

```js
const ok = await engine.renameDataset(handleId, 'orders_2024.csv', 'orders');
```

**CLI:**

```bash
excel_loader_cli \
  --file data/orders_2024.csv \
  --rename-dataset "orders_2024.csv=orders" \
  --list-datasets
```

### 3.2 Отключение источника

```c
// Возвращает 0 при успехе, ненулевое значение при ошибке.
FF_EXPORT int ff_detachSource(FfHandleId handle,
                              const char* sourceFilePath);
```

**Высокоуровневый JS:**

```js
const ok = await engine.detachSource(handleId, 'customers.csv');
```

**CLI:**

```bash
excel_loader_cli \
  --file data/orders_2024.csv \
  --attach-file data/customers.csv \
  --detach-file data/customers.csv \
  --list-datasets
```

---

## 4. Статистика по памяти (workbook и датасеты)

Это **приблизительные** оценки, предназначенные для диагностики и решений LRU (ADR 0017).

### 4.1 C ABI / WASM

```c
// JSON:
// {"approxDbBytes":...,
//  "approxFileBufferBytes":...,
//  "approxTotalBytes":...,
//  "sources":[{sourceFilePath,sourceObjectName,approxBytes},...]}
FF_EXPORT const char* ff_getWorkbookMemoryStats(FfHandleId handle);

// JSON:
// {"datasets":[{technicalName,sourceFilePath,sourceObjectName,approxBytes},...]}
FF_EXPORT const char* ff_listDatasetMemoryStats(FfHandleId handle);
```

**Пример (JS):**

```js
const wmPtr = Module._ff_getWorkbookMemoryStats(handleId);
const wmJson = Module.UTF8ToString(wmPtr);
const workbookStats = JSON.parse(wmJson || '{}');

const dmPtr = Module._ff_listDatasetMemoryStats(handleId);
const dmJson = Module.UTF8ToString(dmPtr);
const datasetStats = JSON.parse(dmJson || '{}');
```

### 4.2 Высокоуровневый JS

```js
const workbookStats = await engine.getWorkbookMemoryStats(handleId);
// { approxDbBytes, approxFileBufferBytes, approxTotalBytes, sources: [...] }

const datasetStats = await engine.listDatasetMemoryStats(handleId);
// { datasets: [ { technicalName, sourceFilePath, sourceObjectName, approxBytes }, ... ] }
```

### 4.3 CLI

```bash
excel_loader_cli \
  --file data/sample.csv \
  --print-memory-stats
```

---

## 5. Фильтры Excel‑объектов (листы)

`SeOpenOptions` теперь включает:

```c++
enum class SeExcelObjectKind {
    Any,
    Sheet,
    NamedRange,
    Table
};

struct SeOpenOptions {
    // ...
    SeExcelObjectKind excelObjectKind = SeExcelObjectKind::Any;
    std::vector<std::string> excelObjectNames;
};
```

В этом релизе:

- Фильтры применяются к **листам** в XLSX/XLS/ODS.
- `excelObjectKind = Sheet` или `Any` работают ожидаемо.
- `excelObjectNames` ограничивает набор листов по именам, например `"Sheet1"`, `"Sheet2"`.

**Пример (C++ / native или WASM):**

```c++
SeOpenOptions opts;
opts.fileName = "workbook.xlsx";
opts.excelObjectKind = SeExcelObjectKind::Sheet;
opts.excelObjectNames = { "Sheet1", "Sheet2" };

SeWorkbookHandle handle = se_openWorkbook(bytes, size, opts);
```

---

## 6. HTML‑страница для тестов (браузер)

Файл `wasm/excel_loader.html` обновлён для работы с новыми API:

- Открытие **нескольких файлов** и автоматическое прикрепление дополнительных файлов в один workbook.
- Прикрепление файлов через кнопку «Attach File(s) to Workbook».
- Просмотр источников датасетов (`ff_listDatasetSources`) и путей к источникам (`ff_getWorkbookSourcePaths`).
- Переименование датасета и отключение источника через UI.
- Просмотр статистики памяти по workbook (`ff_getWorkbookMemoryStats`) и по датасетам (`ff_listDatasetMemoryStats`).

Запуск из каталога сборки WASM:

```bash
cd build_wasm
python3 -m http.server 8080
# Откройте http://localhost:8080/excel_loader.html
```

Далее вы можете перетаскивать файлы, выполнять запросы, прикреплять дополнительные файлы и смотреть новые метаданные и статистику по памяти прямо в браузере.

---

## 7. Проектные манифесты (ADR 0019)

Этот релиз добавляет поддержку **проектных манифестов** – небольших JSON‑файлов,
описывающих многофайловый проект (folder project). Манифест позволяет задать:

- Базовый файл (`baseFile`), который станет основным источником workbook.
- Дополнительные файлы (`sources[]`), прикрепляемые к тому же workbook.
- Необязательные переименования датасетов (`renames[]`) для удобных имён.

### 7.1 Структура манифеста (JSON)

Минимальный пример:

```json
{
  "schemaVersion": 1,
  "projectName": "sales_2024",

  "baseFile": "orders.csv",

  "sources": [
    {
      "path": "orders.csv",
      "format": "csv",
      "delimiter": ",",
      "hasHeaderRow": true
    },
    {
      "path": "customers.parquet",
      "format": "parquet"
    }
  ],

  "renames": [
    { "from": "orders__orders_csv", "to": "orders" }
  ]
}
```

Поля:

- `schemaVersion`: целое число, сейчас `1`.
- `projectName`: опциональное имя проекта (для UI/логов).
- `baseFile`: строка; должен совпадать с одним из `sources[].path`.
- `sources[]`:
  - `path`: относительный путь (CLI: относительно манифеста; браузер: относительно выбранной папки).
  - `format`: строка, соответствующая `SeFileFormat` (`"csv"`, `"tsv"`, `"xlsx"`, `"jsonl"`, `"parquet"`, и т.п.; `"auto"` допустим).
  - `delimiter`: опционально; по умолчанию `","` (CSV) или `"\t"` (TSV).
  - `hasHeaderRow`: логическое значение, по умолчанию `true`.
  - `excel.kind` и `excel.names` – фильтры для Excel/ODS (опционально).
- `renames[]`: опционально; элементы вида `{ "from": "<defaultName>", "to": "<technicalName>" }`.

### 7.2 CLI – импорт манифеста (`--project-config`)

Многофайловый проект можно открыть по манифесту вместо перечисления множества
опций `--attach-file`:

```bash
excel_loader_cli \
  --project-config path/to/project.json \
  --list-datasets
```

Поведение:

- Читает и валидирует JSON‑манифест.
- Разрешает `sources[].path` относительно каталога манифеста.
- Открывает `baseFile` через `se_openWorkbook`, затем прикрепляет остальные файлы через `se_attachFile`.
- Применяет `renames[]` через `se_renameDataset`.
- Печатает подробные диагностические сообщения с префиксом `project-config:` при:
  - ошибке формата манифеста;
  - отсутствии `baseFile` или его отсутствии в `sources`;
  - отсутствии файлов на диске;
  - ошибках движка при открытии/attach/rename.

### 7.3 CLI – экспорт манифеста (`--export-project-config`)

Можно также **экспортировать** манифест, описывающий текущий workbook, с учётом
подключённых файлов и переименований:

```bash
excel_loader_cli \
  --file data/sample.csv \
  --attach-file data/extra.csv \
  --rename-dataset "sample.csv=orders" \
  --export-project-config project.json
```

Для этого используется `se_exportProjectManifest`, который создаёт JSON‑манифест:

- `schemaVersion: 1`;
- `projectName`: по имени базового файла (или задано явно в будущем API);
- `baseFile`: основной файл;
- `sources[]`: по одному элементу на каждый подключённый файл (с сохранёнными `SeOpenOptions`);
- `renames[]`: для датасетов, чьи текущие имена отличаются от канонических (основанных на файле и внутреннем объекте).

### 7.4 WASM / JS – экспорт манифеста

WASM C ABI предоставляет:

```c
// Возвращает строку JSON‑манифеста при успехе; "{}" при ошибке.
FF_EXPORT const char* ff_exportProjectManifest(FfHandleId handleId,
                                               const char* projectName);
```

Высокоуровневая JS‑обёртка:

```js
// excel_loader_engine.js
const manifest = await engine.exportProjectManifest(handleId, {
    projectName: 'sales_2024'
});
console.log(JSON.stringify(manifest, null, 2));
```

### 7.5 JS‑хелпер: открытие проекта по манифесту

Модуль `wasm/excel_loader.js` содержит helper:

```js
ExcelLoaderModule.openProjectFromManifest(loader, manifest, fileMap);
```

Где `fileMap` может быть:

- `Map<string, File|Blob|Uint8Array>`;
- `FileList` из `<input type="file" webkitdirectory>` (используется `webkitRelativePath` или `name`);
- объект вида `{ [relativePath]: File }` или массив `{ path, file }`.

Helper:

- Нормализует пути и находит базовый файл (`baseFile`).
- Открывает базовый файл через `loader.openFileAsync`.
- Прикрепляет остальные источники через `_ff_attachFile`.
- Применяет `renames[]` через `_ff_renameDataset`.
- Агрегирует ошибки (отсутствующие файлы, ошибки attach/rename); при наличии ошибок закрывает workbook и выбрасывает исключение (строгий режим).

### 7.6 HTML‑страница – раздел Projects

`wasm/excel_loader.html` содержит раздел «Project Manifests (ADR 0019)»:

- **Manifest JSON**:
  - `#project-manifest-input` – выбор `project.json`.
  - Распарсенный манифест отображается в `#project-info`.
- **Project Folder**:
  - `#project-folder-input` с `webkitdirectory` – выбор папки с файлами.
  - Файлы маппятся по `webkitRelativePath` во внутреннюю `fileMap`.
- **Open Project**:
  - Кнопка `#btn-open-project` вызывает:

    ```js
    ExcelLoaderModule.openProjectFromManifest(loader,
                                              currentProjectManifest,
                                              currentProjectFiles);
    ```

  - При успехе полученный `Workbook` становится активным; датасеты перечисляются и доступны все обычные операции (запросы, профилирование).

Таким образом обеспечивается полный цикл работы с манифестами:

- Манифесты можно описывать вручную или экспортировать через CLI.
- Их можно использовать в нативной и браузерной средах для стабильного открытия и attach наборов файлов.

### 7.7 Нативный C++ – экспорт манифеста

Хотя этот README описывает в первую очередь WASM‑сборку, базовый C++‑API тоже
предоставляет helper для экспорта манифеста:

```c++
#include "se_api.hxx"

void export_project_example(SeWorkbookHandle handle)
{
    SeProjectManifestExportOptions opts;
    opts.projectName = "my_project";
    opts.includeRenames = true;
    opts.projectRoot = "/absolute/path/to/project/root";

    std::string json;
    if (!se_exportProjectManifest(handle, opts, json)) {
        const char* err = se_getLastError();
        // Обработка ошибки (логирование или исключение)
        return;
    }

    // json now contains a manifest as described above.
    // You can write it to disk as project.json for later use.
}
```

Полученный JSON‑манифест можно использовать в:

- `excel_loader_cli --project-config project.json ...`;
- `ff_exportProjectManifest` / `ExcelLoaderEngine.exportProjectManifest` (WASM);
- `ExcelLoaderModule.openProjectFromManifest` в браузере.

---

## 8. Обзор JavaScript‑API (ExcelLoaderEngine и ExcelLoaderModule)

В этом разделе кратко описаны основные JS‑API, доступные в данном релизе,
включая как существующие, так и новые методы.

### 8.1 Низкоуровневая обёртка (`ExcelLoaderEngine`)

Импорт и создание:

```js
import { ExcelLoaderEngine } from './js/excel_loader_engine.js';

// Module — это модуль Emscripten из excel_loader_wasm.js
const engine = new ExcelLoaderEngine(Module);
```

Ключевые методы:

- **`openFileFromBuffer(data, options)`** → `handleId`

  ```js
  const bytes = new Uint8Array(await file.arrayBuffer());
  const handleId = await engine.openFileFromBuffer(bytes, {
      fileName: file.name,
      format: 0,          // SeFileFormat::Auto
      delimiter: ',',
      hasHeaderRow: true
  });
  ```

- **`attachFileToHandle(handleId, data, options)`** → `boolean`

  ```js
  const moreBytes = new Uint8Array(await otherFile.arrayBuffer());
  const ok = await engine.attachFileToHandle(handleId, moreBytes, {
      fileName: otherFile.name
  });
  ```

- **`listDatasets(handleId)`** → `{ sheets: [...] }`

  ```js
  const { sheets } = await engine.listDatasets(handleId);
  sheets.forEach(s => console.log(s.name, s.rowCount, s.columnCount));
  ```

- **`describeDataset(handleId, name)`** → `{ sheets: [desc] }`

  ```js
  const info = await engine.describeDataset(handleId, 'Sheet1');
  console.log(info);
  ```

- **`query(handleId, sql)`** → `{ columns, rows, meta? }`

  ```js
  const res = await engine.query(handleId, 'SELECT * FROM "Sheet1" LIMIT 10');
  console.log(res.columns, res.rows);
  ```

- **`profileDataset(handleId, datasetName)`** → `{ columns: [...] }`

  ```js
  const profile = await engine.profileDataset(handleId, 'Sheet1');
  console.log(profile.columns);
  ```

- **`evaluateQualityRules(handleId, datasetName, rules)`** → `{ rules: [...] }`

  ```js
  const rules = [
      { id: 'id_not_null', description: 'id must be present', sqlCondition: 'id IS NOT NULL' }
  ];
  const summary = await engine.evaluateQualityRules(handleId, 'Sheet1', rules);
  console.log(summary.rules);
  ```

- **Метаданные источников**:

  ```js
  const { datasets } = await engine.listDatasetSources(handleId);
  // [{ technicalName, sourceFilePath, sourceObjectName }, ...]

  const { paths } = await engine.getWorkbookSourcePaths(handleId);
  // ["orders.csv", "customers.csv", ...]
  ```

- **Переименование и отключение**:

  ```js
  await engine.renameDataset(handleId, 'orders_2024.csv', 'orders');
  await engine.detachSource(handleId, 'customers.csv');
  ```

- **Статистика по памяти**:

  ```js
  const wbStats = await engine.getWorkbookMemoryStats(handleId);
  const dsStats = await engine.listDatasetMemoryStats(handleId);
  console.log(wbStats.approxTotalBytes, dsStats.datasets.length);
  ```

- **Экспорт манифеста**:

  ```js
  const manifest = await engine.exportProjectManifest(handleId, {
      projectName: 'sales_2024'
  });
  console.log(JSON.stringify(manifest, null, 2));
  ```

- **`getLastError()`** – возвращает последнюю строку ошибки движка.

  ```js
  const err = await engine.getLastError();
  if (err) console.error('Engine error:', err);
  ```

### 8.2 Высокоуровневый Loader и Workbook (`ExcelLoaderModule`)

`wasm/excel_loader.js` экспортирует более высокоуровневый API:

```js
// В браузере:
//   window.ExcelLoaderModule
// В Node / бандлере:
//   const ExcelLoaderModule = require('./wasm/excel_loader.js');
```

Создание loader и открытие workbook:

```js
// Module — модуль Emscripten из excel_loader_wasm.js
const loader = ExcelLoaderModule.create(Module);

// Browser File
const file = /* из <input type="file"> */;
const workbook = await loader.openFileAsync(file);
```

Также можно открыть из `Uint8Array`:

```js
const bytes = new Uint8Array(await file.arrayBuffer());
const wb2 = loader.openFile(bytes, file.name, {
    format: ExcelLoaderModule.FileFormat.Auto,
    delimiter: ',',
    hasHeaderRow: true
});
```

Закрыть workbook и освободить handle:

```js
workbook.close();           // предпочтительный путь
// или, если есть только handleId:
loader.closeFile(workbook.handleId);
```

Сбросить loader и закрыть все workbook:

```js
loader.destroy();
```

Методы `Workbook` (браузер/Node):

- **`workbook.listDatasets()`** → `{ sheets: [...] }`  
- **`workbook.describeDataset(name)`** → описание датасета.  
- **`workbook.query(sql)`** → обёртка `QueryResult`:

  ```js
  const res = workbook.query('SELECT * FROM "Sheet1" LIMIT 10');
  console.log(res.columns, res.rows);
  console.log(res.toObjects());   // массив объектов‑строк
  console.log(res.toCsv());       // CSV‑строка
  ```

- **`workbook.profileDataset(name)`** → объект профиля.  
- **`workbook.evaluateQualityRules(name, rules)`** → объект с результатами (нативные workbook; для JS‑Parquet/DuckDB не поддерживается).  
- **`workbook.close()`** → закрывает workbook и освобождает ресурсы.

Дополнительные helper‑ы:

- **`ExcelLoaderModule.FileFormat`** – enum, соответствующий `SeFileFormat`.  
- **`ExcelLoaderModule.isParquetAvailable()`** / `isDuckDBAvailable()` – проверки возможностей.  
- **`ExcelLoaderModule.openProjectFromManifest(loader, manifest, fileMap)`** – helper для импорта проекта по манифесту (см. раздел 7.5).  
- **`loader.getLastError()` / `loader.getLastJson()`** – последние ошибка и JSON‑ответ движка (для низкоуровневой отладки и логирования).  
- **`QueryResult`** – обёртка, возвращаемая `workbook.query(sql)`:

  ```js
  const res = workbook.query('SELECT * FROM "Sheet1" LIMIT 10');
  console.log(res.columns);          // имена колонок
  console.log(res.rows);             // массив строк [..]
  console.log(res.viewName);         // имя runtime‑представления или null
  console.log(res.toObjects());      // [{col: val, ...}, ...]
  console.log(res.getColumn('id'));  // значения одной колонки
  console.log(res.toCsv());          // CSV‑строка
  console.log(res.toJson());         // JSON‑строка с columns/rows
  ```

- **`DataFrame`** – «pandas‑подобная» обёртка для результатов запросов:

  ```js
  const res = workbook.query('SELECT id, country, age FROM "Sheet1"');
  const df = new DataFrame(res);

  console.log(df.columns);     // имена колонок
  console.log(df.shape);       // [строк, колонок]
  console.log(df.head(5));     // первые 5 строк
  console.log(df.tail(5));     // последние 5 строк

  const adults = df.filter(row => row.age >= 18);
  const sorted = adults.sortBy('age', false);  // по убыванию
  const grouped = df.groupBy('country', { age: 'avg' });

  console.log(grouped.toArray());  // [[cols...], [row...], ...]
  console.log(grouped.toString()); // форматированная таблица
  ```

Workbook, основанные на JS‑движке (Parquet/DuckDB), заворачиваются в
`JsWorkbookWrapper`, который предоставляет тот же интерфейс, что и `Workbook`,
для методов `listDatasets`, `describeDataset`, `query`, `profileDataset`,
`close()` (но может не поддерживать `evaluateQualityRules`).
