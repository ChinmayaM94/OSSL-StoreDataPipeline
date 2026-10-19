# OSSL Store Data Pipeline

Spring Boot 3.4 / Java 17 service that moves OSSL store data through a
pipeline: **CSV upload → Kafka → PostgreSQL + MongoDB → Caffeine cache**.
It started as Jira **ACIP-624075** (CSV → Kafka ingestion) and was extended
to cover the Confluence LLD "OSSL Store Data Pipeline" (space **CMS**).

## Architecture overview

```
 POST /api/stores/import (multipart CSV)          POST /api/stores/{storeId}/publish
            │                                                  │
   validate each row, build                       read store from PostgreSQL,
   StoreRecord (version = SQL + 1, or 0)          version = version + 1, save to SQL
            │                                                  │
            └──────────────────────┬───────────────────────────┘
                                   ▼
                  Kafka topic: ossl-store-events   (key = locationId)
                                   │
              ┌────────────────────┴─────────────────────┐
              │ two independent consumer groups           │
              ▼                                           ▼
   StoreEventConsumerService                    StoreEventConsumer
   group: ossl-store-sql-group                  group: ossl-store-mongo-group
   upsert into PostgreSQL (STORES,              apply to MongoDB only if incoming
   STORE_ADDRESS)                               version > stored version
                                                  │ then: cache put + change event
                                                  ▼
                                   Kafka topic: ossl-store-change-events
                                                  │
                                                  ▼
                                   ChangeEventConsumer
                                   group: ossl-store-pipeline-group
                                   evict + reload the Caffeine cache entry

   Failed messages (after retries) ──► <topic>.DLQ   (e.g. ossl-store-events.DLQ)
   POST /api/stores/dlq/replay  ──► republishes the DLQ back to ossl-store-events

   GET /api/stores/{storeId}/sql      -> read from PostgreSQL
   GET /api/stores/{storeId}/mongo    -> read from MongoDB
   GET /api/stores/{storeId}/cache    -> cache-aside: Caffeine, miss -> load from Mongo
   GET /api/stores/{storeId}/compare  -> field-by-field SQL vs Mongo diff
```

Key points about the flow:

- **The import endpoint does not write to SQL.** It only validates rows and
  publishes one Kafka message per valid row. SQL and MongoDB are both filled
  by Kafka consumers, so a CSV import is **asynchronous**: data appears in
  SQL/Mongo shortly after the response returns.
- **`/publish` uses the same topic as the import** (`ossl-store-events`). It
  bumps the version in SQL, then publishes the versioned record. Both
  consumers receive it again; Mongo only accepts it because the version is
  higher.
- **SQL and Mongo consumers are separate consumer groups**, so a failure in
  one does not block the other.

**Layers**: Controller → Service → {SQL repo, Mongo repo} → Kafka
producers/consumers → Cache. Controllers are thin; services own validation,
orchestration and Kafka/cache interaction.

## Tech stack

| Area | Technology |
|---|---|
| Language / build | Java 17, Maven, Spring Boot 3.4.12 |
| REST + validation | spring-boot-starter-web, spring-boot-starter-validation |
| SQL | Spring Data JPA / Hibernate, **PostgreSQL** |
| NoSQL | Spring Data MongoDB |
| Messaging | Apache Kafka via spring-kafka (JSON serialization) |
| Cache | Caffeine (max 2000 entries, 10 min expire-after-write) |
| CSV | Apache Commons CSV |
| API docs | springdoc-openapi (Swagger UI) |
| Web UI | React + Vite (`ossl-store-ui/`), plus a plain HTML console served by Spring |
| Boilerplate | Lombok |
| Local infra | Docker Compose (Kafka + MongoDB) |
| Tests | JUnit 5, Mockito, spring-boot-starter-test, spring-kafka-test |

## Endpoints

All endpoints are under `/api/stores`.

### 1. `POST /api/stores/import` — CSV import

Uploads a CSV (multipart field `file`, max 10 MB). The header must contain all
columns listed in [CSV schema](#csv-schema). Each row is validated; every
valid row is published as one JSON message to `ossl-store-events`, keyed by
`locationId`. New stores are sent with `version = 0`; a store already in
SQL is sent with its current version + 1, so a re-import is accepted by the
Mongo consumer and never lowers the SQL version. A row counts as **processed** once its Kafka
publish succeeds.

Bad rows never abort the import. They are reported in `errors`. Row-level
checks:
- column count matches the header (`field: "csv"`)
- `locationId` is present (`field: "locationId"`)
- `locationId` is not duplicated within the same file (`field: "locationId"`)
- Kafka publish succeeded (`field: "kafka"`)

```bash
curl -X POST http://localhost:8080/api/stores/import \
  -F "file=@sample-data/stores_export.csv"
```

```json
{
  "totalRows": 3,
  "processedCount": 2,
  "failureCount": 1,
  "errors": [
    { "rowNumber": 4, "field": "locationId", "reason": "Duplicate locationId: 1794" }
  ]
}
```

Request-level failures return HTTP 400: missing/empty file, file not a CSV,
file over 10 MB, missing required header columns, or an unparseable CSV.

### 2. `POST /api/stores/{storeId}/publish` — Publish a store

Reads the store from **PostgreSQL**, increments its version, saves the new
version back to SQL, and publishes the versioned `StoreRecord` to
`ossl-store-events`. Returns the published record. `404` if the store is not
in SQL.

```bash
curl -X POST http://localhost:8080/api/stores/1794/publish
```

### 3. `GET /api/stores/{storeId}/sql`
Reads the store directly from PostgreSQL. `404` if not found.

### 4. `GET /api/stores/{storeId}/mongo`
Reads the store directly from MongoDB (the copy written by `StoreEventConsumer`). `404` if not found.

### 5. `GET /api/stores/{storeId}/cache`
Cache-aside read: returns the Caffeine entry if present; otherwise loads from
MongoDB, populates the cache and returns it. `404` if not in cache or Mongo.

### 6. `GET /api/stores/{storeId}/compare`
Flattens the SQL and Mongo copies (including `version`) and returns the
differing fields. `404` only if the store is in neither database.

```json
{
  "storeId": "1794",
  "sqlValues": { "address.city": "Folsom", "version": 2 },
  "mongoValues": { "address.city": "Folsom", "version": 1 },
  "mismatchedFields": ["version"],
  "consistent": false
}
```

`consistent: false` is expected briefly while the async flow converges.
Missing copies show up as `__missing_in_sql__` / `__missing_in_mongo__`.

### 7. `POST /api/stores/dlq/replay` — Replay the dead-letter topic

Reads every message currently in `ossl-store-events.DLQ` and publishes it back
to `ossl-store-events` with its original key, so the SQL and Mongo consumers
retry it. Call it after fixing the cause (for example, MongoDB is back up).
DLQ offsets are committed only after a message is republished, so a failed
replay can be run again. Replaying is safe because both consumers are
idempotent. A message that still fails is retried and dead-lettered again.

```bash
curl -X POST http://localhost:8080/api/stores/dlq/replay
# {"dlqTopic":"ossl-store-events.DLQ","replayed":3}
```

### Error responses

`GlobalExceptionHandler` (`@RestControllerAdvice`) returns a JSON body with
`timestamp`, `status`, `error`, `message` for all request-level failures:
400 (bad file / header / multipart), 404 (`StoreNotFoundException`),
503 (MongoDB or PostgreSQL unreachable, with a short message and no driver
details), 500 (anything unexpected).

## CSV schema

The CSV must contain these header columns (defined once in
`CsvStoreFields.REQUIRED_HEADERS` and shared by import and export). **No
value-level validation** is applied beyond the row checks listed above.

```
locationId, departments, ecomStore.operatingStatus, ecomStore.isDUGStore,
ecomStore.isDeliveryStore, ecomStore.storeFeatures.isExpressStore,
ecomStore.storeFeatures.isWGDEnabled, ecomStore.storeFeatures.isFlashDugEnabled,
ecomStore.storeFeatures.isFlashDeliveryEnabled, isEcomStore, timezone,
brand.name, division.name, address.line1, address.city, address.state,
address.zipcode, phone, latitude, longitude
```

How cells are converted before publishing:
- dotted headers become nested JSON (`address.city` → `{"address": {"city": ...}}`)
- `true` / `false` become booleans
- `departments` (comma-separated) becomes a JSON array
- `latitude` / `longitude` become numbers when parsable
- empty cells become `null`

`sample-data/stores_export.csv` is a valid example file.

## Kafka reliability

| Topic | Producer | Consumer(s) |
|---|---|---|
| `ossl-store-events` | `StoreEventProducerService` (import, publish) | `StoreEventConsumerService` (SQL group), `StoreEventConsumer` (Mongo group) |
| `ossl-store-change-events` | `ChangeEventProducerService` | `ChangeEventConsumer` |
| `ossl-store-events.DLQ` | `DeadLetterPublishingRecoverer` | `DlqReplayService` (on demand, `POST /api/stores/dlq/replay`) |

- **Ordering:** messages are keyed by `locationId`, so updates for one store
  stay in order within a partition.
- **Idempotency:** `StoreEventConsumer` loads the existing Mongo document and
  applies the message only if the document is absent or
  `incoming version > stored version`. Duplicates and out-of-order messages
  are logged and skipped.
- **Retry + DLQ:** `KafkaErrorHandlerConfig` configures a `DefaultErrorHandler`
  with `FixedBackOff(1000 ms, 3 retries)`. Both the SQL and Mongo consumers
  rethrow on failure; after retries are exhausted the record is published to
  `<original topic>.DLQ` (same partition) and the consumer moves on.
- **Bad payloads:** value deserializers are wrapped in
  `ErrorHandlingDeserializer`, so undeserializable messages go through the
  same retry/DLQ path instead of crashing the consumer.
- **Producers:** `acks=all`; each send waits up to 5 s and throws
  `KafkaPublishException` on failure.
- **Change events:** `ChangeEventConsumer` catches and logs its own errors
  (no retry/DLQ). A missed cache refresh is corrected by the 10-minute cache
  expiry.

## Data stores

- **PostgreSQL:** tables `stores` and `store_address` (see
  `create_tables.sql`). `StoreEntity` implements `Persistable` so `save()`
  inserts new stores instead of merging.
- **MongoDB:** collection `stores` (`StoreDocument`: unique `locationId`,
  `storeInfo` map, `version`, `lastUpdated`). A second collection
  (`GroceryStoreDocument`) is the source used by the CSV export utility.
- **Versioning:** a plain `Long version`. CSV import sends `0` for new stores
  and current + 1 for existing ones; `/publish` increments it. The SQL consumer
  never moves a stored version backwards. It is deliberately not a timestamp and not JPA `@Version`.

## CSV export utility

`StoreCsvExportUtility.exportStoresToCsv(fileName)` reads all documents from
the `GroceryStoreDocument` Mongo collection and writes
`sample-data/<fileName>` using the same header list as the import. It is not
exposed as an endpoint; it is exercised by `StoreCsvExportUtilityTest`, which
writes `sample-data/stores_export.csv`.

## Project layout

```
ossl-store-pipeline/
├── pom.xml
├── docker-compose.yml          (PostgreSQL + Kafka + MongoDB for local dev)
├── create_tables.sql           (PostgreSQL tables)
├── .env.example                (environment variables)
├── sample-data/stores_export.csv
├── ossl-store-ui/              (React + Vite console, see Web UI)
└── src/
    ├── main/java/com/safeway/ossl/storepipeline/
    │   ├── OsslStorePipelineApplication.java
    │   ├── controller/   StoreImportController, StorePublishController, StoreQueryController,
    │   │                 StoreDlqController
    │   ├── service/      CsvStoreImportService, StoreEventProducerService,
    │   │                 StorePublishService, ChangeEventProducerService,
    │   │                 StoreEventConsumerService (SQL consumer),
    │   │                 StoreCacheService, StoreQueryService, DlqReplayService
    │   ├── listener/     StoreEventConsumer (Mongo consumer), ChangeEventConsumer
    │   ├── config/       KafkaProducerConfig, KafkaConsumerConfig, KafkaErrorHandlerConfig,
    │   │                 CacheConfig, MongoConverterConfig, OpenApiConfig
    │   ├── dto/          StoreRecord, StoreChangeEvent, ImportResult, RowError,
    │   │                 StoreComparisonResult, ErrorResponse
    │   ├── entity/       StoreEntity, AddressEntity          (JPA)
    │   ├── document/     StoreDocument                        (MongoDB)
    │   ├── models/       GroceryStoreDocument and nested models (export source)
    │   ├── repository/   StoreSqlRepository, StoreMongoRepository, GroceryStoreMongoRepository
    │   ├── mapper/       StoreMapper (StoreRecord <-> StoreEntity <-> StoreDocument)
    │   ├── utility/      StoreCsvExportUtility
    │   ├── constants/    OSSLServiceEndPoints, CsvStoreFields
    │   └── exception/    GlobalExceptionHandler + custom exceptions
    ├── main/resources/application.yml
    ├── main/resources/static/index.html   (plain HTML console served at http://localhost:8080/)
    └── test/java/.../    service/ and utility/ unit tests
```

## Configuration

`application.yml` reads everything from environment variables with local
defaults (see `.env.example`):

| Variable | Default |
|---|---|
| `DB_HOST` / `DB_PORT` / `DB_NAME` | `localhost` / `5432` / `ossldb` |
| `DB_USER` / `DB_PASSWORD` | `cmoo385` / `postgres` |
| `MONGODB_URI` | `mongodb://localhost:27017/ossldb` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `STORE_EVENTS_TOPIC` | `ossl-store-events` |
| `CHANGE_EVENTS_TOPIC` | `ossl-store-change-events` |
| `KAFKA_CONSUMER_GROUP_ID` | `ossl-store-pipeline-group` (change-event consumer) |
| `KAFKA_SQL_CONSUMER_GROUP_ID` | `ossl-store-sql-group` |
| `KAFKA_MONGO_CONSUMER_GROUP_ID` | `ossl-store-mongo-group` |

## Build and run locally

Prerequisites: Java 17, Maven and Docker.

```bash
# 1. PostgreSQL + Kafka + MongoDB
docker compose up -d

# 2. Build and run
mvn clean install
mvn spring-boot:run
```

`docker-compose.yml` starts PostgreSQL (`localhost:5432`, db `ossldb`, user
`cmoo385` / `postgres`, tables created from `create_tables.sql` on first
start), Kafka (`localhost:9092`, topic auto-creation on) and MongoDB
(`localhost:27017`). These match the `application.yml` defaults. To re-run
`create_tables.sql`, wipe the volume with `docker compose down -v`.

## End-to-end smoke test

```bash
# 1. Import (publishes to Kafka; SQL + Mongo are filled by the consumers)
curl -X POST http://localhost:8080/api/stores/import -F "file=@sample-data/stores_export.csv"

# 2. Confirm it reached SQL and Mongo (give the consumers a moment)
curl http://localhost:8080/api/stores/1794/sql
curl http://localhost:8080/api/stores/1794/mongo

# 3. Publish a new version (SQL version+1 -> Kafka -> both consumers)
curl -X POST http://localhost:8080/api/stores/1794/publish

# 4. Read via cache (loads from Mongo on first call)
curl http://localhost:8080/api/stores/1794/cache

# 5. Compare SQL vs Mongo
curl http://localhost:8080/api/stores/1794/compare
```

To see the retry/DLQ path, stop the MongoDB container and import a CSV: the
Mongo consumer retries 3 times, then the records land in
`ossl-store-events.DLQ` while SQL keeps being updated. Start MongoDB again and
call `POST /api/stores/dlq/replay` to push those records through.

## Web UI

Two consoles call the same REST API: import a CSV, publish a store, read it from
SQL / MongoDB / cache, and compare the copies.

- **Built-in page:** `src/main/resources/static/index.html`, served by Spring at
  http://localhost:8080/ (no build step; restart the app after editing).
- **React app:** `ossl-store-ui/` (see its own README).
  ```bash
  cd ossl-store-ui
  npm install
  npm run dev      # http://localhost:5173, proxies /api to http://localhost:8080
  npm run build    # static bundle in dist/
  ```

**Publish and polling.** Publishing is asynchronous: the API saves the new
version to SQL and returns, and the Mongo consumer updates MongoDB a moment
later through Kafka. With "auto-refresh after publish" ticked, the UI re-reads
the store every 2 seconds, up to 3 times (about 6 seconds), until MongoDB
reaches the published version (`POLL_MAX` / `POLL_MS` in `StorePanel.jsx`; the
same limits are in `static/index.html`). If it does not, the UI shows
"MongoDB did not reach vN within 6s". Click **Check status** to re-read later.

**Status messages.**
- *MongoDB is behind SQL*: the Mongo update is lagging or failed. If MongoDB
  was down, the message is in `ossl-store-events.DLQ`; once MongoDB is back, call
  `POST /api/stores/dlq/replay` to replay it.
- *Error reading a layer: MongoDB is currently unavailable*: the API returned
  503 because MongoDB cannot be reached.

## Swagger UI

- Swagger UI: http://localhost:8080/swagger-ui.html
- OpenAPI spec: http://localhost:8080/v3/api-docs

## Tests

```bash
mvn test
```

All tests are unit tests with Mockito (no embedded Kafka or real database):

| Test | Covers |
|---|---|
| `CsvStoreImportServiceTest` | counts, bad rows, Kafka failure isolation |
| `StoreEventProducerServiceTest`, `ChangeEventProducerServiceTest` | publishing and failure handling |
| `StoreEventConsumerServiceTest`, `StoreEventConsumerServiceFailureTest` | SQL consumer success and failure |
| `StoreEventConsumerDlqTest` | Mongo consumer rethrows so the container can retry / dead-letter |
| `DlqReplayServiceTest` | DLQ replay: republishes with key, commits offsets, stops on failure |
| `StorePublishServiceTest` | version increment and publish |
| `StoreQueryServiceTest`, `StoreCacheServiceTest` | reads, compare, cache-aside |
| `GlobalExceptionHandlerTest` | MongoDB / SQL unreachable returns 503 without leaking driver details |
| `StoreCsvExportUtilityTest` | CSV export |
