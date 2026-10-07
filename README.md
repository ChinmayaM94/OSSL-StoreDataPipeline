# OSSL Store Data Pipeline

Spring Boot 3 / Java 17 service implementing the **full Confluence LLD "OSSL
Store Data Pipeline"** (space **CMS**). It started as **Jira ACIP-624075**
(CSV → Kafka ingestion) and has since been expanded to cover the complete
pipeline the LLD describes: CSV import, relational (SQL) persistence,
versioned Kafka publication, MongoDB sync, cache refresh, and
multi-datastore read/consistency-check endpoints.

## Architecture overview

```
                      ┌─────────────────────────────────────────────┐
                      │   POST /api/stores/import (multipart CSV)    │
                      └───────────────────────┬───────────────────────┘
                                               │
                                validate each row (schema)
                                               │
                         ┌─────────────────────┼─────────────────────┐
                         ▼                                           ▼
             Kafka topic: ossl-store-events          SQL upsert (StoreEntity, H2/JPA)
             (one message per valid row)              (also lands valid rows in SQL)

                      ┌─────────────────────────────────────────────┐
                      │ POST /api/stores/{storeId}/publish           │
                      └───────────────────────┬───────────────────────┘
                                               │
                          read StoreEntity from SQL, assign next version
                                               │
                                               ▼
                      Kafka topic: ossl-store-publication (Topic 1)
                                               │
                                               ▼
                 ┌─────────────────────────────────────────────────┐
                 │ StorePublicationConsumer (@KafkaListener)         │
                 │  - load existing StoreDocument from MongoDB       │
                 │  - upsert ONLY IF incoming.version > stored.version│
                 │  - (idempotent / out-of-order / duplicate safe)   │
                 └───────────────────────┬───────────────────────────┘
                                          │ on successful Mongo upsert
                                          ▼
                      Kafka topic: ossl-store-change-events (Topic 2)
                                          │
                                          ▼
                 ┌─────────────────────────────────────────────────┐
                 │ ChangeEventConsumer (@KafkaListener)              │
                 │  - evict Caffeine cache entry for storeId         │
                 │  - (lazy reload on next cache read)               │
                 └─────────────────────────────────────────────────┘

   GET /api/stores/{storeId}/sql      -> reads StoreEntity directly from SQL
   GET /api/stores/{storeId}/mongo    -> reads StoreDocument directly from MongoDB
   GET /api/stores/{storeId}/cache    -> cache-aside: Caffeine, miss -> load from Mongo -> populate cache
   GET /api/stores/{storeId}/compare  -> field-by-field SQL vs Mongo diff (consistency check)
```

**Layers**: Controller → Service → {SQL repo, Mongo repo} → Kafka
producers/consumers → Cache. Controllers are thin and delegate all logic to
services; services own validation, persistence orchestration, and
Kafka/cache interaction.

## Endpoints

### 1. `POST /api/stores/import` — CSV import (Jira ACIP-624075, extended)

Uploads a CSV file of OSSL store records. Each row is parsed and validated;
every valid row is:
1. Published as an individual JSON message to Kafka topic `ossl-store-events`, keyed by `storeId`.
2. Upserted into the SQL datastore (`StoreEntity` via `StoreSqlRepository`).

A row is counted as **processed** once its Kafka publish succeeds (the
original ticket contract). If the subsequent SQL upsert fails, that failure
is reported as an **additional** `RowError` (field `"sql"`) in the response,
but it does **not** reduce `processedCount` or abort the row loop — so the
original response shape, counts semantics, and existing unit tests are
preserved.

```bash
curl -X POST http://localhost:8080/api/stores/import \
  -F "file=@sample-data/sample_stores.csv"
```

```json
{
  "totalRows": 8,
  "processedCount": 4,
  "failureCount": 4,
  "errors": [
    { "rowNumber": 5, "field": "storeId", "reason": "storeId is required and cannot be blank" },
    { "rowNumber": 6, "field": "storeName", "reason": "storeName is required and cannot be blank" },
    { "rowNumber": 7, "field": "zip", "reason": "zip 'ABCDE' is not a valid US zip (expected 5 digits or 5+4)" },
    { "rowNumber": 8, "field": "latitude", "reason": "latitude 'NOTANUMBER' is not numeric" }
  ]
}
```

### 2. `POST /api/stores/{storeId}/publish` — Publish a store to Kafka

Reads the store from the SQL datastore, assigns it the **next version**
(current version + 1, persisted back to SQL), and publishes the versioned
`StoreRecord` to Kafka topic `ossl-store-publication`, keyed by `storeId`.
Returns `404` if the store doesn't exist in SQL (import it first).

```bash
curl -X POST http://localhost:8080/api/stores/1001/publish
```

```json
{
  "storeId": "1001",
  "storeName": "Downtown Seattle",
  "address": "123 Pine St",
  "city": "Seattle",
  "state": "WA",
  "zip": "98101",
  "phone": "206-555-0100",
  "latitude": 47.6097,
  "longitude": -122.3331,
  "storeStatus": "ACTIVE",
  "version": 1
}
```

### 3. `GET /api/stores/{storeId}/sql` — Read from SQL

Reads the store directly from the relational datastore. `404` if not found.

```bash
curl http://localhost:8080/api/stores/1001/sql
```

### 4. `GET /api/stores/{storeId}/mongo` — Read from MongoDB

Reads the store directly from MongoDB — the copy maintained by
`StorePublicationConsumer`. `404` if not found (e.g. the store has never
been published).

```bash
curl http://localhost:8080/api/stores/1001/mongo
```

### 5. `GET /api/stores/{storeId}/cache` — Read from cache (cache-aside)

Checks the Caffeine cache first; on a miss, loads from MongoDB and populates
the cache before returning. `404` if the store isn't in the cache or Mongo.

```bash
curl http://localhost:8080/api/stores/1001/cache
```

### 6. `GET /api/stores/{storeId}/compare` — SQL vs Mongo consistency check

Fetches the store from both SQL and MongoDB and returns a field-by-field
diff — useful to confirm the async publish → Kafka → Mongo-sync pipeline has
converged. `404` only if the store exists in **neither** datastore.

```bash
curl http://localhost:8080/api/stores/1001/compare
```

```json
{
  "storeId": "1001",
  "sqlValues": { "storeName": "Downtown Seattle", "version": 2, "...": "..." },
  "mongoValues": { "storeName": "Downtown Seattle", "version": 1, "...": "..." },
  "mismatchedFields": ["version"],
  "consistent": false
}
```

(A `consistent: false` with a stale `version` in Mongo is expected
transiently right after a publish, before `StorePublicationConsumer`
finishes processing the Kafka message.)

## How this satisfies the Jira ACIP-624075 acceptance criteria

| # | Acceptance Criterion | How it's satisfied |
|---|---|---|
| 1 | POST REST endpoint accepting a multipart CSV upload | `StoreImportController.importStores()` — `POST /api/stores/import`, `consumes = MULTIPART_FORM_DATA`, `@RequestParam("file") MultipartFile file`. |
| 2 | CSV parsed and validated against an expected schema | `CsvStoreImportService` parses with Apache Commons CSV (header-aware), checks required headers exist, then validates each row's fields (required, zip format, numeric lat/long) against the schema documented below. |
| 3 | Each valid row published as a separate Kafka message | For every row that passes validation, `StoreEventProducerService.publish()` sends one `StoreRecord` (one Kafka message per row) to `ossl-store-events`, keyed by `storeId`. |
| 4 | Malformed rows handled/reported without dropping other valid rows | The per-row loop in `CsvStoreImportService.importCsv()` wraps all row logic (validation + Kafka publish + SQL upsert) in a `try/catch`; a failure only adds a `RowError` and `continue`s to the next row — it never aborts the file. Unit tests assert that all valid rows still get published even when malformed rows are interleaved among them. |
| 5 | Response returns processed/failure counts + actionable detail | `ImportResult` returns `totalRows`, `processedCount`, `failureCount`, and `errors: List<RowError>` (each with 1-based `rowNumber`, `field`, and human-readable `reason`). |
| 6 | Swagger / OpenAPI documentation | `springdoc-openapi-starter-webmvc-ui` is on the classpath; `OpenApiConfig` describes the whole pipeline; every controller method is annotated with `@Operation`/`@ApiResponses`/`@Parameter`. Swagger UI at `/swagger-ui.html`, raw spec at `/v3/api-docs`. |

## LLD capability → endpoint mapping

| LLD Capability | Implementing component(s) |
|---|---|
| CSV upload → relational DB (SQL) | `StoreImportController` → `CsvStoreImportService` → `StoreSqlRepository` / `StoreEntity` |
| CSV upload → Kafka "store publication" | `CsvStoreImportService` → `StoreEventProducerService` → topic `ossl-store-events` |
| Explicit versioned publish to Kafka Topic 1 | `StorePublishController` → `StorePublishService` → `StorePublicationProducerService` → topic `ossl-store-publication` |
| Kafka consumer upserts into MongoDB (idempotent/dup/retry-safe) | `StorePublicationConsumer` (`@KafkaListener` on `ossl-store-publication`) → `StoreMongoRepository` / `StoreDocument` |
| Second "downstream completion" event to trigger cache refresh | `ChangeEventProducerService` → topic `ossl-store-change-events`, published by `StorePublicationConsumer` after a successful Mongo upsert |
| Cache refresh (Caffeine) | `ChangeEventConsumer` (`@KafkaListener` on `ossl-store-change-events`) → `StoreCacheService.evict()` |
| Read store from SQL / Mongo / Cache | `StoreQueryController` → `StoreQueryService` → `/sql`, `/mongo`, `/cache` |
| Consistency-check testing between datastores | `StoreQueryController` → `StoreQueryService.compare()` → `/compare` |

## Key design decisions

1. **Kafka value serialization: JSON, not Avro** (unchanged from the
   original ticket). Avoids a schema registry dependency; acceptable
   tradeoff for this stage of the pipeline per the LLD.

2. **Versioning approach**: a simple `Long version` field on `StoreRecord` /
   `StoreEntity` / `StoreDocument`, owned by `StorePublishService` and
   incremented by 1 each time `/publish` is called (not on every SQL write).
   Deliberately **not** a timestamp — timestamps can collide or go backwards
   across clock skew/retries, whereas a monotonically-increasing integer
   makes `StorePublicationConsumer`'s "only apply if incoming version >
   stored version" rule trivial and unambiguous. It is also deliberately
   **not** JPA's `@Version` optimistic-locking annotation, since that's
   auto-managed by Hibernate on every UPDATE and would conflict with the
   publish workflow's need to read/set/propagate the value explicitly.

3. **Idempotency / out-of-order handling in the Mongo-sync consumer**:
   `StorePublicationConsumer` loads the existing `StoreDocument` (if any)
   before upserting, and only applies the incoming message if the document
   is absent or `incoming.version > existing.version`. This makes the
   consumer safe against Kafka's at-least-once delivery (redelivery after a
   rebalance/retry), duplicate publishes, and messages arriving out of
   order across partitions. Messages that lose this check are logged and
   skipped, not retried forever.

4. **Poison-message handling**: Kafka listener methods never let an
   exception escape — all processing is wrapped in try/catch and logged.
   Letting an exception propagate from a `@KafkaListener` would cause the
   container to redeliver the same message indefinitely, blocking that
   partition. `KafkaConsumerConfig` additionally wraps the value
   deserializer in `ErrorHandlingDeserializer` (so an undeserializable
   payload doesn't crash the consumer thread) and attaches a
   `DefaultErrorHandler` with a small bounded retry (2 attempts, 1s
   backoff) before logging and moving on. **A real dead-letter topic (DLQ)
   is the production-grade next step** — out of scope for this iteration
   (see Next Steps).

5. **Cache strategy: cache-aside with evict-and-lazy-reload**, not
   write-through. `ChangeEventConsumer` only **evicts** the Caffeine entry
   for a changed store rather than re-fetching from Mongo and re-populating
   it immediately. This is simpler (no extra Mongo read for stores nobody is
   actively querying) and keeps `StoreCacheService.get()` as the single
   place that knows how to (re)populate an entry (cache-aside on
   `GET /api/stores/{storeId}/cache`).

6. **Dual dedicated Kafka producer services, not one generic
   `KafkaPublisherService<T>`**: `StoreEventProducerService` (import →
   `ossl-store-events`), `StorePublicationProducerService` (publish →
   `ossl-store-publication`), and `ChangeEventProducerService` (Mongo sync →
   `ossl-store-change-events`) are three small, obviously-named classes
   rather than one generic parameterized producer. They share the same
   underlying `KafkaTemplate`/`JsonSerializer` pattern, but each topic has
   distinct semantic intent (raw import event vs. explicit versioned
   publish vs. lightweight change signal) — three small, easy-to-read/test
   classes were judged clearer for review than one generic class with
   topic-switching logic.

7. **H2 (in-memory) over a real SQL database for local/dev.** `pom.xml`
   includes `com.h2database:h2` as a runtime dependency and
   `application.yml` points `spring.datasource.*` at
   `jdbc:h2:mem:ossldb`. This lets the full pipeline (including
   `/publish`, `/sql`, `/compare`) run locally with **zero external SQL
   server setup**. `spring.jpa.hibernate.ddl-auto=update` auto-creates/
   updates the `stores` table from `StoreEntity` on startup. **Swappable for
   Postgres/MySQL in production** by changing four `spring.datasource.*`
   properties and the JDBC driver dependency — no application code changes
   needed, since all SQL access goes through `StoreSqlRepository`
   (`JpaRepository`).

8. **Per-row try/catch isolation, request- vs. row-level failures** —
   unchanged from the original ticket design (see below).

## ⚠️ Assumed CSV Schema (pending confirmation)

The Confluence LLD explicitly lists the **OSSL field dictionary as a future
deliverable** — it does not define an official column list yet. To unblock
this work, the following schema was **assumed** based on the LLD's suggested
columns. **This must be reconciled against the official OSSL field
dictionary once it is published**, and column names/validation rules may
need to change across `StoreRecord`, `StoreEntity`, `StoreDocument`, and
`CsvStoreImportService`.

| Column | Required | Validation |
|---|---|---|
| `storeId` | **Yes** | Non-blank. Used as the Kafka message key and the SQL/Mongo document ID. |
| `storeName` | **Yes** | Non-blank. |
| `address` | No | Free text. |
| `city` | No | Free text. |
| `state` | No | Free text. |
| `zip` | No | If present, must match `^\d{5}(-\d{4})?$` (5-digit or ZIP+4). |
| `phone` | No | Free text, no format check. |
| `latitude` | No | If present, must parse as a double in range `[-90, 90]`. |
| `longitude` | No | If present, must parse as a double in range `[-180, 180]`. |
| `storeStatus` | No | Free text (e.g. `ACTIVE`, `INACTIVE`, `CLOSED`), no enum enforcement yet. |
| `version` | N/A (not in CSV) | Managed internally; assigned on `/publish`, not read from CSV. |

The CSV must have a header row containing at least `storeId` and
`storeName`; otherwise the whole request is rejected with HTTP 400 (a
request-level failure, since without those columns no row could possibly be
processed).

## Project layout

```
ossl-store-pipeline/
├── pom.xml
├── README.md
├── sample-data/
│   └── sample_stores.csv
└── src/
    ├── main/java/com/safeway/ossl/storepipeline/
    │   ├── OsslStorePipelineApplication.java
    │   ├── controller/
    │   │   ├── StoreImportController.java        (POST /api/stores/import)
    │   │   ├── StorePublishController.java        (POST /api/stores/{storeId}/publish)
    │   │   └── StoreQueryController.java          (GET /sql, /mongo, /cache, /compare)
    │   ├── dto/
    │   │   ├── StoreRecord.java  (now includes `version`)
    │   │   ├── RowError.java, ImportResult.java
    │   │   ├── StoreChangeEvent.java
    │   │   └── StoreComparisonResult.java
    │   ├── entity/StoreEntity.java                 (JPA / SQL)
    │   ├── document/StoreDocument.java             (MongoDB)
    │   ├── repository/
    │   │   ├── StoreSqlRepository.java (JpaRepository)
    │   │   └── StoreMongoRepository.java (MongoRepository)
    │   ├── mapper/StoreMapper.java                  (StoreRecord <-> StoreEntity <-> StoreDocument)
    │   ├── service/
    │   │   ├── CsvStoreImportService.java           (CSV -> Kafka + SQL)
    │   │   ├── StoreEventProducerService.java       (-> ossl-store-events)
    │   │   ├── StorePublishService.java             (SQL read -> version -> publish)
    │   │   ├── StorePublicationProducerService.java (-> ossl-store-publication)
    │   │   ├── ChangeEventProducerService.java      (-> ossl-store-change-events)
    │   │   ├── StoreCacheService.java                (cache-aside over Mongo)
    │   │   └── StoreQueryService.java                (sql/mongo/cache/compare reads)
    │   ├── listener/
    │   │   ├── StorePublicationConsumer.java   (ossl-store-publication -> Mongo upsert -> change event)
    │   │   └── ChangeEventConsumer.java        (ossl-store-change-events -> cache evict)
    │   ├── config/
    │   │   ├── KafkaProducerConfig.java  (StoreRecord + StoreChangeEvent producers)
    │   │   ├── KafkaConsumerConfig.java  (listener container factories, error handling)
    │   │   ├── CacheConfig.java          (Caffeine Cache<String, StoreRecord> bean)
    │   │   └── OpenApiConfig.java
    │   └── exception/
    │       ├── GlobalExceptionHandler.java
    │       ├── EmptyFileException.java, InvalidFileTypeException.java,
    │       │   CsvHeaderException.java, KafkaPublishException.java
    │       └── StoreNotFoundException.java
    ├── main/resources/application.yml
    └── test/java/com/safeway/ossl/storepipeline/service/CsvStoreImportServiceTest.java
```

## Build

```bash
mvn clean install
```

## Run locally

The full pipeline needs three backing services. H2 needs no setup at all;
MongoDB and Kafka need to be reachable (locally or via Docker).

### 1. H2 (SQL) — zero setup

In-memory, created automatically on startup (`jdbc:h2:mem:ossldb`). Data is
lost on restart — fine for local/dev. To inspect it while the app is
running, open the **H2 console**:

- URL: http://localhost:8080/h2-console
- JDBC URL: `jdbc:h2:mem:ossldb`
- User: `sa`, Password: *(blank)*

### 2. MongoDB

Defaults to `mongodb://localhost:27017/ossldb` (override with the
`MONGODB_URI` env var). If you don't already have Mongo running locally:

```bash
docker run -d --name ossl-mongo -p 27017:27017 mongo:6
```

### 3. Kafka

Defaults to `localhost:9092` (override with `KAFKA_BOOTSTRAP_SERVERS`). If
you don't have a broker running, spin one up with e.g. the Bitnami
single-node image:

```bash
docker run -d --name ossl-kafka \
  -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=0 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@127.0.0.1:9093 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  bitnami/kafka:latest
```

(Confluent's `confluentinc/cp-kafka` image with a matching `cp-zookeeper` or
KRaft config works equally well if that's your team's standard.) The app
auto-creates the three topics it needs
(`ossl-store-events`, `ossl-store-publication`, `ossl-store-change-events`)
on first publish/subscribe if the broker has topic auto-creation enabled
(the default for most dev images).

### Run the app

```bash
mvn spring-boot:run
```

Override topic names or the consumer group if needed:

```bash
STORE_EVENTS_TOPIC=ossl-store-events-dev \
STORE_PUBLICATION_TOPIC=ossl-store-publication-dev \
CHANGE_EVENTS_TOPIC=ossl-store-change-events-dev \
KAFKA_CONSUMER_GROUP_ID=ossl-store-pipeline-group-dev \
mvn spring-boot:run
```

## End-to-end smoke test

```bash
# 1. Import stores (lands in Kafka + SQL)
curl -X POST http://localhost:8080/api/stores/import -F "file=@sample-data/sample_stores.csv"

# 2. Confirm it's in SQL
curl http://localhost:8080/api/stores/1001/sql

# 3. Publish it (SQL -> versioned Kafka message -> Mongo sync -> cache evict)
curl -X POST http://localhost:8080/api/stores/1001/publish

# 4. Confirm it landed in Mongo (consumer needs a moment to process)
curl http://localhost:8080/api/stores/1001/mongo

# 5. Read via cache (will lazy-load from Mongo on first call)
curl http://localhost:8080/api/stores/1001/cache

# 6. Compare SQL vs Mongo
curl http://localhost:8080/api/stores/1001/compare
```

## Swagger UI

Once running, open:

- Swagger UI: http://localhost:8080/swagger-ui.html
- Raw OpenAPI spec: http://localhost:8080/v3/api-docs

Endpoints are grouped under three tags: **CSV Import**, **Store
Publication**, and **Store Queries**.

## Run unit tests

```bash
mvn test
```

`CsvStoreImportServiceTest` mocks both `StoreEventProducerService` and
`StoreSqlRepository` with Mockito (no embedded Kafka broker or real database
needed) and verifies:
- correct `processedCount`/`failureCount`/`totalRows` for a file mixing
  valid and malformed rows, without throwing;
- all valid rows are still published to Kafka even when malformed rows are
  interleaved among them;
- a Kafka publish failure on one otherwise-valid row is caught, counted as a
  failure, and does not block the other valid rows from being published.

The SQL-upsert addition is stubbed to always succeed in these tests
(`StoreSqlRepository.save()` is lenient-mocked to echo its input), so none
of the original assertions or counts changed.

## Known simplifications / Next steps

- **Dead-letter topic (DLQ) for poison messages.** Both Kafka consumers
  currently log-and-skip unprocessable messages after a short bounded retry
  (see `KafkaConsumerConfig`). Production should route these to a DLQ topic
  for manual inspection/replay instead of just logging.
- **Transactional outbox pattern.** The LLD explicitly calls out that the
  Mongo write (in `StorePublicationConsumer`) and the subsequent Kafka
  publish (to `ossl-store-change-events`) are **not atomic** — a crash
  between the two leaves Mongo updated but the cache-refresh signal never
  sent (stale cache until TTL expiry). Likewise, the CSV import's Kafka
  publish and SQL upsert aren't atomic either. Production hardening should
  introduce a transactional outbox (or an idempotent state machine) with
  fault-injection testing to guarantee eventual consistency across
  failure/retry scenarios.
- **Security / auth.** None implemented — all endpoints are unauthenticated.
  Production would need at minimum service-to-service auth (e.g. mTLS, OAuth2
  client credentials) in front of all endpoints.
- **Pagination.** No list/search endpoints exist yet — only get-by-id. A
  future `GET /api/stores` with pagination would be needed for bulk
  consistency audits across the full ~2,000-store dataset.
- **Observability.** The LLD calls out Micrometer metrics + Grafana
  dashboards as future work (e.g. import throughput, Kafka consumer lag,
  cache hit ratio, SQL/Mongo divergence rate from `/compare`). None of that
  instrumentation is wired up yet — only SLF4J logging.
- **Official OSSL field dictionary reconciliation.** Once published, revisit
  the assumed schema here and in `StoreRecord`/`StoreEntity`/`StoreDocument`/
  `CsvStoreImportService` to match it exactly.
