# EAI – learning project (framework + Person app Test)

Small Enterprise Application Integration (EAI) example used to learn JMS, Kafka and databases.
This file is the **hand-over document**: read it first in a new session before changing code.

## 1. Layout

```
/home/badr/git/eai/
├── pom.xml                  eai-parent: Spring Boot 3.5.0 parent + module list (Java 21, Lombok 1.18.42)
├── .github/workflows/build.yml   CI (see §9)
├── eai-framework/           plain Java kernel, NO Spring code
├── eai-deploy/              Kubernetes config managed by Argo CD (not a Maven module, see §10b)
└── eai-person-app/          Spring Boot app using the kernel
    ├── Dockerfile           container image (temurin 21 JRE)
    ├── docker-compose.yml   Artemis, Kafka, PostgreSQL + web UIs
    └── src/main/resources/  application.yml, application-local.yml, schema.sql
```

Build everything from the root: `mvn clean install` (14 tests, no Docker needed).
Only the app: `mvn -pl eai-person-app -am package`.

## 2. The kernel (`eai-framework`)

`IntegrationEngine.dispatch(Message)` runs every message through one **route**:

```
receive -> decode -> validate -> process -> enrich -> encode -> send   (any failure -> ErrorHandler)
```

- `core/`
  - `Message`: id, headers, `rawPayload` (bytes), `originalPayload`, `body` (decoded object).
  - `Exchange`: one message being processed + status + validation errors + failure.
  - `ExchangeStatus`: RECEIVED, DECODED, VALIDATED/REJECTED, PROCESSED, ENRICHED, ENCODED, SENT, FAILED.
  - `Headers`: header names (`eai.input.format`, `eai.output.format`, `eai.output.channel`,
    `eai.output.destination`, `eai.source.channel`, `eai.route`).
- `spi/` – extension points implemented by apps:
  - `InboundChannel` (source), `OutboundChannel` (target, `type()` + optional `defaultFormat()`),
  - `FormatCodec` (bytes <-> object, keyed by `format()`), `Validator`, `Processor`, `Enricher`, `ErrorHandler`,
  - `SendTransaction`: wraps the encode+send step(s) of a message (default: no transaction).
- `pipeline/Route`: validators, processors, enrichers, error handler, default output (format, channel, destination).
- `registry/Registry`: lookup of codecs / outbound channels by key.
- `support/`: generic `CsvCodec`, `FolderInboundChannel`, `FolderOutboundChannel` (not used by the Person app).

### Output rules (in `IntegrationEngine`)
- Format = header `eai.output.format`, else route default.
- Channel = header `eai.output.channel`, else route default (`db`).
- Destination = header `eai.output.destination`; the route default destination (`PERSON`) is used **only**
  with the route default channel. Other channels then use their own default (topic/queue).
- **Fan-out**: `eai.output.channel` may be a list, e.g. `db,kafka,jms`. Each channel then uses its own
  `defaultFormat()` and default destination; format/destination headers are ignored. Channels run in the
  given order inside one `SendTransaction`.

## 3. The Person app (`eai-person-app`, package `com.eai.person`)

- `model/Person`: Lombok `@Data @Builder` (id, firstName, lastName, email, birthDate, country,
  fullName, age, source, createdAt).
- `channel/`
  - `JmsInboundChannel`: listens on `PERSON.IN`. JMS properties use `_` instead of `.`
    (`eai_output_channel` -> `eai.output.channel`). Sets `jms.messageId` header.
  - `KafkaInboundChannel`: consumes `person.in` (group `eai-person-app`, commit after each record).
  - `FormatSniffer`: detects the input format from the first char (`{`/`[` = json, `<` = xml);
    an explicit `eai_input_format` property/header wins.
  - `JdbcPersonOutboundChannel` (`db`, default format `object`): INSERT into table `PERSON`.
  - `KafkaOutboundChannel` (`kafka`, default `json`): topic `person.out`, key = person id, waits for ack.
  - `JmsOutboundChannel` (`jms`, default `xml`): queue `PERSON.OUT`, correlation id = incoming JMS id,
    **refuses** the inbound queue (loop).
  - Kafka/JMS outbound fall back to JSON when the payload is empty (format `object`).
- `codec/`: `XmlCodec` (flat XML <-> Map; encodes any object via Jackson, root = class name),
  `JsonCodec`, `ObjectCodec` (no bytes, body passed as-is – for the DB).
- `validation/PersonXmlValidator`, `processor/PersonMappingProcessor` (Map -> Person),
  `enrich/PersonEnricher` (fullName = "First LAST", age, source, createdAt).
- `error/`: `DeadLetterErrorHandler` (JMS -> `PERSON.DLQ`, reason in `eai_status/eai_errors/eai_failure`),
  `KafkaDeadLetterErrorHandler` (-> `person.dlt`, original topic/partition/offset in headers),
  `SourceAwareErrorHandler` (chooses one of them by source channel).
- `config/`
  - `EaiConfiguration`: all beans, the route `person-import`, the engine, and the `SendTransaction`
    (Spring `TransactionTemplate`: if a later channel fails, the DB insert is rolled back).
  - `KafkaConfiguration`: Kafka beans, only when `eai.kafka.enabled=true`.
  - `EaiProperties`: prefix `eai` (inbound, output, dead-letter-queue, jms-output-queue, kafka.*).
  - `EngineLifecycle`: starts/stops the engine with Spring.

### Database (`schema.sql`)
`PERSON` with `DB_ID` = identity primary key (generated by the DB) and `ID` = business id from the
message (**not unique**, indexed). The same person received twice gives two rows.
`CREATE TABLE IF NOT EXISTS` does not alter an existing table: schema changes on an existing DB must be
applied by hand (no Flyway/Liquibase yet).

## 4. Message properties (JMS) / headers (Kafka)

- Input: `eai_input_format` = `xml` | `json` (optional, auto-detected).
- Output:
  - nothing -> DB only.
  - `eai_output_channel=kafka` (+ optional `eai_output_format=json`, `eai_output_destination=person.out`).
  - `eai_output_channel=jms` (+ optional `eai_output_format=xml|json`, `eai_output_destination=QUEUE`).
  - `eai_output_channel=db,kafka,jms` -> all three, channel defaults.
- Kafka headers use dots (`eai.output.channel`).

## 5. Infrastructure (`eai-person-app/docker-compose.yml`)

- Artemis `61616`, console http://localhost:8161 (artemis/artemis). `artemis-init` creates
  `PERSON.IN`, `PERSON.OUT`, `PERSON.DLQ`.
- Kafka KRaft single node `localhost:9092` (containers: `kafka:29092`), auto-create topics **off**,
  `kafka-init` creates `person.in`, `person.out`, `person.dlt` (3 partitions). Volume `kafka-data`.
  Retention 7 days / 512 MB per partition, segments 64 MB, max record 1 MB.
- Kafka UI http://localhost:8090.
- PostgreSQL 17 `localhost:5432`, db `persondb`, user/password `eai`/`eai`, volume `postgres-data`.
- Adminer http://localhost:8091 (System PostgreSQL, Server `postgres`).

`docker compose down` keeps the volumes, `down -v` deletes them.

## 6. Run

```sh
cd /home/badr/git/eai && mvn clean install -DskipTests
cd eai-person-app && docker compose up -d
ARTEMIS_USER=artemis ARTEMIS_PASSWORD=artemis java -jar target/eai-person-app-1.0.0-SNAPSHOT.jar
```
- Profile `local` (`--spring.profiles.active=local`): embedded Artemis, H2 file `./data/persondb`
  with console `/h2-console`, Kafka off (unless `KAFKA_ENABLED=true`).
- Env overrides: `DB_URL`, `DB_USER`, `DB_PASSWORD`, `ARTEMIS_URL`, `KAFKA_BOOTSTRAP_SERVERS`,
  `KAFKA_ENABLED`, `EAI_TRACE_LEVEL=DEBUG` (logs payloads), `SERVER_PORT`.
- Health: http://localhost:8080/actuator/health

## 7. Tests (`eai-person-app/src/test`)

Run without Docker: embedded Artemis, in-memory H2, `@EmbeddedKafka`.
- `PersonApplicationTest` (Kafka disabled): JMS->DB, JSON over JMS, rejection to DLQ, same id twice,
  JMS->JMS XML, `db,jms` fan-out, loop refusal.
- `KafkaFlowTest`: Kafka->DB, JMS->Kafka (with/without destination/format), `db,kafka` fan-out,
  rollback when the second channel fails, invalid record -> `person.dlt`.
- `eai-framework`: `IntegrationEngineTest`.

## 8. How to extend

- **New target**: implement `OutboundChannel` (unique `type()`, `defaultFormat()`), declare it as a
  `@Bean` -> it is registered automatically in the engine. Add a test.
- **New source**: implement `InboundChannel`, map transport properties to `Headers`, set
  `eai.source.channel`, call the dispatcher; make `SourceAwareErrorHandler` aware of it if needed.
- **New format**: implement `FormatCodec` with a unique `format()`, declare as `@Bean`.
- Changing the kernel: rebuild from the root so the app picks it up.

## 9. CI

`.github/workflows/build.yml` currently uses `runs-on: java-gh-runner` and
`eai/actions-release-sbd/java-build@v1` (company-specific). On a personal GitHub use `ubuntu-latest`,
`actions/setup-java@v4` (temurin 17, cache maven) and `mvn -B clean verify`.

`.github/workflows/fortify.yml`: Fortify SAST scan (`fortify/github-action@v2`) on push/PR to `main`,
every Monday and on demand. Needs Fortify on Demand **or** SSC + ScanCentral secrets/variables
(listed at the top of the file). Results go to the Fortify server and to GitHub Code Scanning.
The folder must be named `.github` (with the dot), otherwise GitHub ignores it.

## 10. Known limits / ideas

- JMS and Kafka sends are not part of the DB transaction: put `db` first in a fan-out list. A commit
  failure after Kafka acked still leaves the Kafka record (fix: outbox pattern).
- No schema migration tool (Flyway would be the next step).
- Kafka delivery is at-least-once: a record can be processed twice after a failure.
- Ideas: Kafka Connect / Debezium on PostgreSQL, compacted "current person" topic, outbox pattern,
  Testcontainers tests against real PostgreSQL/Kafka.

## 10b. Kubernetes / Argo CD

- `eai-person-app/Dockerfile`: image built from the jar (temurin 21 JRE, non-root uid 1001).
- Configuration is in the folder `eai-deploy/` of this repo (Kustomize): `base/` + `overlays/dev`, `overlays/prod`,
  `argocd/` Applications (dev auto sync, prod manual). All variables come from ConfigMap
  `eai-person-app-config`; credentials from Secret `eai-person-app-secret` (prod Secret not in Git).

## 11. Conventions for future sessions

- Answers in the Eclipse chat: avoid Markdown tables (rendered as raw text), use lists.
- Keep `eai-framework` free of Spring.
- After any change: `mvn clean install` from the root, all tests must pass.
