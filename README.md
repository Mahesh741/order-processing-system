# Order Processing System

[![CI](https://github.com/Mahesh741/order-processing-system/actions/workflows/ci.yml/badge.svg)](https://github.com/Mahesh741/order-processing-system/actions/workflows/ci.yml)

An event-driven order backend: Spring Boot 3, Apache Kafka and PostgreSQL. Orders are accepted
over HTTP, processed asynchronously, retried with exponential backoff, and dead-lettered when the
retries run out. Dead letters can be replayed once the cause is fixed.

**Live walkthrough:** https://mahesh741.github.io/demos/order-processing/

```
POST /api/orders ──► orders (PENDING) ──► orders.created ──► OrderProcessor ──► COMPLETED
                                                │ failure
                                                ▼
                         orders.created-retry-0 (1 s) ─► -retry-1 (2 s) ─► -retry-2 (4 s)
                                                │ still failing, or not retryable
                                                ▼
                                       orders.created-dlt ──► FAILED + dead_letters row
                                                                   │ POST /api/dead-letters/{id}/replay
                                                                   └──► back to orders.created
```

## Design decisions

- **Accept fast, process later.** `POST /api/orders` stores the order and answers `202 Accepted`
  with a `Location` to poll. The event is published only after the database transaction commits.
- **Non-blocking retries.** Failures move to separate retry topics (`@RetryableTopic`) instead of
  blocking the partition, so one failing order does not hold up the orders behind it.
- **Retry only what retrying can fix.** A payment timeout is retried. An unknown SKU is not: it
  goes straight to the dead-letter topic.
- **Idempotent processing.** Kafka delivers at least once. Only a `PENDING` order is processed, so a
  duplicate event for a completed or failed order is ignored, and the payment call is keyed by
  order id, so a redelivery never charges twice.
- **Dead letters are recoverable.** Each one is stored with its reason and can be replayed through
  the API with a fresh set of retries.

Left out to keep the example small: a transactional outbox (today, if the broker is down right
after the commit, the order stays `PENDING` with no event) and authentication.

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/orders` | Place an order (`customerId`, `sku`, `quantity`, `amountCents`) |
| `GET` | `/api/orders/{id}` | Order status, attempts and last failure |
| `GET` | `/api/orders` | 50 most recent orders |
| `GET` | `/api/dead-letters` | 50 most recent dead letters |
| `POST` | `/api/dead-letters/{id}/replay` | Put a dead-lettered order back on the main topic |
| `PUT` | `/api/simulation/payment-outage` | `{"enabled": true}` switches a payment outage on |

## Run the tests

```bash
./gradlew test
```

Eight end-to-end tests drive the HTTP API against an embedded Kafka broker with the real retry
and dead-letter topics: the happy path, transient failures that recover, exhausted retries followed
by a replay, a non-retryable order, duplicate delivery for completed and failed orders, validation and 404s.

They use in-memory H2 by default. To run the same tests against PostgreSQL:

```bash
TEST_DB_URL=jdbc:postgresql://localhost:5432/orders TEST_DB_USER=orders TEST_DB_PASSWORD=orders ./gradlew test
```

## Run it locally

```bash
docker compose up --build
```

```bash
curl -i -X POST localhost:8080/api/orders -H 'Content-Type: application/json' -d '{"customerId":"c-1","sku":"SKU-1","quantity":2,"amountCents":4999}'
```

To watch the retry path, switch the outage on, place an order, wait about eight seconds for the
dead letter, switch the outage off and replay it:

```bash
curl -X PUT localhost:8080/api/simulation/payment-outage -H 'Content-Type: application/json' -d '{"enabled":true}'
```

```bash
curl localhost:8080/api/dead-letters
```
