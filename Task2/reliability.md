# Паттерны надёжности

## Обзор

RTB-платформа работает в условиях жёсткого SLA и высокой нагрузки. Ошибка или таймаут в любом месте hot path ведут к проигрышу аукциона и финансовым потерям. Паттерны надёжности должны обеспечить корректное поведение системы при частичных сбоях — без каскадного распространения отказов.

---

## Circuit Breaker

Circuit Breaker применяется к каждому синхронному вызову зависимого сервиса в RTB hot path. Он предотвращает ситуацию, когда медленный или недоступный сервис блокирует весь поток запросов.

### Применение по сервисам

| Вызов | Где стоит Circuit Breaker | Действие при OPEN |
|---|---|---|
| DSP Gateway → Ad Server | DSP API Gateway | `503` немедленно, без ожидания |
| Ad Server → Bidding Service | Ad Server | Fallback: дефолтная ставка из Redis |
| Bidding Service → Budget Service | Bidding Service | Fallback: кэшированные лимиты из Redis |
| Bidding Service → BiddingDB | Bidding Service | Fallback: конфигурация из Redis |
| Ad Server → Campaign Service | Ad Server | Fallback: устаревшие данные из Redis |

### Параметры (рекомендуемые)

```
window_size:          10s          # скользящее окно анализа
failure_threshold:    50%          # доля ошибок для открытия
slow_call_threshold:  100ms        # запросы дольше этого — ошибка
min_requests:         100          # минимум запросов для анализа
open_timeout:         5s           # время до перехода в HALF-OPEN
half_open_probes:     5            # число probe-запросов
success_threshold:    80%          # процент успешных probe для закрытия
```

### Важно

Circuit Breaker в Bidding Service и Ad Server реализуется на уровне gRPC-клиента (например, через библиотеку Resilience4j для Go или аналог). В DSP API Gateway — через конфигурацию Envoy.

---

## Retry с экспоненциальной задержкой

Retry применяется для временных сбоев (сетевые ошибки, кратковременная недоступность). Он не применяется к бизнес-ошибкам (`NO_BID`, `400 Bad Request`) и не применяется в hot path с жёстким дедлайном, если первая попытка уже заняла значительное время.

### Стратегия по типу вызова

| Вызов | Retry | Обоснование |
|---|---|---|
| DSP Gateway → Ad Server | Нет | Суммарный дедлайн ≤ 80 ms, retry не уложится |
| Ad Server → Bidding Service | Нет | Hot path, deadline propagation |
| Bidding Service → Budget Service | 1 retry | Кратковременный сбой возможен, 1 попытка стоит |
| Campaign Service → Campaign DB | 3 retry | Не hot path, транзакционная запись допускает retry |
| Financial Service → Finance DB | 3 retry с idempotency key | Финансовые операции требуют надёжной доставки |
| Event producers → Kafka | Kafka producer retry (встроенный) | Kafka сам обеспечивает at-least-once |

### Параметры экспоненциальной задержки

```
initial_delay:   50ms
multiplier:      2.0
max_delay:       2000ms
max_attempts:    3
jitter:          ±20%   # случайный разброс для предотвращения thundering herd

Пример задержек: 50ms → 100ms → 200ms (с jitter: ~40-60ms → ~80-120ms → ~160-240ms)
```

### Какие ошибки retry-able

```
retry on:
  - connection timeout
  - 503 Service Unavailable
  - 429 Too Many Requests (с учётом Retry-After)
  - gRPC status: UNAVAILABLE, DEADLINE_EXCEEDED (только вне hot path)

no retry on:
  - 400 Bad Request
  - 401 Unauthorized
  - 404 Not Found
  - бизнес-ошибки (NO_BID, недостаточный бюджет)
```

---

## Идемпотентность финансовых операций

Финансовые операции (списания, пополнения, резервирования бюджета) должны быть идемпотентными: повторный вызов с тем же запросом не должен приводить к двойному списанию или двойному пополнению.

### Механизм: Idempotency Key

Финансовые операции поступают в Financial Service двумя путями:

1. **Kafka-событие** — Bidding Service публикует `auction.win-loss` event, Financial Service потребляет и выполняет списание. Idempotency Key = `auction_result_id` из события. Kafka гарантирует at-least-once: повторная обработка одного события должна быть безопасной.
2. **gRPC-вызов** — внутренние сервисы (например, Budget Service при сверке) вызывают Financial Service напрямую. Idempotency Key передаётся в gRPC metadata.

> Dashboard → Financial Service использует REST (как определено в `interaction.md`), но принцип idempotency key тот же — передаётся в HTTP-заголовке.

**gRPC-контракт для внутренних вызовов:**

```protobuf
service FinancialService {
  rpc Debit(DebitRequest) returns (DebitResponse);
}

message DebitRequest {
  string idempotency_key  = 1; // UUID — уникален на операцию; повтор возвращает сохранённый результат
  string campaign_id      = 2;
  int64  amount_cents     = 3;
  string auction_result_id = 4;
  DebitReason reason      = 5;
}

enum DebitReason {
  AUCTION_WIN     = 0;
  MANUAL_CHARGE   = 1;
  RECONCILIATION  = 2;
}

message DebitResponse {
  string transaction_id = 1;
  DebitStatus status    = 2;
  int64  balance_cents  = 3; // остаток после операции
}

enum DebitStatus {
  SUCCESS           = 0;
  INSUFFICIENT_FUNDS = 1;
  DUPLICATE         = 2; // idempotency key уже обработан, возвращён прежний результат
  ERROR             = 3;
}
```

**Алгоритм обработки в Financial Service:**

1. При получении запроса проверить ключ в таблице `idempotency_keys`.
2. Если ключ уже существует и операция завершена успешно — вернуть сохранённый результат без повторного выполнения.
3. Если ключ уже существует и операция в процессе — вернуть `409 Conflict` (клиент должен подождать).
4. Если ключ новый — выполнить операцию и записать ключ с результатом атомарно в одной транзакции.

### Таблица idempotency_keys (Finance DB)

| Поле | Тип | Описание |
|---|---|---|
| `key` | UUID | Idempotency Key (PRIMARY KEY) |
| `operation_type` | ENUM | `DEBIT` / `CREDIT` / `RESERVE` |
| `request_hash` | VARCHAR | Хэш тела запроса для обнаружения конфликтов |
| `response_body` | JSONB | Сохранённый ответ для повторного возврата |
| `status` | ENUM | `PENDING` / `COMPLETED` / `FAILED` |
| `created_at` | TIMESTAMP | Время создания |
| `expires_at` | TIMESTAMP | TTL записи (30 дней) |

### Идемпотентность резервирования бюджета

Budget Service использует оптимистичные локи при резервировании:

```sql
UPDATE budget
SET reserved = reserved + :amount,
    version  = version + 1
WHERE campaign_id = :campaign_id
  AND version     = :expected_version
  AND available   >= :amount;
```

Если `UPDATE` вернул 0 строк — конкуренция за бюджет, клиент получает `409` и выполняет retry.

---

## Резервные стратегии (Fallback)

При недоступности зависимостей в RTB hot path используются заранее определённые резервные стратегии. Цель — продолжить обслуживание запросов с приемлемым качеством, а не полностью отказывать.

### Bidding Service: недоступен BiddingDB

**Стратегия:** использовать конфигурацию из Redis (`bid:config:{campaign_id}`).

- Redis TTL для bid config — 60 секунд.
- Если ключ устарел или отсутствует в Redis — использовать дефолтные значения из конфига сервиса (`DEFAULT_BID_MULTIPLIER=1.0`, `DEFAULT_AUCTION_TYPE=SECOND_PRICE`).
- Логировать fallback-решения с меткой `fallback=redis` или `fallback=default`.

### Bidding Service: недоступен Budget Service

**Стратегия:** использовать кэшированный бюджет из Redis (`budget:available:{campaign_id}`).

- Redis TTL для бюджета — 5 секунд.
- Если ключ отсутствует — предположить, что бюджет доступен (optimistic fallback), и продолжить аукцион.
- Это допустимо, так как вероятность перерасхода бюджета за 5 секунд при разовом сбое невелика, а проигрыш аукциона обходится дороже.
- Фиксировать метрику `bidding_budget_fallback_total` для мониторинга частоты fallback.

### Ad Server: недоступен Bidding Service

**Стратегия:** вернуть резервный bid response с дефолтной ставкой.

- Дефолтная ставка берётся из конфига Ad Server (минимальная допустимая ставка для данного DSP-партнёра).
- Если дефолтная ставка недопустима (ниже floor price) — вернуть `NO_BID`.
- Это предпочтительнее, чем возвращать ошибку, которая засчитывается DSP-партнёром как нарушение SLA.

### Ad Server: недоступен Campaign Service (cache miss)

**Стратегия:** использовать устаревшие данные из Redis с пометкой `stale`.

- Redis хранит данные кампаний с TTL 60 секунд.
- При недоступности Campaign Service Ad Server использует устаревший кэш ещё до 5 минут (grace period).
- Метрика `adserver_campaign_cache_stale_total` фиксирует использование устаревших данных.

### Delivery Service: недоступен Kafka (Event Sink)

**Стратегия:** отправить ответ пользователю, событие — в локальный буфер.

- Events буферизируются в памяти процесса (ограниченный буфер, например, 10 000 событий).
- При восстановлении Kafka буфер сбрасывается.
- При переполнении буфера события сбрасываются с метрикой `delivery_events_dropped_total`. Потеря части событий допустима (eventual consistency для статистики), но не критична (финансовые операции не зависят от этого потока).

---

## Сводная таблица

| Компонент | Circuit Breaker | Retry | Fallback |
|---|---|---|---|
| DSP API Gateway | ✅ Envoy CB | ❌ Нет (deadline) | `503` немедленно |
| Ad Server → Bidding | ✅ gRPC CB | ❌ Нет (deadline) | Дефолтная ставка / NO_BID |
| Ad Server → Campaign | ✅ gRPC CB | ❌ Нет (hot path) | Устаревший кэш Redis |
| Bidding → Budget | ✅ gRPC CB | ✅ 1 retry | Кэшированный бюджет Redis |
| Bidding → BiddingDB | ✅ DB CB | ✅ 3 retry | Конфигурация из Redis |
| Financial Service | ✅ DB CB | ✅ 3 retry + idempotency | Idempotency Key |
| Delivery → Kafka | ❌ Нет (async) | ✅ Kafka producer | Локальный буфер памяти |
