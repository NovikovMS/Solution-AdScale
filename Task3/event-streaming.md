# Архитектура потоковой обработки событий (Kafka)

## Обоснование выбора Kafka

Обоснование выбора Apache Kafka задокументировано в `../Task1/adr/ADR-003-kafka-event-streaming.md`. Ключевые причины: отделение write-нагрузки от RTB hot path, гарантия at-least-once delivery, replay событий, независимые consumer groups для разных потребителей.

---

## Топология кластера

**Конфигурация:** 3 брокера (минимум для отказоустойчивости) + ZooKeeper или KRaft (Kafka 3.x без ZooKeeper).

```
┌──────────┐  ┌──────────┐  ┌──────────┐
│ Broker 1 │  │ Broker 2 │  │ Broker 3 │
│ Leader   │  │ Follower │  │ Follower │
└──────────┘  └──────────┘  └──────────┘
  replication factor = 3, min.insync.replicas = 2
```

`min.insync.replicas = 2`: сообщение считается записанным только когда 2 из 3 брокеров подтвердили запись. Это обеспечивает durability при потере одного брокера.

---

## Топики

### `ad.impressions`

| Параметр | Значение |
|---|---|
| Назначение | Событие показа рекламного баннера пользователю |
| Производители | Event Sink Service |
| Потребители | Statistics Service (group: `stats-consumer`), Analytics Service (group: `analytics-consumer`) |
| Партиции | 12 (масштабируется с ростом RPS) |
| Ключ партиционирования | `campaign_id` — события одной кампании в одной партиции |
| Retention | 7 дней |
| Replication factor | 3 |

**Ключ партиционирования по `campaign_id`:** гарантирует упорядоченность событий одной кампании и позволяет Statistics Service агрегировать без распределённых джойнов.

### `ad.clicks`

| Параметр | Значение |
|---|---|
| Назначение | Событие клика пользователя по баннеру |
| Производители | Event Sink Service |
| Потребители | Statistics Service (group: `stats-consumer`), Analytics Service (group: `analytics-consumer`) |
| Партиции | 6 |
| Ключ партиционирования | `campaign_id` |
| Retention | 7 дней |
| Replication factor | 3 |

### `auction.win-loss`

| Параметр | Значение |
|---|---|
| Назначение | Результат аукциона: победитель, цена клиринга, проигравшие кандидаты |
| Производители | Bidding Service |
| Потребители | Statistics Service (group: `stats-consumer`), Financial Service (group: `finance-consumer`) |
| Партиции | 12 |
| Ключ партиционирования | `campaign_id` |
| Retention | 7 дней |
| Replication factor | 3 |

### `campaign.changed`

| Параметр | Значение |
|---|---|
| Назначение | Изменение кампании, ставки или таргетинга — для инвалидации кэша |
| Производители | Campaign Service |
| Потребители | Cache Invalidation Worker (group: `cache-invalidation`), Bidding Service (group: `bidding-config-refresh`) |
| Партиции | 3 |
| Ключ партиционирования | `campaign_id` |
| Retention | 1 день (события короткоживущие) |
| Replication factor | 3 |

### `financial.events`

| Параметр | Значение |
|---|---|
| Назначение | Финансовые события: списания, пополнения, сверки |
| Производители | Financial Service |
| Потребители | Analytics Service (group: `analytics-consumer`), Budget Service (group: `budget-sync`) |
| Партиции | 3 |
| Ключ партиционирования | `advertiser_id` |
| Retention | 30 дней (финансовый аудит) |
| Replication factor | 3 |

---

## Схемы событий (Avro)

Schema Registry используется для контроля совместимости схем между производителями и потребителями. Стратегия совместимости: `BACKWARD` — новые потребители могут читать старые сообщения.

### Событие показа (`ad.impressions`)

```json
{
  "type": "record",
  "name": "AdImpressionEvent",
  "namespace": "com.adscale.events.v1",
  "fields": [
    { "name": "event_id",     "type": "string",  "doc": "UUID события" },
    { "name": "request_id",   "type": "string",  "doc": "ID bid request" },
    { "name": "campaign_id",  "type": "string",  "doc": "ID кампании" },
    { "name": "ad_id",        "type": "string",  "doc": "ID объявления" },
    { "name": "user_id",      "type": "string",  "doc": "Анонимный ID пользователя" },
    { "name": "site_id",      "type": "string",  "doc": "ID сайта/приложения" },
    { "name": "placement_id", "type": "string",  "doc": "ID места размещения" },
    { "name": "geo",          "type": "string",  "doc": "Страна/регион" },
    { "name": "device_type",  "type": "string",  "doc": "desktop / mobile / tablet" },
    { "name": "created_at",   "type": "long",    "logicalType": "timestamp-millis" }
  ]
}
```

### Событие клика (`ad.clicks`)

```json
{
  "type": "record",
  "name": "AdClickEvent",
  "namespace": "com.adscale.events.v1",
  "fields": [
    { "name": "event_id",       "type": "string" },
    { "name": "impression_id",  "type": "string", "doc": "Ссылка на событие показа" },
    { "name": "campaign_id",    "type": "string" },
    { "name": "ad_id",          "type": "string" },
    { "name": "user_id",        "type": "string" },
    { "name": "created_at",     "type": "long", "logicalType": "timestamp-millis" }
  ]
}
```

### Результат аукциона (`auction.win-loss`)

```json
{
  "type": "record",
  "name": "AuctionResultEvent",
  "namespace": "com.adscale.events.v1",
  "fields": [
    { "name": "result_id",        "type": "string" },
    { "name": "request_id",       "type": "string" },
    { "name": "winner_campaign_id","type": ["null", "string"], "default": null },
    { "name": "winner_ad_id",     "type": ["null", "string"], "default": null },
    { "name": "final_bid",        "type": ["null", "double"], "default": null },
    { "name": "clearing_price",   "type": ["null", "double"], "default": null },
    { "name": "status",           "type": {
        "type": "enum",
        "name": "AuctionStatus",
        "symbols": ["WIN", "NO_BID", "TIMEOUT", "CANCELED"]
      }
    },
    { "name": "latency_ms",       "type": "int" },
    { "name": "created_at",       "type": "long", "logicalType": "timestamp-millis" }
  ]
}
```

### Изменение кампании (`campaign.changed`)

```json
{
  "type": "record",
  "name": "CampaignChangedEvent",
  "namespace": "com.adscale.events.v1",
  "fields": [
    { "name": "event_id",       "type": "string" },
    { "name": "campaign_id",    "type": "string" },
    { "name": "change_type",    "type": {
        "type": "enum",
        "name": "ChangeType",
        "symbols": ["CREATED", "UPDATED", "PAUSED", "DELETED"]
      }
    },
    { "name": "changed_fields", "type": { "type": "array", "items": "string" } },
    { "name": "created_at",     "type": "long", "logicalType": "timestamp-millis" }
  ]
}
```

---

## Consumer Groups

| Consumer Group | Топики | Сервис | Обработка |
|---|---|---|---|
| `stats-consumer` | `ad.impressions`, `ad.clicks`, `auction.win-loss` | Statistics Service | Batch aggregation → ClickHouse |
| `analytics-consumer` | `ad.impressions`, `ad.clicks`, `financial.events` | Analytics Service | Materialized views → ClickHouse |
| `finance-consumer` | `auction.win-loss` | Financial Service | Debit по auction win |
| `budget-sync` | `financial.events` | Budget Service | Сверка доступного бюджета |
| `cache-invalidation` | `campaign.changed` | Cache Worker | DEL Redis ключей кампании |
| `bidding-config-refresh` | `campaign.changed` | Bidding Service | DEL bid:config Redis ключа |

---

## Политика хранения (Retention)

| Топик | Retention | Обоснование |
|---|---|---|
| `ad.impressions` | 7 дней | Достаточно для replay при сбое Statistics Service |
| `ad.clicks` | 7 дней | Аналогично показам |
| `auction.win-loss` | 7 дней | Достаточно для финансовой сверки |
| `campaign.changed` | 1 день | Краткоживущие events для инвалидации кэша |
| `financial.events` | 30 дней | Финансовый аудит требует длительного хранения |

---

## Мониторинг

| Метрика | Алерт |
|---|---|
| `kafka_consumer_lag{group, topic}` | > 10 000 сообщений → warning, > 100 000 → critical |
| `kafka_producer_error_rate` | > 0.1% → warning |
| `kafka_broker_under_replicated_partitions` | > 0 → critical |
| `kafka_topic_bytes_in_per_sec` | резкий рост → warning |
