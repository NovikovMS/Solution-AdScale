# Масштабирование баз данных

## Стратегии масштабирования

Выбор стратегии для каждого сервиса определяется паттерном нагрузки:
- **Репликация Master-Slave** — разделение read и write потоков.
- **Шардирование** — горизонтальное разделение данных по ключу для write-интенсивных сервисов.
- **CQRS** — разделение модели команд (writes) и запросов (reads) при сильно различающихся требованиях.
- **Native clustering** — ClickHouse управляет масштабированием встроенными средствами.

---

## Bidding Service — PostgreSQL

**Стратегия: Master + Read Replica**

BiddingDB хранит конфигурацию бизнес-правил. Конфигурация читается при cache miss (редко), пишется при обновлении правил кампании (ещё реже). Объём данных небольшой.

```
┌─────────────────┐
│  BiddingDB      │
│   Master        │ ← write: обновление bid config
└────────┬────────┘
         │ streaming replication
         ▼
┌─────────────────┐
│  Read Replica   │ ← read: cache miss fallback
└─────────────────┘
```

- Write: изменения конфигурации идут в Master.
- Read: cache miss читает из Read Replica, чтобы не нагружать Master.
- Шардирование не нужно: объём данных (конфигурации кампаний) не требует горизонтального разбиения.

---

## Campaign Service — PostgreSQL

**Стратегия: Master-Slave + Партиционирование + CQRS**

Campaign Service имеет два принципиально разных потока:
- **Команды (write):** рекламодатель создаёт/редактирует кампании — через Dashboard, нечастые, транзакционные.
- **Запросы (read):** Ad Server читает данные кампаний для подбора кандидатов — через Redis, при cache miss — из БД напрямую.

**Репликация:**

```
┌─────────────────┐
│  CampaignDB     │
│   Master        │ ← write: CRUD кампаний, ставок, таргетинга
└────────┬────────┘
         │ streaming replication
         ▼
┌─────────────────┐
│  Read Replica   │ ← read: cache miss, отчёты Dashboard
└─────────────────┘
```

**CQRS:**

Read model и Write model разделены:

| | Write Model | Read Model |
|---|---|---|
| Хранение | Master (нормализованная схема) | Read Replica или Redis (денормализованный формат) |
| Обновление | Транзакция в Master | Event-driven через `campaign.changed` Kafka → обновление Redis |
| Потребитель | Dashboard (write ops) | Ad Server (read, через Redis) |

Это позволяет оптимизировать Read Model под паттерн чтения Ad Server (flat структура по `campaign_id`), не усложняя Write Model.

**Партиционирование таблицы кампаний:**

```sql
-- Range partitioning по дате создания (для архивирования старых кампаний)
CREATE TABLE campaigns (
    campaign_id   UUID,
    advertiser_id UUID,
    status        VARCHAR,
    created_at    TIMESTAMP,
    ...
) PARTITION BY RANGE (created_at);

-- Партиции по кварталам
CREATE TABLE campaigns_2025_q1 PARTITION OF campaigns
    FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');
```

Партиционирование ускоряет запросы с фильтром по дате и упрощает архивирование неактивных кампаний.

---

## Budget Service — PostgreSQL

**Стратегия: Master + Read Replica + оптимистичные локи**

Бюджеты требуют строгой консистентности, поэтому шардирование не применяется — распределённые транзакции между шардами недопустимы по сложности и latency.

```
┌─────────────────┐
│  BudgetDB       │
│   Master        │ ← write: резервирование и списание бюджета
└────────┬────────┘
         │ replication
         ▼
┌─────────────────┐
│  Read Replica   │ ← read: текущий баланс для отображения в Dashboard
└─────────────────┘
```

RTB hot path читает бюджет **только из Redis**, не из БД. Read Replica используется только для Dashboard (отображение баланса рекламодателю).

---

## Statistics Service — ClickHouse

**Стратегия: ReplicatedMergeTree + Distributed tables**

ClickHouse нативно поддерживает горизонтальное масштабирование. Write-нагрузка (события из Kafka) распределяется между шардами автоматически.

**Схема кластера:**

```
Kafka Consumer
      │
      │ bulk insert (батчи по 10 000 событий)
      ▼
┌─────────────────────────────────────────────┐
│           ClickHouse Cluster                │
│  ┌──────────────┐    ┌──────────────┐       │
│  │  Shard 1     │    │  Shard 2     │  ...  │
│  │  Replica A   │    │  Replica A   │       │
│  │  Replica B   │    │  Replica B   │       │
│  └──────────────┘    └──────────────┘       │
│                                             │
│  Distributed table — единая точка запросов  │
└─────────────────────────────────────────────┘
```

**Ключ шардирования:** `campaign_id` — большинство аналитических запросов фильтруются по кампании. Равномерность обеспечивается хэш-функцией: `sipHash64(campaign_id) % num_shards`.

**Движок таблицы:**

```sql
CREATE TABLE ad_events ON CLUSTER stats_cluster (
    event_id     UUID,
    event_type   Enum8('показ' = 1, 'клик' = 2, 'win' = 3, 'loss' = 4),
    campaign_id  UUID,
    ad_id        UUID,
    user_id      String,
    created_at   DateTime
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ad_events', '{replica}')
PARTITION BY toYYYYMM(created_at)
ORDER BY (campaign_id, created_at)
SETTINGS index_granularity = 8192;
```

Партиционирование по месяцу (`toYYYYMM`) + сортировка по `(campaign_id, created_at)` обеспечивают эффективные запросы по кампании за период.

---

## Analytics Service — ClickHouse / DWH

**Стратегия: Materialized Views + Read Replicas**

Analytics Service строит агрегированные витрины поверх raw events из Statistics.

```sql
-- Ежедневная витрина показателей кампании
CREATE MATERIALIZED VIEW campaign_daily_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (campaign_id, event_date)
AS SELECT
    campaign_id,
    toDate(created_at)   AS event_date,
    countIf(event_type = 'показ') AS impressions,
    countIf(event_type = 'клик')  AS clicks,
    countIf(event_type = 'win')   AS wins
FROM ad_events
GROUP BY campaign_id, event_date;
```

**Read Replicas для аналитики:** тяжёлые пользовательские запросы направляются на Read Replica, не на Primary — изолируя write-нагрузку от аналитических запросов.

---

## Financial Service — PostgreSQL

**Стратегия: Master-Slave без шардирования**

Финансовые данные требуют строгой ACID-консистентности. Шардирование создаёт необходимость в распределённых транзакциях, что недопустимо для финансовой логики.

```
┌─────────────────┐
│  FinanceDB      │
│   Master        │ ← все write: списания, пополнения, счета
└────────┬────────┘
         │ replication (synchronous для финансов)
         ▼
┌─────────────────┐
│  Standby        │ ← hot standby для failover; read — только для аудит-запросов
└─────────────────┘
```

**Synchronous replication** для Finance DB: транзакция подтверждается только после записи на Standby. Это увеличивает latency write-операций, но гарантирует нулевую потерю данных при отказе Primary.

---

## Сводная таблица стратегий

| Сервис | Репликация | Шардирование | CQRS | Примечание |
|---|---|---|---|---|
| Bidding Service | Master + Read Replica | ❌ | ❌ | Данные в Redis; DB — только конфиг |
| Campaign Service | Master + Read Replica | Партиционирование | ✅ | Read model через Redis + Replica |
| Budget Service | Master + Read Replica | ❌ | ❌ | Hot path только через Redis |
| Statistics Service | ClickHouse native | ✅ по campaign_id | ❌ | ReplicatedMergeTree |
| Analytics Service | ClickHouse Read Replica | ✅ (inherited) | ❌ | Materialized views |
| Financial Service | Master + Sync Standby | ❌ | ❌ | Synchronous replication |
