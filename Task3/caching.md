# Стратегия кэширования

## Обзор

Redis Cluster используется как единый кэш горячих данных для всего RTB hot path. Основная цель — убрать синхронные SQL-запросы из критического пути аукциона. Детализация Redis-структур для Bidding Service также описана в `../Task2/bidding-service.md`.

**Размещение:** Redis Cluster развёртывается в той же сети, что и сервисы (Kubernetes cluster), чтобы минимизировать сетевую задержку. Целевой P99 для Redis GET — менее 1 ms.

**Топология:** Redis Cluster в режиме sharded cluster с репликацией (минимум 3 master + 3 replica). Это обеспечивает отказоустойчивость при потере одного узла и горизонтальное масштабирование при росте нагрузки.

---

## Что кэшируется

### 1. Конфигурация кампаний (Campaign Service → Redis)

Данные подбора кандидатов: таргетинг, ставки, статус кампании. Читаются Ad Server на каждый bid request.

| Ключ | Тип | Содержимое |
|---|---|---|
| `campaign:config:{campaign_id}` | Hash | `status`, `bid_price`, `targeting_geo`, `targeting_device`, `start_at`, `end_at` |
| `campaign:active:set` | Set | Множество `campaign_id` с активными кампаниями (для быстрой фильтрации) |

**TTL:** 60 секунд.

**Почему 60 секунд:** рекламодатели меняют настройки кампаний редко; задержка инвалидации в 60 секунд допустима. Более короткий TTL увеличивает нагрузку на Campaign Service при cache miss.

### 2. Конфигурация ставок (Bidding Service → Redis)

Бизнес-правила аукциона: множители, floor prices, тип аукциона. Читаются Bidding Service на каждый Evaluate.

| Ключ | Тип | Содержимое |
|---|---|---|
| `bid:config:{campaign_id}` | Hash | `bid_multiplier`, `floor_price`, `auction_type`, `priority` |

**TTL:** 60 секунд.

### 3. Бюджетные лимиты (Budget Service → Redis)

Доступный бюджет кампании. Читается Bidding Service как fallback при недоступности Budget Service.

| Ключ | Тип | Содержимое |
|---|---|---|
| `budget:available:{campaign_id}` | String | Доступный бюджет в центах (integer) |

**TTL:** 5 секунд — короткий TTL обеспечивает актуальность лимитов; устаревший лимит на 5 секунд допустим (возможен незначительный перерасход, но не катастрофический).

### 4. Frequency Cap (Bidding Service → Redis)

Счётчик показов одного объявления конкретному пользователю. Используется для ограничения назойливости рекламы.

| Ключ | Тип | Команда | Содержимое |
|---|---|---|---|
| `freq:cap:{user_id}:{campaign_id}` | String (counter) | INCR / GET | Количество показов в текущем часе |

**TTL:** 3600 секунд (1 час).

**Атомарность:** Redis INCR атомарен — race condition при параллельных запросах исключён.

### 5. Targeting данные (Ad Server → Redis)

Пользовательский профиль и данные таргетинга. Читается Ad Server для идентификации пользователя.

| Ключ | Тип | Содержимое |
|---|---|---|
| `user:profile:{user_id}` | Hash | `geo`, `device_type`, `language`, `segments` |

**TTL:** 300 секунд (5 минут).

---

## TTL — сводная таблица

| Ключ | TTL | Обоснование |
|---|---|---|
| `campaign:config:{id}` | 60 s | Редкие обновления; задержка допустима |
| `campaign:active:set` | 60 s | Согласовано с campaign config |
| `bid:config:{id}` | 60 s | Редкие обновления конфигурации |
| `budget:available:{id}` | 5 s | Бюджет критичен, нужна актуальность |
| `freq:cap:{user}:{campaign}` | 3600 s | Окно frequency cap = 1 час |
| `user:profile:{id}` | 300 s | Данные пользователя меняются редко |

---

## Стратегия инвалидации

Используется **event-driven cache invalidation**: при изменении данных в БД сервис-владелец публикует domain event в Kafka. Подписчики обновляют или удаляют ключи в Redis.

### Campaign changed

```
Рекламодатель обновляет кампанию
        │
        ▼
Campaign Service записывает в CampaignDB
        │
        ▼
Campaign Service публикует в Kafka: campaign.changed
  { "campaign_id": "...", "changed_fields": ["bid_price", "status"] }
        │
        ▼ (consumer group: cache-invalidation)
Cache Invalidation Worker
        │
        ├── DEL campaign:config:{campaign_id}
        └── SREM campaign:active:set {campaign_id}  (если status = PAUSED/DELETED)
```

При следующем обращении Ad Server получает cache miss и заполняет кэш актуальными данными из Campaign Service.

### Budget changed

Budget Service обновляет `budget:available:{campaign_id}` напрямую после каждого успешного резервирования (write-through). TTL 5 секунд служит страховкой при сбое.

### Bid config changed

Bidding Service подписывается на `campaign.changed` и инвалидирует `bid:config:{campaign_id}`.

---

## Cache Warming (прогрев кэша)

При холодном старте сервиса или после сброса кэша Redis может вызвать лавину cache miss и перегрузить Campaign Service и BiddingDB.

### Стратегия прогрева при старте

**Campaign Service** при старте выполняет bulk load активных кампаний в Redis:

```
1. Campaign Service запускается
2. Запрашивает из CampaignDB все активные кампании (status = ACTIVE)
3. Записывает в Redis батчами по 500 ключей через PIPELINE
4. Ставит флаг ready=true (Kubernetes readiness probe)
5. Начинает обрабатывать входящий трафик
```

**Ad Server и Bidding Service** не имеют readiness зависимости от Redis — они используют Redis как optional cache. При cache miss они обращаются к Campaign Service напрямую.

### Стратегия прогрева при rolling update

Kubernetes rolling update запускает новый под до остановки старого. Новый под прогревает свою часть кэша, пока старый ещё обслуживает трафик. Перехлёст обеспечивает отсутствие периода холодного кэша.

### Приоритизация прогрева

Сначала прогреваются кампании с наибольшим бюджетом и наивысшим приоритетом (top-N по `bid_price DESC`), так как именно они чаще всего участвуют в аукционах.

---

## Мониторинг кэша

| Метрика | Алерт |
|---|---|
| `redis_cache_hit_ratio{key_prefix="campaign:config"}` | < 95% → warning |
| `redis_cache_hit_ratio{key_prefix="bid:config"}` | < 95% → warning |
| `redis_memory_used_bytes` | > 80% max memory → warning |
| `redis_connected_clients` | > threshold → warning |
| `adserver_campaign_cache_miss_total` | резкий рост → critical |
