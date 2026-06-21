# Database per Service — стратегия данных

## Принцип

Каждый сервис владеет собственной базой данных. Другие сервисы не обращаются к ней напрямую — только через API сервиса-владельца. Это обеспечивает независимое масштабирование, изоляцию отказов и свободу выбора технологии под конкретный паттерн нагрузки.

Legacy PostgreSQL сохраняется на период миграции для функций, ещё не вынесенных из монолита. По мере декомпозиции нагрузка из неё выводится.

---

## Стратегия по сервисам

### Bidding Service → PostgreSQL + Redis

**Тип нагрузки:** смешанная. BiddingDB хранит конфигурацию бизнес-правил (редкие write, частые read). Redis обслуживает hot path (тысячи read/s).

**Выбор PostgreSQL для BiddingDB:**
- Конфигурация бизнес-правил требует транзакционных обновлений (изменение bid_multiplier, floor_price).
- Объём данных невелик: правила для кампаний, не события.
- ACID-гарантии важны: некорректная конфигурация прямо влияет на результат аукциона.

**Выбор Redis для hot data:**
- Синхронный SQL-запрос в RTB hot path недопустим по latency.
- Redis даёт P99 < 1 ms для операций GET/HGET.
- Данные в Redis — копия из BiddingDB с TTL; первичным источником правды остаётся PostgreSQL.

Подробная структура Redis-ключей описана в `caching.md` и `../Task2/bidding-service.md`.

---

### Campaign Service → PostgreSQL

**Тип нагрузки:** OLTP. Рекламодатели создают и редактируют кампании, таргетинг, ставки. Чтения — через Redis-кэш (hot path) или напрямую (dashboard).

**Выбор PostgreSQL:**
- Кампании содержат сложные связи: кампания → группы объявлений → объявления → таргетинг → ставки.
- Обновления требуют транзакционности (например, атомарное изменение ставки и таргетинга).
- Rich query capabilities: фильтрация по статусу, таргетингу, дате.
- Зрелая экосистема: поддерживается командой без специализированных знаний.

**Шардирование:** по `campaign_id` (advertiser_id как ключ шардирования на уровне партиционирования). Детали — в `scaling.md`.

---

### Budget Service → PostgreSQL + Redis

**Тип нагрузки:** высокочастотные read (проверка лимитов в RTB hot path) + транзакционные write (резервирование и списание бюджета).

**Выбор PostgreSQL:**
- Бюджеты требуют строгой консистентности: нельзя допустить двойного резервирования или перерасхода.
- Оптимистичные локи по `version` поддерживаются нативно.
- Объём записей небольшой: одна строка на кампанию для текущего состояния бюджета.

**Выбор Redis:**
- Кэшированный доступный бюджет с TTL 5 секунд используется как fallback при недоступности Budget Service.
- Счётчик frequency cap (`freq:cap:{user_id}:{campaign_id}`) требует атомарного INCREMENT и TTL — Redis INCR идеален.

---

### Statistics Service → ClickHouse

**Тип нагрузки:** write-heavy (потребление событий из Kafka), аналитические агрегации (GROUP BY campaign_id, date, event_type).

**Выбор ClickHouse:**
- ClickHouse — column-oriented OLAP БД, оптимизирована для аналитических агрегаций по большим объёмам данных.
- Высокая скорость bulk insert: Kafka consumer пишет батчами, что соответствует паттерну ClickHouse (MergeTree engine).
- COUNT, SUM, GROUP BY по миллионам строк выполняется на порядок быстрее, чем в PostgreSQL.
- Встроенная репликация и шардирование (ReplicatedMergeTree + Distributed tables).

**Почему не PostgreSQL для статистики:**
- PostgreSQL при write-heavy нагрузке (тысячи INSERT/s из Kafka) конкурирует за WAL и создаёт I/O давление.
- Аналитические запросы в PostgreSQL медленнее из-за row-oriented хранения.
- Именно эта конкуренция является узким местом текущей архитектуры.

---

### Analytics Service → ClickHouse / DWH

**Тип нагрузки:** read-heavy. Тяжёлые аналитические запросы рекламодателей (отчёты за период, воронки, cohort analysis).

**Выбор ClickHouse как первый этап:**
- Единый кластер ClickHouse может обслуживать как Statistics Service (raw events), так и Analytics Service (витрины).
- Разделение достигается через отдельные базы/таблицы: `stats_raw` для Statistics и `analytics_views` для Analytics.
- Analytics Service строит materialized views и агрегированные витрины поверх raw events.

**Выбор DWH (ClickHouse Cloud / Apache Doris) как целевой этап (через год):**
- При росте до 50 000 RPS объём raw events станет слишком большим для одного кластера.
- Полноценный DWH с ETL-пайплайном обеспечивает лучшее разделение operational и analytical workloads.
- Apache Airflow или Spark для трансформаций.

---

### Financial Service → PostgreSQL

**Тип нагрузки:** OLTP с требованием строгой транзакционности. Списания, пополнения, сверки, счета.

**Выбор PostgreSQL:**
- Финансовые операции требуют ACID: нельзя потерять транзакцию или допустить двойное списание.
- Idempotency Key хранится в той же БД что и баланс — атомарность без распределённых транзакций.
- Аудит-лог (все проводки) — стандартный append-only паттерн, хорошо работает в PostgreSQL.
- PostgreSQL поддерживает FOR UPDATE / SKIP LOCKED для очередей финансовых операций.

**Почему не NewSQL / NoSQL:**
- Финансовые данные не нуждаются в горизонтальном масштабировании write (объём транзакций невысок).
- Eventual consistency недопустима для балансов.

---

## Сводная таблица

| Сервис | БД | Тип | Обоснование |
|---|---|---|---|
| Bidding Service | PostgreSQL + Redis | OLTP + кэш | ACID для конфигурации, Redis для hot path |
| Campaign Service | PostgreSQL | OLTP | Сложные связи, транзакционные обновления |
| Budget Service | PostgreSQL + Redis | OLTP + кэш | Транзакционные резервирования, Redis для frequency cap |
| Statistics Service | ClickHouse | OLAP | Column-oriented, bulk insert из Kafka, быстрые агрегации |
| Analytics Service | ClickHouse / DWH | OLAP | Тяжёлые аналитические запросы, витрины данных |
| Financial Service | PostgreSQL | OLTP | ACID, idempotency, аудит-лог |
| Legacy Monolith | PostgreSQL (legacy) | OLTP | Временно, до завершения миграции |

---

## Изоляция данных в период миграции

На горизонте 3 месяцев Legacy PostgreSQL остаётся для функций, ещё не вынесенных из монолита. Новые сервисы (Bidding, Campaign, Budget, Financial) получают собственные PostgreSQL-инстанции. ClickHouse разворачивается как новый кластер — никакой нагрузки от него на Legacy PostgreSQL нет.

Это соответствует стратегии Strangler Fig Pattern: постепенный вывод данных из монолита без остановки бизнеса.
