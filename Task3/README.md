# Task3. Данные, масштабирование и отказоустойчивость

## Цель

Спроектировать стратегию работы с данными в микросервисной архитектуре AdScale: выбор БД для каждого сервиса, масштабирование хранилищ, кэширование, потоковая обработка событий и отказоустойчивость данных.

## Состав артефактов

- `database-strategy.md` — выбор типа БД для каждого сервиса с обоснованием (PostgreSQL, ClickHouse, Redis).
- `scaling.md` — стратегии репликации (Master-Slave, Sync Standby), шардирования (ClickHouse), CQRS (Campaign Service).
- `caching.md` — Redis Cluster: что кэшировать, TTL по типу ключа, event-driven инвалидация через Kafka, cache warming при старте.
- `event-streaming.md` — топики Kafka, Avro-схемы событий, consumer groups, политики хранения.
- `failover.md` — RPO/RTO по сервисам, стратегия бэкапов (pg_basebackup + WAL/PITR, ClickHouse backup, Kafka retention), соответствие 12-factor.
- `diagrams/` — [диаграмма архитектуры данных](diagrams/data-architecture.puml).

## Связь с предыдущими заданиями

- `../Task1/TO-BE.md` — высокоуровневый перечень БД по сервисам.
- `../Task1/adr/ADR-003` — обоснование выбора Kafka для event streaming.
- `../Task2/bidding-service.md` — Redis-структуры и модель данных Bidding Service.
- `../Task2/reliability.md` — fallback-стратегии при недоступности кэша и БД.

## Краткий вывод

Каждый сервис владеет собственной БД. PostgreSQL используется для транзакционных данных (кампании, финансы, бюджеты, конфигурация ставок), ClickHouse — для event-аналитики и статистики. Redis Cluster обслуживает hot path RTB без SQL. Kafka обеспечивает асинхронную обработку всех событий с retention 7–30 дней. Financial Service использует synchronous replication с RPO = 0. Statistics Service восстанавливается через Kafka replay без потери данных.
