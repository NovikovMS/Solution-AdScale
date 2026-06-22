# Отказоустойчивость данных

## RPO и RTO по сервисам

**RPO (Recovery Point Objective)** — максимально допустимая потеря данных при сбое (сколько данных можем потерять).  
**RTO (Recovery Time Objective)** — максимально допустимое время восстановления работоспособности.

| Сервис | RPO | RTO | Обоснование |
|---|---|---|---|
| Bidding Service (BiddingDB) | 1 час | 5 минут | Конфигурация бизнес-правил; потеря часа — допустима (восстановим из бэкапа) |
| Campaign Service (CampaignDB) | 15 минут | 10 минут | Кампании — основной бизнес-актив; умеренная потеря допустима |
| Budget Service (BudgetDB) | 1 минута | 2 минуты | Бюджеты критичны: потеря резервирований = финансовые расхождения |
| Statistics Service (ClickHouse) | 1 час | 30 минут | Статистика может быть пересчитана из Kafka (replay 7 дней) |
| Analytics Service (ClickHouse) | 4 часа | 1 час | Аналитика не критична; витрины перестраиваются из Statistics |
| Financial Service (FinanceDB) | 0 (zero data loss) | 5 минут | Финансовые данные нельзя терять — synchronous replication |
| Redis Cluster | N/A | 2 минуты | Redis — кэш, не первичное хранилище; данные перегреваются из БД |
| Kafka Cluster | 0 (at-least-once) | 5 минут | Все события хранятся с retention; replication factor = 3 |

---

## Стратегия резервного копирования

### PostgreSQL (Bidding, Campaign, Budget, Financial)

**Базовый бэкап (Base Backup):**

```
Расписание:  ежедневно в 02:00 UTC
Метод:       pg_basebackup (физический бэкап)
Хранение:    7 дней ежедневных бэкапов + 4 недели еженедельных
Место:       S3-совместимое хранилище (отдельный регион/зона)
```

**Point-in-Time Recovery (PITR) через WAL архивирование:**

```
WAL архивируется непрерывно в S3
Позволяет восстановить БД на любой момент времени за последние 7 дней
```

PITR особенно важен для Financial Service: при обнаружении ошибки в обработке транзакций можно восстановить состояние до конкретного момента.

**Тестирование восстановления:**

Раз в неделю автоматически запускается процедура восстановления из бэкапа в изолированном окружении. Результат (успех/неудача, время восстановления) записывается в мониторинг. Это гарантирует, что бэкапы реально работают, а не просто создаются.

### ClickHouse (Statistics, Analytics)

**Резервное копирование:**

```
Метод:       clickhouse-backup (инкрементальный бэкап по партициям)
Расписание:  ежедневно
Хранение:    14 дней
Место:       S3-совместимое хранилище
```

**Восстановление через Kafka Replay:**

Для Statistics Service полное восстановление из Kafka возможно в пределах 7 дней (retention топиков `ad.impressions`, `ad.clicks`, `auction.win-loss`). Если потеря данных не превышает 7 дней — выгоднее переиграть события из Kafka, чем восстанавливать из бэкапа.

### Kafka Cluster

Kafka не требует классического бэкапа в традиционном смысле:
- Replication factor = 3 защищает от потери одного брокера.
- При потере всего кластера данные восстанавливаются из source систем (повторная публикация) в пределах retention window.

Для долгосрочного архивирования финансовых событий настроен **Kafka MirrorMaker** или **S3 Sink Connector** (Kafka Connect), который архивирует топик `financial.events` в S3 на срок хранения 1 год.

### Redis Cluster

Redis не является первичным хранилищем. Восстановление после полного сбоя — cache warming из PostgreSQL (Campaign Service, Budget Service, Bidding Service). Время восстановления — 2 минуты (прогрев топ-кампаний).

Включена RDB snapshots (раз в 5 минут) для ускорения перезапуска подов, но не как механизм disaster recovery.

---

## Соответствие принципам 12-Factor App

| Фактор | Применение в AdScale |
|---|---|
| **I. Codebase** | Каждый сервис — отдельный репозиторий или монорепо с чёткими границами. Единая кодовая база на сервис, несколько окружений (dev, staging, prod). |
| **II. Dependencies** | Все зависимости объявлены явно (go.mod, requirements.txt, package.json). Нет зависимости на системные пакеты хоста. |
| **III. Config** | Конфигурация (DSN БД, Redis endpoint, Kafka brokers, API ключи) передаётся через переменные окружения или Kubernetes Secrets/ConfigMaps. Нет конфигурации в коде. |
| **IV. Backing Services** | PostgreSQL, Redis, Kafka — backing services, подключаются через URL из окружения. Сервис не знает, где физически запущена БД. |
| **V. Build, Release, Run** | CI/CD пайплайн: build (Docker image) → release (тег + конфигурация) → run (Kubernetes deployment). Этапы строго разделены. |
| **VI. Processes** | Все сервисы stateless: состояние хранится в Redis, PostgreSQL, Kafka. Горизонтальное масштабирование через добавление подов без изменения кода. |
| **VII. Port Binding** | Каждый сервис самостоятельно слушает порт (gRPC :50051, HTTP :8080). Не требует внешнего сервера приложений. |
| **VIII. Concurrency** | Масштабирование через добавление реплик (K8s HPA). Bidding Service и Ad Server масштабируются горизонтально по метрике RPS. |
| **IX. Disposability** | Поды запускаются быстро (< 10 секунд), graceful shutdown обрабатывает in-flight запросы. Kafka consumer корректно коммитит offset перед остановкой. |
| **X. Dev/Prod Parity** | Dev и prod окружения максимально близки: одинаковые Docker-образы, одинаковые версии PostgreSQL и Kafka в docker-compose для локальной разработки. |
| **XI. Logs** | Все сервисы пишут логи в stdout/stderr в формате JSON (structured logging). Агрегация — через Kubernetes log collector (Fluentd/Loki). Нет записи в файлы. |
| **XII. Admin Processes** | Миграции БД (Flyway/Liquibase) запускаются как отдельные Job в Kubernetes до деплоя сервиса. Не встроены в процесс запуска сервиса. |

---

## Дополнительные меры отказоустойчивости

### Kubernetes Pod Disruption Budget

Для критических сервисов (Bidding Service, Ad Server, DSP API Gateway) настроен PodDisruptionBudget:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: bidding-service-pdb
spec:
  minAvailable: 2   # минимум 2 пода работают в любой момент
  selector:
    matchLabels:
      app: bidding-service
```

Это предотвращает одновременную остановку всех реплик при rolling update или обслуживании узла.

### Health Checks

Все сервисы реализуют:

- **Liveness probe:** `/healthz` — сервис жив и не завис. При неответе Kubernetes перезапускает под.
- **Readiness probe:** `/readyz` — сервис готов принимать трафик (Redis доступен, БД-пул инициализирован, кэш прогрет). При неответе под исключается из балансировки.

### Graceful Shutdown

При получении SIGTERM сервис:

1. Прекращает принимать новые запросы (readiness → NOT READY).
2. Дожидается завершения in-flight запросов (timeout: 30 секунд).
3. Коммитит Kafka offset (для consumer-сервисов).
4. Закрывает соединения с БД и Redis.
5. Завершает процесс.

Это обеспечивает zero-downtime деплоя при rolling update в Kubernetes.
