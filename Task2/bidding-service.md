# Bidding Service — спецификация сервиса ставок

## Роль сервиса

Bidding Service — latency-critical компонент RTB-пути. Он принимает список кандидатов рекламы, подобранных Ad Server, применяет бизнес-правила взвешивания ставок и определяет победителя аукциона. Именно этот сервис непосредственно влияет на win rate платформы и выполнение SLA перед DSP-партнёром (P95 ≤ 100 ms, целевое значение около 80 ms).

Обоснование выделения в отдельный сервис и приоритетность этого шага задокументированы в `../Task1/adr/ADR-002-bidding-service-extraction.md`.

---

## Границы сервиса

### Что входит в Bidding Service

- Приём списка кандидатов рекламы от Ad Server.
- Применение бизнес-правил: bid multipliers, floor price, frequency cap.
- Взвешивание ставок с учётом контекста показа.
- Проверка доступного бюджета кампании (через Budget Service).
- Выбор победителя аукциона (First Price или Second Price — конфигурируется).
- Публикация события auction win/loss в Kafka.
- Возврат победителя аукциона в Delivery Service.

### Что не входит в Bidding Service

- Подбор кандидатов рекламы — это ответственность Ad Server.
- Формирование HTML/JS-разметки баннера — это ответственность Delivery Service.
- Управление кампаниями, ставками и таргетингом — это ответственность Campaign Service.
- Управление финансовыми балансами и списания — это ответственность Financial Service.
- Хранение и отображение статистики — это ответственность Statistics Service.

---

## API

Bidding Service предоставляет gRPC API. Протокол выбран из-за жёстких требований по latency: бинарная сериализация Protobuf быстрее JSON, HTTP/2 мультиплексирует запросы без повторного установления соединения, строгие контракты снижают вероятность ошибок интеграции.

### Protobuf-контракт

```protobuf
syntax = "proto3";

package bidding.v1;

service BiddingService {
  // Основной метод: взвешивание ставок и выбор победителя аукциона
  rpc Evaluate(EvaluateRequest) returns (EvaluateResponse);
}

message EvaluateRequest {
  string request_id    = 1; // уникальный ID bid request (для идемпотентности и трейсинга)
  string user_id       = 2; // идентификатор пользователя (анонимный или persistent)
  ImpContext context   = 3; // контекст показа
  repeated AdCandidate candidates = 4; // список кандидатов от Ad Server
}

message ImpContext {
  string site_id      = 1; // сайт или приложение, где показывается реклама
  string placement_id = 2; // конкретное место размещения
  string geo          = 3; // страна/регион пользователя
  string device_type  = 4; // desktop / mobile / tablet
  int64  timestamp_ms = 5; // unix timestamp запроса в ms
}

message AdCandidate {
  string campaign_id  = 1; // ID кампании
  string ad_id        = 2; // ID объявления
  double bid_price    = 3; // базовая ставка кампании (CPM, USD)
  double floor_price  = 4; // минимальная ставка для данного placement
}

message EvaluateResponse {
  string request_id   = 1;
  ResponseStatus status = 2;
  AdWinner winner     = 3; // заполнено при status = OK
}

enum ResponseStatus {
  OK          = 0; // победитель найден
  NO_BID      = 1; // ни один кандидат не прошёл бизнес-правила или бюджет исчерпан
  TIMEOUT     = 2; // сервис не уложился в deadline
  ERROR       = 3; // внутренняя ошибка
  CANCELED    = 4; // запрос отменён вызывающей стороной (deadline exceeded на уровне клиента)
}

message AdWinner {
  string campaign_id    = 1;
  string ad_id          = 2;
  double final_bid      = 3; // итоговая ставка после применения multipliers
  double clearing_price = 4; // цена клиринга (Second Price Auction)
}
```

### Таймауты

| Вызывающая сторона | Дедлайн на вызов Bidding Service |
|---|---|
| Ad Server (внутренний RTB) | 50 ms |
| DSP API Gateway (внешний OpenRTB) | 60 ms |

Bidding Service должен ответить до истечения дедлайна или вернуть `ResponseStatus.TIMEOUT` и дефолтную стратегию (см. reliability.md).

---

## Зависимости

| Зависимость | Протокол | Назначение | Поведение при недоступности |
|---|---|---|---|
| Redis Cluster | Redis protocol | Чтение hot data: bid multipliers, floor prices, campaign config | Fallback на BiddingDB (с увеличением latency) |
| Budget Service | gRPC | Проверка доступного бюджета кампании перед выбором победителя | Fallback: использовать кэшированные лимиты из Redis |
| BiddingDB | SQL / KV | Runtime-конфигурация бизнес-правил, индексы ставок | Fallback на кэш Redis; если недоступен — NO_BID |
| Kafka | Async producer | Публикация auction win/loss events | Fire-and-forget; при недоступности буферизация на стороне producer |
| Campaign Service | gRPC | Cache miss fallback: получить актуальные данные кампании | При недоступности: использовать устаревший кэш с TTL |

Bidding Service **не** делает синхронных запросов к Legacy PostgreSQL в hot path. Это главное архитектурное ограничение, обеспечивающее P95 ≤ 100 ms.

---

## Модель данных

Bidding Service хранит в BiddingDB только то, что необходимо для работы аукциона. Кампании, финансы и статистика хранятся в своих сервисах.

### BiddingConfig

Конфигурация бизнес-правил, которые применяются при взвешивании ставок.

| Поле | Тип | Описание |
|---|---|---|
| `config_id` | UUID | Первичный ключ |
| `campaign_id` | UUID | Кампания, к которой применяется конфигурация |
| `bid_multiplier` | DECIMAL | Коэффициент умножения базовой ставки |
| `floor_price_override` | DECIMAL | Переопределение floor price (опционально) |
| `auction_type` | ENUM | `FIRST_PRICE` / `SECOND_PRICE` |
| `frequency_cap_per_hour` | INT | Максимум показов одному пользователю в час |
| `priority` | INT | Приоритет кампании при одинаковых ставках |
| `valid_from` | TIMESTAMP | Начало действия конфигурации |
| `valid_until` | TIMESTAMP | Конец действия конфигурации |
| `updated_at` | TIMESTAMP | Время последнего обновления |

### AuctionResult

Архив результатов аукционов для аналитики и отладки. Пишется асинхронно, не в hot path.

| Поле | Тип | Описание |
|---|---|---|
| `result_id` | UUID | Первичный ключ |
| `request_id` | UUID | ID bid request (связь с трейсингом) |
| `winner_ad_id` | UUID | ID победившего объявления |
| `winner_campaign_id` | UUID | ID победившей кампании |
| `final_bid` | DECIMAL | Итоговая ставка победителя |
| `clearing_price` | DECIMAL | Цена клиринга |
| `status` | ENUM | `WIN` / `NO_BID` / `TIMEOUT` / `CANCELED` |
| `latency_ms` | INT | Время обработки в Bidding Service |
| `candidates_count` | INT | Количество кандидатов на входе |
| `created_at` | TIMESTAMP | Время создания записи |

### Redis-структуры (hot data)

| Ключ | Тип | TTL | Содержимое |
|---|---|---|---|
| `bid:config:{campaign_id}` | Hash | 60 s | `bid_multiplier`, `floor_price`, `auction_type`, `priority` |
| `budget:available:{campaign_id}` | String | 5 s | Доступный бюджет в центах (синхронизируется из Budget Service) |
| `freq:cap:{user_id}:{campaign_id}` | Counter | 3600 s | Счётчик показов для frequency cap |

---

## Нефункциональные требования

| Метрика | Цель (3 месяца) | Цель (1 год) |
|---|---|---|
| P95 latency (полный Evaluate) | ≤ 100 ms | ≤ 80 ms |
| P99 latency | ≤ 150 ms | ≤ 120 ms |
| Throughput | 18 000 RPS | 50 000 RPS |
| Доступность | 99,5% | 99,9% |
| Масштабирование | Горизонтальное (K8s HPA) | Горизонтальное + мультирегиональное |

---

## Метрики наблюдаемости

Bidding Service экспортирует в Prometheus:

- `bidding_evaluate_duration_seconds` — histogram P50/P95/P99 latency
- `bidding_evaluate_total{status}` — счётчик запросов по статусу (OK / NO_BID / TIMEOUT / ERROR)
- `bidding_cache_hit_ratio` — доля запросов, обслуженных из Redis без обращения к BiddingDB
- `bidding_budget_check_duration_seconds` — latency вызова Budget Service
- `bidding_kafka_publish_errors_total` — ошибки публикации win/loss событий
