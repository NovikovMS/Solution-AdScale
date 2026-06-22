# Схема взаимодействия и выбор протоколов

## Общие принципы

Все взаимодействия в AdScale делятся на три категории:

1. **RTB hot path** — синхронные вызовы в критическом пути аукциона. Требование: P95 ≤ 100 ms конец-в-конец. Любая задержка здесь — потерянный аукцион и выручка.
2. **Domain calls** — управление кампаниями, финансами, аналитикой. Требования по latency мягкие, важны надёжность и корректность данных.
3. **Event streaming** — асинхронная обработка показов, кликов и результатов аукциона. Не должна блокировать RTB-путь.

---

## Протоколы по типам взаимодействия

### RTB hot path: gRPC

**Сервисы:** DSP API Gateway → Ad Server → Bidding Service → Delivery Service

**Выбор: gRPC (HTTP/2 + Protobuf)**

| Критерий | gRPC | REST/JSON |
|---|---|---|
| Сериализация | Protobuf — бинарный формат, быстро | JSON — текстовый, медленнее |
| Соединение | HTTP/2 мультиплексирование, keep-alive | HTTP/1.1 — новое соединение или пул |
| Контракт | `.proto` — строго типизированный | OpenAPI — декларативный, но менее строгий |
| Latency | Ниже за счёт бинарной сериализации и HTTP/2 | Выше |
| Streaming | Двусторонний streaming из коробки | Требует WebSocket или SSE |

На горизонте 50 000 RPS разница в сериализации даёт заметный эффект. gRPC — правильный выбор для synchronous latency-critical вызовов.

**Таймауты и дедлайны:** gRPC поддерживает propagation deadline — deadline, установленный DSP API Gateway, автоматически передаётся во все нижестоящие вызовы. Если общий дедлайн исчерпан, сервис не тратит время на вызовы, которые уже не успеют.

### Domain calls: REST/HTTP

**Сервисы:** Advertiser Dashboard → Campaign Service / Financial Service / Analytics Service / Budget Service

**Выбор: REST (HTTP/1.1 или HTTP/2 + JSON)**

REST выбран для domain calls по следующим причинам:

- CRUD-операции (создание кампаний, управление бюджетами, просмотр отчётов) не требуют latency ниже 100 ms.
- REST лучше совместим с браузерными клиентами и внешними системами без gRPC-клиентов.
- JSON удобен для отладки и интеграций без кодогенерации.
- OpenAPI-спецификации легче читать не-техническим участникам (менеджмент, поддержка).

### Event streaming: Kafka

**Источники:** Delivery Service, Bidding Service, Financial Service  
**Потребители:** Statistics Service, Analytics Service, Financial Service

**Выбор: Apache Kafka**

Обоснование выбора Kafka подробно задокументировано в `../Task1/adr/ADR-003-kafka-event-streaming.md`. Кратко:

- Kafka отделяет производителей событий от потребителей — Delivery Service публикует событие показа и немедленно возвращает ответ пользователю, не ожидая обработки.
- Гарантия at-least-once delivery с хранением событий до 7 дней.
- Consumer groups позволяют Statistics Service и Analytics Service потреблять одни и те же события независимо.
- Replay событий позволяет перестроить агрегаты при сбое или изменении логики.

---

## Нужен ли API Gateway?

Да, DSP API Gateway необходим как отдельный компонент.

**Причины:**

1. Новый DSP-партнёр предъявляет жёсткие SLA (≤ 80 ms) и штрафы за нарушения. Нужен единый слой мониторинга latency и circuit breaker перед тем, как трафик достигнет Ad Server.
2. DSP-партнёры работают по OpenRTB, Ad Server работает по внутреннему протоколу — Gateway выполняет трансляцию и адаптацию.
3. Аутентификация, rate limiting и защита от DDoS должны быть на периметре, а не внутри Ad Server.
4. В будущем (через год) подключится несколько DSP-площадок — Gateway обеспечивает единый ingress с маршрутизацией по партнёру.

**Выбор технологии:** Envoy Proxy или NGINX (в зависимости от инфраструктуры). Envoy предпочтительнее, так как нативно интегрируется с Kubernetes service mesh, поддерживает gRPC transcoding и имеет богатые возможности наблюдаемости.

Детальный дизайн Gateway описан в `api-gateway.md`.

---

## Схема взаимодействия

### RTB hot path (синхронный путь)

```
DSP-партнёр / Пользователь сайта
        │
        ▼  HTTPS / OpenRTB
┌─────────────────┐
│  DSP API Gateway│  ← auth, rate limit, circuit breaker
└────────┬────────┘
         │  HTTP / gRPC
         ▼
┌─────────────────┐
│    Ad Server    │  ← определяет пользователя, подбирает кандидатов
└────────┬────────┘  ← читает из Redis (targeting, кампании)
         │  gRPC (deadline propagation)
         ▼
┌─────────────────┐
│ Bidding Service │  ← взвешивает ставки, проверяет бюджет, выбирает победителя
└────────┬────────┘  ← читает из Redis (bid config, budget)
         │  gRPC
         ▼
┌─────────────────┐
│Delivery Service │  ← формирует HTML/JS баннер и HTTP response
└────────┬────────┘
         │  HTTPS
         ▼
  Пользователь / DSP получает ответ
         │
         │  Async (после ответа)
         ▼
┌────────────────────┐
│ Event Sink / Kafka │  ← показы, клики, win/loss
└────────────────────┘
```

### Поток событий (асинхронный путь)

```
Delivery Service ──► Event Sink Service ──► Kafka (ad.impressions, ad.clicks)
Bidding Service  ──────────────────────────► Kafka (auction.win-loss)
                                                │
                          ┌─────────────────────┤
                          │                     │
                          ▼                     ▼
               Statistics Service      Analytics Service
                          │
                          ▼
                    Statistics DB
```

### Domain path (управление кампаниями)

```
Advertiser Dashboard
        │  REST
        ├──► Campaign Service ──► Campaign DB
        │                     └──► Kafka (campaign.changed) ──► Redis invalidation
        ├──► Financial Service ──► Finance DB
        │                      └──► Budget Service ──► Budget DB + Redis
        └──► Analytics Service ──► Analytics DB / OLAP
```

---

## Сводная таблица протоколов

| Взаимодействие | Протокол | Обоснование |
|---|---|---|
| DSP → DSP API Gateway | HTTPS / OpenRTB | Внешний стандарт отрасли |
| DSP API Gateway → Ad Server | HTTP / gRPC | Низкая latency, deadline propagation |
| Ad Server → Bidding Service | gRPC | Критический путь, бинарная сериализация |
| Bidding Service → Budget Service | gRPC | Синхронная проверка в hot path, строгий контракт |
| Ad Server / Bidding Service → Redis | Redis protocol | Нативный клиент, минимальная latency |
| Bidding Service → Kafka | Async Kafka producer | Не блокирует RTB-ответ |
| Delivery Service → Event Sink Service | HTTP (async/fire-and-forget) | Простая публикация после ответа |
| Event Sink → Kafka | Kafka producer | Буферизация и гарантия доставки |
| Dashboard → Campaign Service | REST / JSON | Не критично по latency, удобно для UI |
| Dashboard → Financial Service | REST / JSON | Не критично по latency |
| Dashboard → Analytics Service | REST / JSON | Аналитика — не hot path |
| Campaign Service → Kafka | Async Kafka producer | Domain events для инвалидации кэша |
