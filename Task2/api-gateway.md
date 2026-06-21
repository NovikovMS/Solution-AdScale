# DSP API Gateway — дизайн API-слоя

## Роль и назначение

DSP API Gateway — единая точка входа для всего внешнего DSP-трафика. Он принимает bid requests от DSP-партнёров по протоколу OpenRTB, обеспечивает безопасность периметра, управляет нагрузкой и маршрутизирует запросы к Ad Server.

Gateway изолирует внутренние сервисы от прямого контакта с внешним миром, что позволяет независимо менять внутренние протоколы и топологию, не нарушая контракт с DSP-партнёрами.

**Технология:** Envoy Proxy, развёрнутый в Kubernetes как отдельный Deployment. Envoy выбран по следующим причинам:
- нативная интеграция с Kubernetes (service discovery через xDS API);
- встроенная поддержка gRPC transcoding: принимает HTTP/JSON от DSP, передаёт gRPC внутрь;
- богатые возможности наблюдаемости: метрики Prometheus, distributed tracing, access logs;
- circuit breaker и retry — конфигурируются декларативно без изменения кода сервисов.

---

## Маршрутизация запросов

### Маршруты

| Путь | Метод | Назначение | Upstream |
|---|---|---|---|
| `/openrtb/v2.5/bid` | POST | Bid request от DSP-партнёра | Ad Server |
| `/openrtb/v3.0/bid` | POST | Bid request (новый стандарт) | Ad Server |
| `/health` | GET | Healthcheck для DSP-партнёра | Gateway (200 OK) |
| `/win` | POST | Win notification от DSP | Event Sink Service |

### Трансляция OpenRTB → внутренний формат

DSP-партнёры отправляют запросы в формате OpenRTB 2.5/3.0 (JSON). Gateway выполняет:

1. Валидацию структуры запроса (обязательные поля: `id`, `imp`, `app`/`site`).
2. Нормализацию: приведение к единому внутреннему формату независимо от версии OpenRTB.
3. Добавление метаданных: идентификатор партнёра, timestamp приёма, trace ID.
4. Проброс нормализованного запроса в Ad Server по HTTP или gRPC.

---

## Аутентификация

Каждый DSP-партнёр аутентифицируется через API Key.

**Схема:**
- Партнёр передаёт ключ в заголовке: `X-DSP-API-Key: <key>`.
- Gateway проверяет ключ по локальному кэшу (обновляется каждые 60 секунд из конфигурационного хранилища).
- При неверном или отсутствующем ключе: `401 Unauthorized`, запрос не передаётся дальше.
- Для будущих интеграций предусмотрена поддержка mTLS (взаимная аутентификация по клиентскому сертификату).

**Привязка партнёра:**
- Каждый API Key однозначно идентифицирует DSP-партнёра.
- Rate limit, circuit breaker и метрики ведутся отдельно для каждого партнёра.

---

## Rate Limiting

Rate limiting защищает внутренние сервисы от перегрузки и обеспечивает честное распределение квот между партнёрами.

### Параметры

| Уровень | Лимит | Действие при превышении |
|---|---|---|
| Глобальный (все партнёры) | 20 000 RPS | `503 Service Unavailable` |
| На партнёра (DSP) | Настраивается индивидуально | `429 Too Many Requests` |
| Новый DSP-партнёр (по умолчанию) | 5 000 RPS | `429 Too Many Requests` |

### Алгоритм

Используется Token Bucket: позволяет кратковременные всплески (burst) сверх среднего лимита в пределах размера буфера, что соответствует реальному профилю RTB-трафика.

```
Burst size = rate_per_second × 1.2
Refill rate = rate_per_second tokens/s
```

### Заголовки ответа

При любом ответе Gateway возвращает заголовки:
```
X-RateLimit-Limit: 5000
X-RateLimit-Remaining: 4823
X-RateLimit-Reset: 1720000060
```

---

## Circuit Breaker

Circuit Breaker защищает DSP-партнёра от длинных ответов при деградации Ad Server, а Ad Server — от лавинообразной нагрузки при восстановлении.

### Состояния

```
         ошибки > threshold
CLOSED ───────────────────► OPEN
  ▲                           │
  │      probe успешен        │  timeout прошёл
  └──── HALF-OPEN ◄───────────┘
```

### Параметры

| Параметр | Значение |
|---|---|
| Окно анализа | 10 секунд |
| Порог ошибок для открытия | 50% запросов или P95 > 120 ms |
| Минимум запросов для анализа | 100 |
| Таймаут до перехода в HALF-OPEN | 5 секунд |
| Количество probe-запросов в HALF-OPEN | 5 |
| Порог успеха для закрытия | 80% probe-запросов успешны |

### Поведение в состоянии OPEN

- Gateway возвращает `503 Service Unavailable` немедленно, не обращаясь к Ad Server.
- В заголовке ответа: `Retry-After: 5`.
- Метрика `gateway_circuit_breaker_state{partner_id, state}` обновляется мгновенно.
- Алерт отправляется в систему мониторинга при каждом открытии circuit breaker.

---

## Мониторинг времени отклика

Gateway собирает и экспортирует метрики latency для каждого DSP-партнёра.

### Метрики Prometheus

| Метрика | Тип | Лейблы | Описание |
|---|---|---|---|
| `gateway_request_duration_seconds` | Histogram | `partner_id`, `status_code`, `route` | Latency запроса от получения до отправки ответа |
| `gateway_requests_total` | Counter | `partner_id`, `status_code` | Количество запросов |
| `gateway_rate_limit_hits_total` | Counter | `partner_id` | Количество отклонённых запросов по rate limit |
| `gateway_circuit_breaker_state` | Gauge | `partner_id` | Текущее состояние: 0=CLOSED, 1=HALF-OPEN, 2=OPEN |
| `gateway_upstream_duration_seconds` | Histogram | `upstream`, `status_code` | Latency обращения к Ad Server |

### SLO-алерты

```yaml
# Алерт: P95 latency превышает 80 ms для любого партнёра
- alert: GatewayHighLatency
  expr: histogram_quantile(0.95, gateway_request_duration_seconds) > 0.08
  for: 1m
  labels:
    severity: critical

# Алерт: error rate выше 1% за последние 5 минут
- alert: GatewayHighErrorRate
  expr: rate(gateway_requests_total{status_code=~"5.."}[5m]) /
        rate(gateway_requests_total[5m]) > 0.01
  for: 2m
  labels:
    severity: warning

# Алерт: circuit breaker открылся
- alert: GatewayCircuitBreakerOpen
  expr: gateway_circuit_breaker_state == 2
  for: 0s
  labels:
    severity: critical
```

---

## Масштабирование и отказоустойчивость

- Gateway развёртывается как минимум в 2 репликах (HPA в Kubernetes).
- HPA масштабирует по CPU и RPS: при превышении 70% CPU или 80% от rate limit добавляются реплики.
- Gateway stateless: все реплики идентичны, состояние (rate limit counters) хранится в Redis.
- Healthcheck: `/health` отвечает 200 OK, если upstream Ad Server доступен; иначе 503.

---

## Сводная схема обработки запроса

```
DSP Partner
    │
    │ POST /openrtb/v2.5/bid
    │ X-DSP-API-Key: <key>
    ▼
┌──────────────────────────────────────┐
│           DSP API Gateway            │
│                                      │
│  1. Аутентификация (API Key)         │
│  2. Rate Limiting (Token Bucket)     │
│  3. Circuit Breaker (CLOSED/OPEN)    │
│  4. Трансляция OpenRTB → internal    │
│  5. Проброс в Ad Server              │
│  6. Метрики latency и статусов       │
└──────────────┬───────────────────────┘
               │ HTTP / gRPC
               ▼
          Ad Server
```
