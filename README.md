# Catalog Category API

Учебный backend-проект сервиса категорий каталога, реализованный на Go.

Проект предоставляет gRPC и REST API для работы с категориями, использует PostgreSQL для хранения данных и Apache Kafka для передачи событий между сервисами.

В рамках проекта реализованы создание, получение, просмотр списка, обновление и удаление категорий, а также событийная интеграция через Kafka с использованием Transactional Outbox.

> За основу проекта был взят шаблон **Ozon Marketplace Template API**.  
> Исходный шаблон использовался как база для инфраструктуры и структуры Go-сервиса. Бизнес-логика категорий, операция Update, Transactional Outbox, Kafka producer/retranslator, facade-сервис, отдельная facade-база и обработка событий были реализованы в рамках данного проекта.

---

## Возможности проекта

Основной сервис поддерживает следующие операции с категориями:

- создание категории;
- получение категории;
- получение списка категорий;
- обновление категории;
- логическое удаление категории.

Также реализованы:

- gRPC API;
- REST API через gRPC-Gateway;
- PostgreSQL;
- миграции через Goose;
- Apache Kafka;
- Transactional Outbox;
- отправка событий о создании, обновлении и удалении категорий;
- отдельный facade-сервис;
- чтение Kafka-событий facade-сервисом;
- сохранение состояния категорий в отдельную facade PostgreSQL;
- ручная фиксация Kafka offset после успешной обработки сообщения;
- поддержка списка Kafka-брокеров;
- Prometheus;
- Grafana;
- Jaeger;
- Graylog;
- Swagger UI;
- health-check endpoints.

---

## Архитектура

В проекте используются два прикладных сервиса:

1. `catalog-category-api` — основной сервис категорий.
2. `catalog-category-facade` — сервис, читающий события из Kafka и формирующий собственное представление данных.

Упрощённая схема:

```text
                       ┌─────────────────────────────┐
                       │    catalog-category-api     │
                       │                             │
REST / gRPC ──────────▶│ Create / Update / Remove    │
                       │              │              │
                       │              ▼              │
                       │     PostgreSQL categories   │
                       │              │              │
                       │              ▼              │
                       │      categories_events      │
                       │              │              │
                       │              ▼              │
                       │         Retranslator        │
                       └──────────────┬──────────────┘
                                      │
                                      ▼
                       ┌─────────────────────────────┐
                       │            Kafka            │
                       │                             │
                       │ catalog.category.created    │
                       │ catalog.category.updated    │
                       │ catalog.category.removed    │
                       └──────────────┬──────────────┘
                                      │
                                      ▼
                       ┌─────────────────────────────┐
                       │  catalog-category-facade    │
                       │                             │
                       │ FetchMessage                │
                       │      ↓                      │
                       │ handleMessage               │
                       │      ↓                      │
                       │ Upsert / MarkRemoved        │
                       │      ↓                      │
                       │ CommitMessages              │
                       └──────────────┬──────────────┘
                                      │
                                      ▼
                       ┌─────────────────────────────┐
                       │      PostgreSQL facade      │
                       │         categories          │
                       └─────────────────────────────┘
```

---

## Transactional Outbox

Для надёжной отправки событий в Kafka в основном сервисе используется паттерн **Transactional Outbox**.

При изменении категории основная операция и создание события выполняются в одной SQL-транзакции.

Например, при обновлении категории:

```text
BEGIN

UPDATE categories

INSERT INTO categories_events

COMMIT
```

Это позволяет избежать ситуации, когда категория была изменена в PostgreSQL, но соответствующее событие не было создано.

После этого отдельный `retranslator` получает события из таблицы `categories_events` и отправляет их в Kafka.

После успешной отправки событие удаляется из outbox.

---

## Kafka

Используются три топика:

```text
catalog.category.created
catalog.category.updated
catalog.category.removed
```

События содержат информацию о категории.

Пример payload:

```json
{
  "category_id": 1,
  "foo": "example"
}
```

Для событий создания и обновления facade выполняет `Upsert`.

Для события удаления facade выполняет логическое удаление категории.

---

## Обработка сообщений в facade

Facade использует ручную фиксацию Kafka offset.

Последовательность обработки:

```text
FetchMessage
    ↓
Декодирование JSON
    ↓
Сохранение в PostgreSQL
    ↓
CommitMessages
```

Offset фиксируется только после успешной записи данных в facade-базу.

Это позволяет избежать потери сообщения в ситуации, когда Kafka уже отдала событие consumer'у, но PostgreSQL временно недоступна.

В случае ошибки обработки текущее сообщение повторяется до успешной обработки.

---

## PostgreSQL

В проекте используются две отдельные базы данных.

### Основная база

Хранит основное состояние категорий и outbox-события.

Основные таблицы:

```text
categories
categories_events
```

### Facade-база

Хранит состояние категорий, построенное на основе Kafka-событий.

Основная таблица:

```text
categories
```

Категории удаляются логически через поле:

```text
removed
```

---

## Сборка проекта

### Установка зависимостей

```bash
make deps
```

### Сборка основного сервиса

```bash
make build
```

### Сборка facade

```bash
make build-facade
```

После сборки бинарник facade появляется в:

```text
./bin/facade-server
```

---

## Запуск проекта

Основной способ запуска инфраструктуры:

```bash
docker compose up -d --build
```

Для просмотра состояния контейнеров:

```bash
docker compose ps
```

Для остановки:

```bash
docker compose down
```

---

## Локальный запуск facade

Для локального запуска используется конфигурация:

```text
config.facade.local.yml
```

Запуск:

```bash
make run-facade
```

или:

```bash
go run ./cmd/facade-server \
  -config config.facade.local.yml \
  -migration=true
```

При локальном запуске facade использует адреса сервисов, опубликованные Docker на localhost.

Например:

```text
PostgreSQL facade: localhost:5433
Kafka:             localhost:9094
```

---

## Запуск facade в Docker

Для Docker используется отдельный конфигурационный файл:

```text
config.facade.docker.yml
```

Контейнеры обращаются друг к другу по именам Docker Compose-сервисов:

```text
PostgreSQL facade: postgres-facade:5432
Kafka:             kafka:9092
```

Facade собирается через:

```text
Dockerfile.facade
```

и запускается как отдельный сервис:

```text
catalog-category-facade
```

---

## Проверка facade

Логи:

```bash
docker compose logs -f catalog-category-facade
```

После запуска должны появиться сообщения о подписке на Kafka-топики:

```text
Listening to topic: catalog.category.created
Listening to topic: catalog.category.updated
Listening to topic: catalog.category.removed
```

После получения события:

```text
Processed and committed message
```

означает, что:

1. Kafka-сообщение было получено;
2. событие было обработано;
3. данные были сохранены в PostgreSQL;
4. offset был успешно зафиксирован в Kafka.

---

## Проверка facade PostgreSQL

Подключение к facade-базе:

```bash
docker compose exec postgres-facade \
  psql -U docker -d catalog_category_facade
```

Просмотр таблиц:

```sql
\dt
```

Просмотр категорий:

```sql
SELECT
    id,
    foo,
    removed,
    created,
    updated
FROM categories
ORDER BY id;
```

После события создания или обновления категория должна присутствовать в таблице с:

```text
removed = false
```

После события удаления:

```text
removed = true
```

---

## Swagger UI

Swagger UI используется для просмотра и тестирования REST API.

```text
http://localhost:8081
```

---

## gRPC

gRPC-сервер:

```text
localhost:8082
```

Основные RPC:

```text
CreateCategoryV1
DescribeCategoryV1
ListCategoriesV1
UpdateCategoryV1
RemoveCategoryV1
```

---

## REST Gateway

gRPC-Gateway предоставляет REST API поверх gRPC-сервиса.

```text
http://localhost:8080
```

Например, операции с категориями доступны через маршруты вида:

```text
/v1/categories
```

---

## Метрики

Метрики gRPC-сервера:

```text
http://localhost:9100/metrics
```

---

## Status API

Endpoints состояния сервиса:

```text
http://localhost:8000
```

Доступны:

```text
/live
/ready
/version
```

`/live` показывает, работает ли процесс сервиса.

`/ready` показывает, готов ли сервис принимать запросы.

`/version` содержит информацию о версии и сборке.

---

## Prometheus

Prometheus используется для сбора метрик.

```text
http://localhost:9090
```

---

## Grafana

Grafana используется для визуализации метрик.

```text
http://localhost:3000
```

Данные для входа:

```text
login: admin
password: MYPASSWORT
```

---

## Kafka

Kafka доступна с локального компьютера по адресу:

```text
localhost:9094
```

Внутри Docker-сети используется:

```text
kafka:9092
```

Приложение поддерживает передачу списка Kafka-брокеров через конфигурацию:

```yaml
kafka:
  brokers:
    - "kafka-1:9092"
    - "kafka-2:9092"
    - "kafka-3:9092"
```

---

## Kafka UI

Kafka UI позволяет просматривать:

- brokers;
- topics;
- partitions;
- messages;
- consumer groups;
- offsets.

Адрес:

```text
http://localhost:9001
```

---

## Jaeger UI

Jaeger используется для distributed tracing.

```text
http://localhost:16686
```

---

## Graylog

Graylog используется для централизованного сбора логов.

```text
http://localhost:9000
```

Данные для входа:

```text
login: admin
password: admin
```

---

## Миграции

Для миграций используется [Goose](https://github.com/pressly/goose).

Миграции основной базы находятся в:

```text
./migrations
```

Миграции facade-базы:

```text
./migrations-facade
```

При запуске сервисов миграции могут применяться автоматически.

---

## Структура проекта

Основные директории:

```text
cmd/
├── grpc-server/
└── facade-server/

internal/
├── api/
├── config/
├── database/
├── facade/
├── kafka/
├── repo/
└── retranslator/

migrations/
migrations-facade/

pkg/
```

`cmd/grpc-server` содержит точку входа основного API.

`cmd/facade-server` содержит Kafka consumer facade-сервиса.

`internal/retranslator` отвечает за получение событий из Transactional Outbox.

`internal/kafka` содержит работу с Kafka producer.

`internal/facade` содержит репозиторий facade-базы.

---

## Используемые технологии

Основные технологии проекта:

- Go;
- gRPC;
- Protocol Buffers;
- gRPC-Gateway;
- PostgreSQL;
- sqlx;
- Squirrel;
- Goose;
- Apache Kafka;
- kafka-go;
- Docker;
- Docker Compose;
- Prometheus;
- Grafana;
- Jaeger;
- Graylog;
- Swagger / OpenAPI.

---

## О проекте

Проект выполнен как учебная реализация микросервисного backend-приложения для работы с категориями каталога.

Основная цель разработки — изучение и практическое применение:

- Go backend-разработки;
- gRPC;
- PostgreSQL;
- SQL-транзакций;
- Transactional Outbox;
- Apache Kafka;
- consumer groups;
- Kafka offsets;
- идемпотентной обработки событий;
- Docker Compose;
- observability-инструментов.

---

## Основа проекта

В качестве первоначальной основы использовался проект **Ozon Marketplace Template API**.

Оригинальный template предоставил готовую базовую инфраструктуру Go-сервиса, которая была адаптирована и расширена под сервис категорий и требования текущего задания.

Благодарность авторам исходного шаблона:

- [Evald Smalyakov](https://github.com/evald24)
- [Michael Morgoev](https://github.com/zerospiel)
