# Репликация данных из MongoDB в PostgreSQL через Airflow

## Описание

В этом проекте реализован пайплайн для репликации данных из MongoDB в PostgreSQL с использованием Apache Airflow.

DAG выполняет следующие действия:

* Создаёт схему и таблицы в PostgreSQL (если их нет)
* Забирает данные из MongoDB
* Распрямляет вложенные структуры (массивы и вложенные объекты)
* Загружает данные в PostgreSQL

Вся логика создания таблиц и переливки данных реализована в одном DAG-файле.

---

## Используемые технологии

* MongoDB - источник данных
* PostgreSQL - целевая база
* Apache Airflow - оркестрация
* Python - логика трансформации

---

## Схема в PostgreSQL

Данные загружаются в схему:

```
analytics_data
```

Создаются таблицы:

* user_sessions
* user_session_pages
* user_session_actions
* event_logs
* event_log_details
* support_tickets
* support_ticket_messages
* user_recommendations
* user_recommendation_products
* moderation_queue
* moderation_flags

Если таблицы уже существуют - они не пересоздаются.

Перед загрузкой данные очищаются (TRUNCATE).

---

## Что делает DAG

1. Подключается к MongoDB
2. Подключается к PostgreSQL
3. Создаёт схему и таблицы (если их нет)
4. Очищает таблицы
5. Переливает данные из MongoDB в PostgreSQL
6. Распрямляет массивы в отдельные таблицы

Пример:

* pages_visited → user_session_pages
* actions → user_session_actions
* messages → support_ticket_messages
* flags → moderation_flags

---

## Запуск

Через интерфейс Airflow:

* Открыть DAG
* Нажать Trigger

Или через CLI:

```
airflow dags trigger mongo_to_postgres_flatten
```

---

## Подключения

В Airflow должен быть настроен:

* postgres_default - подключение к PostgreSQL

MongoDB подключается через URI внутри DAG.

---

## Итог

Реализован пайплайн репликации MongoDB → PostgreSQL с автоматическим созданием схемы и таблиц, а также с распрямлением вложенных данных.
Логика полностью описана в DAG-файле.
