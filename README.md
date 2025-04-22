# Jenkins ETL Project

## Описание

ETL-процесс извлекает данные из базы PostgreSQL (форум), агрегирует статистику по дням и сохраняет в `.csv`.

## Шаги по запуску:

1. Установите Jenkins: https://www.jenkins.io/doc/book/installing/
2. Установите плагины:
   - Git Plugin
   - Pipeline Plugin
3. Создайте нового pipeline-проект в Jenkins.
4. Вставьте содержимое `Jenkinsfile`.
5. Убедитесь, что база данных PostgreSQL работает и содержит нужную схему.
6. Запуск будет происходить ежедневно по cron (`H H * * *`).

## Скрипты

- `aggregate_logs.py`: основной ETL-скрипт (см. предыдущий проект)
- `requirements.txt`: зависимости Python

## Пример команды

```bash
python aggregate_logs.py 2025-04-01 2025-04-30
```
