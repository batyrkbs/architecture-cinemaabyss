# Проектная работа 2 спринта — «Кинобездна»

## Задание 1. Проектирование архитектуры

To-Be архитектура спроектирована как поэтапная миграция монолита по паттерну **Strangler Fig**. Внешние клиенты больше не обращаются напрямую к монолиту: все запросы проходят через **Proxy / API Gateway**, который является единой точкой входа и позволяет постепенно переключать трафик на новые микросервисы через feature flag.

Файл контейнерной C4-диаграммы:

[docs/diagrams/cinemaabyss-to-be-container.puml](docs/diagrams/cinemaabyss-to-be-container.puml)

Исходная As-Is диаграмма также сохранена в проекте:

[docs/diagrams/cinemaabyss-as-is.drawio](docs/diagrams/cinemaabyss-as-is.drawio)

### Выделенные домены

- **Identity / Users** — пользователи, профили, избранное, оценки.
- **Movies Metadata** — фильмы, жанры, актёры, рейтинги, описание, поиск.
- **Billing / Subscriptions** — платежи, подписки, интеграции с платёжными системами.
- **Content Access** — доступность контента, проверка права просмотра, интеграции с внешними источниками.
- **Events** — MVP событийной интеграции через Kafka для User/Payment/Movie events.
- **Recommendations Integration** — интеграция со сторонней рекомендательной системой.

### Архитектурные решения

1. **Единая точка входа** — `proxy-service`.
2. **Бесшовная миграция** — `MOVIES_MIGRATION_PERCENT` управляет долей трафика `/api/movies`, который уходит в новый `movies-service`.
3. **Событийная интеграция** — `events-service` принимает события и обрабатывает их через MVP event bus с Kafka-ready конфигурацией.
4. **Kubernetes** — для оркестрации, самовосстановления и дальнейшего масштабирования.
5. **Helm** — для воспроизводимого развёртывания и обновления.
6. **CI/CD** — GitHub Actions собирает сервисы, запускает API-тесты и публикует Docker-образы в GHCR.

## Задание 2. Реализация прокси-сервиса и Kafka MVP

### 2.1 Proxy / API Gateway

Реализован сервис:

[src/microservices/proxy](src/microservices/proxy)

Основные файлы:

- [src/microservices/proxy/main.go](src/microservices/proxy/main.go)
- [src/microservices/proxy/Dockerfile](src/microservices/proxy/Dockerfile)
- [src/microservices/proxy/go.mod](src/microservices/proxy/go.mod)

### Что делает proxy-service

- `GET /health` — health-check.
- `/api/movies` — маршрутизируется либо в монолит, либо в `movies-service`.
- `/api/events` — маршрутизируется в `events-service`.
- Все остальные запросы по умолчанию проксируются в монолит.

### Feature flag для Strangler Fig

Переменные окружения:

```yaml
GRADUAL_MIGRATION: "true"
MOVIES_MIGRATION_PERCENT: "50"
```

Примеры:

- `MOVIES_MIGRATION_PERCENT=0` — весь трафик `/api/movies` идёт в монолит.
- `MOVIES_MIGRATION_PERCENT=50` — примерно половина запросов идёт в новый сервис.
- `MOVIES_MIGRATION_PERCENT=100` — весь трафик `/api/movies` идёт в `movies-service`.

Проверка:

```bash
docker compose up --build -d
curl http://localhost:8000/health
curl http://localhost:8000/api/movies
```

### 2.2 Events service / Kafka MVP

Реализован сервис:

[src/microservices/events](src/microservices/events)

Основные файлы:

- [src/microservices/events/main.go](src/microservices/events/main.go)
- [src/microservices/events/Dockerfile](src/microservices/events/Dockerfile)
- [src/microservices/events/go.mod](src/microservices/events/go.mod)

### API событий

- `GET /api/events/health`
- `POST /api/events/movie`
- `POST /api/events/user`
- `POST /api/events/payment`
- `GET /api/events`

При создании события сервис:

1. принимает JSON-запрос;
2. формирует событие с типом, топиком и идентификатором;
3. публикует событие во внутреннюю очередь MVP;
4. consumer читает событие из очереди;
5. пишет в лог строки `PRODUCED` и `CONSUMED`.

Kafka подключена в `docker-compose.yml`, топики создаются через `KAFKA_CREATE_TOPICS`:

- `movie-events`
- `user-events`
- `payment-events`

Kafka UI доступен по адресу:

```text
http://localhost:8090
```

### Запуск тестов

```bash
docker compose up --build -d
cd tests/postman
npm install
npm run test:local
```

Для просмотра логов обработки событий:

```bash
docker compose logs -f events-service
```

## Задание 3. CI/CD и Kubernetes

### 3.1 CI/CD

Доработан workflow:

[.github/workflows/docker-build-push.yml](.github/workflows/docker-build-push.yml)

Что делает workflow:

1. поднимает локальный стек через Docker Compose;
2. ждёт готовности `proxy-service`, `movies-service`, `events-service`;
3. запускает Postman/Newman API-тесты;
4. собирает и публикует Docker-образы в GitHub Container Registry:
   - `monolith`
   - `movies-service`
   - `events-service`
   - `proxy-service`

Также исправлен workflow API-тестов:

[.github/workflows/api-tests.yml](.github/workflows/api-tests.yml)

### 3.2 Kubernetes manifests

Доработаны файлы:

- [src/kubernetes/proxy-service.yaml](src/kubernetes/proxy-service.yaml)
- [src/kubernetes/events-service.yaml](src/kubernetes/events-service.yaml)
- [src/kubernetes/ingress.yaml](src/kubernetes/ingress.yaml)
- [src/kubernetes/configmap.yaml](src/kubernetes/configmap.yaml)

### Проверка Kubernetes

```bash
kubectl apply -f src/kubernetes/namespace.yaml
kubectl apply -f src/kubernetes/configmap.yaml
kubectl apply -f src/kubernetes/secret.yaml
kubectl apply -f src/kubernetes/dockerconfigsecret.yaml
kubectl apply -f src/kubernetes/postgres-init-configmap.yaml
kubectl apply -f src/kubernetes/postgres.yaml
kubectl apply -f src/kubernetes/kafka/kafka.yaml
kubectl apply -f src/kubernetes/monolith.yaml
kubectl apply -f src/kubernetes/movies-service.yaml
kubectl apply -f src/kubernetes/events-service.yaml
kubectl apply -f src/kubernetes/proxy-service.yaml
kubectl apply -f src/kubernetes/ingress.yaml
```

Проверка:

```bash
kubectl -n cinemaabyss get pods
curl http://cinemaabyss.example.com/api/movies
```

События:

```bash
curl -X POST http://cinemaabyss.example.com/api/events/movie \
  -H "Content-Type: application/json" \
  -d '{"movie_id":1,"title":"Test Movie","action":"viewed","user_id":1}'

kubectl -n cinemaabyss logs deploy/events-service
```

Перед деплоем в свой кластер нужно заменить `ghcr.io/db-exp/cinemaabysstest/...` на путь к образам своего GitHub-репозитория, если репозиторий называется иначе.

## Задание 4. Helm-чарт

Доработан Helm-чарт:

[src/kubernetes/helm](src/kubernetes/helm)

Особенно важные файлы:

- [src/kubernetes/helm/values.yaml](src/kubernetes/helm/values.yaml)
- [src/kubernetes/helm/templates/services/proxy-service.yaml](src/kubernetes/helm/templates/services/proxy-service.yaml)
- [src/kubernetes/helm/templates/services/events-service.yaml](src/kubernetes/helm/templates/services/events-service.yaml)
- [src/kubernetes/helm/templates/ingress.yaml](src/kubernetes/helm/templates/ingress.yaml)
- [src/kubernetes/helm/templates/configmap.yaml](src/kubernetes/helm/templates/configmap.yaml)

### Установка через Helm

```bash
kubectl delete all --all -n cinemaabyss || true
kubectl delete namespace cinemaabyss || true
helm install cinemaabyss ./src/kubernetes/helm --namespace cinemaabyss --create-namespace
kubectl get pods -n cinemaabyss
```

Проверка:

```bash
curl http://cinemaabyss.example.com/api/movies
curl -X POST http://cinemaabyss.example.com/api/events/user \
  -H "Content-Type: application/json" \
  -d '{"user_id":1,"username":"testuser","action":"logged_in"}'
```

### Управление миграцией через Helm

В `src/kubernetes/helm/values.yaml`:

```yaml
config:
  gradualMigration: "true"
  moviesMigrationPercent: "100"
```

Обновление:

```bash
helm upgrade cinemaabyss ./src/kubernetes/helm --namespace cinemaabyss
```

## Краткое резюме решения

В проект добавлены:

- Proxy/API Gateway для паттерна Strangler Fig.
- Feature flag для постепенного переключения `/api/movies`.
- Events service для MVP Kafka/event-driven подхода.
- Dockerfile для новых сервисов.
- Docker Compose интеграция новых сервисов.
- Kubernetes Deployment/Service для proxy и events.
- Ingress-маршруты для API Gateway и events API.
- Helm-шаблоны для proxy и events.
- CI/CD workflow с API-тестами и публикацией образов.
- C4 To-Be диаграмма контейнеров.
