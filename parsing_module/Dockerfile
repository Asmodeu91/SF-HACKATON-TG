# Многоэтапная сборка для оптимизации размера образа
FROM golang:1.25 AS builder

# Копирование файлов
COPY ./parsing_module /app

# Установка рабочей директории
WORKDIR /app

# Загрузка зависимостей
RUN go mod download

# Собираем приложение
RUN go build -o app .

# Финальный образ
FROM golang:1.25 AS runner

WORKDIR /root/

# Копируем бинарник из builder
COPY --from=builder /app/app .

CMD ["./app"]