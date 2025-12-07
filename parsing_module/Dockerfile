# Многоэтапная сборка для оптимизации размера образа
FROM golang:1.24-alpine AS builder

# Копирование файлов
COPY . /app

# Установка рабочей директории
WORKDIR /app/parsing_module

# Загрузка зависимостей
RUN go mod download

# Сборка приложения
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app .

# Финальный образ
FROM alpine:latest AS run

WORKDIR /root/

# Копирование бинарного файла из builder
COPY --from=builder /app/parsing_module/app .

# Запуск приложения
CMD ["./app"]