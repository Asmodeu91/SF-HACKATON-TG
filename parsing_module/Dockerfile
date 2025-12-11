# # Многоэтапная сборка для оптимизации размера образа
# FROM golang:1.24-alpine AS builder

# # Копирование файлов
# COPY . /app

# # Установка рабочей директории
# WORKDIR /app

# RUN ls -lah

# # Загрузка зависимостей
# RUN go mod download

# # Сборка приложения
# # RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app .
# RUN go build -o app .

# # Финальный образ
# FROM alpine:latest AS run

# WORKDIR /root/

# # Копирование бинарного файла из builder
# COPY --from=builder /app/app .

# # Запуск приложения
# CMD ["./app"]

# Многоэтапная сборка для оптимизации размера образа
FROM golang:1.24-alpine AS builder

# Копирование файлов
COPY ./parsing_module /app

# Установка рабочей директории
WORKDIR /app

# Загрузка зависимостей
RUN go mod download

# Сборка приложения
# RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app .
RUN go build -o app .

ENTRYPOINT ["./app"]