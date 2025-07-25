FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /app/api .

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/api .

EXPOSE 9999

CMD ["./api"]
