FROM golang:1.24.5-alpine3.22
WORKDIR /app
COPY go.* .

RUN go mod tidy && \
  go mod verify && \
  go mod download

COPY . .
