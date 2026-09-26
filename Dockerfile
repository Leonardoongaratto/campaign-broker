# Build stage
FROM golang:1.27-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o broker-service ./cmd/server

# Run stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

COPY --from=builder /app/broker-service /broker-service
RUN ln -s /broker-service /root/broker-service

EXPOSE 80 8080

CMD ["/broker-service"]

