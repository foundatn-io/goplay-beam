FROM golang:1.16.5-alpine as builder

WORKDIR /src
COPY cmd/streaminginserts/main.go main.go
COPY vendor vendor
COPY internal internal
COPY go.mod go.mod
COPY go.sum go.sum

RUN go build -v -o ./streaminginserts .

FROM alpine
COPY --from=builder ./src/streaminginserts /streaminginserts

ENTRYPOINT ["/streaminginserts"]