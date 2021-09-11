FROM golang:1.16.5-alpine as builder

WORKDIR /src
COPY cmd/gopictospanner/main.go main.go
COPY vendor vendor
COPY internal internal
COPY go.mod go.mod
COPY go.sum go.sum

RUN go build -v -o ./gopictospanner .

FROM alpine
COPY --from=builder ./src/gopictospanner /gopictospanner

ENTRYPOINT ["/gopictospanner"]