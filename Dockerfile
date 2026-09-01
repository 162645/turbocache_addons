# syntax=docker/dockerfile:1
FROM golang:1.27.0 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 \
    go build -trimpath -ldflags="-s -w" -o /out/turbocached ./cmd/turbocached && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/turbocachectl ./cmd/turbocachectl

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /out/turbocached /turbocached
COPY --from=builder /out/turbocachectl /turbocachectl
EXPOSE 7070 9090
USER nonroot:nonroot
ENTRYPOINT ["/turbocached"]
