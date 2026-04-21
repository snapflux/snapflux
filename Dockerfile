FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w" \
      -o snapflux \
      ./cmd/api

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /build/snapflux /snapflux
EXPOSE 5050
ENTRYPOINT ["/snapflux"]
