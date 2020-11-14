FROM golang:1.15 as builder

# Enable modules support
ENV GO111MODULE=on

RUN mkdir -p /build
WORKDIR /build

COPY go.mod go.sum ./

COPY . .

RUN go mod download

ENV CGO_ENABLED=0
# Compile static binary
RUN go build -ldflags "-s -w" ./cmd/webhooks

##################################################
# Final image
FROM alpine

RUN mkdir -p /app/config
WORKDIR /app/anova

# Copy pipeline config
COPY --from=builder /build/config/pipeline.yaml config/pipeline.yaml
# Copy the binary from the previous stage
COPY --from=builder /build/webhooks webhooks

CMD ["/app/webhooks", "-c", "/app/config/pipeline.yaml"]
