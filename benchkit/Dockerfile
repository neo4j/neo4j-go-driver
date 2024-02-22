FROM golang:1.18 as builder

# Create and change to the driver directory.
WORKDIR /driver

# Copy local files to the container's workspace.
COPY . /driver/

# Fetch driver dependencies (if any).
RUN go mod download
RUN go mod verify

# Build the binary.
RUN CGO_ENABLED=0 GOOS=linux go build -v -o benchkit/benchkit ./benchkit-backend

# Only copy the compiled binary from the builder stage.
FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /driver/benchkit/benchkit .

# Run the benchkit binary.
CMD ["./benchkit"]
