# Use the official Golang image
FROM golang:1.25-alpine AS builder
# Set the working directory inside the container
WORKDIR /app

# Copy the entire project from the context (the root folder)
COPY src/server/go.mod src/server/go.sum ./

# Download dependencies
RUN go mod download

COPY src/server/ .

# Build the Paxos server binary
RUN go build -o paxos_server main.go

#runtime
FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/paxos_server .


CMD ["./paxos_server"]