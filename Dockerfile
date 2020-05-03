FROM golang:1.13.7-alpine3.11 AS builder
RUN apk --no-cache add gcc musl-dev
WORKDIR /go/src/github.com/rancher/kine
COPY . .
RUN GO111MODULE=on go build -o /bin/kine

FROM alpine:3.11
COPY --from=builder /bin/kine /bin/kine
ENTRYPOINT ["/bin/kine"]