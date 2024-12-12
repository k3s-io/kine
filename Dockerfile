
FROM golang:1.23-alpine3.20 AS infra
ARG ARCH=amd64

RUN apk -U add bash coreutils git gcc musl-dev vim less curl wget ca-certificates
# go imports version gopls/v0.15.3
# https://github.com/golang/tools/releases/latest
RUN go install golang.org/x/tools/cmd/goimports@cd70d50baa6daa949efa12e295e10829f3a7bd46
RUN rm -rf /go/src /go/pkg
RUN if [ "${ARCH}" == "amd64" ]; then \
    curl -sL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s;  \
    fi

ENV SRC_DIR=/go/src/github.com/k3s-io/kine
WORKDIR ${SRC_DIR}/

# Validate needs everything in the project, so we separate it out for better caching
FROM infra AS validate
ARG SKIP_VALIDATE
ENV SKIP_VALIDATE=${SKIP_VALIDATE}
COPY . .
RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    --mount=type=cache,id=lint,target=/root/.cache/golangci-lint \
    ./scripts/validate

FROM infra AS build
ARG CROSS
ARG TAG
ARG DRONE_TAG
ARG ARCH=amd64
ENV ARCH=${ARCH}

COPY ./scripts/build ./scripts/version ./scripts/
COPY ./go.mod ./go.sum ./main.go ./
COPY ./pkg ./pkg
COPY ./.git ./.git
COPY ./.golangci.json ./.golangci.json

RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    ./scripts/build

FROM scratch AS binary
ENV SRC_DIR=/go/src/github.com/k3s-io/kine
COPY --from=build ${SRC_DIR}/bin /bin
