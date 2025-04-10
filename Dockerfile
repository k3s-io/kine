
FROM golang:1.24-alpine3.20 AS infra
ARG ARCH=amd64

RUN apk -U add bash coreutils git gcc musl-dev vim less curl wget ca-certificates
# go imports version gopls/v0.15.3
# https://github.com/golang/tools/releases/latest
RUN go install golang.org/x/tools/cmd/goimports@cd70d50baa6daa949efa12e295e10829f3a7bd46
RUN rm -rf /go/src /go/pkg
RUN if [ "${ARCH}" == "amd64" ]; then \
    curl -sL https://raw.githubusercontent.com/golangci/golangci-lint/refs/tags/v1.64.8/install.sh | sh -s -- v1.64.8;  \
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

FROM alpine:3.21 AS package
COPY bin/kine /bin/kine
RUN mkdir /db && chown nobody /db
VOLUME /db
EXPOSE 2379/tcp
USER nobody
ENTRYPOINT ["/bin/kine"]

FROM --platform=$BUILDPLATFORM tonistiigi/xx AS xx

FROM --platform=$BUILDPLATFORM golang:1.24-alpine3.20 AS multi-arch-build
COPY --from=xx / /
ARG TARGETOS
ARG TARGETARCH
ENV CGO_ENABLED=1
RUN apk -U add bash coreutils git vim less curl wget ca-certificates clang lld
RUN xx-apk add musl-dev gcc
# go imports version gopls/v0.15.3
# https://github.com/golang/tools/releases/latest
RUN xx-go install golang.org/x/tools/cmd/goimports@cd70d50baa6daa949efa12e295e10829f3a7bd46
RUN rm -rf /go/src /go/pkg
ENV SRC_DIR=/go/src/github.com/k3s-io/kine
WORKDIR ${SRC_DIR}/
COPY ./scripts/buildx ./scripts/version ./scripts/
COPY ./go.mod ./go.sum ./main.go ./
COPY ./pkg ./pkg
COPY ./.git ./.git
COPY ./.golangci.json ./.golangci.json

RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    ./scripts/buildx


FROM scratch AS multi-arch-binary
ENV SRC_DIR=/go/src/github.com/k3s-io/kine
COPY --from=multi-arch-build ${SRC_DIR}/bin /

FROM alpine:3.21 AS multi-arch-package
ARG TARGETARCH
ENV ARCH=${TARGETARCH}
RUN if [ "${TARGETARCH}" == "arm/v7" ]; then \
    ARCH=arm; \
    fi
COPY --from=multi-arch-build /go/src/github.com/k3s-io/kine/bin/kine-$ARCH /bin/kine
RUN mkdir /db && chown nobody /db
VOLUME /db
EXPOSE 2379/tcp
USER nobody
ENTRYPOINT ["/bin/kine"]
