
FROM golang:1.25-alpine3.23 AS infra
ARG ARCH=amd64

RUN apk -U add bash coreutils git gcc musl-dev vim less curl wget ca-certificates
# goimports version gopls/v0.20.0
# https://github.com/golang/tools/releases/tag/gopls%2Fv0.20.0
RUN GOPROXY=direct go install golang.org/x/tools/cmd/goimports@2e31135b736b96cd609904370c71563ce5447826
RUN GOLANGCI_VERSION=v2.7.2 && \
    case "${ARCH}" in \
        amd64) GOLANGCI_SHA256="ce46a1f1d890e7b667259f70bb236297f5cf8791a9b6b98b41b283d93b5b6e88" ;; \
        arm64) GOLANGCI_SHA256="7028e810837722683dab679fb121336cfa303fecff39dfe248e3e36bc18d941b" ;; \
        *) echo "Unsupported architecture for golangci-lint: ${ARCH}" && exit 1 ;; \
    esac && \
    cd /tmp && \
    curl -fsSL "https://github.com/golangci/golangci-lint/releases/download/${GOLANGCI_VERSION}/golangci-lint-${GOLANGCI_VERSION#v}-linux-${ARCH}.tar.gz" -o golangci-lint.tar.gz && \
    echo "${GOLANGCI_SHA256}  golangci-lint.tar.gz" | sha256sum -c - && \
    tar --strip-components=1 -xzf golangci-lint.tar.gz "golangci-lint-${GOLANGCI_VERSION#v}-linux-${ARCH}/golangci-lint" && \
    install -m 0755 golangci-lint /usr/local/bin/golangci-lint && \
    rm -f /tmp/golangci-lint /tmp/golangci-lint.tar.gz
WORKDIR /go/src/github.com/k3s-io/kine

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
ARG DIRTY
ARG ARCH=amd64
ENV TAG=${TAG} DIRTY=${DIRTY} ARCH=${ARCH}

COPY ./scripts/build ./scripts/version ./scripts/
COPY ./go.mod ./go.sum ./main.go ./
COPY ./pkg ./pkg
COPY ./.git ./.git
COPY ./.golangci.yml ./.golangci.yml

RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    ./scripts/build

FROM scratch AS binary
COPY --from=build /go/src/github.com/k3s-io/kine/bin /bin

FROM alpine:3.23 AS package
COPY --from=build /go/src/github.com/k3s-io/kine/bin/kine /bin/kine
RUN mkdir /db && chown nobody /db
VOLUME /db
EXPOSE 2379/tcp
USER nobody
ENTRYPOINT ["/bin/kine"]

FROM --platform=$BUILDPLATFORM tonistiigi/xx AS xx

FROM --platform=$BUILDPLATFORM golang:1.25-alpine3.23 AS multi-arch-build
COPY --from=xx / /
ARG TAG
ARG DIRTY
ARG TARGETOS
ARG TARGETARCH
ENV TAG=${TAG} DIRTY=${DIRTY} CGO_ENABLED=1
RUN apk -U add bash coreutils git vim less curl wget ca-certificates clang lld
RUN xx-apk add musl-dev gcc
# go imports version gopls/v0.15.3
# https://github.com/golang/tools/releases/latest
RUN xx-go install golang.org/x/tools/cmd/goimports@cd70d50baa6daa949efa12e295e10829f3a7bd46
RUN rm -rf /go/src /go/pkg
WORKDIR /go/src/github.com/k3s-io/kine
COPY ./scripts/buildx ./scripts/version ./scripts/
COPY ./go.mod ./go.sum ./main.go ./
COPY ./pkg ./pkg
COPY ./.git ./.git
COPY ./.golangci.yml ./.golangci.yml

RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    ./scripts/buildx


FROM scratch AS multi-arch-binary
COPY --from=multi-arch-build /go/src/github.com/k3s-io/kine/bin /

FROM alpine:3.23 AS multi-arch-package
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
