
FROM golang:1.26-alpine3.23 AS infra
ARG ARCH=amd64

RUN apk -U add bash coreutils git gcc musl-dev vim less curl wget ca-certificates
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
ARG CGO_ENABLED=1
ENV TAG=${TAG} DIRTY=${DIRTY} ARCH=${ARCH} CGO_ENABLED=${CGO_ENABLED}

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

FROM alpine:3.24 AS package
ARG NOCGO
COPY --from=build /go/src/github.com/k3s-io/kine/bin/kine${NOCGO} /bin/kine
RUN mkdir /db && chown nobody /db
VOLUME /db
EXPOSE 2379/tcp
USER nobody
ENTRYPOINT ["/bin/kine"]

FROM --platform=$BUILDPLATFORM tonistiigi/xx AS xx

FROM --platform=$BUILDPLATFORM golang:1.26-alpine3.23 AS multi-arch-build
COPY --from=xx / /
ARG TARGETOS
ARG TARGETARCH
RUN apk -U add bash coreutils git vim less curl wget ca-certificates clang lld
RUN xx-apk add musl-dev gcc
# go imports version gopls/v0.15.3
# https://github.com/golang/tools/releases/latest
RUN xx-go install golang.org/x/tools/cmd/goimports@cd70d50baa6daa949efa12e295e10829f3a7bd46
RUN rm -rf /go/src /go/pkg
ARG TAG
ARG DIRTY
ARG CGO_ENABLED=1
WORKDIR /go/src/github.com/k3s-io/kine
COPY ./scripts/buildx ./scripts/version ./scripts/
COPY ./go.mod ./go.sum ./main.go ./
COPY ./pkg ./pkg
COPY ./.git ./.git
COPY ./.golangci.yml ./.golangci.yml
ENV TAG=${TAG} DIRTY=${DIRTY} CGO_ENABLED=${CGO_ENABLED}
RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    ./scripts/buildx


FROM scratch AS multi-arch-binary
COPY --from=multi-arch-build /go/src/github.com/k3s-io/kine/bin /

FROM alpine:3.24 AS multi-arch-package
ARG TARGETARCH
ARG NOCGO
ENV ARCH=${TARGETARCH}
RUN if [ "${TARGETARCH}" == "arm/v7" ]; then \
    ARCH=arm; \
    fi
COPY --from=multi-arch-build /go/src/github.com/k3s-io/kine/bin/kine-${ARCH}${NOCGO} /bin/kine
RUN mkdir /db && chown nobody /db
VOLUME /db
EXPOSE 2379/tcp
USER nobody
ENTRYPOINT ["/bin/kine"]
