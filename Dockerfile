
FROM golang:1.21-alpine3.18 AS infra
ARG ARCH=amd64

RUN apk -U add bash coreutils git gcc musl-dev docker-cli vim less file curl wget ca-certificates
# go imports version gopls/v0.14.1
# https://github.com/golang/tools/releases/latest
RUN go install golang.org/x/tools/cmd/goimports@e985f842fa05caad2f3486f0711512aedffbcda8
RUN rm -rf /go/src /go/pkg
RUN if [ "${ARCH}" == "amd64" ]; then \
    curl -sL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s;  \
    fi

ENV SRC_DIR=/go/src/github.com/k3s-io/kine
WORKDIR ${SRC_DIR}/

# Validate needs everything in the project, so we separate it out better caching
FROM infra as validate
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

COPY ./scripts/package ./scripts/entry ./scripts/
COPY ./package ./package
CMD ./scripts/entry package

FROM scratch as binary
ENV SRC_DIR=/go/src/github.com/k3s-io/kine
COPY --from=build ${SRC_DIR}/bin /bin