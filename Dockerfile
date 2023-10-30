FROM --platform=$BUILDPLATFORM golang:1.20.7-bullseye AS base
WORKDIR /opt/app

COPY go.mod go.sum ./

COPY . .
RUN go mod download

#RUN go get -d ./...
RUN go build -o kine





ENTRYPOINT ["/bin/kine"]
