.DEFAULT_GOAL := ci

ARCH ?= amd64
REPO ?= rancher
CGO_ENABLED ?= 1
SHELL := bash
PKGSRC := $(shell find pkg/ -type f)
NOCGO := $(shell test $(CGO_ENABLED) = 0 && echo -nocgo)
DEFAULT_BUILD_ARGS=--build-arg="REPO=$(REPO)" --build-arg="TAG=$(TAG)" --build-arg="ARCH=$(ARCH)" --build-arg="DIRTY=$(DIRTY)"
DIRTY := $(shell git status --porcelain --untracked-files=no)
ifneq ($(DIRTY),)
	DIRTY="-dirty"
endif

clean:
	rm -rf ./bin ./dist

.PHONY: validate
validate:
	DOCKER_BUILDKIT=1 docker build \
		$(DEFAULT_BUILD_ARGS) --build-arg="SKIP_VALIDATE=$(SKIP_VALIDATE)" \
		--target=validate -f Dockerfile .

.PHONY: build
build:
	DOCKER_BUILDKIT=1 docker build \
		$(DEFAULT_BUILD_ARGS) --build-arg="CGO_ENABLED=$(CGO_ENABLED)" \
		-f Dockerfile --target=binary --output=. .

.PHONY: multi-arch-build
PLATFORMS = linux/amd64,linux/arm64,linux/arm/v7,linux/riscv64
multi-arch-build:
	docker buildx build --build-arg="REPO=$(REPO)" --build-arg="CGO_ENABLED=$(CGO_ENABLED)" --build-arg="TAG=$(TAG)" --build-arg="DIRTY=$(DIRTY)" --platform=$(PLATFORMS) --target=multi-arch-binary --output=type=local,dest=bin .
	mv bin/linux*/kine* bin/
	rmdir bin/linux*
	mkdir -p dist/artifacts
	cp bin/kine* dist/artifacts/

.PHONY: package
package:
	ARCH=$(ARCH) CGO_ENABLED=$(CGO_ENABLED) ./scripts/package

.PHONY: multi-arch-package
multi-arch-package:
	docker buildx build --build-arg="REPO=$(REPO)" --build-arg="CGO_ENABLED=$(CGO_ENABLED)" --build-arg="NOCGO=$(NOCGO)" --build-arg="TAG=$(TAG)" --build-arg="DIRTY=$(DIRTY)" --platform=$(PLATFORMS) --target=multi-arch-package --output=type=image,name=ghcr.io/k3s-io/kine:dev$(NOCGO) .

.PHONY: ci
ci: validate build package

.PHONY: apiserver-tests
apiserver-tests: bin/etcd3.test bin/metrics.test bin/preflight.test

bin/etcd3.test bin/metrics.test bin/preflight.test: $(PKGSRC)
	. ./scripts/test-helpers && build-apiserver-tests

