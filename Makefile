.DEFAULT_GOAL := ci

ARCH ?= amd64
REPO ?= rancher
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
		$(DEFAULT_BUILD_ARGS) --build-arg="DRONE_TAG=$(DRONE_TAG)" \
		-f Dockerfile --target=binary --output=. .

.PHONY: multi-arch-build
PLATFORMS = linux/amd64,linux/arm64,linux/arm/v7,linux/riscv64
multi-arch-build:
	docker buildx build --platform=$(PLATFORMS) --target=multi-arch-binary --output=type=local,dest=bin .
	mv bin/linux*/kine* bin/
	rmdir bin/linux*
	mkdir -p dist/artifacts
	cp bin/kine* dist/artifacts/

.PHONY: package
package:
	ARCH=$(ARCH) ./scripts/package

.PHONY: ci
ci: validate build package