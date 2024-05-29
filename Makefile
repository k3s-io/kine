.DEFAULT_GOAL := ci

ARCH ?= amd64
REPO ?= rancher
DEFAULT_BUILD_ARGS=--build-arg="REPO=$(REPO)" --build-arg="TAG=$(TAG)" --build-arg="ARCH=$(ARCH)" --build-arg="DIRTY=$(DIRTY)"
DIRTY := $(shell git status --porcelain --untracked-files=no)
ifneq ($(DIRTY),)
	DIRTY="-dirty"
endif

.PHONY: validate
validate:
	DOCKER_BUILDKIT=1 docker build \
		$(DEFAULT_BUILD_ARGS) --build-arg="SKIP_VALIDATE=$(SKIP_VALIDATE)" \
		--target=validate -f Dockerfile .

.PHONY: build
build:
	DOCKER_BUILDKIT=1 docker build \
		$(DEFAULT_BUILD_ARGS) --build-arg="DRONE_TAG=$(DRONE_TAG)" --build-arg="CROSS=$(CROSS)" \
		-f Dockerfile --target=binary --output=. .

.PHONY: ci
ci: validate build
	ARCH=$(ARCH) ./scripts/package