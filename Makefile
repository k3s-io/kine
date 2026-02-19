.DEFAULT_GOAL := ci

ARCH ?= amd64
REPO ?= rancher
CGO_ENABLED ?= 1
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
		--build-arg="CGO_ENABLED=$(CGO_ENABLED)" \
		$(DEFAULT_BUILD_ARGS) --build-arg="DRONE_TAG=$(DRONE_TAG)" \
		-f Dockerfile --target=binary --output=. .

PLATFORMS = linux/amd64,linux/arm64,linux/arm/v7,linux/riscv64
.PHONY: multi-arch-build
multi-arch-build:
	docker buildx build --build-arg="REPO=$(REPO)" \
		--build-arg="CGO_ENABLED=$(CGO_ENABLED)" \
		--build-arg="TAG=$(TAG)" \
		--build-arg="DIRTY=$(DIRTY)" \
		--platform=$(PLATFORMS) \
		--target=multi-arch-binary --output=type=local,dest=bin .
	mv bin/linux*/kine* bin/
	rmdir bin/linux*
	mkdir -p dist/artifacts
	cp bin/kine* dist/artifacts/

.PHONY: package
package:
	ARCH=$(ARCH) CGO_ENABLED=$(CGO_ENABLED) ./scripts/package

.PHONY: ci
ci: validate build package

.PHONY: test
TEST_TAG := $(TAG)
ifeq ($(CGO_ENABLED),0)
	TEST_TAG := $(TAG)-nocgo
endif
TESTS ?= sqlite \
		 litestream \
		 mysql \
		 postgres \
		 cockroachdb \
		 schema-migration \
		 nats \
		 nats-embedded \
		 nats-socket
test:
	DOCKER_BUILDKIT=1 docker build \
		--build-arg="CGO_ENABLED=$(CGO_ENABLED)" \
		--tag "$(REPO)/kine-test:$(TEST_TAG)" \
		--build-arg="ARCH=$(ARCH)" \
		-f Dockerfile.test .
	for test in $(TESTS); do \
		echo "Running test: $$test"; \
		docker run \
			--name run-test-$$test \
			--rm -v /var/run/docker.sock:/var/run/docker.sock \
			-e TAG=$(TEST_TAG) \
			$(REPO)/kine-test:$(TEST_TAG) \
			scripts/test $$test; \
	done
