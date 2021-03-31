FROM alpine:3.12
ARG ARCH=amd64
COPY dist/artifacts/kine-${ARCH} /bin/kine
RUN mkdir /db && chown nobody /db
VOLUME /db
EXPOSE 2379/tcp
USER nobody
ENTRYPOINT ["/bin/kine"]
