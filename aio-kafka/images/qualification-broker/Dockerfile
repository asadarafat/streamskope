ARG KAFKA_IMAGE
FROM ${KAFKA_IMAGE}
COPY --chmod=755 entrypoint.sh /opt/streamskope-fixture/entrypoint.sh
ENTRYPOINT ["/opt/streamskope-fixture/entrypoint.sh"]
