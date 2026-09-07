FROM minio/mc:RELEASE.2022-01-07T06-01-38Z

FROM minio/minio:RELEASE.2022-01-08T03-11-54Z

COPY --from=0 /usr/bin/mc /usr/bin/mc

ENV MINIO_ACCESS_KEY {{ conf.s3.access_key_id }}
ENV MINIO_SECRET_KEY {{ conf.s3.access_secret_key }}

# ClickHouse checks that these buckets exist during startup.
RUN mkdir -p \
    /export/{{ conf.s3.bucket }} \
    /export/{{ conf.s3.cloud_storage_bucket }}-01 \
    /export/{{ conf.s3.cloud_storage_bucket }}-02

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

HEALTHCHECK --interval=500ms --timeout=1s --retries=60 \
    CMD curl --fail --silent http://localhost:9000/minio/health/ready

EXPOSE 9000 9001

CMD ["server", "/export", "--console-address", ":9001"]
