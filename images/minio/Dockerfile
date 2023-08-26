FROM minio/mc:RELEASE.2023-08-18T21-57-55Z

FROM minio/minio:RELEASE.2023-08-23T10-07-06Z

COPY --from=0 /usr/bin/mc /usr/bin/mc

ENV MINIO_ACCESS_KEY {{ conf.s3.access_key_id }}
ENV MINIO_SECRET_KEY {{ conf.s3.access_secret_key }}

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

HEALTHCHECK --interval=30s --timeout=5s CMD /usr/bin/healthcheck.sh

EXPOSE 9000

CMD ["server", "/export"]
