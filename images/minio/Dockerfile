FROM minio/mc:RELEASE.2022-01-07T06-01-38Z

FROM minio/minio:RELEASE.2022-01-08T03-11-54Z

COPY --from=0 /usr/bin/mc /usr/bin/mc

ENV MINIO_ACCESS_KEY {{ conf.s3.access_key_id }}
ENV MINIO_SECRET_KEY {{ conf.s3.access_secret_key }}

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

HEALTHCHECK --interval=30s --timeout=5s CMD /usr/bin/healthcheck.sh

EXPOSE 9000

CMD ["server", "/export"]
