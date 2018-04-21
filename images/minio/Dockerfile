FROM registry.yandex.net/minio:20171025

ENV MINIO_ACCESS_KEY {{conf.dynamic.s3.access_key_id}}
ENV MINIO_SECRET_KEY {{conf.dynamic.s3.access_secret_key}}

RUN mc config host add fake-s3 http://localhost:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

EXPOSE 9000
ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

HEALTHCHECK --interval=30s --timeout=5s \
    CMD /usr/bin/healthcheck.sh

CMD ["server", "/export"]
