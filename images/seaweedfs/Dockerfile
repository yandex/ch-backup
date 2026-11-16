FROM chrislusf/seaweedfs:4.40@sha256:52194fba4fecd0083c842158b3a902ba6e04a63619b2b0efcd08007bdb6a4602

ENV AWS_ACCESS_KEY_ID {{ conf.s3.access_key_id }}
ENV AWS_SECRET_ACCESS_KEY {{ conf.s3.access_secret_key }}

ENV S3_BUCKET {{ conf.s3.buckets | join(',') }}

HEALTHCHECK --interval=500ms --timeout=1s --retries=240 \
    CMD IFS=,; for bucket in $S3_BUCKET; do wget -q --spider "http://127.0.0.1:8888/buckets/$bucket/" || exit 1; done

EXPOSE 9000

# Keep Admin gRPC (HTTP port + 10000) outside Linux's ephemeral port range.
CMD ["mini", "-s3.port=9000", "-admin.port=9346"]
