ARG CLICKHOUSE_VERSION=latest

FROM alpine:3.22 AS certificates

RUN apk add --no-cache openssl

COPY staging/images/{{ instance_name }}/ssl/ /certificates/

RUN openssl genrsa -out /certificates/server.key 2048 && \
    openssl req -new -key /certificates/server.key \
        -out /certificates/server.csr \
        -subj "/CN=localhost" && \
    openssl x509 -req -in /certificates/server.csr \
        -CA /certificates/rootCACert.pem \
        -CAkey /certificates/rootCAKey.pem \
        -CAcreateserial -out /certificates/server.crt \
        -days 365 -sha256 -extfile /certificates/server.ext

FROM clickhouse/clickhouse-keeper:${CLICKHOUSE_VERSION}

COPY --from=certificates --chown=clickhouse:clickhouse \
    /certificates/rootCACert.pem \
    /certificates/server.crt \
    /certificates/server.key \
    /etc/clickhouse-keeper/

COPY staging/images/{{ instance_name }}/keeper_config.xml /etc/clickhouse-keeper/keeper_config.xml
