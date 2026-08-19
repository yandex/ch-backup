ARG CLICKHOUSE_VERSION=latest

FROM clickhouse/clickhouse-keeper:${CLICKHOUSE_VERSION}

RUN apk add --no-cache openssl

COPY staging/images/{{ instance_name }}/ssl/ /etc/clickhouse-keeper/

RUN openssl genrsa -out /etc/clickhouse-keeper/server.key 2048 && \
    openssl req -new -key /etc/clickhouse-keeper/server.key \
        -out /etc/clickhouse-keeper/server.csr \
        -subj "/CN=localhost" && \
    openssl x509 -req -in /etc/clickhouse-keeper/server.csr \
        -CA /etc/clickhouse-keeper/rootCACert.pem \
        -CAkey /etc/clickhouse-keeper/rootCAKey.pem \
        -CAcreateserial -out /etc/clickhouse-keeper/server.crt \
        -days 365 -sha256 -extfile /etc/clickhouse-keeper/server.ext && \
    rm /etc/clickhouse-keeper/server.csr \
        /etc/clickhouse-keeper/rootCAKey.pem \
        /etc/clickhouse-keeper/rootCACert.srl && \
    chown clickhouse:clickhouse \
        /etc/clickhouse-keeper/server.key \
        /etc/clickhouse-keeper/server.crt \
        /etc/clickhouse-keeper/rootCACert.pem

COPY staging/images/{{ instance_name }}/keeper_config.xml /etc/clickhouse-keeper/keeper_config.xml
