ARG PYTHON_VERSION=3

FROM python:${PYTHON_VERSION}-bullseye

ARG CLICKHOUSE_VERSION=latest

ENV TZ=Europe/Moscow

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    apt-get update -qq && \
    apt-get upgrade -y && \
    apt-get install -y \
        apt-transport-https \
        ca-certificates \
        tzdata \
        locales \
        python3-pip \
        openssh-server \
        supervisor \
        less && \
    pip3 install --upgrade pip && \
    pip3 install kazoo && \
    echo 'en_US.UTF-8 UTF-8' > /etc/locale.gen && \
    locale-gen

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# setup ssh for debugging
RUN echo "root:root" | chpasswd && \
    sed -i -e '/PermitRootLogin/ s/.*/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    mkdir /var/run/sshd

COPY staging/images/{{ instance_name }}/config/clickhouse-keyring.gpg /usr/share/keyrings/clickhouse-keyring.gpg
RUN mkdir -p /etc/apt/sources.list.d && \
    echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | tee /etc/apt/sources.list.d/clickhouse.list && \
    apt-get update -qq && \
    if [ "${CLICKHOUSE_VERSION}" = "latest" ]; then \
        DEBIAN_FRONTEND=noninteractive apt-get install -y \
            clickhouse-server \
            clickhouse-client \
            clickhouse-common-static; \
    else \
        DEBIAN_FRONTEND=noninteractive apt-get install -y \
            clickhouse-server=${CLICKHOUSE_VERSION} \
            clickhouse-client=${CLICKHOUSE_VERSION} \
            clickhouse-common-static=${CLICKHOUSE_VERSION}; \
    fi && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf && \
    apt-get clean

COPY dist/ch_backup*.whl /var/tmp/
RUN cd /var/tmp/ && \
    pip3 install ch_backup*.whl && \
    mkdir -p /etc/yandex/ch-backup && \
    ln -s /config/ch-backup.conf /etc/yandex/ch-backup/ch-backup.conf && \
    rm -rf /etc/supervisor && \
    ln --force -s /config/supervisor /etc/supervisor

COPY staging/images/{{ instance_name }}/ssl/ /var/tmp/ssl/
RUN openssl genrsa -out /var/tmp/ssl/server.key 2048 && \
    openssl req -new -key /var/tmp/ssl/server.key -out /var/tmp/ssl/server.csr \
        -subj "/CN=localhost" -days 365 -nodes -x509 && \
    openssl x509 -x509toreq -days 365 -in /var/tmp/ssl/server.csr \
        -signkey /var/tmp/ssl/server.key -out /var/tmp/ssl/server.req && \
    openssl x509 -req -in /var/tmp/ssl/server.req -CA /var/tmp/ssl/rootCACert.pem \
        -CAkey /var/tmp/ssl/rootCAKey.pem -CAcreateserial -out /var/tmp/ssl/server.crt \
        -days 365 -sha256 -extfile /var/tmp/ssl/server.ext && \
    mkdir -p /usr/share/ca-certificates/extra && \
    cp /var/tmp/ssl/rootCACert.pem /usr/share/ca-certificates/extra/ && \
    dpkg-reconfigure ca-certificates && \
    update-ca-certificates && \
    cp /var/tmp/ssl/server.key /etc/clickhouse-server/ && \
    cp /var/tmp/ssl/server.crt /etc/clickhouse-server/ && \
    cp /var/tmp/ssl/rootCACert.pem /etc/clickhouse-server/allCAs.pem && \
    mkdir /hdd1/ /hdd2/ && \
    mkdir -p /etc/clickhouse-server/conf.d/ && \
    ln -s /config/clickhouse-server.xml /etc/clickhouse-server/conf.d/ && \
    ln -s /config/storage_configuration.xml /etc/clickhouse-server/conf.d/ && \
    ln -s /config/named_collections/no_storage.xml /etc/clickhouse-server/conf.d/ && \
    mkdir -p /etc/clickhouse-server/users.d/ && \
    ln -s /config/users.xml /etc/clickhouse-server/users.d/ && \
    chown -R clickhouse:clickhouse /hdd1/ /hdd2/ /etc/clickhouse-server/ /usr/bin/clickhouse

COPY staging/images/{{ instance_name }}/entrypoint.py /entrypoint.py

EXPOSE 8123 8443 9000 9440

CMD ["python3", "/entrypoint.py"]
