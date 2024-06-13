ARG PYTHON_VERSION=3

FROM python:${PYTHON_VERSION}-bullseye

ARG CLICKHOUSE_VERSION=latest

ENV TZ=Europe/Moscow
ENV CH_TMP_DIR=/var/tmp/ch-backup

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

RUN mkdir -p ${CH_TMP_DIR}
COPY setup.py requirements.txt ${CH_TMP_DIR}/

COPY staging/images/{{ instance_name }}/config/clickhouse-keyring.gpg /usr/share/keyrings/clickhouse-keyring.gpg
RUN cd ${CH_TMP_DIR} && \
    pip3 install -r requirements.txt && \
    mkdir -p /etc/apt/sources.list.d && \
    apt-get update -qq && \
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

COPY ch_backup/ ${CH_TMP_DIR}/ch_backup/
RUN cd ${CH_TMP_DIR} && \
    pip3 install -e . && \
    mkdir -p /etc/yandex/ch-backup && \
    ln -s /config/ch-backup.conf /etc/yandex/ch-backup/ch-backup.conf && \
    rm -rf /etc/supervisor && \
    ln --force -s /config/supervisor /etc/supervisor

COPY staging/images/{{ instance_name }}/ssl/ ${CH_TMP_DIR}/ssl/
RUN openssl genrsa -out ${CH_TMP_DIR}/ssl/server.key 2048 && \
    openssl req -new -key ${CH_TMP_DIR}/ssl/server.key -out ${CH_TMP_DIR}/ssl/server.csr \
        -subj "/CN=localhost" -days 365 -nodes -x509 && \
    openssl x509 -x509toreq -days 365 -in ${CH_TMP_DIR}/ssl/server.csr \
        -signkey ${CH_TMP_DIR}/ssl/server.key -out ${CH_TMP_DIR}/ssl/server.req && \
    openssl x509 -req -in ${CH_TMP_DIR}/ssl/server.req -CA ${CH_TMP_DIR}/ssl/rootCACert.pem \
        -CAkey ${CH_TMP_DIR}/ssl/rootCAKey.pem -CAcreateserial -out ${CH_TMP_DIR}/ssl/server.crt \
        -days 365 -sha256 -extfile ${CH_TMP_DIR}/ssl/server.ext && \
    mkdir -p /usr/share/ca-certificates/extra && \
    cp ${CH_TMP_DIR}/ssl/rootCACert.pem /usr/share/ca-certificates/extra/ && \
    dpkg-reconfigure ca-certificates && \
    update-ca-certificates && \
    cp ${CH_TMP_DIR}/ssl/server.key /etc/clickhouse-server/ && \
    cp ${CH_TMP_DIR}/ssl/server.crt /etc/clickhouse-server/ && \
    cp ${CH_TMP_DIR}/ssl/rootCACert.pem /etc/clickhouse-server/allCAs.pem && \
    mkdir /hdd1/ /hdd2/ && \
    mkdir -p /etc/clickhouse-server/conf.d/ && \
    ln -s /config/clickhouse-server.xml /etc/clickhouse-server/conf.d/ && \
    ln -s /config/storage_configuration.xml /etc/clickhouse-server/conf.d/ && \
    mkdir -p /etc/clickhouse-server/users.d/ && \
    ln -s /config/users.xml /etc/clickhouse-server/users.d/ && \
    chown -R clickhouse:clickhouse /hdd1/ /hdd2/ /etc/clickhouse-server/ /usr/bin/clickhouse

COPY staging/images/{{ instance_name }}/entrypoint.py /entrypoint.py

EXPOSE 8123 8443 9000 9440

CMD ["python3", "/entrypoint.py"]
