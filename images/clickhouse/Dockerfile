FROM registry.yandex.net/ubuntu:bionic

ENV TZ=Europe/Moscow
ENV CH_TMP_DIR /var/tmp/ch-backup

ARG CLICKHOUSE_VERSION

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    apt-get update -qq && \
    apt-get install -y \
        apt-transport-https \
        tzdata \
        locales \
        python3-pip \
        openssh-server \
        supervisor && \
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
COPY setup.py requirements.txt images/clickhouse/clickhouse.gpg ${CH_TMP_DIR}/

RUN cd ${CH_TMP_DIR} && \
    pip3 install -r requirements.txt && \
    mkdir -p /etc/apt/sources.list.d && \
    echo "deb https://packages.clickhouse.com/deb prestable main"   | tee /etc/apt/sources.list.d/clickhouse-prestable.list && \
    echo "deb https://packages.clickhouse.com/deb stable main"   | tee /etc/apt/sources.list.d/clickhouse-stable.list && \
    echo "deb https://packages.clickhouse.com/deb lts main"   | tee /etc/apt/sources.list.d/clickhouse-lts.list && \
    echo "deb http://dist.yandex.ru/mdb-bionic-secure testing/all/"     | tee /etc/apt/sources.list.d/mdb-bionic-secure-testing.list && \
    echo "deb http://dist.yandex.ru/mdb-bionic-secure testing/\$(ARCH)/" | tee -a /etc/apt/sources.list.d/mdb-bionic-secure-testing.list && \
    echo "deb http://dist.yandex.ru/mdb-bionic-secure stable/all/"      | tee /etc/apt/sources.list.d/mdb-bionic-secure-stable.list && \
    echo "deb http://dist.yandex.ru/mdb-bionic-secure stable/\$(ARCH)/"  | tee -a /etc/apt/sources.list.d/mdb-bionic-secure-stable.list && \
    apt-key add clickhouse.gpg

RUN apt-get update -qq && \
    apt-get upgrade -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        clickhouse-common-static=$CLICKHOUSE_VERSION \
        clickhouse-server=$CLICKHOUSE_VERSION \
        clickhouse-client=$CLICKHOUSE_VERSION && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf && \
    apt-get clean

COPY ch_backup/ ${CH_TMP_DIR}/ch_backup/
RUN cd ${CH_TMP_DIR} && \
    pip3 install -e . && \
    mkdir -p /etc/yandex/ch-backup && \
    ln -s /config/ch-backup.conf /etc/yandex/ch-backup/ch-backup.conf && \
    rm -rf /etc/supervisor && \
    ln --force -s /config/supervisor /etc/supervisor

COPY staging/images/{{instance_name}}/ssl/ ${CH_TMP_DIR}/ssl/
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
    mkdir -p /etc/clickhouse-server/conf.d/ && \
    ln -s /config/clickhouse-server.xml /etc/clickhouse-server/conf.d/ && \
    mkdir -p /etc/clickhouse-server/users.d/ && \
    ln -s /config/users.xml /etc/clickhouse-server/users.d/ && \
    chown -R clickhouse:clickhouse /etc/clickhouse-server/ /usr/bin/clickhouse

COPY staging/images/{{instance_name}}/version_cmp.py /version_cmp.py

# Add storage policies to CH configuration (multiple disks, S3) for versions >= 20.8.
RUN if [ "$(python3 /version_cmp.py $CLICKHOUSE_VERSION 20.8)" = "1" ] ; then \
    ln -s /config/storage_configuration.xml /etc/clickhouse-server/conf.d/ ; \
    mkdir /hdd1/ ; \
    mkdir /hdd2/ ; \
    chown -R clickhouse:clickhouse /hdd1/ /hdd2/ ; \
    fi

COPY staging/images/{{instance_name}}/entrypoint.py /entrypoint.py

EXPOSE 8123 8443 9000 9440

CMD ["python3", "/entrypoint.py"]
