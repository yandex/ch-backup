FROM registry.yandex.net/ubuntu:xenial

ENV LANG en_US.utf8
ENV CLICKHOUSE_USER clickhouse
ENV CLICKHOUSE_GROUP clickhouse
ENV CH_TMP_DIR /var/tmp/ch-backup

ARG CLICKHOUSE_REPOSITORY="deb https://repo.yandex.ru/clickhouse/deb/stable/ main/"
ARG CLICKHOUSE_REPOSITORY_KEY=C8F1E19FE0C56BD4
ARG CLICKHOUSE_VERSION

RUN echo 'en_US.UTF-8 UTF-8' > /etc/locale.gen && \
    locale-gen && \
    apt-get update -qq && \
    apt-get install -y \
        apt-transport-https tzdata \
        python3-pip \
        openssh-server \
        supervisor && \
    pip3 install --upgrade pip

# setup ssh for debugging
RUN echo "root:root" | chpasswd && \
    sed -i -e '/PermitRootLogin/ s/ .*/ yes/' /etc/ssh/sshd_config && \
    mkdir /var/run/sshd

RUN mkdir -p ${CH_TMP_DIR}
COPY ch_backup ${CH_TMP_DIR}/ch_backup
COPY setup.py ${CH_TMP_DIR}/

RUN cd ${CH_TMP_DIR} && pip3 install -e . && \
    mkdir -p /etc/yandex/ch-backup && \
    ln -s /config/ch-backup.conf /etc/yandex/ch-backup/ch-backup.conf && \
    rm -rf /etc/supervisor && \
    ln --force -s /config/supervisor /etc/supervisor

RUN mkdir -p /etc/apt/sources.list.d && \
    echo $CLICKHOUSE_REPOSITORY | tee /etc/apt/sources.list.d/clickhouse.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys $CLICKHOUSE_REPOSITORY_KEY && \
    apt-get update -qq && \
    apt-get install -y \
        clickhouse-server-common=$CLICKHOUSE_VERSION \
        clickhouse-server-base=$CLICKHOUSE_VERSION \
        clickhouse-client=$CLICKHOUSE_VERSION && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf && \
    apt-get clean

RUN chown -R clickhouse /etc/clickhouse-server/ && \
    openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 \
        -keyout /etc/clickhouse-server/server.key \
        -out /etc/clickhouse-server/server.crt && \
    mkdir -p /etc/clickhouse-server/conf.d && \
    ln -s /config/clickhouse-server.xml /etc/clickhouse-server/conf.d/

EXPOSE 8123 8443 9000 9440

CMD ["supervisord", "-c", "/etc/supervisor/supervisord.conf"]
