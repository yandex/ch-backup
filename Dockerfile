# Largely a copy-paste from
# https://hub.docker.com/r/yandex/clickhouse-server/~/dockerfile/
FROM dbaas-ch-backup-base

ENV CLICKHOUSE_USER clickhouse
ENV CLICKHOUSE_GROUP clickhouse
ENV CH_BACKUP_CONFIG /etc/yandex/ch-backup/ch-backup.conf
ENV CH_TMP_DIR /var/tmp/ch-backup

ARG repository="deb https://repo.yandex.ru/clickhouse/deb/stable/ main/"
ARG version=1.1.54343

RUN apt-get update -qq && \
    apt-get install -y apt-transport-https tzdata && \
    mkdir -p /etc/apt/sources.list.d && \
    echo $repository | tee /etc/apt/sources.list.d/clickhouse.list && \
    apt-get update -qq && \
    apt-get install --allow-unauthenticated -y clickhouse-server-common=$version clickhouse-server-base=$version clickhouse-client=$version && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf && \
    apt-get clean

RUN chown -R clickhouse /etc/clickhouse-server/ && \
    mkdir -p /etc/clickhouse-server/conf.d && \
    ln -s /config/clickhouse-server.xml /etc/clickhouse-server/conf.d/

RUN mkdir -p ${CH_TMP_DIR}
COPY ch_backup ${CH_TMP_DIR}/ch_backup
COPY setup.py ${CH_TMP_DIR}/
RUN cd ${CH_TMP_DIR} && pip3 install -e . && \
    mkdir -p /etc/yandex/ch-backup && \
    ln -s /config/ch-backup.conf /etc/yandex/ch-backup/ch-backup.conf

USER clickhouse
VOLUME /var/lib/clickhouse

ENTRYPOINT exec /usr/bin/clickhouse-server --config=/etc/clickhouse-server/config.xml
