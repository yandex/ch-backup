# Largely a copy-paste from
# https://hub.docker.com/r/yandex/clickhouse-server/~/dockerfile/
FROM dbaas-ch-backup-base

ENV CLICKHOUSE_USER clickhouse
ENV CLICKHOUSE_GROUP clickhouse
ENV CH_BACKUP_CONFIG /etc/yandex/ch-backup/ch-backup.conf
ENV CH_TMP_DIR /var/tmp/ch-backup


ARG repository="deb https://repo.yandex.ru/clickhouse/xenial/ dists/stable/main/binary-amd64/"
# ARG version=\*
ARG version=1.1.54327

ENV CLICKHOUSE_CONFIG /etc/clickhouse-server/config.xml
ENV CLICKHOUSE_USERS /etc/clickhouse-server/users.xml

RUN apt-get update -qq && \
    apt-get install -y apt-transport-https tzdata && \
    mkdir -p /etc/apt/sources.list.d && \
    echo $repository | tee /etc/apt/sources.list.d/clickhouse.list && \
    apt-get update -qq && \
    apt-get install --allow-unauthenticated -y clickhouse-server-common=$version clickhouse-server-base=$version clickhouse-client=$version && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf && \
    apt-get clean

RUN sed -i 's,<listen_host>127.0.0.1</listen_host>,<listen_host>0.0.0.0</listen_host>,' /etc/clickhouse-server/config.xml && \
    sed -i 's,<listen_host>::1</listen_host>,<listen_host>::</listen_host>,' /etc/clickhouse-server/config.xml
RUN chown -R clickhouse /etc/clickhouse-server/


RUN mkdir -p ${CH_TMP_DIR}
COPY ch_backup ${CH_TMP_DIR}/ch_backup
COPY setup.py ${CH_TMP_DIR}/

RUN cd ${CH_TMP_DIR} && pip3 install -e .


RUN ln --force -s /config/users.xml $CLICKHOUSE_USERS && \
    mkdir -p /etc/yandex/ch-backup && \
    ln --force -s /config/ch-backup.conf /etc/yandex/ch-backup/ch-backup.conf


USER clickhouse
VOLUME /var/lib/clickhouse

ENTRYPOINT exec /usr/bin/clickhouse-server --config=${CLICKHOUSE_CONFIG}
