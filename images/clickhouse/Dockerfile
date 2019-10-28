FROM ubuntu:bionic

ENV TZ=Europe/Moscow
ENV CH_TMP_DIR /var/tmp/ch-backup

ARG CLICKHOUSE_REPOSITORY="deb https://repo.yandex.ru/clickhouse/deb/stable/ main/"
ARG CLICKHOUSE_REPOSITORY_KEY=C8F1E19FE0C56BD4
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
COPY setup.py requirements.txt staging/images/clickhouse/clickhouse.gpg ${CH_TMP_DIR}/

RUN cd ${CH_TMP_DIR} && \
    pip3 install -r requirements.txt && \
    mkdir -p /etc/apt/sources.list.d && \
    echo $CLICKHOUSE_REPOSITORY | tee /etc/apt/sources.list.d/clickhouse.list && \
    apt-key add clickhouse.gpg

RUN apt-get update -qq && \
    if [ "$CLICKHOUSE_VERSION" = "19.1.16" ]; then \
        apt-get install -y \
            clickhouse-common-static=$CLICKHOUSE_VERSION \
            clickhouse-server-base=$CLICKHOUSE_VERSION \
            clickhouse-server=$CLICKHOUSE_VERSION \
            clickhouse-client=$CLICKHOUSE_VERSION; \
    else \
        DEBIAN_FRONTEND=noninteractive apt-get install -y \
            clickhouse-common-static=$CLICKHOUSE_VERSION \
            clickhouse-server=$CLICKHOUSE_VERSION \
            clickhouse-client=$CLICKHOUSE_VERSION; \
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


RUN openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 \
        -keyout /etc/clickhouse-server/server.key \
        -out /etc/clickhouse-server/server.crt && \
    mkdir -p /etc/clickhouse-server/conf.d && \
    ln -s /config/clickhouse-server.xml /etc/clickhouse-server/conf.d/ && \
    chown -R clickhouse:clickhouse /etc/clickhouse-server/ /usr/bin/clickhouse

EXPOSE 8123 8443 9000 9440

CMD ["supervisord", "-c", "/etc/supervisor/supervisord.conf"]
