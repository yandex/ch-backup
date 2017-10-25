# vim:set ft=dockerfile:
FROM registry.yandex.net/ubuntu:xenial

ENV DEBIAN_FRONTEND noninteractive
ENV LANG en_US.utf8

# Configure apt and locale
COPY ./config/apt/ /var/lib/dist/config/apt
RUN cp --force --recursive --verbose \
    /var/lib/dist/config/apt/* /etc/apt/ && \
    echo 'en_US.UTF-8 UTF-8' > /etc/locale.gen && \
    locale-gen && \
    apt-get -qq update && \
    \
    apt-get -y install \
    supervisor=3.3.1-1yandex \
    openssl git openssh-client \
    python3-pip \
    python3-setuptools \
    python3-dev \
    build-essential \
    netcat-openbsd \
    iputils-ping \
    net-tools \
    dnsutils \
    nginx=1.8.1-1.yandex.16 \
    nginx-full=1.8.1-1.yandex.16 \
    nginx-common=1.8.1-1.yandex.16

RUN mkdir -p /opt/yandex /code /config

RUN pip3 install --upgrade pip
