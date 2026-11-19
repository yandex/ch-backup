FROM ubuntu:bionic

COPY --from=python:3-slim /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

RUN sed -i 's|http://\(archive\|security\)\.ubuntu\.com/ubuntu|https://mirror.yandex.ru/ubuntu|g' /etc/apt/sources.list && \
    apt-get update -qq && apt-get -y install tinyproxy-bin

COPY staging/images/{{ instance_name }}/tinyproxy.conf /tinyproxy.conf

EXPOSE 4080

ENTRYPOINT ["tinyproxy", "-d", "-c", "/tinyproxy.conf"]