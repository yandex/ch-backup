FROM registry.yandex.net/ubuntu:bionic

RUN apt-get update -qq && apt-get -y install tinyproxy-bin

COPY staging/images/{{instance_name}}/tinyproxy.conf /tinyproxy.conf

EXPOSE 4080

ENTRYPOINT ["tinyproxy", "-d", "-c", "/tinyproxy.conf"]