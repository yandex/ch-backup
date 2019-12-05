FROM ubuntu:18.04

RUN apt-get update -qq && apt-get -y install tinyproxy-bin

COPY staging/images/proxy/tinyproxy.conf /tinyproxy.conf

EXPOSE 4080

ENTRYPOINT ["tinyproxy", "-d", "-c", "/tinyproxy.conf"]