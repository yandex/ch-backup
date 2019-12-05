FROM ubuntu:18.04

RUN apt-get update -qq && apt-get -y install python3-bottle --no-install-recommends

COPY staging/images/proxy-api/app.py /app.py

EXPOSE 8080

ENTRYPOINT ["python3", "/app.py"]