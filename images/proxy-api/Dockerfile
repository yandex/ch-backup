FROM ubuntu:bionic

RUN apt-get update -qq && apt-get -y install python3-bottle --no-install-recommends

COPY staging/images/{{instance_name}}/app.py /app.py

EXPOSE 8080

ENTRYPOINT ["python3", "/app.py"]