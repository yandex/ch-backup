FROM zookeeper:3.6.2

COPY staging/images/{{instance_name}}/ssl/ /conf/ssl/

RUN openssl genrsa -out /conf/ssl/server.key 2048 && \
    openssl req -new -key /conf/ssl/server.key -out /conf/ssl/server.csr \
        -subj "/CN=localhost" -days 365 -nodes -x509 && \
    openssl x509 -x509toreq -days 365 -in /conf/ssl/server.csr \
        -signkey /conf/ssl/server.key -out /conf/ssl/server.req && \
    openssl x509 -req -in /conf/ssl/server.req -CA /conf/ssl/rootCACert.pem \
        -CAkey /conf/ssl/rootCAKey.pem -CAcreateserial -out /conf/ssl/server.crt \
        -days 365 -sha256 -extfile /conf/ssl/server.ext && \
    keytool -import -trustcacerts -alias yandex -file /conf/ssl/rootCACert.pem \
        -keystore /conf/ssl/truststore.jks -storepass password123 -noprompt && \
    openssl pkcs12 -export -in /conf/ssl/server.crt -inkey /conf/ssl/server.key \
        -out /conf/ssl/server.p12 -name zookeeper.net -passout pass:password123 && \
    keytool -importkeystore -destkeystore /conf/ssl/server.jks -srckeystore /conf/ssl/server.p12 \
        -deststorepass password123 -srcstoretype PKCS12 -srcstorepass password123 -alias zookeeper.net && \
    mkdir -p /usr/share/ca-certificates/extra && \
    cp /conf/ssl/rootCACert.pem /usr/share/ca-certificates/extra/ && \
    dpkg-reconfigure ca-certificates && \
    update-ca-certificates

COPY staging/images/{{instance_name}}/zoo.cfg /conf/zoo.cfg
