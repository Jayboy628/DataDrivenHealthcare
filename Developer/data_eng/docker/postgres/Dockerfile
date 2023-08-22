FROM postgres:15.3-alpine

COPY init-hive-db.sh /docker-entrypoint-initdb.d/init-hive-db.sh

RUN chmod +x /docker-entrypoint-initdb.d/init-hive-db.sh

EXPOSE 5432