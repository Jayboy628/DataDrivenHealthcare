FROM postgres:latest

# Copy initialization script
COPY ./init-postgres-db.sh /docker-entrypoint-initdb.d/init-postgres-db.sh

# Expose the PostgreSQL port
EXPOSE 5432
