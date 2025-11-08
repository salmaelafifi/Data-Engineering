# Base image: official PostgreSQL
FROM postgres:15

# Environment variables (optional: predefines user, password, database)
ENV POSTGRES_USER=myuser
ENV POSTGRES_PASSWORD=mypassword
ENV POSTGRES_DB=trades_db

# Expose default PostgreSQL port
EXPOSE 5432
