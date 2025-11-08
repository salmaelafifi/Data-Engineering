# Base image: official PostgreSQL
FROM postgres:15


# Environment variables (optional: predefines user, password, database)
ENV POSTGRES_USER=myuser
ENV POSTGRES_PASSWORD=mypassword
ENV POSTGRES_DB=trades_db

# Expose default PostgreSQL port
EXPOSE 5432

FROM python:3.11-slim

# Install PostgreSQL development packages
RUN apt-get update && \
    apt-get install -y libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt


ENTRYPOINT ["python", "src/main.py"]