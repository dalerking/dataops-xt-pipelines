FROM 064859874041.dkr.ecr.us-east-1.amazonaws.com/data-ops/base-data-python:3.10-slim-bullseye-1.0.10

ARG FUNCTION_CODE_DIR

ENV TDSVER=8.0
ENV DB_DRIVER=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so

RUN apt-get update && \
    apt-get install freetds-dev tdsodbc unixodbc-dev g++ libcurl3-gnutls=7.74.0-1.3+deb11u2 -y

WORKDIR /src/

COPY app ./app
#COPY tests ./tests
COPY Pipfile* ./

RUN pipenv sync --system --clear

ENTRYPOINT ["/entry.sh", "app.main.lambda_handler"]
