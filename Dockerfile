FROM python:3.10-slim-bullseye

ENV APP_DIR=/app
WORKDIR $APP_DIR
ADD . .

RUN apt-get update \
    && apt-get -y install build-essential \
    && pip install --upgrade pip -r requirements.txt \
    && apt-get remove -y build-essential \
    && apt-get autoremove -y \
    && apt-get clean
