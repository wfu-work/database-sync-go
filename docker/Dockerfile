FROM debian:bookworm

# set environment
ENV PREFER_HOST_MODE="ip"\
    BASE_DIR="/app/database" \
    TIME_ZONE="Asia/Shanghai"

WORKDIR $BASE_DIR

COPY databaseSyncApp .

RUN chmod +x databaseSyncApp

EXPOSE 3010

ENTRYPOINT ["/app/database/databaseSyncApp"]