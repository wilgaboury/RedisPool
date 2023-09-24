FROM debian:bookworm-slim

RUN apt update
RUN apt install -y --no-install-recommends ca-certificates lsb-release curl gpg
RUN curl -fsSL https://packages.redis.io/gpg | gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
RUN apt update
RUN apt install -y --no-install-recommends redis
RUN apt purge

COPY redis-cluster.conf.template .
COPY run-cluster.sh .

CMD ["bash"]