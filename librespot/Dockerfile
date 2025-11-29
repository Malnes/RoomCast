FROM flori4n/librespot:latest

RUN apk add --no-cache bash jq

WORKDIR /app
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

VOLUME ["/config", "/var/lib/snapserver"]

CMD ["/app/run.sh"]
