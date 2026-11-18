FROM haproxy:2.8-alpine

USER root
RUN apk add --no-cache ca-certificates bash

COPY entrypoint.sh /usr/local/bin/polypulse-proxy-entrypoint.sh
RUN chmod +x /usr/local/bin/polypulse-proxy-entrypoint.sh

# Build-time config syntax validation against synthetic mixed https/http backends
RUN PORT=8080 UPSTREAM_BACKENDS="https://api1.example.com,http://127.0.0.1:8000" \
    DRY_RUN=1 /usr/local/bin/polypulse-proxy-entrypoint.sh

USER haproxy
ENTRYPOINT ["/usr/local/bin/polypulse-proxy-entrypoint.sh"]

