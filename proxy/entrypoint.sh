#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-8080}"
UPSTREAM_BACKENDS="${UPSTREAM_BACKENDS:-http://api1:8000,http://api2:8000}"
HEALTH_CHECK_INTERVAL="${HEALTH_CHECK_INTERVAL:-5s}"
HEALTH_CHECK_FALL="${HEALTH_CHECK_FALL:-3}"
HEALTH_CHECK_RISE="${HEALTH_CHECK_RISE:-1}"
CONNECT_TIMEOUT="${CONNECT_TIMEOUT:-15s}"
CFG_PATH="/tmp/haproxy.cfg"

cat >"$CFG_PATH" <<EOF
global
    log stdout format raw local0
    maxconn 2048

defaults
    log global
    mode http
    option httplog
    option forwardfor
    timeout connect ${CONNECT_TIMEOUT}
    timeout client 60s
    timeout server 60s
    timeout tunnel 1h

resolvers dns
    parse-resolv-conf
    resolve_retries 3
    timeout resolve 2s
    timeout retry 1s
    hold valid 10s

frontend http_in
    bind *:${PORT}
    acl is_proxy_health path /proxy/health
    http-request return status 200 content-type application/json string "{\"status\":\"ok\",\"role\":\"proxy\",\"algorithm\":\"leastconn\"}" if is_proxy_health
    http-request set-header X-Forwarded-Proto https if { ssl_fc } !{ req.hdr(X-Forwarded-Proto) -m found }
    http-request set-header X-Forwarded-Proto http if !{ ssl_fc } !{ req.hdr(X-Forwarded-Proto) -m found }
    default_backend api_servers

backend api_servers
    balance leastconn
    http-send-name-header Host
    option httpchk
    http-check send meth GET uri /ready ver HTTP/1.1 hdr Host %[srv_name]
    http-check expect status 200
EOF

IFS=',' read -ra BACKENDS <<<"$UPSTREAM_BACKENDS"
COUNT=0
for raw in "${BACKENDS[@]}"; do
    entry="$(echo "$raw" | xargs)"
    [[ -z "$entry" ]] && continue
    entry="${entry%/}"

    scheme="http"
    rest="$entry"
    if [[ "$entry" == https://* ]]; then
        scheme="https"
        rest="${entry#https://}"
    elif [[ "$entry" == http://* ]]; then
        scheme="http"
        rest="${entry#http://}"
    fi
    rest="${rest%%/*}"

    if [[ "$rest" == *:* ]]; then
        host="${rest%:*}"
        port="${rest##*:}"
    else
        host="$rest"
        if [[ "$scheme" == "https" ]]; then
            port="443"
        else
            port="80"
        fi
    fi

    srv_name="$host"
    if [[ "$scheme" == "http" && "$port" != "80" ]] || [[ "$scheme" == "https" && "$port" != "443" ]]; then
        srv_name="${host}:${port}"
    fi

    is_ip=0
    if [[ "$host" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        is_ip=1
    fi

    line="    server ${srv_name} ${host}:${port} check inter ${HEALTH_CHECK_INTERVAL} fall ${HEALTH_CHECK_FALL} rise ${HEALTH_CHECK_RISE}"
    if [[ "$is_ip" -eq 0 ]]; then
        line+=" resolvers dns init-addr last,libc,none resolve-prefer ipv4"
    fi
    if [[ "$scheme" == "https" ]]; then
        line+=" ssl verify required ca-file /etc/ssl/certs/ca-certificates.crt check-ssl"
        if [[ "$is_ip" -eq 0 ]]; then
            line+=" sni str(${host}) check-sni ${host}"
        fi
    fi
    echo "$line" >>"$CFG_PATH"
    COUNT=$((COUNT + 1))
done

if [[ "$COUNT" -eq 0 ]]; then
    echo "ERROR: UPSTREAM_BACKENDS must contain at least one backend URL" >&2
    exit 1
fi

haproxy -c -f "$CFG_PATH"

if [[ "${DRY_RUN:-0}" == "1" ]]; then
    echo "HAProxy config validated (${COUNT} backends)."
    exit 0
fi

exec haproxy -W -db -f "$CFG_PATH"

