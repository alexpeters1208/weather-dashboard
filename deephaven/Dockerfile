FROM ghcr.io/deephaven/web-plugin-packager:latest as js-plugins

RUN ./pack-plugins.sh @deephaven/js-plugin-plotly-express @deephaven/js-plugin-plotly @deephaven/js-plugin-ui @deephaven/js-plugin-matplotlib

FROM deephaven/server-jetty:local-build

WORKDIR /

COPY requirements.txt /

RUN apt update && \
    apt install -y dnsutils

RUN \
    pip install -v -r requirements.txt && \
    rm requirements.txt

COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
COPY data /data