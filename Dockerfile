FROM python:3.12-slim
LABEL maintainer="JuanmaDevG"

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    WM_VOLUME=/cache \
    WM_PORT=5000 \
    WM_HTTP_PORT=8080

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends tini \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /cache

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY WM.py wm_common.py ./
COPY WM_Central/WM_Central.py WM_FO/WM_FO.py WM_WS/WM_WS.py ./

# CENTRAL: sockets for the WS monitors + web panel
EXPOSE 5000 8080

# Pick the module with the command:
#   docker run wm WM_Central 5000 <kafka_host>:9092
#   docker run -it wm WM_FO <kafka_host>:9092 FO-01 [file]
#   docker run -it wm WM_WS_M 6001 <central_host>:5000 1 "River Park"
#   docker run -it wm WM_WS_E <kafka_host>:9092 <monitor_host>:6001
ENTRYPOINT ["/usr/bin/tini", "--", "python3", "WM.py"]
CMD ["WM_Central"]
