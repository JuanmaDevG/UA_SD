FROM python:3.12-slim
LABEL maintainer="JuanmaDevG"

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    DOCKER_VOLUME=/cache

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends tini \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /cache

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY WM_Central/ WM_Central/
COPY WM_FO/ WM_FO/
COPY WM_WS/ WM_WS/

# CENTRAL: login socket for the watering stations
EXPOSE 5000
VOLUME /cache

# Pick the module with the command:
#   docker run wm WM_Central/WM_Central.py 5000 <kafka_host>:9092
#   docker run -it wm WM_FO/WM_FO.py <kafka_host>:9092
#   docker run -it wm WM_WS/WM_WS_M.py [args...]
#   docker run -it wm WM_WS/WM_WS_E.py [args...]
ENTRYPOINT ["/usr/bin/tini", "--", "python3"]
CMD ["WM_Central/WM_Central.py", "here"]
