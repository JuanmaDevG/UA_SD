FROM python:3.12-slim
LABEL maintainer="JuanmaDevG"

WORKDIR /app
ENV WM_VOLUME="/cache"
VOLUME ["/cache"]
ENV WM_PORT=5000
EXPOSE 5000

RUN apt-get update && apt-get install -y --no-install-recommends \
  curl rename
RUN curl "https://dlcdn.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz" > kafka.tgz
RUN tar -xf kafka.tgz
RUN rename 's/kafka_.*/kafka/' kafka_*

COPY requirements.txt .
COPY --chmod=700 WM.py .
COPY --chmod=600 WM_Central/WM_Central.py .
COPY --chmod=600 WM_FO/WM_FO.py .
COPY --chmod=600 WM_WS/WM_WS.py .
COPY --from=krallin/ubuntu-tini:latest /usr/bin/tini /usr/bin/tini
RUN pip install --no-cache-dir -r requirements.txt

# TODO: the container closes when the browser window closes
ENTRYPOINT ["/usr/bin/tini", "--", "WM.py"]
CMD ["WM_FO"]
