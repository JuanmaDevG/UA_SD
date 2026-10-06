FROM python:3.12-slim
LABEL maintainer="JuanmaDevG and (tu nombre o nickname)"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Add if build fails
#RUN apt-get update && apt-get install -y --no-install-recommends \
#  build-essential librdkafka-dev curl \
#  && rm -rf /var/lib/apt/lists/*

COPY --chmod=700 WM.py .
COPY --chmod=600 WM_Central/WM_Central.py .
COPY --chmod=600 WM_FO/WM_FO.py .
COPY --chmod=600 WM_WS/WM_WS.py .
COPY --from=krallin/ubuntu-tini:latest /usr/bin/tini /usr/bin/tini

ENV WM_VOLUME="/cache"
VOLUME ["/cache"]
ENV WM_PORT=8080
EXPOSE 8080

ENTRYPOINT [ "tini", "--", "WM.py"]
CMD ["WM_FO"]
