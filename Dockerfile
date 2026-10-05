FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential librdkafka-dev curl \
  && rm -rf /var/lib/apt/lists/*
COPY WM_Central/WM_Central.py .
COPY WM_FO/WM_FO.py .
COPY WM_WS/WM_WS.py .
EXPOSE 80
CMD ["python", "main.py"]
