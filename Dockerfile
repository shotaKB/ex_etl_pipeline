FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline.yaml .
COPY scripts ./scripts
ENTRYPOINT [ "python","-m", "scripts.main" ]