FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY portal_rpc_socket_exporter.py .

ENV PYTHONUNBUFFERED=1
EXPOSE 9840

ENTRYPOINT ["python3", "portal_rpc_socket_exporter.py"]
CMD ["--config", "/etc/portal-rpc-socket-exporter/clusters.json"]
