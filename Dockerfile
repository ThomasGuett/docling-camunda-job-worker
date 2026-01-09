FROM python:3.13-slim

WORKDIR /app

COPY .env .
COPY gateway_pb2_grpc.py .
COPY gateway_pb2.py .
COPY gateway_pb2.pyi .
COPY worker.py .
COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["python", "worker.py"]