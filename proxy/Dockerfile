FROM python:3.13-alpine
WORKDIR /app
RUN pip install pyzmq
COPY pubsub_proxy.py .
CMD ["python", "pubsub_proxy.py"]