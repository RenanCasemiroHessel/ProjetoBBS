FROM python:3.13-alpine
WORKDIR /app
RUN pip install pyzmq msgpack
COPY server.py .
CMD ["python", "server.py"]