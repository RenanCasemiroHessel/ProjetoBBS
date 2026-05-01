FROM python:3.13-alpine
WORKDIR /app
RUN pip install pyzmq msgpack
COPY referencia.py .
CMD ["python", "referencia.py"]