FROM python:3.9-slim

WORKDIR /app

COPY . .

ENV PYTHONUNBUFFERED=1
CMD ["python", "node/server.py"]
