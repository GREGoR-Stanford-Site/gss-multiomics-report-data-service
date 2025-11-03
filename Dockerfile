FROM tiangolo/uvicorn-gunicorn:python3.11-slim

RUN apt-get update && apt-get install -y apt-utils netcat-openbsd vim less procps

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
