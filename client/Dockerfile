FROM python:3.12-slim

WORKDIR /app

COPY client/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY utils ./utils
COPY client/src ./src

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["python", "-u", "src/config.py"]
