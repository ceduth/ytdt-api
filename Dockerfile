FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install-deps
RUN playwright install
RUN mkdir -p /app/data

COPY ./api /app/api
COPY ./lib /app/lib
COPY ./models /app/models
COPY ./helpers.py /app/helpers.py

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]